#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>

#include <raikv/util.h>
#include <raikv/atom.h>

using namespace rai;
using namespace kv;

static const uint64_t newhash_magic = ( (uint64_t) 0x9e3779b9U << 32 ) |
                                        (uint64_t) 0x7f4a7c13U;
inline void
newhash_mix( uint64_t &a,  uint64_t &b,  uint64_t &c ) /* Bob Jenkins */
{
  a = a - b;  a = a - c;  a = a ^ ( c >> 43 );
  b = b - c;  b = b - a;  b = b ^ ( a << 9 );
  c = c - a;  c = c - b;  c = c ^ ( b >> 8 );
  a = a - b;  a = a - c;  a = a ^ ( c >> 38 );
  b = b - c;  b = b - a;  b = b ^ ( a << 23 );
  c = c - a;  c = c - b;  c = c ^ ( b >> 5 );
  a = a - b;  a = a - c;  a = a ^ ( c >> 35 );
  b = b - c;  b = b - a;  b = b ^ ( a << 49 );
  c = c - a;  c = c - b;  c = c ^ ( b >> 11 );
  a = a - b;  a = a - c;  a = a ^ ( c >> 12 );
  b = b - c;  b = b - a;  b = b ^ ( a << 18 );
  c = c - a;  c = c - b;  c = c ^ ( b >> 22 );
}

static void
xor_random_bytes( void *out,  uint16_t outsz,  void *in,  uint16_t insz )
{
  for ( uint16_t i = 0, j = 0; i < outsz || j < insz; i++, j++ ) {
    ((uint8_t *) out)[ i % outsz ] ^= ((uint8_t *) in)[ j % insz ];
  }
}

bool
rai::kv::rand::fill_urandom_bytes( void *buf,  uint16_t sz )
{
  /* static pattern, doesn't change from run to run */
  static uint64_t counter = 100001;
  static uint64_t x = newhash_magic, y = newhash_magic;
  uint64_t tmp[ 64 * 1024 / sizeof( uint64_t ) ];
  uint16_t i;
  for ( i = 0; i < sz; i += sizeof( uint64_t ) ) {
    tmp[ i / sizeof( uint64_t ) ] = counter++;
    newhash_mix( x, y, tmp[ i / sizeof( uint64_t ) ] );
  }
  ::memcpy( buf, tmp, sz );
  return true;

#if 0
  int fd = ::open( "/dev/urandom", O_RDONLY );
  uint16_t i = 0, j;
  if ( fd != -1 ) {
    do {
      ssize_t n = ::read( fd, &((uint8_t *) buf)[ i ], sz - i );
      if ( n < 0 )
        break;
      i += (uint16_t) n;
    } while ( i < sz );
    ::close( fd );

    if ( i == sz )
      return true;
  }
  ::memset( &((uint8_t *) buf)[ i ], 0, sz - i );
  do {
    uint64_t h[ 6 ];
    h[ 0 ] = current_monotonic_time_ns();
    h[ 1 ] = get_rdtsc();
    h[ 2 ] = newhash_magic;
    for ( j = 0; j < ( h[ 0 ] & 15 ); j++ )
      __sync_pause();
    h[ 3 ] = current_monotonic_time_ns();
    for ( j = 0; j < ( h[ 1 ] & 15 ); j++ )
      __sync_mfence();
    h[ 4 ] = newhash_magic;
    h[ 5 ] = get_rdtsc();

    newhash_mix( h[ 0 ], h[ 1 ], h[ 2 ] );
    newhash_mix( h[ 3 ], h[ 4 ], h[ 5 ] );
    xor_random_bytes( &((uint8_t *) buf)[ i ], sz - i, h, sizeof( h ) );
    i += sizeof( h );
  } while ( i < sz );
  return false;
#endif
}

bool
rand::xorshift1024star::init( void *seed,  uint16_t sz )
{
  this->p = 0;
  if ( sz == 0 )
    return fill_urandom_bytes( this->state, sizeof( this->state ) );
  ::memset( this->state, 0, sizeof( this->state ) );
  xor_random_bytes( this->state, sizeof( this->state ), seed, sz );
  return true;
}

uint64_t
rand::xorshift1024star::next( void )
{
  const uint64_t s0 = this->state[ this->p ];
  uint64_t       s1 = this->state[ this->p = (this->p + 1) & 15 ];
  s1 ^= s1 << 31; // a
  this->state[ this->p ] = s1 ^ s0 ^ (s1 >> 11) ^ (s0 >> 30); // b,c
  return this->state[ this->p ] * 0x9e3779b97f4a7c13;
}

bool
rand::xoroshiro128plus::init( void *seed,  uint16_t sz )
{
  if ( sz == 0 )
    return fill_urandom_bytes( this->state, sizeof( this->state ) );
  ::memset( this->state, 0, sizeof( this->state ) );
  xor_random_bytes( this->state, sizeof( this->state ), seed, sz );
  return true;
}

char *
rai::kv::timestamp( uint64_t ns,  int precision,  char *buf,
                    size_t len,  const char *fmt )
{
  static const char *default_fmt = "%Y-%m-%d %H:%M:%S";
  static const uint64_t ONE_NS = 1000 * 1000 * 1000;
  struct tm stamp;
  time_t t = (time_t) ( ns / ONE_NS );

  ::localtime_r( &t, &stamp );

  buf[ 0 ] = '\0';
  ::strftime( buf, len, fmt ? fmt : default_fmt, &stamp );
  buf[ len - 1 ] = '\0';

  if ( precision > 0 ) {
    size_t   i = ::strlen( buf );
    uint64_t j = ONE_NS,
             k = ONE_NS / 10;
    if ( i < len - 1 )
      buf[ i++ ] = '.';
    for ( ; i < len - 1 && precision > 0; precision-- ) {
      buf[ i++ ] = '0' + ( ( ns % j ) / k );
      j = k; k /= 10;
    }
    buf[ i ] = '\0';
  }
  return buf;
}

uint64_t
rai::kv::get_rdtsc( void )
{
   uint32_t lo, hi;
  __asm__ __volatile__("rdtsc" : "=a" (lo), "=d" (hi));
  return ( (uint64_t) hi << 32 ) | (uint64_t) lo;
}

uint64_t
rai::kv::current_monotonic_time_ns( void )
{
  timespec ts;
  clock_gettime( CLOCK_MONOTONIC, &ts );
  return ts.tv_sec * (uint64_t) 1000000000 + ts.tv_nsec;
}

double
rai::kv::current_monotonic_time_s( void )
{
  timespec ts;
  clock_gettime( CLOCK_MONOTONIC, &ts );
  return (double) ts.tv_sec + (double) ts.tv_nsec / 1000000000.0;
}

uint64_t
rai::kv::current_monotonic_coarse_ns( void )
{
  timespec ts;
  clock_gettime( CLOCK_MONOTONIC_COARSE, &ts );
  return ts.tv_sec * (uint64_t) 1000000000 + ts.tv_nsec;
}

double
rai::kv::current_monotonic_coarse_s( void )
{
  timespec ts;
  clock_gettime( CLOCK_MONOTONIC_COARSE, &ts );
  return (double) ts.tv_sec + (double) ts.tv_nsec / 1000000000.0;
}

uint64_t
rai::kv::current_realtime_ns( void )
{
  timespec ts;
  clock_gettime( CLOCK_REALTIME, &ts );
  return ts.tv_sec * (uint64_t) 1000000000 + ts.tv_nsec;
}

double
rai::kv::current_realtime_s( void )
{
  timespec ts;
  clock_gettime( CLOCK_REALTIME, &ts );
  return (double) ts.tv_sec + (double) ts.tv_nsec / 1000000000.0;
}

uint64_t
rai::kv::current_realtime_coarse_ns( void )
{
  timespec ts;
  clock_gettime( CLOCK_REALTIME_COARSE, &ts );
  return ts.tv_sec * (uint64_t) 1000000000 + ts.tv_nsec;
}

double
rai::kv::current_realtime_coarse_s( void )
{
  timespec ts;
  clock_gettime( CLOCK_REALTIME_COARSE, &ts );
  return (double) ts.tv_sec + (double) ts.tv_nsec / 1000000000.0;
}
extern "C" {

double kv_current_realtime_coarse_s( void ) {
  return rai::kv::current_realtime_coarse_s();
}
uint64_t kv_current_monotonic_time_ns( void ) {
  return rai::kv::current_monotonic_time_ns();
}
double kv_current_monotonic_time_s( void ) {
  return rai::kv::current_monotonic_time_s();
}
uint64_t kv_current_monotonic_coarse_ns( void ) {
  return rai::kv::current_monotonic_coarse_ns();
}
double kv_current_monotonic_coarse_s( void ) {
  return rai::kv::current_monotonic_coarse_s();
}
uint64_t kv_current_realtime_ns( void ) {
  return rai::kv::current_realtime_ns();
}
double kv_current_realtime_s( void ) {
  return rai::kv::current_realtime_s();
}
uint64_t kv_current_realtime_coarse_ns( void ) {
  return rai::kv::current_realtime_coarse_ns();
}

static kv_signal_handler_t * the_sh;

static void
kv_termination_handler( int signum )
{
  if ( the_sh != NULL ) {
    the_sh->sig = signum;
    the_sh->signaled = 1;
  }
}

void
kv_sighndl_install( kv_signal_handler_t *sh )
{
  struct sigaction new_action, old_action;

  sh->signaled = 0;
  sh->sig = 0;
  the_sh = sh;

  /* Set up the structure to specify the new action. */
  new_action.sa_handler = kv_termination_handler;
  sigemptyset (&new_action.sa_mask);
  new_action.sa_flags = 0;

  sigaction (SIGINT, NULL, &old_action);
  if (old_action.sa_handler != SIG_IGN)
    sigaction (SIGINT, &new_action, NULL);
  sigaction (SIGHUP, NULL, &old_action);
  if (old_action.sa_handler != SIG_IGN)
    sigaction (SIGHUP, &new_action, NULL);
  sigaction (SIGTERM, NULL, &old_action);
  if (old_action.sa_handler != SIG_IGN)
    sigaction (SIGTERM, &new_action, NULL);
}
}

void
SignalHandler::install( void )
{
  kv_sighndl_install( this );
}

char *
rai::kv::mem_to_string( int64_t m,  char *b,  int64_t k )
{
  char *s = b;
  if ( m == 0 )
    *b++ = '0';
  else if ( ( m > 0 && m < 100 * k ) || ( m < 0 && -m < 100 * k ) )
    int_to_string<int64_t>( m, b, &b );
  else if ( ( m > 0 && m < 100 * k * k ) || ( m < 0 && -m < 100 * k * k ) ) {
    int_to_string<int64_t>( ( m + k / 2 ) / k, b, &b );
    *b++ = 'K';
    if ( k == 1024 )
      *b++ = 'B';
  }
  else {
    int_to_string<int64_t>( ( m + ( k * k / 2 ) ) / ( k * k ), b, &b );
    *b++ = 'M';
    if ( k == 1024 )
      *b++ = 'B';
  }
  *b = '\0';
  return s; 
}   
