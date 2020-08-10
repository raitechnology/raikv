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

static const uint64_t newhash_magic = _U64( 0x9e3779b9U, 0x7f4a7c13U );
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
xor_bytes( void *out,  uint16_t outsz,  void *in,  uint16_t insz )
{
  if ( insz > 0 )
    for ( uint32_t i = 0; i < outsz; i++ )
      ((uint8_t *) out)[ i ] ^= ((uint8_t *) in)[ i % insz ];
}

void
rai::kv::rand::fill_urandom_bytes( void *buf,  uint16_t sz ) noexcept
{
  if ( ::getenv( "RAIKV_STATIC_RANDOM" ) != NULL ) {
    /* static pattern, doesn't change from run to run, for debug */
    static uint64_t counter = 100001;
    static uint64_t x = newhash_magic, y = newhash_magic;
    uint64_t tmp[ 64 * 1024 / sizeof( uint64_t ) ];
    uint16_t i;
    for ( i = 0; i < sz; i += sizeof( uint64_t ) ) {
      tmp[ i / sizeof( uint64_t ) ] = counter++;
      newhash_mix( x, y, tmp[ i / sizeof( uint64_t ) ] );
    }
    ::memcpy( buf, tmp, sz );
  }
  else {
    static uint8_t ubuf[ 16384 ];
    static int32_t nbytes;
    while ( sz > 0 ) {
      if ( nbytes <= 0 ) {
        int fd = ::open( "/dev/urandom", O_RDONLY );
        if ( fd >= 0 ) {
          nbytes = ::read( fd, ubuf, sizeof( buf ) );
          ::close( fd );
        }
        if ( nbytes <= 0 ) {
          uint64_t h[ 6 ] = { 1, 2, 3, 4, 5, 6 };
          for ( uint32_t i = 0; i < sizeof( ubuf ); ) {
            h[ 0 ] ^= current_monotonic_time_ns();
            h[ 1 ] ^= get_rdtsc();
            h[ 2 ] ^= newhash_magic;
            if ( ( h[ 0 ] & 63 ) < 32 )
              kv_sync_pause();
            h[ 3 ] ^= current_monotonic_time_ns();
            if ( ( h[ 1 ] & 63 ) < 32 )
              kv_sync_mfence();
            h[ 4 ] ^= newhash_magic;
            h[ 5 ] ^= get_rdtsc();

            newhash_mix( h[ 0 ], h[ 1 ], h[ 2 ] );
            newhash_mix( h[ 3 ], h[ 4 ], h[ 5 ] );
            ::memcpy( &ubuf[ i ], h, 32 );
            i += 32;
          }
          nbytes = sizeof( ubuf );
        }
      }
      while ( nbytes > 0 && sz > 0 )
        ((uint8_t *) buf)[ --sz ] = ubuf[ --nbytes ];
    }
  }
}

bool
rai::kv::rand::xorshift1024star::init( void *seed,  uint16_t sz ) noexcept
{
  this->p = 0;
  fill_urandom_bytes( this->state, sizeof( this->state ) );
  xor_bytes( this->state, sizeof( this->state ), seed, sz );
  return true;
}

uint64_t
rai::kv::rand::xorshift1024star::next( void ) noexcept
{
  const uint64_t s0 = this->state[ this->p ];
  uint64_t       s1 = this->state[ this->p = (this->p + 1) & 15 ];
  s1 ^= s1 << 31; // a
  this->state[ this->p ] = s1 ^ s0 ^ (s1 >> 11) ^ (s0 >> 30); // b,c
  return this->state[ this->p ] * _U64( 0x9e3779b9, 0x7f4a7c13 );
}

bool
rai::kv::rand::xoroshiro128plus::init( void *seed,  uint16_t sz ) noexcept
{
  fill_urandom_bytes( this->state, sizeof( this->state ) );
  xor_bytes( this->state, sizeof( this->state ), seed, sz );
  return true;
}

char *
rai::kv::timestamp( uint64_t ns,  int precision,  char *buf,
                    size_t len,  const char *fmt ) noexcept
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
rai::kv::get_rdtsc( void ) noexcept
{
   uint32_t lo, hi;
  __asm__ __volatile__("rdtsc" : "=a" (lo), "=d" (hi));
  return ( (uint64_t) hi << 32 ) | (uint64_t) lo;
}

uint64_t
rai::kv::get_rdtscp( void ) noexcept
{
   uint32_t lo, hi;
  __asm__ __volatile__("rdtscp" : "=a" (lo), "=d" (hi));
  return ( (uint64_t) hi << 32 ) | (uint64_t) lo;
}

uint64_t
rai::kv::current_monotonic_time_ns( void ) noexcept
{
  timespec ts;
  clock_gettime( CLOCK_MONOTONIC, &ts );
  return ts.tv_sec * (uint64_t) 1000000000 + ts.tv_nsec;
}

double
rai::kv::current_monotonic_time_s( void ) noexcept
{
  timespec ts;
  clock_gettime( CLOCK_MONOTONIC, &ts );
  return (double) ts.tv_sec + (double) ts.tv_nsec / 1000000000.0;
}

uint64_t
rai::kv::current_monotonic_coarse_ns( void ) noexcept
{
#ifndef CLOCK_MONOTONIC_COARSE
#define CLOCK_MONOTONIC_COARSE 6
#endif
  timespec ts;
  clock_gettime( CLOCK_MONOTONIC_COARSE, &ts );
  return ts.tv_sec * (uint64_t) 1000000000 + ts.tv_nsec;
}

double
rai::kv::current_monotonic_coarse_s( void ) noexcept
{
  timespec ts;
  clock_gettime( CLOCK_MONOTONIC_COARSE, &ts );
  return (double) ts.tv_sec + (double) ts.tv_nsec / 1000000000.0;
}

uint64_t
rai::kv::current_realtime_ns( void ) noexcept
{
  timespec ts;
  clock_gettime( CLOCK_REALTIME, &ts );
  return ts.tv_sec * (uint64_t) 1000000000 + ts.tv_nsec;
}

uint64_t
rai::kv::current_realtime_ms( void ) noexcept
{
  timespec ts;
  clock_gettime( CLOCK_REALTIME, &ts );
  return ts.tv_sec * (uint64_t) 1000 + ts.tv_nsec / 1000000;
}

uint64_t
rai::kv::current_realtime_us( void ) noexcept
{
  timespec ts;
  clock_gettime( CLOCK_REALTIME, &ts );
  return ts.tv_sec * (uint64_t) 1000000 + ts.tv_nsec / 1000;
}

double
rai::kv::current_realtime_s( void ) noexcept
{
  timespec ts;
  clock_gettime( CLOCK_REALTIME, &ts );
  return (double) ts.tv_sec + (double) ts.tv_nsec / 1000000000.0;
}

uint64_t
rai::kv::current_realtime_coarse_ns( void ) noexcept
{
#ifndef CLOCK_REALTIME_COARSE
#define CLOCK_REALTIME_COARSE 5
#endif
  timespec ts;
  clock_gettime( CLOCK_REALTIME_COARSE, &ts );
  return ts.tv_sec * (uint64_t) 1000000000 + ts.tv_nsec;
}

uint64_t
rai::kv::current_realtime_coarse_ms( void ) noexcept
{
  timespec ts;
  clock_gettime( CLOCK_REALTIME_COARSE, &ts );
  return ts.tv_sec * (uint64_t) 1000 + ts.tv_nsec / 1000000;
}

uint64_t
rai::kv::current_realtime_coarse_us( void ) noexcept
{
  timespec ts;
  clock_gettime( CLOCK_REALTIME_COARSE, &ts );
  return ts.tv_sec * (uint64_t) 1000000 + ts.tv_nsec / 1000;
}

double
rai::kv::current_realtime_coarse_s( void ) noexcept
{
  timespec ts;
  clock_gettime( CLOCK_REALTIME_COARSE, &ts );
  return (double) ts.tv_sec + (double) ts.tv_nsec / 1000000000.0;
}
extern "C" {

uint64_t kv_get_rdtsc( void ) {
  return rai::kv::get_rdtsc(); /* intel rdtsc */
}
uint64_t kv_get_rdtscp( void ) {
  return rai::kv::get_rdtscp(); /* intel rdtscp */
}
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
uint64_t kv_current_realtime_ms( void ) {
  return rai::kv::current_realtime_ms();
}
uint64_t kv_current_realtime_us( void ) {
  return rai::kv::current_realtime_us();
}
double kv_current_realtime_s( void ) {
  return rai::kv::current_realtime_s();
}
uint64_t kv_current_realtime_coarse_ns( void ) {
  return rai::kv::current_realtime_coarse_ns();
}
uint64_t kv_current_realtime_coarse_ms( void ) {
  return rai::kv::current_realtime_coarse_ms();
}
uint64_t kv_current_realtime_coarse_us( void ) {
  return rai::kv::current_realtime_coarse_us();
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
  struct sigaction new_action, old_action, ign_action;

  sh->signaled = 0;
  sh->sig = 0;
  the_sh = sh;

  /* Set up the structure to specify the new action. */
  new_action.sa_handler = kv_termination_handler;
  sigemptyset (&new_action.sa_mask);
  new_action.sa_flags = 0;

  ign_action.sa_handler = SIG_IGN;
  sigemptyset (&ign_action.sa_mask);
  ign_action.sa_flags = 0;

  sigaction (SIGINT, NULL, &old_action);
  if (old_action.sa_handler != SIG_IGN)
    sigaction (SIGINT, &new_action, NULL);
  sigaction (SIGHUP, NULL, &old_action);
  if (old_action.sa_handler != SIG_IGN)
    sigaction (SIGHUP, &new_action, NULL);
  sigaction (SIGTERM, NULL, &old_action);
  if (old_action.sa_handler != SIG_IGN)
    sigaction (SIGTERM, &new_action, NULL);
  /* Ignore pipe */
  sigaction (SIGPIPE, NULL, &old_action);
  if (old_action.sa_handler != SIG_IGN)
    sigaction (SIGPIPE, &ign_action, NULL);
}
}

void
SignalHandler::install( void ) noexcept
{
  kv_sighndl_install( this );
}

char *
rai::kv::mem_to_string( int64_t m,  char *b,  int64_t k ) noexcept
{
  char * s = b;
  size_t i = 0;
  if ( m == 0 )
    b[ i++ ] = '0';
  else if ( ( m > 0 && m < 100 * k ) || ( m < 0 && -m < 100 * k ) )
    i = int64_to_string( m, b );
  else if ( ( m > 0 && m < 100 * k * k ) || ( m < 0 && -m < 100 * k * k ) ) {
    i = int64_to_string( ( m + k / 2 ) / k, b );
    b[ i++ ] = 'K';
    if ( k == 1024 )
      b[ i++ ] = 'B';
  }
  else {
    i = int64_to_string( ( m + ( k * k / 2 ) ) / ( k * k ), b );
    b[ i++ ] = 'M';
    if ( k == 1024 )
      b[ i++ ] = 'B';
  }
  b[ i ] = '\0';
  return s; 
}   

/* via Andrei Alexandrescu */
static inline size_t
uint64_digits( uint64_t v )
{
  for ( size_t n = 1; ; n += 4 ) {
    if ( v < 10 )    return n;
    if ( v < 100 )   return n + 1;
    if ( v < 1000 )  return n + 2;
    if ( v < 10000 ) return n + 3;
    v /= 10000;
  }
}

size_t
rai::kv::uint64_to_string( uint64_t v,  char *buf ) noexcept
{
  const size_t len = uint64_digits( v );
  buf[ len ] = '\0';
  for ( size_t pos = len; v >= 10; ) {
    const uint64_t q = v / 10,
                   r = v % 10;
    buf[ --pos ] = '0' + r;
    v = q;
  }
  buf[ 0 ] = '0' + v;
  return len;
}

size_t
rai::kv::int64_to_string( int64_t v,  char *buf ) noexcept
{
  size_t len = 0;
  if ( v < 0 ) { len++; *buf++ = '-'; v = -v; }
  return len + uint64_to_string( (uint64_t) v, buf );
}

uint64_t
rai::kv::string_to_uint64( const char *b,  size_t len ) noexcept
{
  /* max is 1844674407,3709551615, this table doesnn't overflow 32bits */
  static const uint32_t pow10[] = {     10000U * 10000U * 10,
    10000U * 10000, 10000U * 1000, 10000U * 100, 10000U * 10,
             10000,          1000,          100,          10,
                 1
  };
  static const size_t max_pow10 = sizeof( pow10 ) / sizeof( pow10[ 0 ] );
  size_t i, j;
  uint64_t n = 0;
  i = j = 0;
  if ( len < max_pow10 )
    i = max_pow10 - len;
  else
    j = len - max_pow10;
  len = j;
  for (;;) {
    n += (uint64_t) pow10[ i++ ] * ( b[ j++ ] - '0' );
    if ( i == max_pow10 )
      break;
  }
  if ( len != 0 ) {
    i = j = 0;
    if ( len < max_pow10 )
      i = max_pow10 - len;
    else
      j = len - max_pow10;
    len = j;
    const uint64_t m = (uint64_t) 10 * (uint64_t) pow10[ 0 ];
    for (;;) {
      n += m * (uint64_t) pow10[ i++ ] * ( b[ j++ ] - '0' );
      if ( i == max_pow10 )
        break;
    }
  }
  return n;
}

int64_t
rai::kv::string_to_int64( const char *b,  size_t len ) noexcept
{
  uint64_t x;
  bool     is_neg = false;
  if ( b[ 0 ] == '-' ) { is_neg = true; b++; len -= 1; }
  x = string_to_uint64( b, len );
  if ( is_neg ) return -x;
  return x;
}

