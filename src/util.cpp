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
          nbytes = ::read( fd, ubuf, sizeof( ubuf ) );
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

void
rai::kv::rand::xoroshiro128plus::static_init( uint64_t x,  uint64_t y ) noexcept
{
  this->state[ 0 ] = _U64( 0x9e3779b9, 0x7f4a7c13 );
  this->state[ 1 ] = _U64( 0x2b916c87, 0x261fe609 );
  this->state[ 0 ] ^= x;
  this->state[ 1 ] ^= y;
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

uint64_t
rai::kv::current_monotonic_time_us( void ) noexcept
{
  timespec ts;
  clock_gettime( CLOCK_MONOTONIC, &ts );
  return ts.tv_sec * (uint64_t) 1000000 + ts.tv_nsec;
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
uint64_t kv_current_monotonic_time_us( void ) {
  return rai::kv::current_monotonic_time_us();
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
#if 0
size_t
rai::kv::uint64_to_string( uint64_t v,  char *buf,  size_t len ) noexcept
{
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
rai::kv::int64_to_string( int64_t v,  char *buf,  size_t len ) noexcept
{
  if ( v < 0 ) {
    len--; *buf++ = '-';
    v = -v;
    return 1 + uint64_to_string( neg64( v ), buf, len - 1 );
  }
  return uint64_to_string( (uint64_t) v, buf, len );
}
#endif
static inline uint64_t
hex_value( int c )
{
  if ( c >= '0' && c <= '9' )
    return (uint64_t) ( c - '0' );
  if ( c >= 'a' && c <= 'f' )
    return (uint64_t) ( c - 'a' + 10 );
  if ( c >= 'A' && c <= 'F' )
    return (uint64_t) ( c - 'A' + 10 );
  return 0;
}

static uint64_t
hex_string_to_uint64( const char *b,  size_t len ) noexcept
{
  uint64_t n = 0, pow16 = 1;
  for (;;) {
    n += pow16 * hex_value( b[ --len ] );
    if ( len == 0 )
      return n;
    pow16 *= 16;
  }
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
  if ( len > 2 && b[ 0 ] == '0' && ( b[ 1 ] == 'x' || b[ 1 ] == 'X' ) )
    return hex_string_to_uint64( &b[ 2 ], len - 2 );
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

static const uint8_t AZ = 'Z' - 'A' + 1;

static inline uint8_t b64( uint8_t b ) {
  /* ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/ */
  if ( b < AZ )
    return 'A' + b;
  if ( b < AZ * 2 )
    return 'a' + ( b - AZ );
  if ( b < AZ * 2 + 10 )
    return '0' + ( b - AZ*2 );
  if ( b == AZ * 2 + 10 )
    return '+';
  return '/';
}

static inline uint8_t c64( uint8_t b ) {
  /* ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/ */
  static const uint8_t AZ = 'Z' - 'A' + 1;
  if ( b >= 'a' )
    return b - 'a' + AZ;
  if ( b >= 'A' )
    return b - 'A';
  if ( b >= '0')
    return b - '0' + AZ * 2;
  if ( b == '+' )
    return 62;
  return 63;
}

size_t
rai::kv::bin_to_base64( const void *inp,  size_t in_len,  void *outp,
                        bool eq_padding ) noexcept
{
  uint64_t        val;
  size_t          out_len      = 0;
  const uint8_t * in           = (const uint8_t *) inp;
  uint8_t       * out          = (uint8_t *) outp;
  size_t          out_len_calc = KV_BASE64_SIZE( in_len );

  while ( in_len >= 6 ) {
    val = ( (uint64_t) in[ 0 ] << 40 ) |
          ( (uint64_t) in[ 1 ] << 32 ) |
          ( (uint64_t) in[ 2 ] << 24 ) |
          ( (uint64_t) in[ 3 ] << 16 ) |
          ( (uint64_t) in[ 4 ] << 8 ) |
            (uint64_t) in[ 5 ]; 
    out[ out_len + 0 ] = b64( ( val >> 42 ) & 63U );
    out[ out_len + 1 ] = b64( ( val >> 36 ) & 63U );
    out[ out_len + 2 ] = b64( ( val >> 30 ) & 63U );
    out[ out_len + 3 ] = b64( ( val >> 24 ) & 63U );
    out[ out_len + 4 ] = b64( ( val >> 18 ) & 63U );
    out[ out_len + 5 ] = b64( ( val >> 12 ) & 63U );
    out[ out_len + 6 ] = b64( ( val >> 6 ) & 63U );
    out[ out_len + 7 ] = b64( val & 63U );
    in_len -= 6; in = &in[ 6 ];
    out_len += 8;
  }
  if ( in_len >= 3 ) {
    val = ( (uint64_t) in[ 0 ] << 16 ) |
          ( (uint64_t) in[ 1 ] << 8 ) |
            (uint64_t) in[ 2 ]; 
    out[ out_len + 0 ] = b64( ( val >> 18 ) & 63U );
    out[ out_len + 1 ] = b64( ( val >> 12 ) & 63U );
    out[ out_len + 2 ] = b64( ( val >> 6 ) & 63U );
    out[ out_len + 3 ] = b64( val & 63U );
    in_len -= 3; in = &in[ 3 ];
    out_len += 4;
  }
  if ( in_len > 0 ) {
    val = ( (uint64_t) in[ 0 ] << 16 );
    if ( in_len > 1 )
      val |= ( (uint64_t) in[ 1 ] << 8 );
    out[ out_len++ ] = b64( ( val >> 18 ) & 63U );
    if ( out_len < out_len_calc ) {
      out[ out_len++ ] = b64( ( val >> 12 ) & 63U );
      if ( out_len < out_len_calc )
        out[ out_len++ ] = b64( ( val >> 6 ) & 63U );
    }
    if ( eq_padding ) {
      size_t out_len_calc_eq = KV_ALIGN( out_len_calc, 4 );
      if ( out_len < out_len_calc_eq ) {
        out[ out_len++ ] = '=';
        if ( out_len < out_len_calc_eq )
          out[ out_len++ ] = '=';
      }
    }
  }
  return out_len;
}

size_t
rai::kv::base64_to_bin( const void *inp,  size_t in_len,  void *outp ) noexcept
{
  uint64_t        val;
  size_t          out_len = 0;
  const uint8_t * in      = (const uint8_t *) inp;
  uint8_t       * out     = (uint8_t *) outp;
  size_t          out_len_calc;

  while ( in_len > 0 && in[ in_len - 1 ] == '=' )
    in_len--;
  out_len_calc = KV_BASE64_BIN_SIZE( in_len );
  while ( in_len >= 8 ) {
    val = ( (uint64_t) c64( in[ 0 ] ) << 42 ) |
          ( (uint64_t) c64( in[ 1 ] ) << 36 ) |
          ( (uint64_t) c64( in[ 2 ] ) << 30 ) |
          ( (uint64_t) c64( in[ 3 ] ) << 24 ) |
          ( (uint64_t) c64( in[ 4 ] ) << 18 ) |
          ( (uint64_t) c64( in[ 5 ] ) << 12 ) |
          ( (uint64_t) c64( in[ 6 ] ) << 6 ) |
          ( (uint64_t) c64( in[ 7 ] ) );
    out[ out_len ]     = ( val >> 40 ) & 0xffU;
    out[ out_len + 1 ] = ( val >> 32 ) & 0xffU;
    out[ out_len + 2 ] = ( val >> 24 ) & 0xffU;
    out[ out_len + 3 ] = ( val >> 16 ) & 0xffU;
    out[ out_len + 4 ] = ( val >> 8 ) & 0xffU;
    out[ out_len + 5 ] = val & 0xffU;
    in_len -= 8; in = &in[ 8 ];
    out_len += 6;
  }
  if ( in_len >= 4 ) {
    val = ( (uint64_t) c64( in[ 0 ] ) << 18 ) |
          ( (uint64_t) c64( in[ 1 ] ) << 12 ) |
          ( (uint64_t) c64( in[ 2 ] ) << 6 ) |
          ( (uint64_t) c64( in[ 3 ] ) );
    out[ out_len ]     = ( val >> 16 ) & 0xffU;
    out[ out_len + 1 ] = ( val >> 8 ) & 0xffU;
    out[ out_len + 2 ] = val & 0xffU;
    in_len -= 4; in = &in[ 4 ];
    out_len += 3;
  }
  if ( in_len > 0 ) {
    val = ( (uint64_t) c64( in[ 0 ] ) << 18 );
    if ( in_len > 1 ) {
      val |= ( (uint64_t) c64( in[ 1 ] ) << 12 );
      if ( in_len > 2 )
        val |= ( (uint64_t) c64( in[ 2 ] ) << 6 );
    }
    if ( out_len < out_len_calc ) {
      out[ out_len++ ] = ( val >> 16 ) & 0xff;
      if ( out_len < out_len_calc ) {
        out[ out_len++ ] = ( val >> 8 ) & 0xffU;
        if ( out_len < out_len_calc )
          out[ out_len++ ] = val & 0xffU;
      }
    }
  }
  return out_len;
}


