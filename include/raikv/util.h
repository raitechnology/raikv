#ifndef __rai__raikv__util_h__
#define __rai__raikv__util_h__

/* also include stdint.h, string.h */
#ifdef __cplusplus
extern "C" {
#endif
char *kv_timestamp( uint64_t ns,  int precision,  char *buf,  size_t len,
                    const char *fmt ); /* fmt=NULL sets default */
uint64_t kv_get_rdtsc( void ); /* intel rdtsc */
uint64_t kv_get_rdtscp( void ); /* intel rdtscp */

uint64_t kv_current_monotonic_time_ns( void ); /* nanosecs */
uint64_t kv_current_monotonic_time_us( void ); /* microsecs */
double kv_current_monotonic_time_s( void ); /* seconds + fraction (52b prec) */

uint64_t kv_current_monotonic_coarse_ns( void ); /* nanosecs */
double kv_current_monotonic_coarse_s( void ); /* seconds + fraction (52b prec) */

uint64_t kv_current_realtime_ns( void ); /* nanosecs */
uint64_t kv_current_realtime_ms( void ); /* millisecs */
uint64_t kv_current_realtime_us( void ); /* microsecs */
double kv_current_realtime_s( void ); /* seconds + fraction (52b prec) */

uint64_t kv_current_realtime_coarse_ns( void ); /* nanosecs */
uint64_t kv_current_realtime_coarse_ms( void ); /* millisecs */
uint64_t kv_current_realtime_coarse_us( void ); /* microsecs */
double kv_current_realtime_coarse_s( void ); /* seconds + fraction (52b pr) */

typedef struct kv_signal_handler_s {
  volatile int signaled;
  int sig;
} kv_signal_handler_t;

void kv_sighndl_install( kv_signal_handler_t *sh );

#define kv_likely(x) (__builtin_expect((x),1))
#define kv_unlikely(x) (__builtin_expect((x),0))
#define _U64( x, y ) ( ( ( (uint64_t) (uint32_t) x ) << 32 ) | \
                           (uint64_t) (uint32_t) y )
#define kv_stringify(S) kv_str(S)
#define kv_str(S) #S
#ifdef __cplusplus
}
#endif

#ifdef __cplusplus
namespace rai {
namespace kv {

#define KV_ALIGN( sz, a ) ( ( ( sz ) + ( ( a ) - 1 ) ) & ~( ( a ) - 1 ) )
/* align<int>( 11, 8 ) == 16 */
template <class Int> static inline Int align( Int sz,  Int a ) {
  return (Int) KV_ALIGN( (size_t) sz, (size_t) a );
}
template <class Int> static inline Int max( Int i,  Int j ) { return i>j?i:j; }
template <class Int> static inline Int min( Int i,  Int j ) { return i<j?i:j; }
template <class Int> static inline Int unaligned( const void *p ) {
  Int i; ::memcpy( &i, p, sizeof( Int ) ); return i;
}

static const size_t KV_CACHE_ALIGN = 64;
static inline void *aligned_malloc( size_t sz ) {
#ifdef _ISOC11_SOURCE
  return ::aligned_alloc( KV_CACHE_ALIGN, sz ); /* >= RH7 */
#else
  return ::memalign( KV_CACHE_ALIGN, sz ); /* RH5, RH6.. */
#endif
}

namespace rand {
/* Derived from xoroshiro128star, 2016 by David Blackman and Sebastiano Vigna */

/* Note that the two lowest bits of this generator are LFSRs of degree
   1024, and thus will fail binary rank tests. The other bits needs a much
   higher degree to be represented as LFSRs.

   We suggest to use a sign test to extract a random Boolean value, and
   right shifts to extract subsets of bits.

   The state must be seeded so that it is not everywhere zero. If you have
   a 64-bit seed, we suggest to seed a splitmix64 generator and use its
   output to fill s. */
struct xorshift1024star {
  uint64_t state[ 16 ];
  uint64_t p;

  xorshift1024star() : p( 0 ) {}

  bool init( void *seed = 0,  uint16_t sz = 0 ) noexcept; /* init state */

  uint64_t next( void ) noexcept;

  double next_double( void ) {
    return (double) ( this->next() & ( ( (uint64_t) 1 << 53 ) - 1 ) ) /
           (double) ( (uint64_t) 1 << 53 );
  }
  xorshift1024star & operator=( const xorshift1024star &x ) {
    for ( int i = 0; i < 16; i++ )
      this->state[ i ] = x.state[ i ];
    this->p = x.p;
    return *this;
  }
};

/* Derived from xoroshiro128plus, 2016 by David Blackman and Sebastiano Vigna */
static inline uint64_t rotl( const uint64_t x,  int k ) {
  return (x << k) | (x >> (64 - k));
}
/* the lowest bit of this generator is an LFSR of degree 128. The next bit
   can be described by an LFSR of degree 8256, but in the long run it will
   fail linearity tests, too. The other bits needs a much higher degree to
   be represented as LFSRs. */
struct xoroshiro128plus {
  uint64_t state[ 2 ];

  xoroshiro128plus() {}
  /* static_init to the same state */
  void static_init( uint64_t x = 0,  uint64_t y = 0 ) noexcept;
  bool init( void *seed = 0,  uint16_t sz = 0 ) noexcept; /* init state */

  uint64_t current( void ) const {
    return this->state[ 0 ] + this->state[ 1 ];
  }
  void incr( void ) {
    const uint64_t s0 = this->state[ 0 ];
    const uint64_t s1 = this->state[ 1 ] ^ s0;
    this->state[ 0 ] = rotl(s0, 55) ^ s1 ^ (s1 << 14); /* a, b */
    this->state[ 1 ] = rotl(s1, 36); /* c */
  }
  uint64_t next( void ) {
    const uint64_t result = this->current();
    this->incr();
    return result;
  }
  double next_double( void ) {
    return (double) ( this->next() & ( ( (uint64_t) 1 << 53 ) - 1 ) ) /
           (double) ( (uint64_t) 1 << 53 );
  }
  xoroshiro128plus & operator=( const xoroshiro128plus &x ) {
    this->state[ 0 ] = x.state[ 0 ];
    this->state[ 1 ] = x.state[ 1 ];
    return *this;
  }
};
/* read /dev/urandom */
void fill_urandom_bytes( void *buf,  uint16_t sz ) noexcept;
}

struct SignalHandler : public kv_signal_handler_s {
  void install( void ) noexcept;
};

/* call strftime() with nanosecond stamp and subsecond precision (3=ms) */
char *timestamp( uint64_t ns,  int precision,  char *buf,  size_t len,
                 const char *fmt = NULL ) noexcept; /* fmt=NULL sets default */
uint64_t get_rdtsc( void ) noexcept; /* intel rdtsc */
uint64_t get_rdtscp( void ) noexcept; /* intel rdtscp */

uint64_t current_monotonic_time_ns( void ) noexcept; /* nanosecs */
uint64_t current_monotonic_time_us( void ) noexcept; /* microsecs */
double current_monotonic_time_s( void ) noexcept; /* s + fraction (52b prec) */

uint64_t current_monotonic_coarse_ns( void ) noexcept; /* nanosecs */
double current_monotonic_coarse_s( void ) noexcept; /* s + fraction (52b prec)*/

uint64_t current_realtime_ns( void ) noexcept; /* nanosecs */
uint64_t current_realtime_ms( void ) noexcept; /* millisecs */
uint64_t current_realtime_us( void ) noexcept; /* microsecs */
double current_realtime_s( void ) noexcept; /* seconds + fraction (52b prec) */

uint64_t current_realtime_coarse_ns( void ) noexcept; /* nanosecs */
uint64_t current_realtime_coarse_ms( void ) noexcept; /* millisecs */
uint64_t current_realtime_coarse_us( void ) noexcept; /* microsecs */
double current_realtime_coarse_s( void ) noexcept; /* s + fraction (52b pr) */

 /* round 10001 to 10K, 10000123 to 10M */
char *mem_to_string( int64_t m,  char *b,  int64_t k = 1000 ) noexcept;

/* via Andrei Alexandrescu */
template <class UInt>
UInt neg( UInt v ) {
  static const int b = sizeof( v ) * 8 - 1; /* 31, 63 */
  if ( (UInt) v == ( (UInt) 1 << b ) )
    return ( (UInt) 1 << b );
  return (UInt) -v;
}
inline uint64_t neg64( int64_t v ) {
  return neg<uint64_t>( v );
}
inline uint32_t neg32( int32_t v ) {
  return neg<uint32_t>( v );
}
template <class UInt>
inline size_t uint_to_string( UInt v,  char *buf,  size_t len ) {
  buf[ len ] = '\0';
  for ( size_t pos = len; v >= 10; ) {
    const UInt q = v / 10,
               r = v % 10;
    buf[ --pos ] = '0' + r;
    v = q;
  }
  buf[ 0 ] = '0' + v;
  return len;
}
template <class Int, class UInt>
size_t int_to_string( Int v,  char *buf,  size_t len ) {
  if ( v < 0 ) {
    len--; *buf++ = '-';
    return 1 + uint_to_string<UInt>( neg<UInt>( v ), buf, len - 1 );
  }
  return uint_to_string<UInt>( (UInt) v, buf, len );
}
template <class UInt>
inline size_t uint_digits( UInt v ) {
  for ( size_t n = 1; ; n += 4 ) {
    if ( v < 10 )    return n;
    if ( v < 100 )   return n + 1;
    if ( v < 1000 )  return n + 2;
    if ( v < 10000 ) return n + 3;
    v /= 10000;
  }
}
inline size_t uint64_digits( uint64_t v ) {
  return uint_digits<uint64_t>( v );
}
inline size_t uint32_digits( uint32_t v ) {
  return uint_digits<uint32_t>( v );
}
inline size_t int64_digits( int64_t v ) {
  return v < 0 ? ( uint64_digits( neg64( v ) ) + 1 ) : uint64_digits( v );
}
inline size_t int32_digits( int32_t v ) {
  return v < 0 ? ( uint32_digits( neg32( v ) ) + 1 ) : uint32_digits( v );
}
inline size_t uint64_to_string( uint64_t v,  char *buf,  size_t len ) {
  return uint_to_string<uint64_t>( v, buf, len );
}
inline size_t uint32_to_string( uint32_t v,  char *buf,  size_t len ) {
  return uint_to_string<uint32_t>( v, buf, len );
}
inline size_t int64_to_string( int64_t v,  char *buf,  size_t len ) {
  return int_to_string<int64_t, uint64_t>( v, buf, len );
}
inline size_t int32_to_string( int32_t v,  char *buf,  size_t len ) {
  return int_to_string<int32_t, uint32_t>( v, buf, len );
}
inline size_t uint64_to_string( uint64_t v,  char *buf ) {
  return uint64_to_string( v, buf, uint64_digits( v ) );
}
inline size_t int64_to_string( int64_t v,  char *buf ) {
  return int64_to_string( v, buf, int64_digits( v ) );
}
inline size_t uint32_to_string( uint32_t v,  char *buf ) {
  return uint32_to_string( v, buf, uint32_digits( v ) );
}
inline size_t int32_to_string( int32_t v,  char *buf ) {
  return int32_to_string( v, buf, int32_digits( v ) );
}

uint64_t string_to_uint64( const char *b,  size_t len ) noexcept;

int64_t string_to_int64( const char *b,  size_t len ) noexcept;

/* no eq padding */
#define KV_BASE64_SIZE( sz ) ( ( ( sz ) * 8 + 5 ) / 6 )
/* w/ eq padding, aligned multiple of 4 */
#define KV_BASE64_EQPAD( sz ) KV_ALIGN( BASE64_SIZE( sz ), 4 )
/* only correct wihtout eq padding (will be greater, since eq trimmed) */
#define KV_BASE64_BIN_SIZE( sz ) ( ( ( sz ) * 3 ) / 4 )

size_t bin_to_base64( const void *inp,  size_t in_len,  void *outp,
                      bool eq_padding = true ) noexcept;
size_t base64_to_bin( const void *inp,  size_t in_len,  void *outp ) noexcept;

} /* namespace kv */
} /* namespace rai */
#endif
#endif
