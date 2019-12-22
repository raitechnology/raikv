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

/* align<int>( 11, 8 ) == 16 */
template <class Int> static inline Int align( Int sz,  Int a ) {
  return ( sz + ( a - 1 ) ) & ~( a - 1 );
}
template <class Int> static inline Int max( Int i,  Int j ) { return i>j?i:j; }
template <class Int> static inline Int min( Int i,  Int j ) { return i<j?i:j; }
template <class Int> static inline Int unaligned( const void *p ) {
  Int i; ::memcpy( &i, p, sizeof( Int ) ); return i;
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

  bool init( void *seed = 0,  uint16_t sz = 0 ); /* init state */

  uint64_t next( void );
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

  bool init( void *seed = 0,  uint16_t sz = 0 ); /* init state */

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
};
/* read /dev/urandom */
bool fill_urandom_bytes( void *buf,  uint16_t sz );
}

struct SignalHandler : public kv_signal_handler_s {
  void install( void );
};

/* call strftime() with nanosecond stamp and subsecond precision (3=ms) */
char *timestamp( uint64_t ns,  int precision,  char *buf,  size_t len,
                 const char *fmt = NULL ); /* fmt=NULL sets default */
uint64_t get_rdtsc( void ); /* intel rdtsc */
uint64_t get_rdtscp( void ); /* intel rdtscp */

uint64_t current_monotonic_time_ns( void ); /* nanosecs */
double current_monotonic_time_s( void ); /* seconds + fraction (52b prec) */

uint64_t current_monotonic_coarse_ns( void ); /* nanosecs */
double current_monotonic_coarse_s( void ); /* seconds + fraction (52b prec) */

uint64_t current_realtime_ns( void ); /* nanosecs */
uint64_t current_realtime_ms( void ); /* millisecs */
uint64_t current_realtime_us( void ); /* microsecs */
double current_realtime_s( void ); /* seconds + fraction (52b prec) */

uint64_t current_realtime_coarse_ns( void ); /* nanosecs */
uint64_t current_realtime_coarse_ms( void ); /* millisecs */
uint64_t current_realtime_coarse_us( void ); /* microsecs */
double current_realtime_coarse_s( void ); /* seconds + fraction (52b pr) */

 /* round 10001 to 10K, 10000123 to 10M */
char *mem_to_string( int64_t m,  char *b,  int64_t k = 1000 );

size_t uint64_to_string( uint64_t v,  char *buf );

size_t int64_to_string( int64_t v,  char *buf );

uint64_t string_to_uint64( const char *b,  size_t len );

int64_t string_to_int64( const char *b,  size_t len );
} /* namespace kv */
} /* namespace rai */
#endif
#endif
