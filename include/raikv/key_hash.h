#ifndef __rai__raikv__key_hash_h__
#define __rai__raikv__key_hash_h__

#ifdef __cplusplus
extern "C" {
#endif
/* intel crc_c instruction */
uint32_t kv_hash_uint( uint32_t i );
uint32_t kv_hash_uint2( uint32_t r,  uint32_t i );
uint32_t kv_ll_hash_uint( uint32_t i );
uint32_t kv_crc_c( const void *p, size_t sz, uint32_t seed );
void kv_crc_c_2_diff( const void *p,  size_t sz,   uint32_t *seed,
                      const void *p2,  size_t sz2,  uint32_t *seed2 );
void kv_crc_c_4_diff( const void *p,   size_t sz,   uint32_t *seed,
                      const void *p2,  size_t sz2,  uint32_t *seed2,
                      const void *p3,  size_t sz3,  uint32_t *seed3,
                      const void *p4,  size_t sz4,  uint32_t *seed4 );
void kv_crc_c_array( const void **p,   size_t *psz,   uint32_t *seed,
                     size_t count );
void kv_crc_c_key_array( const void *p,   size_t *psz,   uint32_t *seed,
                         size_t count );

static inline uint32_t kv_djb( const char *s,  size_t len ) {
  uint32_t key = 5381;
  for ( ; len > 0; len -= 1 ) {
    uint8_t c = (uint8_t) *s++;
    key = (uint32_t) c ^ ( ( key << 5 ) + key );
  }
  return key;
}

typedef uint64_t (* kv_hash64_func_t )( const void *p, size_t sz,
                                        uint64_t seed );
typedef void (* kv_hash128_func_t )( const void *p, size_t sz,
                                     uint64_t *h1,  uint64_t *h2 );
#define USE_KV_MURMUR_HASH
/*#define USE_KV_CITY_HASH*/
#define USE_KV_SPOOKY_HASH
#define USE_KV_AES_HASH
#define USE_KV_MEOW_HASH

/* h1 + h2 are the seed and the hash result */
#ifdef USE_KV_MURMUR_HASH
uint64_t kv_hash_murmur64( const void *p, size_t sz, uint64_t seed );
void kv_hash_murmur128( const void *p, size_t sz, uint64_t *h1, uint64_t *h2 );
#endif
#ifdef USE_KV_CITY_HASH
uint64_t kv_hash_cityhash64( const void *p, size_t sz, uint64_t seed );
uint64_t kv_hash_citymur64( const void *p, size_t sz, uint64_t seed );
void kv_hash_citymur128( const void *p, size_t sz, uint64_t *h1, uint64_t *h2 );
#endif
#ifdef USE_KV_SPOOKY_HASH
uint64_t kv_hash_spooky64( const void *p, size_t sz, uint64_t seed );
void kv_hash_spooky128( const void *p, size_t sz, uint64_t *h1, uint64_t *h2 );
#endif
#ifdef USE_KV_AES_HASH
uint64_t kv_hash_aes64( const void *p, size_t sz, uint64_t seed );
void kv_hash_aes128( const void *p, size_t sz, uint64_t *h1, uint64_t *h2 );
#endif
#ifdef USE_KV_MEOW_HASH
uint64_t kv_hash_meow64( const void *p, size_t sz, uint64_t seed );
void kv_hash_meow128( const void *p, size_t sz, uint64_t *h1, uint64_t *h2 );
#endif

#ifdef USE_KV_MEOW_HASH

#ifdef _MSC_VER
typedef __declspec(align(64)) struct {
  uint64_t ctx[ 8 ];
} meow_ctx_t;

typedef __declspec(align(64)) struct {
  uint8_t block[ 64 ];
  size_t  off, total_update_sz;
} meow_block_t;

#else
typedef struct {
  uint64_t ctx[ 8 ];
} meow_ctx_t __attribute__((__aligned__(64)));

typedef struct {
  uint8_t block[ 64 ];
  size_t  off, total_update_sz;
} meow_block_t __attribute__((__aligned__(64)));
#endif

typedef struct {
  const void *p;
  size_t sz;
} meow_vec_t;

void kv_hash_meow128_vec( const meow_vec_t *vec, size_t vec_sz,
                          uint64_t *h1, uint64_t *h2 );
void kv_meow128_init( meow_ctx_t *m, meow_block_t *b,  uint64_t k1, uint64_t k2,
                      size_t total_update_sz );
void kv_meow128_update( meow_ctx_t *m, meow_block_t *b, const void *p,
                        size_t sz );
void kv_meow128_final( meow_ctx_t *m, meow_block_t *b, uint64_t *k1,
                       uint64_t *k2 );
void kv_meow_test( const void *p, size_t sz, uint64_t *k1, uint64_t *k2 );

void kv_hmac_meow_init( meow_ctx_t *m, meow_block_t *b,
                        uint64_t k1, uint64_t k2 );
void kv_hmac_meow_update( meow_ctx_t *m, meow_block_t *b,
                          const void *p, size_t sz );
void kv_hmac_meow_final( meow_ctx_t *m, meow_block_t *b,
                         uint64_t *k1, uint64_t *k2 );
void kv_hmac_meow( const void *p, size_t sz, uint64_t *k1, uint64_t *k2 );
/* uses the same seed in k1, k2 for both results in k1_2 k3_4 */
void kv_hash_meow128_2_same_length( const void *p, const void *p2, size_t sz,
                                    uint64_t *x4 );
void kv_hash_meow128_4_same_length_a( const void **p, size_t sz, uint64_t *x );
void kv_hash_meow128_8_same_length_a( const void **p, size_t sz, uint64_t *x );
void kv_hash_meow128_4_same_length( const void *p, const void *p2,
                                    const void *p3, const void *p4,
                                    size_t sz, uint64_t *x );
/* 4 same length, 4 different seeds */
void kv_hash_meow128_4_same_length_4_seed( const void *p, const void *p2,
                                           const void *p3, const void *p4,
                                           size_t sz, uint64_t *x );
void kv_hash_meow128_8_same_length( const void *p, const void *p2,
                                    const void *p3, const void *p4,
                                    const void *p5, const void *p6,
                                    const void *p7, const void *p8,
                                    size_t sz, uint64_t *x );
void kv_hash_meow128_2_diff_length( const void *p, size_t sz, const void *p2,
                                    size_t sz2, uint64_t *x );
void kv_hash_meow128_4_diff_length( const void *p, size_t sz, const void *p2,
                                    size_t sz2, const void *p3, size_t s3,
                                    const void *p4, size_t s4, uint64_t *x );
#endif

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif
