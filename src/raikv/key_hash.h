#ifndef __rai__raikv__key_hash_h__
#define __rai__raikv__key_hash_h__

#ifdef __cplusplus
extern "C" {
#endif
extern uint32_t kv_crc_c( const void *p, size_t sz, uint32_t seed );

typedef uint64_t (* kv_hash64_func_t )( const void *p, size_t sz,
                                        uint64_t seed );
/* h1 + h2 are the seed and the hash result */
typedef void (* kv_hash128_func_t )( const void *p, size_t sz,
                                     uint64_t *h1,  uint64_t *h2 );
#define USE_KV_MURMUR_HASH
/*#define USE_KV_CITY_HASH*/
#define USE_KV_SPOOKY_HASH
#define USE_KV_AES_HASH

#ifdef USE_KV_MURMUR_HASH
extern uint64_t kv_hash_murmur64( const void *p, size_t sz, uint64_t seed );
extern void kv_hash_murmur128( const void *p, size_t sz, uint64_t *h1, uint64_t *h2 );
#endif
#ifdef USE_KV_CITY_HASH
extern uint64_t kv_hash_cityhash64( const void *p, size_t sz, uint64_t seed );
extern uint64_t kv_hash_citymur64( const void *p, size_t sz, uint64_t seed );
extern void kv_hash_citymur128( const void *p, size_t sz, uint64_t *h1, uint64_t *h2 );
#endif
#ifdef USE_KV_SPOOKY_HASH
extern uint64_t kv_hash_spooky64( const void *p, size_t sz, uint64_t seed );
extern void kv_hash_spooky128( const void *p, size_t sz, uint64_t *h1, uint64_t *h2 );
#endif
#ifdef USE_KV_AES_HASH
extern uint64_t kv_hash_aes64( const void *p, size_t sz, uint64_t seed );
extern void kv_hash_aes128( const void *p, size_t sz, uint64_t *h1, uint64_t *h2 );
#endif
#ifdef __cplusplus
} /* extern "C" */
#endif

#endif
