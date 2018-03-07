#ifndef __rai__raikv__key_hash_h__
#define __rai__raikv__key_hash_h__

#ifdef __cplusplus
extern "C" {
#endif

typedef unsigned __int128 uint128_t;

typedef uint64_t (* kv_hash64_func_t )( const void *p, size_t sz,
                                        uint64_t seed );
typedef uint128_t (* kv_hash128_func_t )( const void *p, size_t sz,
                                          uint128_t seed );
extern uint128_t kv_hash_citymur128( const void *p, size_t sz, uint128_t seed );
extern uint64_t kv_hash_cityhash64( const void *p, size_t sz, uint64_t seed );
extern uint64_t kv_hash_murmur64_a( const void *p, size_t sz, uint64_t seed );
extern uint64_t kv_hash_xxh64( const void *p, size_t sz, uint64_t seed );
extern uint32_t kv_crc_c( const void *p, size_t sz, uint32_t seed );

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif
