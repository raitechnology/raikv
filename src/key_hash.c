#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>

#include <raikv/shm_ht.h>
/* Most of this is derived from Google sources
 *
 * Copyright (c) 2011 Google, Inc.
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", yada yada
 *
 * MurmurHash, by Austin Appleby
 * xxh64, by Yann Collet
 * CityHash, by Geoff Pike and Jyrki Alakuijala
 */

static inline uint64_t
MurmurHash64A ( const void * key, int len, uint64_t seed )
{
  static const uint64_t m = _U64( 0xc6a4a793, 0x5bd1e995 );
  static const int r = 47;

  uint64_t h = seed ^ ( len * m );

  const uint64_t * data  = (const uint64_t *) key;
  const uint64_t * end   = data + ( len / 8 );
  const uint8_t  * data2 = (const uint8_t *) end;

  while ( data != end ) {
    uint64_t k = *data++;

    k *= m; 
    k ^= k >> r; 
    k *= m; 
    
    h ^= k;
    h *= m; 
  }

  switch ( len & 7 ) {
    case 7: h ^= ((uint64_t) data2[ 6 ] ) << 48;
    case 6: h ^= ((uint64_t) data2[ 5 ] ) << 40;
    case 5: h ^= ((uint64_t) data2[ 4 ] ) << 32;
    case 4: h ^= ((uint64_t) data2[ 3 ] ) << 24;
    case 3: h ^= ((uint64_t) data2[ 2 ] ) << 16;
    case 2: h ^= ((uint64_t) data2[ 1 ] ) << 8;
    case 1: h ^= ((uint64_t) data2[ 0 ] );
	    h *= m;
  };
 
  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

uint64_t
kv_hash_murmur64_a( const void *p, size_t sz, uint64_t seed )
{
  return MurmurHash64A( p, sz, seed );
}

static inline uint64_t
XXH_rotl64(uint64_t x, int r) { return ((x << r) | (x >> (64 - r))); }

static const uint64_t PRIME64_1 = _U64( 0x9e3779b1, 0x85ebca87 );
static const uint64_t PRIME64_2 = _U64( 0xc2b2ae3d, 0x27d4eb4f );
static const uint64_t PRIME64_3 = _U64( 0x165667b1, 0x9e3779f9 );
static const uint64_t PRIME64_4 = _U64( 0x85ebca77, 0xc2b2ae63 );
static const uint64_t PRIME64_5 = _U64( 0x27d4eb2f, 0x165667c5 );

static inline uint64_t
XXH64_round(uint64_t acc, uint64_t input)
{
    acc += input * PRIME64_2;
    acc  = XXH_rotl64(acc, 31);
    acc *= PRIME64_1;
    return acc;
}

static inline uint64_t
XXH64_mergeRound(uint64_t acc, uint64_t val)
{
    val  = XXH64_round(0, val);
    acc ^= val;
    acc  = acc * PRIME64_1 + PRIME64_4;
    return acc;
}

static inline uint64_t
XXH_get64bits(const uint8_t *p)
{
    return *(uint64_t *) p;
}

static inline uint64_t
XXH_get32bits(const uint8_t *p)
{
    return (uint64_t) *(uint32_t *) p;
}

static inline uint64_t
XXH64_hash(const void* input, size_t len, uint64_t seed)
{
    const uint8_t* p = (const uint8_t*)input;
    const uint8_t* bEnd = p + len;
    uint64_t h64;

    if (len>=32) {
        const uint8_t* const limit = bEnd - 32;
        uint64_t v1 = seed + PRIME64_1 + PRIME64_2;
        uint64_t v2 = seed + PRIME64_2;
        uint64_t v3 = seed + 0;
        uint64_t v4 = seed - PRIME64_1;

        do {
            v1 = XXH64_round(v1, XXH_get64bits(p)); p+=8;
            v2 = XXH64_round(v2, XXH_get64bits(p)); p+=8;
            v3 = XXH64_round(v3, XXH_get64bits(p)); p+=8;
            v4 = XXH64_round(v4, XXH_get64bits(p)); p+=8;
        } while (p<=limit);

        h64 = XXH_rotl64(v1, 1) + XXH_rotl64(v2, 7) +
              XXH_rotl64(v3, 12) + XXH_rotl64(v4, 18);
        h64 = XXH64_mergeRound(h64, v1);
        h64 = XXH64_mergeRound(h64, v2);
        h64 = XXH64_mergeRound(h64, v3);
        h64 = XXH64_mergeRound(h64, v4);

    } else {
        h64  = seed + PRIME64_5;
    }

    h64 += (uint64_t) len;

    while (p+8<=bEnd) {
        uint64_t const k1 = XXH64_round(0, XXH_get64bits(p));
        h64 ^= k1;
        h64  = XXH_rotl64(h64,27) * PRIME64_1 + PRIME64_4;
        p+=8;
    }

    if (p+4<=bEnd) {
        h64 ^= (uint64_t)(XXH_get32bits(p)) * PRIME64_1;
        h64 = XXH_rotl64(h64, 23) * PRIME64_2 + PRIME64_3;
        p+=4;
    }

    while (p<bEnd) {
        h64 ^= (*p) * PRIME64_5;
        h64 = XXH_rotl64(h64, 11) * PRIME64_1;
        p++;
    }

    h64 ^= h64 >> 33;
    h64 *= PRIME64_2;
    h64 ^= h64 >> 29;
    h64 *= PRIME64_3;
    h64 ^= h64 >> 32;

    return h64;
}

uint64_t
kv_hash_xxh64( const void *p, size_t sz, uint64_t seed )
{
  return XXH64_hash( p, sz, seed );
}

uint32_t
kv_crc_c( const void *p,  size_t sz,  uint32_t seed )
{
#define CRC32q( CRC, X ) \
  __asm__ __volatile__ ( "crc32q %1, %0" : "+r" (CRC) : "m" (X) )
#define CRC32l( CRC, X ) \
  __asm__ __volatile__ ( "crc32l %1, %0" : "+r" (CRC) : "m" (X) )
#define CRC32w( CRC, X ) \
  __asm__ __volatile__ ( "crc32w %1, %0" : "+r" (CRC) : "m" (X) )
#define CRC32b( CRC, X ) \
  __asm__ __volatile__ ( "crc32b %1, %0" : "+r" (CRC) : "m" (X) )

  register const uint8_t * s =   (uint8_t *) p;
  register const uint8_t * e = &((uint8_t *) p) [ sz ];
  register       uint64_t  hash64 = seed;
  register       uint32_t  hash32;

  while ( e >= &s[ sizeof( uint64_t ) ] ) {
    CRC32q( hash64, ((const uint64_t *) (const void *) s)[ 0 ] );
    s = &s[ 8 ];
  }
  hash32 = (uint32_t) hash64;

  switch ( e - s ) {
    case 7:
      CRC32b( hash32, s[ 0 ] ); s++;
    case 6:
      CRC32w( hash32, ((const uint16_t *) s)[ 0 ] ); s = &s[ 2 ];
    case 4:
      CRC32l( hash32, ((const uint32_t *) s)[ 0 ] );
      break;
    case 3:
      CRC32b( hash32, s[ 0 ] ); s++;
    case 2:
      CRC32w( hash32, ((const uint16_t *) s)[ 0 ] );
      break;
    case 5:
      CRC32l( hash32, ((const uint32_t *) s)[ 0 ] ); s = &s[ 4 ];
    case 1:
      CRC32b( hash32, s[ 0 ] );
      break;
    default:
    case 0:
      break;
  }

  return hash32;
}

static inline uint64_t Fetch64(const void *p) {
  return *(uint64_t *) p;
}
static inline uint32_t Fetch32(const void *p) {
  return *(uint32_t *) p;
}
static inline uint64_t Rotate64(const uint64_t x,  int k) {
  return (x << k) | (x >> (64 - k));
}
static inline uint64_t ShiftMix64(const uint64_t val) {
  return val ^ (val >> 47);
}

static inline uint64_t
Hash128to64(const uint64_t hi, const uint64_t lo)
{
  /* Murmur-inspired hashing. */
  const uint64_t kMul = _U64( 0x9ddfea08, 0xeb382d69 );
  uint64_t a = (hi ^ lo) * kMul, b;
  a ^= (a >> 47);
  b  = (hi ^ a) * kMul;
  b ^= (b >> 47);
  b *= kMul;
  return b;
}

static inline uint64_t
HashLen16(uint64_t u, uint64_t v, uint64_t mul)
{
  uint64_t a = (u ^ v) * mul, b;
  a ^= (a >> 47);
  b  = (v ^ a) * mul;
  b ^= (b >> 47);
  b *= mul;
  return b;
}

/* primes between 2*63 and 2^64 */
static const uint64_t k0 = _U64( 0xc3a5c85c, 0x97cb3127 );
static const uint64_t k1 = _U64( 0xb492b66f, 0xbe98f273 );
static const uint64_t k2 = _U64( 0x9ae16a3b, 0x2f90404f );

static inline uint64_t
HashLen0to16(const char *s, size_t len)
{
  if (len >= 8) {
    uint64_t mul = k2 + len * 2;
    uint64_t a = Fetch64(s) + k2;
    uint64_t b = Fetch64(s + len - 8);
    uint64_t c = Rotate64(b, 37) * mul + a;
    uint64_t d = (Rotate64(a, 25) + b) * mul;
    return HashLen16(c, d, mul);
  }
  if (len >= 4) {
    uint64_t mul = k2 + len * 2;
    uint64_t a = Fetch32(s);
    return HashLen16(len + (a << 3), Fetch32(s + len - 4), mul);
  }
  if (len > 0) {
    uint8_t a = s[0];
    uint8_t b = s[len >> 1];
    uint8_t c = s[len - 1];
    uint32_t y = (uint32_t) a + ((uint32_t) b << 8);
    uint32_t z = len + ((uint32_t) c << 2);
    return ShiftMix64(y * k2 ^ z * k0) * k2;
  }
  return k2;
}

static inline uint64_t
HashLen17to32(const char *s, size_t len)
{
  uint64_t mul = k2 + len * 2;
  uint64_t a = Fetch64(s) * k1;
  uint64_t b = Fetch64(s + 8);
  uint64_t c = Fetch64(s + len - 8) * mul;
  uint64_t d = Fetch64(s + len - 16) * k2;
  return HashLen16(Rotate64(a + b, 43) + Rotate64(c, 30) + d,
                   a + Rotate64(b + k2, 18) + c, mul);
}

#if __GNUC__ > 4 || ( __GNUC__ == 4 && __GNUC_MINOR__ > 3 )
#define bswap64( X ) __builtin_bswap64( X )
#else
static inline uint64_t
bswap64( uint64_t x )
{
#define BSWAP64( X )  __asm__ __volatile__ ( "bswapq %0" : "=r" (X) : "0" (X) );
  BSWAP64( x );
  return x;
}
#endif

static inline uint64_t
HashLen33to64(const char *s, size_t len)
{
  uint64_t mul = k2 + len * 2;
  uint64_t a = Fetch64(s) * k2;
  uint64_t b = Fetch64(s + 8);
  uint64_t c = Fetch64(s + len - 24);
  uint64_t d = Fetch64(s + len - 32);
  uint64_t e = Fetch64(s + 16) * k2;
  uint64_t f = Fetch64(s + 24) * 9;
  uint64_t g = Fetch64(s + len - 8);
  uint64_t h = Fetch64(s + len - 16) * mul;
  uint64_t u = Rotate64(a + g, 43) + (Rotate64(b, 30) + c) * 9;
  uint64_t v = ((a + g) ^ d) + f + 1;
  uint64_t w = bswap64((u + v) * mul) + h;
  uint64_t x = Rotate64(e + f, 42) + c;
  uint64_t y = (bswap64((v + w) * mul) + g) * mul;
  uint64_t z = e + f + c;
  a = bswap64((x + z) * mul + y) + b;
  b = ShiftMix64((z + a) * mul + d + h) * mul;
  return b + x;
}
#if 0
/* A subroutine for CityHash128().  Returns a decent 128-bit hash for strings
   of any length representable in signed long.  Based on City and Murmur. */
static inline uint128_t
CityMurmur(const char *s, size_t len, uint128_t seed)
{
  uint64_t a = (uint64_t) seed;
  uint64_t b = (uint64_t) ( seed >> 64 );
  uint64_t c = 0;
  uint64_t d = 0;
  signed long l = len - 16;
  if (l <= 0) {
    a = ShiftMix64(a * k1) * k1;
    c = b * k1 + HashLen0to16(s, len);
    d = ShiftMix64(a + (len >= 8 ? Fetch64(s) : c));
  } else {
    c = Hash128to64(Fetch64(s + len - 8) + k1, a);
    d = Hash128to64(b + len, c + Fetch64(s + len - 16));
    a += d;
    do {
      a ^= ShiftMix64(Fetch64(s) * k1) * k1;
      a *= k1;
      b ^= a;
      c ^= ShiftMix64(Fetch64(s + 8) * k1) * k1;
      c *= k1;
      d ^= c;
      s += 16;
      l -= 16;
    } while (l > 0);
  }
  a = Hash128to64(a, c);
  b = Hash128to64(d, b);
  return ( (uint128_t) ( a ^ b ) << 64 ) | (uint128_t) Hash128to64(b, a);
}

uint128_t
kv_hash_citymur128( const void *p, size_t sz, uint128_t seed )
{
  return CityMurmur( p, sz, seed );
}
#endif
typedef struct {
  uint64_t first, second;
} pair128_t;

static inline pair128_t
WeakHashLen32WithSeeds2(
    uint64_t w, uint64_t x, uint64_t y, uint64_t z, uint64_t a, uint64_t b) {
  pair128_t p;
  uint64_t c;
  a += w;
  b  = Rotate64(b + a + z, 21);
  c  = a;
  a += x;
  a += y;
  b += Rotate64(a, 44);
  p.first  = a + z;
  p.second = b + c;
  return p;
}

static inline pair128_t
WeakHashLen32WithSeeds(const char *s, uint64_t a, uint64_t b)
{
  return WeakHashLen32WithSeeds2(Fetch64(s),
                                 Fetch64(s + 8),
                                 Fetch64(s + 16),
                                 Fetch64(s + 24),
                                 a,
                                 b);
}

static inline uint64_t
HashLen64toN(const char *s, size_t len)
{
  /* For strings over 64 bytes we hash the end first, and then as we
     loop we keep 56 bytes of state: v, w, x, y, and z.*/
  uint64_t x = Fetch64(s + len - 40);
  uint64_t y = Fetch64(s + len - 16) + Fetch64(s + len - 56);
  uint64_t z = Hash128to64(Fetch64(s + len - 48) + len, Fetch64(s + len - 24));
  uint64_t swp;
  pair128_t v = WeakHashLen32WithSeeds(s + len - 64, len, z);
  pair128_t w = WeakHashLen32WithSeeds(s + len - 32, y + k1, x);
  x = x * k1 + Fetch64(s);

  /* Decrease len to the nearest multiple of 64, and operate on 64-byte chunks*/
  len = (len - 1) & ~((size_t) 63);
  do {
    x = Rotate64(x + y + v.first + Fetch64(s + 8), 37) * k1;
    y = Rotate64(y + v.second + Fetch64(s + 48), 42) * k1;
    x ^= w.second;
    y += v.first + Fetch64(s + 40);
    z = Rotate64(z + w.first, 33) * k1;
    v = WeakHashLen32WithSeeds(s, v.second * k1, x + w.first);
    w = WeakHashLen32WithSeeds(s + 32, z + w.second, y + Fetch64(s + 16));
    swp = z; z = x; x = swp;
    s += 64;
    len -= 64;
  } while (len != 0);
  return Hash128to64(Hash128to64(v.first, w.first) + ShiftMix64(y) * k1 + z,
		     Hash128to64(v.second, w.second) + x);
}

static inline uint64_t
CityHash64(const char *s, size_t len)
{
  if (len <= 16)
    return HashLen0to16(s, len);
  if (len <= 32)
    return HashLen17to32(s, len);
  if (len <= 64)
    return HashLen33to64(s, len);
  return HashLen64toN(s, len);
}

uint64_t
kv_hash_cityhash64( const void *p, size_t sz, uint64_t seed )
{
  uint64_t h = CityHash64( p, sz );
  if ( seed != 0 )
    h = Hash128to64( h, seed );
  return h;
}

