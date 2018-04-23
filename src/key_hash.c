#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>

#include <raikv/shm_ht.h>
/* Most of this is derived from Google sources,
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
 * SpookyHash, by Bob Jenkins, C version Andi Kleen
 */

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

#ifdef USE_KV_MURMUR_HASH
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
kv_hash_murmur64( const void *p, size_t sz, uint64_t seed )
{
  return MurmurHash64A( p, sz, seed );
}
#endif /* USE_KV_MURMUR_HASH */

#ifdef USE_KV_XXH_HASH
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
#endif /* USE_KV_XXH_HASH */

#ifdef USE_KV_CITY_HASH
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

/* A subroutine for CityHash128().  Returns a decent 128-bit hash for strings
   of any length representable in signed long.  Based on City and Murmur. */
static void
CityMurmur(const char *s, size_t len, uint64_t *h1, uint64_t *h2)
{
  uint64_t a = *h1;
  uint64_t b = *h2;
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
  *h1 = (a ^ b);
  *h2 = Hash128to64(b, a);
}

uint64_t
kv_hash_citymur64( const void *p, size_t sz, uint64_t seed )
{
  uint64_t h1 = seed, h2 = seed;
  CityMurmur( p, sz, &h1, &h2 );
  return h1;
}

void
kv_hash_citymur128( const void *p, size_t sz, uint64_t *h1, uint64_t *h2 )
{
  CityMurmur( p, sz, h1, h2 );
}

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
#endif /* USE_KV_CITY_HASH */

#ifdef USE_KV_SPOOKY_HASH
// A C version of Bob Jenkins' spooky hash
//   (via Andi Kleen https://github.com/andikleen)
//
// Spooky Hash
// A 128-bit noncryptographic hash, for checksums and table lookup
// By Bob Jenkins. Bob's version was under Public Domain
// The C version is under the BSD license

#define ALLOW_UNALIGNED_READS 1

#define SC_NUMVARS        12
#define SC_BLOCKSIZE    (8 * SC_NUMVARS)
#define SC_BUFSIZE        (2 * SC_BLOCKSIZE)

struct spooky_state
{
    uint64_t m_data[2 * SC_NUMVARS];
    uint64_t m_state[SC_NUMVARS];
    size_t m_length;
    unsigned char m_remainder;
};

// SC_CONST: a constant which:
//  * is not zero
//  * is odd
//  * is a not-very-regular mix of 1's and 0's
//  * does not need any other special mathematical properties
#define SC_CONST _U64( 0xdeadbeef, 0xdeadbeef )

static inline uint64_t rot64(uint64_t x, int k)
{
    return (x << k) | (x >> (64 - k));
}

//
// This is used if the input is 96 bytes long or longer.
//
// The internal state is fully overwritten every 96 bytes.
// Every input bit appears to cause at least 128 bits of entropy
// before 96 other bytes are combined, when run forward or backward
//   For every input bit,
//   Two inputs differing in just that input bit
//   Where "differ" means xor or subtraction
//   And the base value is random
//   When run forward or backwards one Mix
// I tried 3 pairs of each; they all differed by at least 212 bits.
//
static inline void mix
(
    const uint64_t *data,
    uint64_t *s0, uint64_t *s1, uint64_t *s2,  uint64_t *s3,
    uint64_t *s4, uint64_t *s5, uint64_t *s6,  uint64_t *s7,
    uint64_t *s8, uint64_t *s9, uint64_t *s10, uint64_t *s11
)
{
    *s0 += data[0];      *s2 ^= *s10;    *s11 ^= *s0;    *s0 = rot64(*s0, 11);    *s11 += *s1;
    *s1 += data[1];      *s3 ^= *s11;    *s0 ^= *s1;     *s1 = rot64(*s1, 32);    *s0 += *s2;
    *s2 += data[2];      *s4 ^= *s0;     *s1 ^= *s2;     *s2 = rot64(*s2, 43);    *s1 += *s3;
    *s3 += data[3];      *s5 ^= *s1;     *s2 ^= *s3;     *s3 = rot64(*s3, 31);    *s2 += *s4;
    *s4 += data[4];      *s6 ^= *s2;     *s3 ^= *s4;     *s4 = rot64(*s4, 17);    *s3 += *s5;
    *s5 += data[5];      *s7 ^= *s3;     *s4 ^= *s5;     *s5 = rot64(*s5, 28);    *s4 += *s6;
    *s6 += data[6];      *s8 ^= *s4;     *s5 ^= *s6;     *s6 = rot64(*s6, 39);    *s5 += *s7;
    *s7 += data[7];      *s9 ^= *s5;     *s6 ^= *s7;     *s7 = rot64(*s7, 57);    *s6 += *s8;
    *s8 += data[8];      *s10 ^= *s6;    *s7 ^= *s8;     *s8 = rot64(*s8, 55);    *s7 += *s9;
    *s9 += data[9];      *s11 ^= *s7;    *s8 ^= *s9;     *s9 = rot64(*s9, 54);    *s8 += *s10;
    *s10 += data[10];    *s0 ^= *s8;     *s9 ^= *s10;    *s10 = rot64(*s10, 22);    *s9 += *s11;
    *s11 += data[11];    *s1 ^= *s9;     *s10 ^= *s11;   *s11 = rot64(*s11, 46);    *s10 += *s0;
}

//
// Mix all 12 inputs together so that h0, h1 are a hash of them all.
//
// For two inputs differing in just the input bits
// Where "differ" means xor or subtraction
// And the base value is random, or a counting value starting at that bit
// The final result will have each bit of h0, h1 flip
// For every input bit,
// with probability 50 +- .3%
// For every pair of input bits,
// with probability 50 +- 3%
//
// This does not rely on the last Mix() call having already mixed some.
// Two iterations was almost good enough for a 64-bit result, but a
// 128-bit result is reported, so End() does three iterations.
//
static inline void endPartial
(
    uint64_t *h0, uint64_t *h1, uint64_t *h2,  uint64_t *h3,
    uint64_t *h4, uint64_t *h5, uint64_t *h6,  uint64_t *h7,
    uint64_t *h8, uint64_t *h9, uint64_t *h10, uint64_t *h11
)
{
    *h11+= *h1;     *h2 ^= *h11;    *h1 = rot64(*h1, 44);
    *h0 += *h2;     *h3 ^= *h0;     *h2 = rot64(*h2, 15);
    *h1 += *h3;     *h4 ^= *h1;     *h3 = rot64(*h3, 34);
    *h2 += *h4;     *h5 ^= *h2;     *h4 = rot64(*h4, 21);
    *h3 += *h5;     *h6 ^= *h3;     *h5 = rot64(*h5, 38);
    *h4 += *h6;     *h7 ^= *h4;     *h6 = rot64(*h6, 33);
    *h5 += *h7;     *h8 ^= *h5;     *h7 = rot64(*h7, 10);
    *h6 += *h8;     *h9 ^= *h6;     *h8 = rot64(*h8, 13);
    *h7 += *h9;     *h10^= *h7;     *h9 = rot64(*h9, 38);
    *h8 += *h10;    *h11^= *h8;     *h10= rot64(*h10, 53);
    *h9 += *h11;    *h0 ^= *h9;     *h11= rot64(*h11, 42);
    *h10+= *h0;     *h1 ^= *h10;    *h0 = rot64(*h0, 54);
}

static inline void end
(
    uint64_t *h0,    uint64_t *h1,    uint64_t *h2,    uint64_t *h3,
    uint64_t *h4,    uint64_t *h5,    uint64_t *h6,    uint64_t *h7,
    uint64_t *h8,    uint64_t *h9,    uint64_t *h10,    uint64_t *h11
)
{
    endPartial(h0, h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11);
    endPartial(h0, h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11);
    endPartial(h0, h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11);
}

//
// The goal is for each bit of the input to expand into 128 bits of
//   apparent entropy before it is fully overwritten.
// n trials both set and cleared at least m bits of h0 h1 h2 h3
//   n: 2   m: 29
//   n: 3   m: 46
//   n: 4   m: 57
//   n: 5   m: 107
//   n: 6   m: 146
//   n: 7   m: 152
// when run forwards or backwards
// for all 1-bit and 2-bit diffs
// with diffs defined by either xor or subtraction
// with a base of all zeros plus a counter, or plus another bit, or random
//
static inline void short_mix
(
    uint64_t *h0,
    uint64_t *h1,
    uint64_t *h2,
    uint64_t *h3
)
{
    *h2 = rot64(*h2, 50);   *h2 += *h3;  *h0 ^= *h2;
    *h3 = rot64(*h3, 52);   *h3 += *h0;  *h1 ^= *h3;
    *h0 = rot64(*h0, 30);   *h0 += *h1;  *h2 ^= *h0;
    *h1 = rot64(*h1, 41);   *h1 += *h2;  *h3 ^= *h1;
    *h2 = rot64(*h2, 54);   *h2 += *h3;  *h0 ^= *h2;
    *h3 = rot64(*h3, 48);   *h3 += *h0;  *h1 ^= *h3;
    *h0 = rot64(*h0, 38);   *h0 += *h1;  *h2 ^= *h0;
    *h1 = rot64(*h1, 37);   *h1 += *h2;  *h3 ^= *h1;
    *h2 = rot64(*h2, 62);   *h2 += *h3;  *h0 ^= *h2;
    *h3 = rot64(*h3, 34);   *h3 += *h0;  *h1 ^= *h3;
    *h0 = rot64(*h0, 5);    *h0 += *h1;  *h2 ^= *h0;
    *h1 = rot64(*h1, 36);   *h1 += *h2;  *h3 ^= *h1;
}

//
// Mix all 4 inputs together so that h0, h1 are a hash of them all.
//
// For two inputs differing in just the input bits
// Where "differ" means xor or subtraction
// And the base value is random, or a counting value starting at that bit
// The final result will have each bit of h0, h1 flip
// For every input bit,
// with probability 50 +- .3% (it is probably better than that)
// For every pair of input bits,
// with probability 50 +- .75% (the worst case is approximately that)
//
static inline void short_end
(
    uint64_t *h0,
    uint64_t *h1,
    uint64_t *h2,
    uint64_t *h3
)
{
    *h3 ^= *h2;  *h2 = rot64(*h2, 15);  *h3 += *h2;
    *h0 ^= *h3;  *h3 = rot64(*h3, 52);  *h0 += *h3;
    *h1 ^= *h0;  *h0 = rot64(*h0, 26);  *h1 += *h0;
    *h2 ^= *h1;  *h1 = rot64(*h1, 51);  *h2 += *h1;
    *h3 ^= *h2;  *h2 = rot64(*h2, 28);  *h3 += *h2;
    *h0 ^= *h3;  *h3 = rot64(*h3, 9);   *h0 += *h3;
    *h1 ^= *h0;  *h0 = rot64(*h0, 47);  *h1 += *h0;
    *h2 ^= *h1;  *h1 = rot64(*h1, 54);  *h2 += *h1;
    *h3 ^= *h2;  *h2 = rot64(*h2, 32);  *h3 += *h2;
    *h0 ^= *h3;  *h3 = rot64(*h3, 25);  *h0 += *h3;
    *h1 ^= *h0;  *h0 = rot64(*h0, 63);  *h1 += *h0;
}

static void spooky_shorthash
(
    const void *message,
    size_t length,
    uint64_t *hash1,
    uint64_t *hash2
)
{
    uint64_t buf[2 * SC_NUMVARS];
    union
    {
        const uint8_t *p8;
        uint32_t *p32;
        uint64_t *p64;
        size_t i;
    } u;
    size_t remainder;
    uint64_t a, b, c, d;
    u.p8 = (const uint8_t *)message;

    if (!ALLOW_UNALIGNED_READS && (u.i & 0x7))
    {
        memcpy(buf, message, length);
        u.p64 = buf;
    }

    remainder = length % 32;
    a = *hash1;
    b = *hash2;
    c = SC_CONST;
    d = SC_CONST;

    if (length > 15)
    {
        const uint64_t *endp = u.p64 + (length/32)*4;

        // handle all complete sets of 32 bytes
        for (; u.p64 < endp; u.p64 += 4)
        {
            c += u.p64[0];
            d += u.p64[1];
            short_mix(&a, &b, &c, &d);
            a += u.p64[2];
            b += u.p64[3];
        }

        // Handle the case of 16+ remaining bytes.
        if (remainder >= 16)
        {
            c += u.p64[0];
            d += u.p64[1];
            short_mix(&a, &b, &c, &d);
            u.p64 += 2;
            remainder -= 16;
        }
    }

    // Handle the last 0..15 bytes, and its length
    d += ((uint64_t)length) << 56;
    switch (remainder)
    {
        case 15:
            d += ((uint64_t)u.p8[14]) << 48;
        case 14:
            d += ((uint64_t)u.p8[13]) << 40;
        case 13:
            d += ((uint64_t)u.p8[12]) << 32;
        case 12:
            d += u.p32[2];
            c += u.p64[0];
            break;
        case 11:
            d += ((uint64_t)u.p8[10]) << 16;
        case 10:
            d += ((uint64_t)u.p8[9]) << 8;
        case 9:
            d += (uint64_t)u.p8[8];
        case 8:
            c += u.p64[0];
            break;
        case 7:
            c += ((uint64_t)u.p8[6]) << 48;
        case 6:
            c += ((uint64_t)u.p8[5]) << 40;
        case 5:
            c += ((uint64_t)u.p8[4]) << 32;
        case 4:
            c += u.p32[0];
            break;
        case 3:
            c += ((uint64_t)u.p8[2]) << 16;
        case 2:
            c += ((uint64_t)u.p8[1]) << 8;
        case 1:
            c += (uint64_t)u.p8[0];
            break;
        case 0:
            c += SC_CONST;
            d += SC_CONST;
    }
    short_end(&a, &b, &c, &d);
    *hash1 = a;
    *hash2 = b;
}

static void spooky_hash128
(
    const void *message,
    size_t length,
    uint64_t *hash1,
    uint64_t *hash2
)
{
    uint64_t h0, h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11;
    uint64_t buf[SC_NUMVARS];
    uint64_t *endp;
    union
    {
        const uint8_t *p8;
        uint64_t *p64;
        uintptr_t i;
    } u;
    size_t remainder;

    if (length < SC_BUFSIZE)
    {
        spooky_shorthash(message, length, hash1, hash2);
        return;
    }

    h0 = h3 = h6 = h9  = *hash1;
    h1 = h4 = h7 = h10 = *hash2;
    h2 = h5 = h8 = h11 = SC_CONST;

    u.p8 = (const uint8_t *)message;
    endp = u.p64 + (length/SC_BLOCKSIZE)*SC_NUMVARS;

    // handle all whole blocks of SC_BLOCKSIZE bytes
    if (ALLOW_UNALIGNED_READS || (u.i & 0x7) == 0)
    {
        while (u.p64 < endp)
        {
            mix(u.p64, &h0, &h1, &h2, &h3, &h4, &h5, &h6, &h7, &h8, &h9, &h10, &h11);
            u.p64 += SC_NUMVARS;
        }
    }
    else
    {
        while (u.p64 < endp)
        {
            memcpy(buf, u.p64, SC_BLOCKSIZE);
            mix(buf, &h0, &h1, &h2, &h3, &h4, &h5, &h6, &h7, &h8, &h9, &h10, &h11);
            u.p64 += SC_NUMVARS;
        }
    }

    // handle the last partial block of SC_BLOCKSIZE bytes
    remainder = (length - ((const uint8_t *)endp-(const uint8_t *)message));
    memcpy(buf, endp, remainder);
    memset(((uint8_t *)buf)+remainder, 0, SC_BLOCKSIZE-remainder);
    ((uint8_t *)buf)[SC_BLOCKSIZE-1] = remainder;

    // do some final mixing
    end(&h0, &h1, &h2, &h3, &h4, &h5, &h6, &h7, &h8, &h9, &h10, &h11);
    *hash1 = h0;
    *hash2 = h1;
}

uint64_t
kv_hash_spooky64( const void *p, size_t sz, uint64_t seed )
{
  uint64_t h1 = seed, h2 = seed;
  spooky_hash128( p, sz, &h1, &h2 );
  return h1;
}

void
kv_hash_spooky128( const void *p, size_t sz, uint64_t *h1, uint64_t *h2 )
{
  spooky_hash128( p, sz, h1, h2 );
}
#endif /* USE_KV_SPOOKY_HASH */

#ifdef USE_KV_AES_HASH
#include <immintrin.h>
#include <wmmintrin.h>

/* This is a simple mix of city and aes. When used with kv hashing, it has
 * collision properties close to cityhash but has more hash bits and the
 * speed is similar

cityhash w/ints @ 90%
90% 2+4: 0:55987(49.0), 1:16308(63.3), 2:9342(71.5), 3:7306(77.8),
114.7ns  4:9445(86.1), 5:6041(91.4), 6:4987(95.8), 7:4840 max=7

cityhash w/hungarian wiki tokens @ 80%
80% 2+4: 0:18425429(56.5), 1:5254684(72.7), 2:2662151(80.8), 3:1703292(86.1),
         4:1947581(92.1), 5:1123486(95.5), 6:807750(98.0), 7:659065 max=7

aes w/ints @ 90%
90% 2+4: 0:55655(48.7), 1:16289(63.0), 2:9551(71.3), 3:7452(77.8),
112.3ns  4:9461(86.1), 5:5922(91.3), 6:5027(95.7), 7:4899 max=7

aes w/hungarian wiki tokens @ 80%
80% 2+4: 0:18422163(56.5), 1:5258965(72.7), 2:2664351(80.9), 3:1702533(86.1),
         4:1946539(92.1), 5:1121497(95.5), 6:806933(98.0), 7:660457 max=7
*/
#define ROT64( x, y ) ( ( x << y ) | ( x >> ( 64 - y ) ) )
#define KCONST _U64( 0x9ae16a3b, 0x2f90404f )
static void
hash_aes128( const void *p, const size_t sz, uint64_t *h1, uint64_t *h2 )
{
  __m128i m, digest = _mm_set_epi64x( *h1, *h2 );
  const uint8_t *e = &((const uint8_t *) p)[ sz ];

  for ( const uint8_t *b = (const uint8_t *) p; b < e; b += 16 ) {
    uint64_t lo = 0, hi = 0, mul, c, d;
    switch ( e - b ) {
      default:
        hi = *(uint64_t *) (void *) &b[ 8 ];
        lo = *(uint64_t *) (void *) &b[ 0 ];
        break;
      case 15: hi |= ( (uint64_t) b[ 14 ] ) << 56;
      case 14: lo |= ( (uint64_t) b[ 13 ] ) << 48;
      case 13: hi |= ( (uint64_t) b[ 12 ] ) << 48;
      case 12:
        lo |= ( (uint64_t) *(const uint32_t *) (void *) &b[ 8 ] ) << 16;
        hi |= ( (uint64_t) *(const uint32_t *) (void *) &b[ 4 ] ) << 16;
        lo |= (uint64_t) *(const uint16_t *) (void *) &b[ 2 ];
        hi |= (uint64_t) *(const uint16_t *) (void *) &b[ 0 ];
        break;
      case 11: hi |= ( (uint64_t) b[ 10 ] ) << 40;
      case 10: lo |= ( (uint64_t) b[ 9 ] ) << 32;
      case 9: hi |= ( (uint64_t) b[ 8 ] ) << 32;
      case 8:
        lo |= (uint64_t) *(const uint32_t *) (void *) &b[ 4 ];
        hi |= (uint64_t) *(const uint32_t *) (void *) &b[ 0 ];
        break;
      case 7: hi |= ( (uint64_t) b[ 6 ] ) << 24;
      case 6: lo |= ( (uint64_t) b[ 5 ] ) << 16;
      case 5: hi |= ( (uint64_t) b[ 4 ] ) << 16;
      case 4:
        lo |= (uint64_t) *(const uint16_t *) (void *) &b[ 2 ];
        hi |= (uint64_t) *(const uint16_t *) (void *) &b[ 0 ];
        break;
      case 3: hi |= ( (uint64_t) b[ 2 ] ) << 8;
      case 2: lo |= ( (uint64_t) b[ 1 ] );
      case 1: hi |= ( (uint64_t) b[ 0 ] );
        break;
    }
    mul = KCONST + ( e - b ) * 2;
    lo += KCONST;
    c = ROT64( hi, 37 ) * mul + lo;
    d = ( ROT64( lo, 25 ) + hi ) * mul;
    m = _mm_set_epi64x( c, d );
    m = _mm_xor_si128( m, digest );
    m = _mm_aesenc_si128( m, digest );
    digest = _mm_aesenclast_si128( m, digest );
  }
  *h1 = _mm_extract_epi64( digest, 0 );
  *h2 = _mm_extract_epi64( digest, 1 );
}

uint64_t
kv_hash_aes64( const void *p, size_t sz, uint64_t seed )
{
  uint64_t h1 = seed, h2 = seed;
  hash_aes128( p, sz, &h1, &h2 );
  return h1;
}

void
kv_hash_aes128( const void *p, size_t sz, uint64_t *h1, uint64_t *h2 )
{
  hash_aes128( p, sz, h1, h2 );
}
#endif /* USE_KV_AES_HASH */

