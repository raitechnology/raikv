#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <immintrin.h>
#include <wmmintrin.h>

#include <raikv/shm_ht.h>
/* Permission is hereby granted, free of charge, to any person obtaining a copy
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
 * CityHash, by Geoff Pike and Jyrki Alakuijala
 * SpookyHash, by Bob Jenkins, C version Andi Kleen
 * aes128, falkhash derivative, by gamozolabs
 */

uint32_t
kv_hash_uint( uint32_t i )
{
  return _mm_crc32_u32( 0, i );
}

uint32_t
kv_hash_uint2( uint32_t r, uint32_t i )
{
  return _mm_crc32_u32( r, i );
}

static inline uint32_t rotl32( uint32_t x,  int k ) {
  return (x << k) | (x >> (32 - k));
}
/* lossless hash, unique hash for every input, useful for hashing counters */
uint32_t
kv_ll_hash_uint( uint32_t i )
{
  return ( i >> 10 ) ^ kv_hash_uint( i & 1023 ); /* cycle period of 1025 */
}

static inline uint32_t
trail_crc_c( uint32_t r,  const uint8_t *s,  size_t sz )
{
  if ( ( sz & 4 ) != 0 ) {
    r = _mm_crc32_u32( r, *(uint32_t *) s ); s+=4;
  }
  if ( ( sz & 2 ) != 0 ) {
    r = _mm_crc32_u16( r, *(uint16_t *) s ); s+=2;
  }
  if ( ( sz & 1 ) != 0 ) {
    r = _mm_crc32_u8( r, *(uint8_t *) s );
  }
  return r;
}

uint32_t
kv_crc_c( const void *p,  size_t sz,  uint32_t seed )
{
  const uint8_t * s = (uint8_t *) p;
  uint64_t        r = seed;

  while ( sz >= 8 ) {
    r = _mm_crc32_u64( r, *(uint64_t *) s ); s+=8; sz-=8;
  }
  return trail_crc_c( (uint32_t) r, s, sz );
}

void
kv_crc_c_2_diff( const void *p,   size_t sz,   uint32_t *seed,
                 const void *p2,  size_t sz2,  uint32_t *seed2 )
{
  const uint8_t * s  = (uint8_t *) p,
                * s2 = (uint8_t *) p2;
  uint64_t        r  = *seed,
                  r2 = *seed2;
  for (;;) {
    if ( sz >= 8 ) {
      r  = _mm_crc32_u64( r , *(uint64_t *) s  ); s+=8;  sz-=8;
    }
    if ( sz2 >= 8 ) {
      r2 = _mm_crc32_u64( r2, *(uint64_t *) s2 ); s2+=8; sz2-=8;
    }
    else if ( sz < 8 )
      break;
  }
  *seed  = trail_crc_c( (uint32_t) r,  s,  sz );
  *seed2 = trail_crc_c( (uint32_t) r2, s2, sz2 );
}

void
kv_crc_c_4_diff( const void *p,   size_t sz,   uint32_t *seed,
                 const void *p2,  size_t sz2,  uint32_t *seed2,
                 const void *p3,  size_t sz3,  uint32_t *seed3,
                 const void *p4,  size_t sz4,  uint32_t *seed4 )
{
  const uint8_t * s  = (uint8_t *) p,
                * s2 = (uint8_t *) p2,
                * s3 = (uint8_t *) p3,
                * s4 = (uint8_t *) p4;
  uint64_t        r  = *seed,
                  r2 = *seed2,
                  r3 = *seed3,
                  r4 = *seed4;
  size_t a;
  do {
    a = 0;
    if ( sz >= 8 ) {
      r  = _mm_crc32_u64( r , *(uint64_t *) s  ); s+=8;  sz-=8;
      a |= sz;
    }
    if ( sz2 >= 8 ) {
      r2 = _mm_crc32_u64( r2, *(uint64_t *) s2 ); s2+=8; sz2-=8;
      a |= sz2;
    }
    if ( sz3 >= 8 ) {
      r3 = _mm_crc32_u64( r3, *(uint64_t *) s3 ); s3+=8; sz3-=8;
      a |= sz3;
    }
    if ( sz4 >= 8 ) {
      r4 = _mm_crc32_u64( r4, *(uint64_t *) s4 ); s4+=8; sz4-=8;
      a |= sz4;
    }
  } while ( a >= 8 );
  *seed  = trail_crc_c( (uint32_t) r,  s,  sz );
  *seed2 = trail_crc_c( (uint32_t) r2, s2, sz2 );
  *seed3 = trail_crc_c( (uint32_t) r3, s3, sz3 );
  *seed4 = trail_crc_c( (uint32_t) r4, s4, sz4 );
}

void
kv_crc_c_array( const void **p,   size_t *psz,   uint32_t *seed,
                size_t count )
{
  while ( count >= 4 ) {
    kv_crc_c_4_diff( p[ 0 ], psz[ 0 ], &seed[ 0 ],
                     p[ 1 ], psz[ 1 ], &seed[ 1 ],
                     p[ 2 ], psz[ 2 ], &seed[ 2 ],
                     p[ 3 ], psz[ 3 ], &seed[ 3 ] );
    p += 4; psz += 4; seed += 4;
    count -= 4;
  }
  if ( ( count & 2 ) != 0 ) {
    kv_crc_c_2_diff( p[ 0 ], psz[ 0 ], &seed[ 0 ],
                     p[ 1 ], psz[ 1 ], &seed[ 1 ] );
    p += 2; psz += 2; seed += 2;
  }
  if ( ( count & 1 ) != 0 ) {
    seed[ 0 ] = kv_crc_c( p[ 0 ], psz[ 0 ], seed[ 0 ] );
  }
}

static int
do_crc_c_key( uint32_t *r,  const uint8_t *x,  size_t *sz )
{
  if ( *sz >= 8 ) {
    *r   = _mm_crc32_u64( *r , *(uint64_t *) x );
    *sz -= 8;
    return *sz > 0;
  }
  *r  = trail_crc_c( (uint32_t) *r, x, *sz );
  *sz = 0;
  return 0;
}

static void
kv_crc_c_2_key_sz( const void *p,  size_t sz,   uint32_t *seed,
                                   size_t sz2,  uint32_t *seed2 )
{
  for ( const uint8_t * x = (uint8_t *) p; ; x = &x[ 8 ] ) {
    int b = 0;
    if ( sz  > 0 ) b |= do_crc_c_key( seed , x, &sz );
    if ( sz2 > 0 ) b |= do_crc_c_key( seed2, x, &sz2 );
    if ( ! b ) return;
  }
}

static void
kv_crc_c_4_key_sz( const void *p,  size_t sz,   uint32_t *seed,
                                   size_t sz2,  uint32_t *seed2,
                                   size_t sz3,  uint32_t *seed3,
                                   size_t sz4,  uint32_t *seed4 )
{
  for ( const uint8_t * x = (uint8_t *) p; ; x = &x[ 8 ] ) {
    int b = 0;
    if ( sz  > 0 ) b |= do_crc_c_key( seed , x, &sz );
    if ( sz2 > 0 ) b |= do_crc_c_key( seed2, x, &sz2 );
    if ( sz3 > 0 ) b |= do_crc_c_key( seed3, x, &sz3 );
    if ( sz4 > 0 ) b |= do_crc_c_key( seed4, x, &sz4 );
    if ( ! b ) return;
  }
}

void
kv_crc_c_key_array( const void *p,   size_t *psz,   uint32_t *seed,  size_t count )
{
  while ( count >= 4 ) {
    kv_crc_c_4_key_sz( p, psz[ 0 ], &seed[ 0 ],
                          psz[ 1 ], &seed[ 1 ],
                          psz[ 2 ], &seed[ 2 ],
                          psz[ 3 ], &seed[ 3 ] );
    psz += 4; seed += 4; count -= 4;
  }
  if ( ( count & 2 ) != 0 ) {
    kv_crc_c_2_key_sz( p, psz[ 0 ], &seed[ 0 ],
                          psz[ 1 ], &seed[ 1 ] );
    psz += 2; seed += 2;
  }
  if ( ( count & 1 ) != 0 ) {
    seed[ 0 ] = kv_crc_c( p, psz[ 0 ], seed[ 0 ] );
  }
}

#ifdef USE_KV_MURMUR_HASH
static inline uint64_t
MurmurHash64A ( const void * key, size_t len, uint64_t seed )
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
    case 7: h ^= ((uint64_t) data2[ 6 ] ) << 48; /* FALLTHRU */
    case 6: h ^= ((uint64_t) data2[ 5 ] ) << 40; /* FALLTHRU */
    case 5: h ^= ((uint64_t) data2[ 4 ] ) << 32; /* FALLTHRU */
    case 4: h ^= ((uint64_t) data2[ 3 ] ) << 24; /* FALLTHRU */
    case 3: h ^= ((uint64_t) data2[ 2 ] ) << 16; /* FALLTHRU */
    case 2: h ^= ((uint64_t) data2[ 1 ] ) << 8; /* FALLTHRU */
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


static inline uint64_t
MH_rotl64(uint64_t x, int r) { return ((x << r) | (x >> (64 - r))); }

static inline uint64_t
MH_fmix64 ( uint64_t k )
{
  k ^= k >> 33;
  k *= _U64(0xff51afd7,0xed558ccd);
  k ^= k >> 33;
  k *= _U64(0xc4ceb9fe,0x1a85ec53);
  k ^= k >> 33;

  return k;
}

static inline void
MurmurHash3_x64_128 ( const void * key, const size_t len, uint64_t *x1,
                      uint64_t *x2 )
{
  const size_t nblocks = len / 16;
  const uint8_t * tail = &((const uint8_t*) key)[ nblocks*16 ];

  uint64_t h1 = *x1;
  uint64_t h2 = *x2;
  uint64_t k1, k2;

  const uint64_t c1 = _U64(0x87c37b91,0x114253d5);
  const uint64_t c2 = _U64(0x4cf5ad43,0x2745937f);

  /*----------
  /  body   */
  const uint64_t * blocks = (const uint64_t *) key;
  size_t i;

  for(i = 0; i < nblocks; i++)
  {
    k1 = blocks[i*2+0];
    k2 = blocks[i*2+1];

    k1 *= c1; k1  = MH_rotl64(k1,31); k1 *= c2; h1 ^= k1;

    h1 = MH_rotl64(h1,27); h1 += h2; h1 = h1*5+0x52dce729;

    k2 *= c2; k2  = MH_rotl64(k2,33); k2 *= c1; h2 ^= k2;

    h2 = MH_rotl64(h2,31); h2 += h1; h2 = h2*5+0x38495ab5;
  }

  /*----------
  /  tail   */
  k1 = 0;
  k2 = 0;

  switch(len & 15)
  {
  case 15: k2 ^= ((uint64_t)tail[14]) << 48; /* FALLTHRU */
  case 14: k2 ^= ((uint64_t)tail[13]) << 40; /* FALLTHRU */
  case 13: k2 ^= ((uint64_t)tail[12]) << 32; /* FALLTHRU */
  case 12: k2 ^= ((uint64_t)tail[11]) << 24; /* FALLTHRU */
  case 11: k2 ^= ((uint64_t)tail[10]) << 16; /* FALLTHRU */
  case 10: k2 ^= ((uint64_t)tail[ 9]) << 8; /* FALLTHRU */
  case  9: k2 ^= ((uint64_t)tail[ 8]) << 0;
           k2 *= c2; k2  = MH_rotl64(k2,33); k2 *= c1; h2 ^= k2; /* FALLTHRU */

  case  8: k1 ^= ((uint64_t)tail[ 7]) << 56; /* FALLTHRU */
  case  7: k1 ^= ((uint64_t)tail[ 6]) << 48; /* FALLTHRU */
  case  6: k1 ^= ((uint64_t)tail[ 5]) << 40; /* FALLTHRU */
  case  5: k1 ^= ((uint64_t)tail[ 4]) << 32; /* FALLTHRU */
  case  4: k1 ^= ((uint64_t)tail[ 3]) << 24; /* FALLTHRU */
  case  3: k1 ^= ((uint64_t)tail[ 2]) << 16; /* FALLTHRU */
  case  2: k1 ^= ((uint64_t)tail[ 1]) << 8; /* FALLTHRU */
  case  1: k1 ^= ((uint64_t)tail[ 0]) << 0;
           k1 *= c1; k1  = MH_rotl64(k1,31); k1 *= c2; h1 ^= k1;
  };

  /*----------
  /  finalization */
  h1 ^= len; h2 ^= len;

  h1 += h2;
  h2 += h1;

  h1 = MH_fmix64(h1);
  h2 = MH_fmix64(h2);

  h1 += h2;
  h2 += h1;

  *x1 = h1;
  *x2 = h2;
}

void
kv_hash_murmur128( const void *p, size_t sz, uint64_t *h1, uint64_t *h2 )
{
  MurmurHash3_x64_128 ( p, sz, h1, h2 );
}
#endif /* USE_KV_MURMUR_HASH */

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
/* A C version of Bob Jenkins' spooky hash
//   (via Andi Kleen https://github.com/andikleen)
//
// Spooky Hash
// A 128-bit noncryptographic hash, for checksums and table lookup
// By Bob Jenkins. Bob's version was under Public Domain
// The C version is under the BSD license
*/
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

/* SC_CONST: a constant which:
//  * is not zero
//  * is odd
//  * is a not-very-regular mix of 1's and 0's
//  * does not need any other special mathematical properties
*/
#define SC_CONST _U64( 0xdeadbeef, 0xdeadbeef )

static inline uint64_t rot64(uint64_t x, int k)
{
    return (x << k) | (x >> (64 - k));
}

/*
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
*/
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

/*
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
*/
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

/*
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
*/
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

/*
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
*/
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

        /* handle all complete sets of 32 bytes */
        for (; u.p64 < endp; u.p64 += 4)
        {
            c += u.p64[0];
            d += u.p64[1];
            short_mix(&a, &b, &c, &d);
            a += u.p64[2];
            b += u.p64[3];
        }

        /* Handle the case of 16+ remaining bytes. */
        if (remainder >= 16)
        {
            c += u.p64[0];
            d += u.p64[1];
            short_mix(&a, &b, &c, &d);
            u.p64 += 2;
            remainder -= 16;
        }
    }

    /* Handle the last 0..15 bytes, and its length */
    d += ((uint64_t)length) << 56;
    switch (remainder)
    {
        case 15:
            d += ((uint64_t)u.p8[14]) << 48; /* FALLTHRU */
        case 14:
            d += ((uint64_t)u.p8[13]) << 40; /* FALLTHRU */
        case 13:
            d += ((uint64_t)u.p8[12]) << 32; /* FALLTHRU */
        case 12:
            d += u.p32[2];
            c += u.p64[0];
            break;
        case 11:
            d += ((uint64_t)u.p8[10]) << 16; /* FALLTHRU */
        case 10:
            d += ((uint64_t)u.p8[9]) << 8; /* FALLTHRU */
        case 9:
            d += (uint64_t)u.p8[8]; /* FALLTHRU */
        case 8:
            c += u.p64[0];
            break;
        case 7:
            c += ((uint64_t)u.p8[6]) << 48; /* FALLTHRU */
        case 6:
            c += ((uint64_t)u.p8[5]) << 40; /* FALLTHRU */
        case 5:
            c += ((uint64_t)u.p8[4]) << 32; /* FALLTHRU */
        case 4:
            c += u.p32[0];
            break;
        case 3:
            c += ((uint64_t)u.p8[2]) << 16; /* FALLTHRU */
        case 2:
            c += ((uint64_t)u.p8[1]) << 8; /* FALLTHRU */
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

    /* handle all whole blocks of SC_BLOCKSIZE bytes */
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

    /* handle the last partial block of SC_BLOCKSIZE bytes */
    remainder = (length - ((const uint8_t *)endp-(const uint8_t *)message));
    memcpy(buf, endp, remainder);
    memset(((uint8_t *)buf)+remainder, 0, SC_BLOCKSIZE-remainder);
    ((uint8_t *)buf)[SC_BLOCKSIZE-1] = (uint8_t) remainder;

    /* do some final mixing */
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

/* based on falkhash (https://github.com/gamozolabs/falkhash)
 * 3 rounds of AES, similar to go internal:
 * https://github.com/golang/go/blob/master/src/runtime/asm_amd64.s#L886 */
void
kv_hash_aes128( const void *p, size_t sz, uint64_t *x1, uint64_t *x2 )
{
  __m128i data[ 8 ], hash, seed = _mm_set_epi64x( *x1 + sz, *x2 );
  size_t i = 0;

  const uint8_t *e = &((const uint8_t *) p)[ sz ];
  const uint8_t *b = (const uint8_t *) p;
  hash = seed;
  if ( b != e ) {
    do {
      uint64_t lo = 0, hi = 0;
      switch ( e - b ) {
	default:
	  hi = *(uint64_t *) (void *) &b[ 8 ];
	  lo = *(uint64_t *) (void *) &b[ 0 ];
	  break;
	case 15: hi |= ( (uint64_t) b[ 14 ] ) << 56; /* FALLTHRU */
	case 14: lo |= ( (uint64_t) b[ 13 ] ) << 48; /* FALLTHRU */
	case 13: hi |= ( (uint64_t) b[ 12 ] ) << 48; /* FALLTHRU */
	case 12:
	  lo |= ( (uint64_t) *(const uint32_t *) (void *) &b[ 8 ] ) << 16;
	  hi |= ( (uint64_t) *(const uint32_t *) (void *) &b[ 4 ] ) << 16;
	  lo |= (uint64_t) *(const uint16_t *) (void *) &b[ 2 ];
	  hi |= (uint64_t) *(const uint16_t *) (void *) &b[ 0 ];
	  break;
	case 11: hi |= ( (uint64_t) b[ 10 ] ) << 40; /* FALLTHRU */
	case 10: lo |= ( (uint64_t) b[ 9 ] ) << 32; /* FALLTHRU */
	case 9: hi |= ( (uint64_t) b[ 8 ] ) << 32; /* FALLTHRU */
	case 8:
	  lo |= (uint64_t) *(const uint32_t *) (void *) &b[ 4 ];
	  hi |= (uint64_t) *(const uint32_t *) (void *) &b[ 0 ];
	  break;
	case 7: hi |= ( (uint64_t) b[ 6 ] ) << 24; /* FALLTHRU */
	case 6: lo |= ( (uint64_t) b[ 5 ] ) << 16; /* FALLTHRU */
	case 5: hi |= ( (uint64_t) b[ 4 ] ) << 16; /* FALLTHRU */
	case 4:
	  lo |= (uint64_t) *(const uint16_t *) (void *) &b[ 2 ];
	  hi |= (uint64_t) *(const uint16_t *) (void *) &b[ 0 ];
	  break;
	case 3: hi |= ( (uint64_t) b[ 2 ] ) << 8; /* FALLTHRU */
	case 2: lo |= ( (uint64_t) b[ 1 ] ); /* FALLTHRU */
	case 1: hi |= ( (uint64_t) b[ 0 ] );
	  break;
      }
      data[ i ] = _mm_xor_si128( _mm_set_epi64x( hi, lo ), seed );
      if ( ++i == 8 ) {
	while ( i > 1 ) {
	  data[ 0 ] = _mm_aesenc_si128( data[ 0 ], data[ --i ] );
	  data[ 0 ] = _mm_aesenc_si128( data[ 0 ], data[ i ] );
        }
	data[ 0 ] = _mm_aesenc_si128( data[ 0 ], seed );
	hash = _mm_aesenc_si128( hash, data[ 0 ] );
	i = 0;
      }
      b += 16;
    } while ( b < e );
    while ( i > 1 ) {
      data[ 0 ] = _mm_aesenc_si128( data[ 0 ], data[ --i ] );
      data[ 0 ] = _mm_aesenc_si128( data[ 0 ], data[ i ] );
    }
  }
  else {
    data[ 0 ] = seed;
  }
  hash = _mm_aesenc_si128( hash, data[ 0 ] );
  hash = _mm_aesenc_si128( hash, seed );
  hash = _mm_aesenc_si128( hash, seed );
  hash = _mm_aesenc_si128( hash, seed );
  *x1 = _mm_extract_epi64( hash, 0 );
  *x2 = _mm_extract_epi64( hash, 1 );
}

uint64_t
kv_hash_aes64( const void *p, size_t sz, uint64_t seed )
{
  uint64_t h1 = seed, h2 = seed;
  kv_hash_aes128( p, sz, &h1, &h2 );
  return h1;
}

#endif /* USE_KV_AES_HASH */

#ifdef USE_KV_MEOW_HASH
/* based on meow_hash v0.4 (https://github.com/cmuratori/meow_hash)
 * Same algo, except that it uses 128 bit seed and mixes the seed into the
 * initial state and uses 2 AESDEC rounds instead of 1.
 */
static inline __m128i
Meow_AESDECx2( __m128i R, __m128i S )
{
  R = _mm_aesdec_si128( R, S );
  R = _mm_aesdec_si128( R, S );
  return R;
}

static inline __m128i
Meow_AESDEC_Memx2( __m128i R, const uint8_t *S )
{
  __m128i Q = _mm_loadu_si128( (__m128i *) S );
  return Meow_AESDECx2( R, Q );
}

#ifdef _MSC_VER
#define ALIGN_ARR( A ) __declspec(align(64)) A
#else
#define ALIGN_ARR( A ) A __attribute__((__aligned__(64)))
#endif

static const uint8_t ALIGN_ARR( MeowShiftAdjust[] ) =
{0,1,2,3,         4,5,6,7,
 8,9,10,11,       12,13,14,15,
 128,128,128,128, 128,128,128,128,
 128,128,128,128, 128,128,128,0};
static const uint8_t ALIGN_ARR( MeowMaskLen[] ) =
{255,255,255,255, 255,255,255,255,
 255,255,255,255, 255,255,255,255,
 0,0,0,0,         0,0,0,0,
 0,0,0,0,         0,0,0,0};
static const uint8_t ALIGN_ARR( MeowS0Init[] ) =
{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,15};
static const uint8_t ALIGN_ARR( MeowS1Init[] ) =
{16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31};
static const uint8_t ALIGN_ARR( MeowS2Init[] ) =
{32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47};
static const uint8_t ALIGN_ARR( MeowS3Init[] ) =
{48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63};

/* Load trailing bytes into xmm reg,
 * the Len8 is size & 7,
 * the Len128 is size & 0x30, which is ( 32 | 16 ) */
static inline __m128i
Meow_AESDECx2_Partial( __m128i R, const uint8_t *S, uint32_t Len8,
                       uint32_t Len128 )
{
  __m128i         Partial;
  const uint8_t * Overhang = S + Len128;
  uint32_t        Align    = ((size_t)(intptr_t) Overhang) & 15;

  if ( Align != 0 ) { /* presumes page size is >= 4k */
    uint32_t End = ((size_t)(intptr_t) Overhang) & (size_t) (4096 - 1);

    if ( End <= (4096 - 16) || (End + Len8) > 4096 )
      Align = 0;

    Partial = _mm_shuffle_epi8(
      _mm_loadu_si128(
        (__m128i *)( Overhang - Align ) ),
          _mm_loadu_si128( (__m128i *) &MeowShiftAdjust[ Align ] ) );

    Partial =
      _mm_and_si128( Partial,
        _mm_loadu_si128( (__m128i *) &MeowMaskLen[ 16 - Len8 ] ) );
  }
  else {
    Partial =
      _mm_and_si128( *(__m128i *) Overhang,
        _mm_loadu_si128( (__m128i *) &MeowMaskLen[ 16 - Len8 ] ) );
  }
  return Meow_AESDECx2( R, Partial );
}

#define Declare_Meow( S0, S1, S2, S3 ) \
  __m128i S0 = *(__m128i *) MeowS0Init; \
  __m128i S1 = *(__m128i *) MeowS1Init; \
  __m128i S2 = *(__m128i *) MeowS2Init; \
  __m128i S3 = *(__m128i *) MeowS3Init

#define Mix_Meow( S3, S2, S1, S0, Mixer ) do { \
  S3 = _mm_aesdec_si128( S3, Mixer ); \
  S2 = _mm_aesdec_si128( S2, Mixer ); \
  S1 = _mm_aesdec_si128( S1, Mixer ); \
  S0 = _mm_aesdec_si128( S0, Mixer ); \
} while ( 0 )

#define Mix_Meow1( S1, S0, Mixer ) do { \
  S1 = _mm_aesdec_si128( S1, Mixer ); \
  S0 = _mm_aesdec_si128( S0, Mixer ); \
} while ( 0 )

#define Compress_Meow( S2, S3, S0, S1 ) do { \
  S2 = _mm_aesdec_si128( S2, S3 ); \
  S0 = _mm_aesdec_si128( S0, S1 ); \
} while( 0 )

#define Compress_Meow2( S2, S3, S0, S1, S4, M ) do { \
  S2 = _mm_aesdec_si128( S2, S3 ); \
  S0 = _mm_aesdec_si128( S0, S1 ); \
  S4 = _mm_aesdec_si128( S4, M ); \
} while( 0 )

#define Xor_Meow( S0, S1, S2, S3, Mixer ) do { \
  S0 = _mm_xor_si128( S0, Mixer ); \
  S1 = _mm_xor_si128( S1, Mixer ); \
  S2 = _mm_xor_si128( S2, Mixer ); \
  S3 = _mm_xor_si128( S3, Mixer ); \
} while( 0 )

#define Meow_Loop64( S0, S1, S2, S3, p, sz ) do { \
  const uint8_t * Source = (const uint8_t *) p; \
  size_t          Len    = sz; \
 \
  do { \
    S0 = Meow_AESDEC_Memx2( S0, Source ); \
    S1 = Meow_AESDEC_Memx2( S1, Source + 16 ); \
    S2 = Meow_AESDEC_Memx2( S2, Source + 32 ); \
    S3 = Meow_AESDEC_Memx2( S3, Source + 48 ); \
 \
    Len    -= 64; \
    Source += 64; \
  } while ( Len > 0 ); \
} while( 0 )

#define Meow_Loop_Trail( S0, S1, S2, S3, Source, sz ) do { \
  const uint32_t Len8   = (uint32_t) sz & 15; \
  const uint32_t Len128 = (uint32_t) sz & 48; \
  if ( Len8 ) \
    S3 = Meow_AESDECx2_Partial( S3, Source, Len8, Len128 ); \
  switch ( Len128 ) { \
    case 48: S2 = Meow_AESDEC_Memx2( S2, Source + 32 ); /* FALLTHRU */ \
    case 32: S1 = Meow_AESDEC_Memx2( S1, Source + 16 ); /* FALLTHRU */ \
    case 16: S0 = Meow_AESDEC_Memx2( S0, Source ); break; \
  } \
} while( 0 )

#define Meow_Loop( S0, S1, S2, S3, p, sz ) do { \
  const uint8_t * Source = (const uint8_t *) p; \
  size_t          Len    = sz; \
 \
  while ( Len >= 64 ) { \
    S0 = Meow_AESDEC_Memx2( S0, Source ); \
    S1 = Meow_AESDEC_Memx2( S1, Source + 16 ); \
    S2 = Meow_AESDEC_Memx2( S2, Source + 32 ); \
    S3 = Meow_AESDEC_Memx2( S3, Source + 48 ); \
 \
    Len    -= 64; \
    Source += 64; \
  } \
  Meow_Loop_Trail( S0, S1, S2, S3, Source, sz ); \
} while( 0 )

#define Meow_Loop_2( S0, S1, S2, S3, S4, S5, S6, S7, p, p2, sz ) do {\
  const uint8_t * Source  = (const uint8_t *) p; \
  const uint8_t * Source2 = (const uint8_t *) p2; \
  size_t          Len     = sz; \
  const uint32_t  Len8    = (uint32_t) Len & 15; \
  const uint32_t  Len128  = (uint32_t) Len & 48; \
 \
  while ( Len >= 64 ) { \
    S0 = Meow_AESDEC_Memx2( S0, Source ); \
    S1 = Meow_AESDEC_Memx2( S1, Source + 16 ); \
    S2 = Meow_AESDEC_Memx2( S2, Source + 32 ); \
    S3 = Meow_AESDEC_Memx2( S3, Source + 48 ); \
    S4 = Meow_AESDEC_Memx2( S4, Source2 ); \
    S5 = Meow_AESDEC_Memx2( S5, Source2 + 16 ); \
    S6 = Meow_AESDEC_Memx2( S6, Source2 + 32 ); \
    S7 = Meow_AESDEC_Memx2( S7, Source2 + 48 ); \
 \
    Len     -= 64; \
    Source  += 64; \
    Source2 += 64; \
  } \
  if ( Len8 ) { \
    S3 = Meow_AESDECx2_Partial( S3, Source, Len8, Len128 ); \
    S7 = Meow_AESDECx2_Partial( S7, Source2, Len8, Len128 ); \
  } \
  switch ( Len128 ) { \
    case 48: S2 = Meow_AESDEC_Memx2( S2, Source + 32 ); /* FALLTHRU */ \
             S6 = Meow_AESDEC_Memx2( S6, Source2 + 32 ); /* FALLTHRU */ \
    case 32: S1 = Meow_AESDEC_Memx2( S1, Source + 16 ); /* FALLTHRU */ \
             S5 = Meow_AESDEC_Memx2( S5, Source2 + 16 ); /* FALLTHRU */ \
    case 16: S0 = Meow_AESDEC_Memx2( S0, Source ); \
             S4 = Meow_AESDEC_Memx2( S4, Source2 ); break; \
  } \
} while( 0 )

#define Meow_Loop_4( S0, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13, S14, S15, p, p2, p3, p4, sz ) do {\
  const uint8_t * Source  = (const uint8_t *) p; \
  const uint8_t * Source2 = (const uint8_t *) p2; \
  const uint8_t * Source3 = (const uint8_t *) p3; \
  const uint8_t * Source4 = (const uint8_t *) p4; \
  size_t          Len     = sz; \
  const uint32_t  Len8    = (uint32_t) Len & 15; \
  const uint32_t  Len128  = (uint32_t) Len & 48; \
 \
  while ( Len >= 64 ) { \
    S0 = Meow_AESDEC_Memx2( S0, Source ); \
    S1 = Meow_AESDEC_Memx2( S1, Source + 16 ); \
    S2 = Meow_AESDEC_Memx2( S2, Source + 32 ); \
    S3 = Meow_AESDEC_Memx2( S3, Source + 48 ); \
    S4 = Meow_AESDEC_Memx2( S4, Source2 ); \
    S5 = Meow_AESDEC_Memx2( S5, Source2 + 16 ); \
    S6 = Meow_AESDEC_Memx2( S6, Source2 + 32 ); \
    S7 = Meow_AESDEC_Memx2( S7, Source2 + 48 ); \
    S8 = Meow_AESDEC_Memx2( S8, Source3 ); \
    S9 = Meow_AESDEC_Memx2( S9, Source3 + 16 ); \
    S10 = Meow_AESDEC_Memx2( S10, Source3 + 32 ); \
    S11 = Meow_AESDEC_Memx2( S11, Source3 + 48 ); \
    S12 = Meow_AESDEC_Memx2( S12, Source4 ); \
    S13 = Meow_AESDEC_Memx2( S13, Source4 + 16 ); \
    S14 = Meow_AESDEC_Memx2( S14, Source4 + 32 ); \
    S15 = Meow_AESDEC_Memx2( S15, Source4 + 48 ); \
 \
    Len     -= 64; \
    Source  += 64; \
    Source2 += 64; \
    Source3 += 64; \
    Source4 += 64; \
  } \
  if ( Len8 ) { \
    S3 = Meow_AESDECx2_Partial( S3, Source, Len8, Len128 ); \
    S7 = Meow_AESDECx2_Partial( S7, Source2, Len8, Len128 ); \
    S11 = Meow_AESDECx2_Partial( S11, Source3, Len8, Len128 ); \
    S15 = Meow_AESDECx2_Partial( S15, Source4, Len8, Len128 ); \
  } \
  switch ( Len128 ) { \
    case 48: S2 = Meow_AESDEC_Memx2( S2, Source + 32 ); /* FALLTHRU */ \
             S6 = Meow_AESDEC_Memx2( S6, Source2 + 32 ); /* FALLTHRU */ \
             S10 = Meow_AESDEC_Memx2( S10, Source3 + 32 ); /* FALLTHRU */ \
             S14 = Meow_AESDEC_Memx2( S14, Source4 + 32 ); /* FALLTHRU */ \
    case 32: S1 = Meow_AESDEC_Memx2( S1, Source + 16 ); /* FALLTHRU */ \
             S5 = Meow_AESDEC_Memx2( S5, Source2 + 16 ); /* FALLTHRU */ \
             S9 = Meow_AESDEC_Memx2( S9, Source3 + 16 ); /* FALLTHRU */ \
             S13 = Meow_AESDEC_Memx2( S13, Source4 + 16 ); /* FALLTHRU */ \
    case 16: S0 = Meow_AESDEC_Memx2( S0, Source ); \
             S4 = Meow_AESDEC_Memx2( S4, Source2 ); \
             S8 = Meow_AESDEC_Memx2( S8, Source3 ); \
             S12 = Meow_AESDEC_Memx2( S12, Source4 ); break; \
  } \
} while( 0 )

#define Meow_Loop_8( S0, S1, S2, S3, S4, S5, S6, S7, S8, \
                     S9, S10, S11, S12, S13, S14, S15, \
                     S16, S17, S18, S19, S20, S21, S22, S23, \
                     S24, S25, S26, S27, S28, S29, S30, S31, \
                     p, p2, p3, p4, p5, p6, p7, p8, sz ) do {\
  const uint8_t * Source  = (const uint8_t *) p; \
  const uint8_t * Source2 = (const uint8_t *) p2; \
  const uint8_t * Source3 = (const uint8_t *) p3; \
  const uint8_t * Source4 = (const uint8_t *) p4; \
  const uint8_t * Source5 = (const uint8_t *) p5; \
  const uint8_t * Source6 = (const uint8_t *) p6; \
  const uint8_t * Source7 = (const uint8_t *) p7; \
  const uint8_t * Source8 = (const uint8_t *) p8; \
  size_t          Len     = sz; \
  const uint32_t  Len8    = (uint32_t) Len & 15; \
  const uint32_t  Len128  = (uint32_t) Len & 48; \
 \
  while ( Len >= 64 ) { \
    S0 = Meow_AESDEC_Memx2( S0, Source ); \
    S1 = Meow_AESDEC_Memx2( S1, Source + 16 ); \
    S2 = Meow_AESDEC_Memx2( S2, Source + 32 ); \
    S3 = Meow_AESDEC_Memx2( S3, Source + 48 ); \
    S4 = Meow_AESDEC_Memx2( S4, Source2 ); \
    S5 = Meow_AESDEC_Memx2( S5, Source2 + 16 ); \
    S6 = Meow_AESDEC_Memx2( S6, Source2 + 32 ); \
    S7 = Meow_AESDEC_Memx2( S7, Source2 + 48 ); \
    S8 = Meow_AESDEC_Memx2( S8, Source3 ); \
    S9 = Meow_AESDEC_Memx2( S9, Source3 + 16 ); \
    S10 = Meow_AESDEC_Memx2( S10, Source3 + 32 ); \
    S11 = Meow_AESDEC_Memx2( S11, Source3 + 48 ); \
    S12 = Meow_AESDEC_Memx2( S12, Source4 ); \
    S13 = Meow_AESDEC_Memx2( S13, Source4 + 16 ); \
    S14 = Meow_AESDEC_Memx2( S14, Source4 + 32 ); \
    S15 = Meow_AESDEC_Memx2( S15, Source4 + 48 ); \
    S16 = Meow_AESDEC_Memx2( S16, Source5 ); \
    S17 = Meow_AESDEC_Memx2( S17, Source5 + 16 ); \
    S18 = Meow_AESDEC_Memx2( S18, Source5 + 32 ); \
    S19 = Meow_AESDEC_Memx2( S19, Source5 + 48 ); \
    S20 = Meow_AESDEC_Memx2( S20, Source6 ); \
    S21 = Meow_AESDEC_Memx2( S21, Source6 + 16 ); \
    S22 = Meow_AESDEC_Memx2( S22, Source6 + 32 ); \
    S23 = Meow_AESDEC_Memx2( S23, Source6 + 48 ); \
    S24 = Meow_AESDEC_Memx2( S24, Source7 ); \
    S25 = Meow_AESDEC_Memx2( S25, Source7 + 16 ); \
    S26 = Meow_AESDEC_Memx2( S26, Source7 + 32 ); \
    S27 = Meow_AESDEC_Memx2( S27, Source7 + 48 ); \
    S28 = Meow_AESDEC_Memx2( S28, Source8 ); \
    S29 = Meow_AESDEC_Memx2( S29, Source8 + 16 ); \
    S30 = Meow_AESDEC_Memx2( S30, Source8 + 32 ); \
    S31 = Meow_AESDEC_Memx2( S31, Source8 + 48 ); \
\
    Len     -= 64; \
    Source  += 64; Source2 += 64; \
    Source3 += 64; Source4 += 64; \
    Source5 += 64; Source6 += 64; \
    Source7 += 64; Source8 += 64; \
  } \
  if ( Len8 ) { \
    S3 = Meow_AESDECx2_Partial( S3, Source, Len8, Len128 ); \
    S7 = Meow_AESDECx2_Partial( S7, Source2, Len8, Len128 ); \
    S11 = Meow_AESDECx2_Partial( S11, Source3, Len8, Len128 ); \
    S15 = Meow_AESDECx2_Partial( S15, Source4, Len8, Len128 ); \
    S19 = Meow_AESDECx2_Partial( S19, Source5, Len8, Len128 ); \
    S23 = Meow_AESDECx2_Partial( S23, Source6, Len8, Len128 ); \
    S27 = Meow_AESDECx2_Partial( S27, Source7, Len8, Len128 ); \
    S31 = Meow_AESDECx2_Partial( S31, Source8, Len8, Len128 ); \
  } \
  switch ( Len128 ) { \
    case 48: S2 = Meow_AESDEC_Memx2( S2, Source + 32 ); /* FALLTHRU */ \
             S6 = Meow_AESDEC_Memx2( S6, Source2 + 32 ); /* FALLTHRU */ \
             S10 = Meow_AESDEC_Memx2( S10, Source3 + 32 ); /* FALLTHRU */ \
             S14 = Meow_AESDEC_Memx2( S14, Source4 + 32 ); /* FALLTHRU */ \
             S18 = Meow_AESDEC_Memx2( S18, Source5 + 32 ); /* FALLTHRU */ \
             S22 = Meow_AESDEC_Memx2( S22, Source6 + 32 ); /* FALLTHRU */ \
             S26 = Meow_AESDEC_Memx2( S26, Source7 + 32 ); /* FALLTHRU */ \
             S30 = Meow_AESDEC_Memx2( S30, Source8 + 32 ); /* FALLTHRU */ \
    case 32: S1 = Meow_AESDEC_Memx2( S1, Source + 16 ); /* FALLTHRU */ \
             S5 = Meow_AESDEC_Memx2( S5, Source2 + 16 ); /* FALLTHRU */ \
             S9 = Meow_AESDEC_Memx2( S9, Source3 + 16 ); /* FALLTHRU */ \
             S13 = Meow_AESDEC_Memx2( S13, Source4 + 16 ); /* FALLTHRU */ \
             S17 = Meow_AESDEC_Memx2( S17, Source5 + 16 ); /* FALLTHRU */ \
             S21 = Meow_AESDEC_Memx2( S21, Source6 + 16 ); /* FALLTHRU */ \
             S25 = Meow_AESDEC_Memx2( S25, Source7 + 16 ); /* FALLTHRU */ \
             S29 = Meow_AESDEC_Memx2( S29, Source8 + 16 ); /* FALLTHRU */ \
    case 16: S0 = Meow_AESDEC_Memx2( S0, Source ); \
             S4 = Meow_AESDEC_Memx2( S4, Source2 ); \
             S8 = Meow_AESDEC_Memx2( S8, Source3 ); \
             S12 = Meow_AESDEC_Memx2( S12, Source4 ); \
             S16 = Meow_AESDEC_Memx2( S16, Source5 ); \
             S20 = Meow_AESDEC_Memx2( S20, Source6 ); \
             S24 = Meow_AESDEC_Memx2( S24, Source7 ); \
             S28 = Meow_AESDEC_Memx2( S28, Source8 ); break; \
  } \
} while( 0 )

void
kv_hash_meow128( const void *p, size_t sz, uint64_t *x1, uint64_t *x2 )
{
  Declare_Meow( S0, S1, S2, S3 );

  __m128i Mixer = _mm_set_epi64x( *x2 + sz + 1, *x1 - sz );

  Xor_Meow( S0, S1, S2, S3, Mixer );
  Meow_Loop( S0, S1, S2, S3, p, sz );
  Mix_Meow( S3, S2, S1, S0, Mixer );

  Compress_Meow2( S2, S3, S0, S1, S2, Mixer );
  Compress_Meow( S0, S2, S0, Mixer );

  *x1 = _mm_extract_epi64( S0, 0 );
  *x2 = _mm_extract_epi64( S0, 1 );
}

void
kv_hash_meow128_vec( const meow_vec_t *vec,  size_t vec_sz,
                     uint64_t *x1, uint64_t *x2 )
{
  if ( vec_sz == 1 ) {
    kv_hash_meow128( vec->p, vec->sz, x1, x2 );
    return;
  }
  Declare_Meow( S0, S1, S2, S3 );

  size_t i, total_sz = vec[ 0 ].sz + vec[ 1 ].sz;
  for ( i = 2; i < vec_sz; i++ )
    total_sz += vec[ i ].sz;

  __m128i Mixer = _mm_set_epi64x( *x2 + total_sz + 1, *x1 - total_sz );

  Xor_Meow( S0, S1, S2, S3, Mixer );
  uint8_t ALIGN_ARR( block[ 64 ] );
  size_t  off = 0;
  for ( i = 0; i < vec_sz; i++ ) {
    const uint8_t * src = (const uint8_t *) vec[ i ].p;
    size_t          len = vec[ i ].sz;
    if ( off > 0 ) {
      size_t fill = 64 - off;
      if ( fill > len )
        fill = len;
      memcpy( &block[ off ], src, fill );
      off += fill;
      len -= fill;
      src += fill;
      if ( off == 64 ) {
        Meow_Loop64( S0, S1, S2, S3, block, 64 );
        off = 0;
      }
    }
    if ( len > 0 ) {
      off = len & (size_t) 63;
      if ( len > off )
        Meow_Loop64( S0, S1, S2, S3, src, len - off );
      memcpy( block, &src[ len - off ], off );
    }
  }
  if ( off > 0 ) {
    Meow_Loop( S0, S1, S2, S3, block, off );
  }
  Mix_Meow( S3, S2, S1, S0, Mixer );

  Compress_Meow2( S2, S3, S0, S1, S2, Mixer ); /* S2 <- S3, S0 <- S1, S2<-M */
  Compress_Meow( S0, S2, S0, Mixer );    /* S0 <- S2, S0 <- Mixer */

  *x1 = _mm_extract_epi64( S0, 0 );
  *x2 = _mm_extract_epi64( S0, 1 );
}

uint64_t
kv_hash_meow64( const void *p, size_t sz, uint64_t seed )
{
  uint64_t h1 = seed, h2 = seed;
  kv_hash_meow128( p, sz, &h1, &h2 );
  return h1;
}

static inline void
Meow_Save_Ctx( meow_ctx_t *m, __m128i S0, __m128i S1, __m128i S2, __m128i S3 )
{
  *(__m128i *) &m->ctx[ 0 ] = S0;
  *(__m128i *) &m->ctx[ 2 ] = S1;
  *(__m128i *) &m->ctx[ 4 ] = S2;
  *(__m128i *) &m->ctx[ 6 ] = S3;
}

#define Meow_Load_Ctx( m, S0, S1, S2, S3 ) \
  __m128i S0 = *(__m128i *) &m->ctx[ 0 ]; \
  __m128i S1 = *(__m128i *) &m->ctx[ 2 ]; \
  __m128i S2 = *(__m128i *) &m->ctx[ 4 ]; \
  __m128i S3 = *(__m128i *) &m->ctx[ 6 ]

/* same as kv_hash_meow128, in update parts */
void
kv_meow128_init( meow_ctx_t *m,  meow_block_t *b, uint64_t k1,  uint64_t k2,
                 size_t total_update_sz )
{
  Declare_Meow( S0, S1, S2, S3 );
  __m128i Mixer = _mm_set_epi64x( k2 + total_update_sz + 1,
                                  k1 - total_update_sz );
  Xor_Meow( S0, S1, S2, S3, Mixer );
  Meow_Save_Ctx( m, S0, S1, S2, S3 );
  b->off = 0;
  b->total_update_sz = total_update_sz;
}

void
kv_meow128_update( meow_ctx_t *m,  meow_block_t *b, const void *p,  size_t sz )
{
  Meow_Load_Ctx( m, S0, S1, S2, S3 );

  const uint8_t * src = (const uint8_t *) p;
  size_t          len = sz;
  if ( b->off > 0 ) {
    size_t fill = 64 - b->off;
    if ( fill > len )
      fill = len;
    memcpy( &b->block[ b->off ], src, fill );
    b->off += fill;
    len    -= fill;
    src    += fill;
    if ( b->off == 64 ) {
      Meow_Loop64( S0, S1, S2, S3, b->block, 64 );
      b->off = 0;
    }
  }
  if ( len > 0 ) {
    b->off = len & (size_t) 63;
    if ( len > b->off )
      Meow_Loop64( S0, S1, S2, S3, src, len - b->off );
    memcpy( b->block, &src[ len - b->off ], b->off );
  }

  Meow_Save_Ctx( m, S0, S1, S2, S3 );
}

void
kv_meow128_final( meow_ctx_t *m, meow_block_t *b, uint64_t *k1, uint64_t *k2 )
{
  Meow_Load_Ctx( m, S0, S1, S2, S3 );
  if ( b->off > 0 ) {
    Meow_Loop( S0, S1, S2, S3, b->block, b->off );
  }
  __m128i Mixer = _mm_set_epi64x( *k2 + b->total_update_sz + 1,
                                  *k1 - b->total_update_sz );

  Mix_Meow( S3, S2, S1, S0, Mixer );
  Compress_Meow2( S2, S3, S0, S1, S2, Mixer ); /* S2 <- S3, S0 <- S1, S2<-M */
  Compress_Meow( S0, S2, S0, Mixer );    /* S0 <- S2, S0 <- Mixer */

  *k1 = _mm_extract_epi64( S0, 0 );
  *k2 = _mm_extract_epi64( S0, 1 );
}
/* should be the same as kv_hash_meow128 */
void
kv_meow_test( const void *p, size_t sz, uint64_t *k1, uint64_t *k2 )
{
  meow_ctx_t m;
  meow_block_t b;

  kv_meow128_init( &m, &b, *k1, *k2, sz );
  kv_meow128_update( &m, &b, p, sz );
  kv_meow128_final( &m, &b, k1, k2 );
}

/* construct hmac:
 *
 * HMAC( K, m ) = H( H( K ^ OPAD ) + H( K ^ IPAD + m ) )
 *
 * hmac = meow_hash( H ^ OPAD + meow_hash( H ^ IPAD, m ) )
 */
static const uint64_t OPAD = _U64( 0x5c5c5c5c, 0x5c5c5c5c ),
                      IPAD = _U64( 0x36363636, 0x36363636 );
void
kv_hmac_meow_init( meow_ctx_t *m, meow_block_t *b,  uint64_t k1,  uint64_t k2 )
{
  Declare_Meow( S0, S1, S2, S3 );
  __m128i ipad = _mm_set_epi64x( k2 ^ IPAD, k1 ^ IPAD ); /* inner hash */

  Mix_Meow( S3, S2, S1, S0, ipad );
  Mix_Meow( S3, S2, S1, S0, ipad );

  Meow_Save_Ctx( m, S0, S1, S2, S3 );
  b->off = 0;
  b->total_update_sz = 0;
}

void
kv_hmac_meow_update( meow_ctx_t *m, meow_block_t *b, const void *p, size_t sz )
{
  kv_meow128_update( m, b, p, sz );
  b->total_update_sz += sz;
}

void
kv_hmac_meow_final( meow_ctx_t *m,  meow_block_t *b, uint64_t *k1,
                    uint64_t *k2 )
{
  Meow_Load_Ctx( m, S0, S1, S2, S3 );
  if ( b->off > 0 ) {
    Meow_Loop( S0, S1, S2, S3, b->block, b->off );
  }

  __m128i Mixer = _mm_set_epi64x( b->total_update_sz + 1, -(int64_t) b->total_update_sz );

  Compress_Meow2( S2, S3, S0, S1, S2, Mixer ); /* S2 <- S3, S0 <- S1, S2<-M */
  Compress_Meow( S0, S2, S0, Mixer );    /* S0 <- S2, S0 <- Mixer */

  __m128i ipad = S0;
  S0 = *(__m128i *) MeowS0Init;
  S1 = *(__m128i *) MeowS1Init;
  S2 = *(__m128i *) MeowS2Init;
  S3 = *(__m128i *) MeowS3Init;

  Mix_Meow( S3, S2, S1, S0, ipad );
  Mix_Meow( S3, S2, S1, S0, ipad );

  __m128i opad = _mm_set_epi64x( *k2 ^ OPAD, *k1 ^ OPAD );

  Mix_Meow( S3, S2, S1, S0, opad );
  Mix_Meow( S3, S2, S1, S0, opad );
  Mix_Meow( S3, S2, S1, S0, Mixer );

  Compress_Meow2( S2, S3, S0, S1, S2, Mixer ); /* S2 <- S3, S0 <- S1, S2<-M */
  Compress_Meow( S0, S2, S0, Mixer );    /* S0 <- S2, S0 <- Mixer */

  *k1 = _mm_extract_epi64( S0, 0 );
  *k2 = _mm_extract_epi64( S0, 1 );
}

void
kv_hmac_meow( const void *p,  size_t sz,  uint64_t *k1,  uint64_t *k2 )
{
  meow_ctx_t m;
  meow_block_t b;

  kv_hmac_meow_init( &m, &b, *k1, *k2 );
  kv_hmac_meow_update( &m, &b, p, sz );
  kv_hmac_meow_final( &m, &b, k1, k2 );
}

void
kv_hash_meow128_2_same_length( const void *p, const void *p2, size_t sz,
                               uint64_t *x )
{
  Declare_Meow( S0, S1, S2, S3 );
  Declare_Meow( S4, S5, S6, S7 );

  __m128i Mixer = _mm_set_epi64x( x[ 1 ] + sz + 1, x[ 0 ] - sz );

  Xor_Meow( S0, S1, S2, S3, Mixer );
  Xor_Meow( S4, S5, S6, S7, Mixer );

  Meow_Loop_2( S0, S1, S2, S3, S4, S5, S6, S7, p, p2, sz );

  Mix_Meow( S3, S2, S1, S0, Mixer );
  Mix_Meow( S7, S6, S5, S4, Mixer );

  Compress_Meow2( S2, S3, S0, S1, S2, Mixer ); /* S2 <- S3, S0 <- S1, S2 <- M */
  Compress_Meow2( S6, S7, S4, S5, S6, Mixer ); /* S6 <- S7, S4 <- S5, S6 <- M */
  Compress_Meow( S0, S2, S0, Mixer );    /* S0 <- S2, S0 <- Mixer */
  Compress_Meow( S4, S6, S4, Mixer );    /* S4 <- S6, S4 <- Mixer */

  x[ 0 ] = _mm_extract_epi64( S0, 0 );
  x[ 1 ] = _mm_extract_epi64( S0, 1 );
  x[ 2 ] = _mm_extract_epi64( S4, 0 );
  x[ 3 ] = _mm_extract_epi64( S4, 1 );
}

void
kv_hash_meow128_2_diff_length( const void *p, size_t sz,
                               const void *p2, size_t sz2, uint64_t *x )
{
  Declare_Meow( S0, S1, S2, S3 );
  Declare_Meow( S4, S5, S6, S7 );

  __m128i Mixer = _mm_set_epi64x( x[ 1 ] + sz + 1, x[ 0 ] - sz );
  __m128i Mixer2 = _mm_set_epi64x( x[ 1 ] + sz2 + 1, x[ 0 ] - sz2 );

  Xor_Meow( S0, S1, S2, S3, Mixer );
  Xor_Meow( S4, S5, S6, S7, Mixer2 );

  const uint8_t * Source  = (const uint8_t *) p;
  const uint8_t * Source2 = (const uint8_t *) p2;
  size_t          Len     = sz;
  size_t          Len2    = sz2;

  for (;;) {
    if ( Len >= 64 ) {
      S0 = Meow_AESDEC_Memx2( S0, Source );
      S1 = Meow_AESDEC_Memx2( S1, Source + 16 );
      S2 = Meow_AESDEC_Memx2( S2, Source + 32 );
      S3 = Meow_AESDEC_Memx2( S3, Source + 48 );
      Len     -= 64;
      Source  += 64;
    }
    if ( Len2 >= 64 ) {
      S4 = Meow_AESDEC_Memx2( S4, Source2 );
      S5 = Meow_AESDEC_Memx2( S5, Source2 + 16 );
      S6 = Meow_AESDEC_Memx2( S6, Source2 + 32 );
      S7 = Meow_AESDEC_Memx2( S7, Source2 + 48 );
      Len2    -= 64;
      Source2 += 64;
    }
    else if ( Len < 64 )
      break;
  }
  Meow_Loop_Trail( S0, S1, S2, S3, Source, sz );
  Meow_Loop_Trail( S4, S5, S6, S7, Source2, sz2 );

  Mix_Meow( S3, S2, S1, S0, Mixer );
  Mix_Meow( S7, S6, S5, S4, Mixer2 );

  Compress_Meow2( S2, S3, S0, S1, S2, Mixer ); /* S2 <- S3, S0 <- S1, S2 <- M */
  Compress_Meow2( S6, S7, S4, S5, S6, Mixer2 ); /* S6 <- S7, S4 <- S5, S6 <- M */
  Compress_Meow( S0, S2, S0, Mixer );    /* S0 <- S2, S0 <- Mixer */
  Compress_Meow( S4, S6, S4, Mixer2 );    /* S4 <- S6, S4 <- Mixer */

  x[ 0 ] = _mm_extract_epi64( S0, 0 );
  x[ 1 ] = _mm_extract_epi64( S0, 1 );
  x[ 2 ] = _mm_extract_epi64( S4, 0 );
  x[ 3 ] = _mm_extract_epi64( S4, 1 );
}

void
kv_hash_meow128_4_diff_length( const void *p, size_t sz,
                               const void *p2, size_t sz2,
                               const void *p3, size_t sz3,
                               const void *p4, size_t sz4,
                               uint64_t *x )
{
  Declare_Meow( S0, S1, S2, S3 );
  Declare_Meow( S4, S5, S6, S7 );
  Declare_Meow( S8, S9, S10, S11 );
  Declare_Meow( S12, S13, S14, S15 );

  __m128i Mixer  = _mm_set_epi64x( x[ 1 ] + sz + 1, x[ 0 ] - sz );
  __m128i Mixer2 = _mm_set_epi64x( x[ 1 ] + sz2 + 1, x[ 0 ] - sz2 );
  __m128i Mixer3 = _mm_set_epi64x( x[ 1 ] + sz3 + 1, x[ 0 ] - sz3 );
  __m128i Mixer4 = _mm_set_epi64x( x[ 1 ] + sz4 + 1, x[ 0 ] - sz4 );

  Xor_Meow( S0, S1, S2, S3, Mixer );
  Xor_Meow( S4, S5, S6, S7, Mixer2 );
  Xor_Meow( S8, S9, S10, S11, Mixer3 );
  Xor_Meow( S12, S13, S14, S15, Mixer4 );

  const uint8_t * Source  = (const uint8_t *) p,
                * Source2 = (const uint8_t *) p2,
                * Source3 = (const uint8_t *) p3,
                * Source4 = (const uint8_t *) p4;
  size_t          Len     = sz,
                  Len2    = sz2,
                  Len3    = sz3,
                  Len4    = sz4;
  int             active  = 0;
  for (;;) {
    if ( Len >= 64 ) {
      S0 = Meow_AESDEC_Memx2( S0, Source );
      S1 = Meow_AESDEC_Memx2( S1, Source + 16 );
      S2 = Meow_AESDEC_Memx2( S2, Source + 32 );
      S3 = Meow_AESDEC_Memx2( S3, Source + 48 );
      Len     -= 64;
      Source  += 64;
      active   = 1;
    }
    if ( Len2 >= 64 ) {
      S4 = Meow_AESDEC_Memx2( S4, Source2 );
      S5 = Meow_AESDEC_Memx2( S5, Source2 + 16 );
      S6 = Meow_AESDEC_Memx2( S6, Source2 + 32 );
      S7 = Meow_AESDEC_Memx2( S7, Source2 + 48 );
      Len2    -= 64;
      Source2 += 64;
      active   = 1;
    }
    if ( Len3 >= 64 ) {
      S8 = Meow_AESDEC_Memx2( S8, Source3 );
      S9 = Meow_AESDEC_Memx2( S9, Source3 + 16 );
      S10 = Meow_AESDEC_Memx2( S10, Source3 + 32 );
      S11 = Meow_AESDEC_Memx2( S11, Source3 + 48 );
      Len3    -= 64;
      Source3 += 64;
      active   = 1;
    }
    if ( Len4 >= 64 ) {
      S12 = Meow_AESDEC_Memx2( S12, Source4 );
      S13 = Meow_AESDEC_Memx2( S13, Source4 + 16 );
      S14 = Meow_AESDEC_Memx2( S14, Source4 + 32 );
      S15 = Meow_AESDEC_Memx2( S15, Source4 + 48 );
      Len4    -= 64;
      Source4 += 64;
    }
    else if ( ! active )
      break;
    active = 0;
  }
  Meow_Loop_Trail( S0, S1, S2, S3, Source, sz );
  Meow_Loop_Trail( S4, S5, S6, S7, Source2, sz2 );
  Meow_Loop_Trail( S8, S9, S10, S11, Source3, sz3 );
  Meow_Loop_Trail( S12, S13, S14, S15, Source4, sz4 );

  Mix_Meow( S3, S2, S1, S0, Mixer );
  Mix_Meow( S7, S6, S5, S4, Mixer2 );
  Mix_Meow( S11, S10, S9, S8, Mixer3 );
  Mix_Meow( S15, S14, S13, S12, Mixer4 );

  Compress_Meow2( S2, S3, S0, S1, S2, Mixer );  /* S2 <- S3, S0 <- S1, S2 <- M*/
  Compress_Meow2( S6, S7, S4, S5, S6, Mixer2 );  /* S6 <- S7, S4 <- S5, S6 <- M*/
  Compress_Meow2( S10, S11, S8, S9, S10, Mixer3 ); /* S10 <- S11, S8 <- S9 */
  Compress_Meow2( S14, S15, S12, S13, S14, Mixer4 );/* S14 <- S15, S12 <- S13 */
  Compress_Meow( S0, S2, S0, Mixer );    /* S0 <- S2, S0 <- Mixer */
  Compress_Meow( S4, S6, S4, Mixer2 );    /* S4 <- S6, S4 <- Mixer */
  Compress_Meow( S8, S10, S8, Mixer3 );   /* S8 <- S10, S8 <- Mixer */
  Compress_Meow( S12, S14, S12, Mixer4 ); /* S12 <- S14, S12 <- Mixer */

  x[ 0 ] = _mm_extract_epi64( S0, 0 );
  x[ 1 ] = _mm_extract_epi64( S0, 1 );
  x[ 2 ] = _mm_extract_epi64( S4, 0 );
  x[ 3 ] = _mm_extract_epi64( S4, 1 );
  x[ 4 ] = _mm_extract_epi64( S8, 0 );
  x[ 5 ] = _mm_extract_epi64( S8, 1 );
  x[ 6 ] = _mm_extract_epi64( S12, 0 );
  x[ 7 ] = _mm_extract_epi64( S12, 1 );
}

void
kv_hash_meow128_4_same_length_a( const void **p, size_t sz, uint64_t *x )
{
  kv_hash_meow128_4_same_length( p[ 0 ], p[ 1 ], p[ 2 ], p[ 3 ], sz, x );
}

void
kv_hash_meow128_4_same_length( const void *p, const void *p2,
                               const void *p3, const void *p4, size_t sz,
                               uint64_t *x )
{
  Declare_Meow( S0, S1, S2, S3 );
  Declare_Meow( S4, S5, S6, S7 );
  Declare_Meow( S8, S9, S10, S11 );
  Declare_Meow( S12, S13, S14, S15 );

  __m128i Mixer = _mm_set_epi64x( x[ 1 ] + sz + 1, x[ 0 ] - sz );

  Xor_Meow( S0, S1, S2, S3, Mixer );
  Xor_Meow( S4, S5, S6, S7, Mixer );
  Xor_Meow( S8, S9, S10, S11, Mixer );
  Xor_Meow( S12, S13, S14, S15, Mixer );

  Meow_Loop_4( S0, S1, S2, S3, S4, S5, S6, S7,
               S8, S9, S10, S11, S12, S13, S14, S15,
               p, p2, p3, p4, sz );

  Mix_Meow( S3, S2, S1, S0, Mixer );
  Mix_Meow( S7, S6, S5, S4, Mixer );
  Mix_Meow( S11, S10, S9, S8, Mixer );
  Mix_Meow( S15, S14, S13, S12, Mixer );

  Compress_Meow2( S2, S3, S0, S1, S2, Mixer );  /* S2 <- S3, S0 <- S1, S2 <- M*/
  Compress_Meow2( S6, S7, S4, S5, S6, Mixer );  /* S6 <- S7, S4 <- S5, S6 <- M*/
  Compress_Meow2( S10, S11, S8, S9, S10, Mixer ); /* S10 <- S11, S8 <- S9 */
  Compress_Meow2( S14, S15, S12, S13, S14, Mixer );/* S14 <- S15, S12 <- S13 */
  Compress_Meow( S0, S2, S0, Mixer );    /* S0 <- S2, S0 <- Mixer */
  Compress_Meow( S4, S6, S4, Mixer );    /* S4 <- S6, S4 <- Mixer */
  Compress_Meow( S8, S10, S8, Mixer );   /* S8 <- S10, S8 <- Mixer */
  Compress_Meow( S12, S14, S12, Mixer ); /* S12 <- S14, S12 <- Mixer */

  x[ 0 ] = _mm_extract_epi64( S0, 0 );
  x[ 1 ] = _mm_extract_epi64( S0, 1 );
  x[ 2 ] = _mm_extract_epi64( S4, 0 );
  x[ 3 ] = _mm_extract_epi64( S4, 1 );
  x[ 4 ] = _mm_extract_epi64( S8, 0 );
  x[ 5 ] = _mm_extract_epi64( S8, 1 );
  x[ 6 ] = _mm_extract_epi64( S12, 0 );
  x[ 7 ] = _mm_extract_epi64( S12, 1 );
}

void
kv_hash_meow128_4_same_length_4_seed( const void *p, const void *p2,
                                      const void *p3, const void *p4, size_t sz,
                                      uint64_t *x )
{
  Declare_Meow( S0, S1, S2, S3 );
  Declare_Meow( S4, S5, S6, S7 );
  Declare_Meow( S8, S9, S10, S11 );
  Declare_Meow( S12, S13, S14, S15 );

  __m128i Mixer  = _mm_set_epi64x( x[ 1 ] + sz + 1, x[ 0 ] - sz );
  __m128i Mixer2 = _mm_set_epi64x( x[ 3 ] + sz + 1, x[ 2 ] - sz );
  __m128i Mixer3 = _mm_set_epi64x( x[ 5 ] + sz + 1, x[ 4 ] - sz );
  __m128i Mixer4 = _mm_set_epi64x( x[ 7 ] + sz + 1, x[ 6 ] - sz );

  Xor_Meow( S0, S1, S2, S3, Mixer );
  Xor_Meow( S4, S5, S6, S7, Mixer2 );
  Xor_Meow( S8, S9, S10, S11, Mixer3 );
  Xor_Meow( S12, S13, S14, S15, Mixer4 );

  Meow_Loop_4( S0, S1, S2, S3, S4, S5, S6, S7,
               S8, S9, S10, S11, S12, S13, S14, S15,
               p, p2, p3, p4, sz );

  Mix_Meow( S3, S2, S1, S0, Mixer );
  Mix_Meow( S7, S6, S5, S4, Mixer2 );
  Mix_Meow( S11, S10, S9, S8, Mixer3 );
  Mix_Meow( S15, S14, S13, S12, Mixer4 );

  Compress_Meow2( S2, S3, S0, S1, S2, Mixer );
  Compress_Meow2( S6, S7, S4, S5, S6, Mixer2 );
  Compress_Meow2( S10, S11, S8, S9, S10, Mixer3 );
  Compress_Meow2( S14, S15, S12, S13, S14, Mixer4 );
  Compress_Meow( S0, S2, S0, Mixer );
  Compress_Meow( S4, S6, S4, Mixer2 );
  Compress_Meow( S8, S10, S8, Mixer3 );
  Compress_Meow( S12, S14, S12, Mixer4 );

  x[ 0 ] = _mm_extract_epi64( S0, 0 );
  x[ 1 ] = _mm_extract_epi64( S0, 1 );
  x[ 2 ] = _mm_extract_epi64( S4, 0 );
  x[ 3 ] = _mm_extract_epi64( S4, 1 );
  x[ 4 ] = _mm_extract_epi64( S8, 0 );
  x[ 5 ] = _mm_extract_epi64( S8, 1 );
  x[ 6 ] = _mm_extract_epi64( S12, 0 );
  x[ 7 ] = _mm_extract_epi64( S12, 1 );
}

void
kv_hash_meow128_8_same_length_a( const void **p, size_t sz, uint64_t *x )
{
  kv_hash_meow128_8_same_length( p[ 0 ], p[ 1 ], p[ 2 ], p[ 3 ],
                                 p[ 4 ], p[ 5 ], p[ 6 ], p[ 7 ], sz, x );
}

void
kv_hash_meow128_8_same_length( const void *p, const void *p2, const void *p3,
                               const void *p4, const void *p5, const void *p6,
                               const void *p7, const void *p8,
                               size_t sz, uint64_t *x )
{
  Declare_Meow( S0, S1, S2, S3 );
  Declare_Meow( S4, S5, S6, S7 );
  Declare_Meow( S8, S9, S10, S11 );
  Declare_Meow( S12, S13, S14, S15 );
  Declare_Meow( S16, S17, S18, S19 );
  Declare_Meow( S20, S21, S22, S23 );
  Declare_Meow( S24, S25, S26, S27 );
  Declare_Meow( S28, S29, S30, S31 );

  __m128i Mixer = _mm_set_epi64x( x[ 1 ] + sz + 1, x[ 0 ] - sz );

  Xor_Meow( S0, S1, S2, S3, Mixer );
  Xor_Meow( S4, S5, S6, S7, Mixer );
  Xor_Meow( S8, S9, S10, S11, Mixer );
  Xor_Meow( S12, S13, S14, S15, Mixer );
  Xor_Meow( S16, S17, S18, S19, Mixer );
  Xor_Meow( S20, S21, S22, S23, Mixer );
  Xor_Meow( S24, S25, S26, S27, Mixer );
  Xor_Meow( S28, S29, S30, S31, Mixer );

  Meow_Loop_8( S0, S1, S2, S3, S4, S5, S6, S7,
               S8, S9, S10, S11, S12, S13, S14, S15,
               S16, S17, S18, S19, S20, S21, S22, S23,
               S24, S25, S26, S27, S28, S29, S30, S31,
               p, p2, p3, p4, p5, p6, p7, p8, sz );

  Mix_Meow( S3, S2, S1, S0, Mixer );
  Mix_Meow( S7, S6, S5, S4, Mixer );
  Mix_Meow( S11, S10, S9, S8, Mixer );
  Mix_Meow( S15, S14, S13, S12, Mixer );
  Mix_Meow( S19, S18, S17, S16, Mixer );
  Mix_Meow( S23, S22, S21, S20, Mixer );
  Mix_Meow( S27, S26, S25, S24, Mixer );
  Mix_Meow( S31, S30, S29, S28, Mixer );

  Compress_Meow2( S2, S3, S0, S1, S2, Mixer );  /* S2 <- S3, S0 <- S1, S2 <- M*/
  Compress_Meow2( S6, S7, S4, S5, S6, Mixer );  /* S6 <- S7, S4 <- S5, S6 <- M*/
  Compress_Meow2( S10, S11, S8, S9, S10, Mixer ); /* S10 <- S11, S8 <- S9 */
  Compress_Meow2( S14, S15, S12, S13, S14, Mixer );/* S14 <- S15, S12 <- S13 */
  Compress_Meow2( S18, S19, S16, S17, S18, Mixer );
  Compress_Meow2( S22, S23, S20, S21, S22, Mixer );
  Compress_Meow2( S26, S27, S24, S25, S26, Mixer );
  Compress_Meow2( S30, S31, S28, S29, S30, Mixer );
  Compress_Meow( S0, S2, S0, Mixer );    /* S0 <- S2, S0 <- Mixer */
  Compress_Meow( S4, S6, S4, Mixer );    /* S4 <- S6, S4 <- Mixer */
  Compress_Meow( S8, S10, S8, Mixer );   /* S8 <- S10, S8 <- Mixer */
  Compress_Meow( S12, S14, S12, Mixer ); /* S12 <- S14, S12 <- Mixer */
  Compress_Meow( S16, S18, S16, Mixer );
  Compress_Meow( S20, S22, S20, Mixer );
  Compress_Meow( S24, S26, S24, Mixer );
  Compress_Meow( S28, S30, S28, Mixer );

  x[ 0 ] = _mm_extract_epi64( S0, 0 );
  x[ 1 ] = _mm_extract_epi64( S0, 1 );
  x[ 2 ] = _mm_extract_epi64( S4, 0 );
  x[ 3 ] = _mm_extract_epi64( S4, 1 );
  x[ 4 ] = _mm_extract_epi64( S8, 0 );
  x[ 5 ] = _mm_extract_epi64( S8, 1 );
  x[ 6 ] = _mm_extract_epi64( S12, 0 );
  x[ 7 ] = _mm_extract_epi64( S12, 1 );
  x[ 8 ] = _mm_extract_epi64( S16, 0 );
  x[ 9 ] = _mm_extract_epi64( S16, 1 );
  x[10 ] = _mm_extract_epi64( S20, 0 );
  x[11 ] = _mm_extract_epi64( S20, 1 );
  x[12 ] = _mm_extract_epi64( S24, 0 );
  x[13 ] = _mm_extract_epi64( S24, 1 );
  x[14 ] = _mm_extract_epi64( S28, 0 );
  x[15 ] = _mm_extract_epi64( S28, 1 );
}

#endif /* USE_KV_MEOW_HASH */

