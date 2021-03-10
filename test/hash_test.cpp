#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <raikv/shm_ht.h>
#include <raikv/key_buf.h>

using namespace rai;
using namespace kv;

void
incr_key( KeyFragment &kb )
{
  for ( uint8_t j = kb.keylen - 1; ; ) {
    if ( ++kb.u.buf[ --j ] <= '9' )
      break;
    kb.u.buf[ j ] = '0';
    if ( j == 0 ) {
      ::memmove( &kb.u.buf[ 1 ], kb.u.buf, kb.keylen );
      kb.u.buf[ 0 ] = '1';
      kb.keylen++;
      break;
    }
  }
}

struct Content {
  const char *name;
  Content( const char *n ) : name( n ) {}
  virtual ~Content() {}
  virtual void nextKey( KeyFragment &/*kb*/,  uint8_t /*keylen*/ ) {}
  void operator delete( void * ) {}
};

struct RandContent : public Content {
  kv::rand::xorshift1024star rand;
  RandContent() : Content( "rand" ) {
    this->rand.init();
  }

  virtual void nextKey( KeyFragment &kb,  uint8_t keylen ) {
    kb.keylen = this->rand.next() % (keylen*2) + 1;
    for ( uint8_t j = 0; j < kb.keylen; j++ ) {
      kb.u.buf[ j ] = "abcdefghijklmnopqrstuvwxyz.:0123456789"[
        this->rand.next() % 38 ];
    }
  }
  void operator delete( void * ) {}
};

struct IntContent : public Content {
  uint64_t counter;
  IntContent() : Content( "int" ), counter( 0 ) {}

  virtual void nextKey( KeyFragment &kb,  uint8_t keylen ) {
    kb.keylen = keylen;
    uint8_t j = 0;
    do {
      ::memcpy( &kb.u.buf[ j ], &this->counter, sizeof( this->counter ) );
      this->counter++;
      j += sizeof( this->counter );
    } while ( j < keylen );
  }
  void operator delete( void * ) {}
};

struct IncrContent : public Content {
  uint64_t counter;
  IncrContent() : Content( "incr" ), counter( 0 ) {}

  virtual void nextKey( KeyFragment &kb,  uint8_t keylen ) {
    if ( keylen < 2 ) keylen = 2;
    kb.keylen = keylen;
    uint8_t j = 0;
    do {
      kb.u.buf[ j++ ] = '0';
    } while ( j < keylen - 1 );
    kb.u.buf[ j ] = 0;
    for ( uint64_t k = this->counter++; ; ) {
      kb.u.buf[ --j ] += ( k % 10 );
      k /= 10;
      if ( j == 0 || k == 0 )
        break;
    }
    return;
  }
  void operator delete( void * ) {}
};

int
main( int argc, char *argv[] )
{
  static const uint32_t MAP_COUNT = 256;
  kv_hash128_func_t func   = NULL;
  uint64_t i, j, k, keycount;
  uint32_t map[ MAP_COUNT ];
  uint16_t keylen = 32;
  double      t1, t2;
  IntContent  ifill;
  RandContent rfill;
  IncrContent xfill;
  Content   * fill;
  const char * name =
#if defined( USE_KV_MEOW_HASH )
               "meow";
  bool meow_x2 = false,
       meow_x2_diff = false,
       meow_x4 = false,
       meow_x4_diff = false,
       meow_x8 = false;
#elif defined( USE_KV_AES_HASH )
               "aes";
#elif defined( USE_KV_SPOOKY_HASH )
               "spooky";
#else
               "murmur";
#endif
  const uint64_t TEST_COUNT = 10000000;

  if ( argc == 1 ) {
cmd_error:;
    fprintf( stderr, "raikv version: %s\n", kv_stringify( KV_VER ) );
    fprintf( stderr, "%s (int rand incr) ("
#if defined( USE_KV_CITY_HASH )
             "citymur "
#endif
#if defined( USE_KV_AES_HASH )
             "aes "
#endif
#if defined( USE_KV_MEOW_HASH )
             "meow "
             "meow_hmac "
             "meow_test "
             "meowx2_same "
             "meowx2_diff "
             "meowx4_same "
             "meowx4_diff "
             "meowx8_same "
#endif
#if defined( USE_KV_SPOOKY_HASH )
             "spooky "
#endif
#if defined( USE_KV_MURMUR_HASH )
             "murmur"
#endif
             ") (keylen)\n",
             argv[ 0 ] );
    return 1;
  }

  if ( argc > 1 && ::strcmp( argv[ 1 ], "int" ) == 0 )
    fill = &ifill;
  else if ( argc > 1 && ::strcmp( argv[ 1 ], "rand" ) == 0 )
    fill = &rfill;
  else
    fill = &xfill;
  printf( "content=%s\n", fill->name );
  if ( argc > 2 )
    name = argv[ 2 ];
#if defined( USE_KV_CITY_HASH )
  if ( ::strcmp( name, "citymur" ) == 0 )
    func = kv_hash_citymur128;
  else
#endif
#if defined( USE_KV_AES_HASH )
  if ( ::strcmp( name, "aes" ) == 0 )
    func = kv_hash_aes128;
  else
#endif
#if defined( USE_KV_MEOW_HASH )
  if ( ::strcmp( name, "meow" ) == 0 )
    func = kv_hash_meow128;
  else if ( ::strcmp( name, "meow_hmac" ) == 0 )
    func = kv_hmac_meow;
  else if ( ::strcmp( name, "meow_test" ) == 0 )
    func = kv_meow_test;
  else if ( ::strcmp( name, "meowx2_same" ) == 0 ) {
    func = NULL;
    meow_x2 = true;
  }
  else if ( ::strcmp( name, "meowx2_diff" ) == 0 ) {
    func = NULL;
    meow_x2_diff = true;
  }
  else if ( ::strcmp( name, "meowx4_same" ) == 0 ) {
    func = NULL;
    meow_x4 = true;
  }
  else if ( ::strcmp( name, "meowx4_diff" ) == 0 ) {
    func = NULL;
    meow_x4_diff = true;
  }
  else if ( ::strcmp( name, "meowx8_same" ) == 0 ) {
    func = NULL;
    meow_x8 = true;
  }
  else
#endif
#if defined( USE_KV_SPOOKY_HASH )
  if ( ::strcmp( name, "spooky" ) == 0 )
    func = kv_hash_spooky128;
  else
#endif
#if defined( USE_KV_MURMUR_HASH )
  if ( ::strcmp( name, "murmur" ) == 0 )
    func = kv_hash_murmur128;
#endif
#if defined( USE_KV_MEOW_HASH )
  if ( func == NULL &&
       ! meow_x2 && ! meow_x2_diff && ! meow_x4 && ! meow_x4_diff && ! meow_x8 )
#else
  if ( func == NULL )
#endif
  {
    fprintf( stderr, "hash %s not available\n", name );
    goto cmd_error;
  }
  if ( argc > 3 ) {
    keylen = atoi( argv[ 3 ] );
    if ( keylen == 0 )
      goto cmd_error;
  }
  const char *example = "any carnal pleasure."; /* base64 wikipedia examples */
  const char *exout[] = { "YW55IGNhcm5hbCBwbGVhc3VyZS4=",
                          "YW55IGNhcm5hbCBwbGVhc3VyZQ==",
                          "YW55IGNhcm5hbCBwbGVhc3Vy",
                          "YW55IGNhcm5hbCBwbGVhc3U=",
                          "YW55IGNhcm5hbCBwbGVhcw==" };

  char buf[ 128 ];
  size_t bsz;

  bsz = bin_to_base64( "ABCDEFG", 7, buf );
  if ( bsz != 12 || memcmp( buf, "QUJDREVGRw==", 12 ) != 0 )
    printf( "not eq\n" );

  bsz = bin_to_base64( "ABCDEFGH", 8, buf );
  if ( bsz != 12 || memcmp( buf, "QUJDREVGR0g=", 12 ) != 0 )
    printf( "not eq\n" );

  for ( i = 0; i < 5; i++ ) {
    bsz = base64_to_bin( exout[ i ], ::strlen( exout[ i ] ), buf );
    if ( bsz != 20 - i )
      printf( "len not right\n" );
    if ( ::memcmp( buf, example, bsz ) != 0 )
      printf( "not eq\n" );
    bsz = bin_to_base64( example, 20 - i, buf );
    if ( bsz != ::strlen( exout[ i ] ) )
      printf( "len not right\n" );
    if ( ::memcmp( buf, exout[ i ], bsz ) != 0 )
      printf( "not eq\n" );
  }
#if defined( USE_KV_MEOW_HASH )
  const char *ar[] = {
    "security is for the messaging layer",
    "authenticate the publisher to the s",
    "subscribers must be able to trust t",
    "uniquely serialized, and authentic "
  };

  uint64_t x[ 8 ], y[ 8 ], d[ 8 ], e[ 8 ], z[ 8 ], u[ 8 ], v[ 8 ], w[ 16 ];
  size_t n;
  for ( n = 0; n < 8; n += 2 ) {
    x[ n ] = 1010; x[ n + 1 ] = 2020;
    y[ n ] = 1010; y[ n + 1 ] = 2020;
    d[ n ] = 1010; d[ n + 1 ] = 2020;
    e[ n ] = 1010; e[ n + 1 ] = 2020;
    z[ n ] = 1010; z[ n + 1 ] = 2020;
    u[ n ] = 1010; u[ n + 1 ] = 2020;
    v[ n ] = 1010; v[ n + 1 ] = 2020;
    w[ n ] = 1010; w[ n + 1 ] = 2020;
    w[ n*2 ] = 1010; w[ n*2 + 1 ] = 2020;
  }
  size_t len = ::strlen( ar[ 0 ] );
  kv_hash_meow128( ar[ 0 ], len, &x[ 0 ], &x[ 1 ] );
  kv_hash_meow128( ar[ 1 ], len, &x[ 2 ], &x[ 3 ] );
  kv_hash_meow128( ar[ 2 ], len, &x[ 4 ], &x[ 5 ] );
  kv_hash_meow128( ar[ 3 ], len, &x[ 6 ], &x[ 7 ] );
  kv_hash_meow128_2_same_length( ar[ 0 ], ar[ 1 ], len, y );
  kv_hash_meow128_2_same_length( ar[ 2 ], ar[ 3 ], len, &y[ 4 ] );
  kv_hash_meow128_2_diff_length( ar[ 0 ], len, ar[ 1 ], len, d );
  kv_hash_meow128_2_diff_length( ar[ 2 ], len, ar[ 3 ], len, &d[ 4 ] );
  kv_hash_meow128_4_same_length( ar[ 0 ], ar[ 1 ], ar[ 2 ], ar[ 3 ],
                                 len, z );
  kv_hash_meow128_4_diff_length( ar[ 0 ], len, ar[ 1 ], len, ar[ 2 ], len,
                                 ar[ 3 ], len, e );
  meow_ctx_t ctx[ 4 ];
  meow_block_t block;

  kv_meow128_init( &ctx[ 0 ], &block, u[ 0 ], u[ 1 ], len );
  kv_meow128_update( &ctx[ 0 ], &block, ar[ 0 ], len );
  kv_meow128_final( &ctx[ 0 ], &block, &u[ 0 ], &u[ 1 ] );

  kv_meow128_init( &ctx[ 1 ], &block, u[ 2 ], u[ 3 ], len );
  kv_meow128_update( &ctx[ 1 ], &block, ar[ 1 ], len );
  kv_meow128_final( &ctx[ 1 ], &block, &u[ 2 ], &u[ 3 ] );

  kv_meow128_init( &ctx[ 2 ], &block, u[ 4 ], u[ 5 ], len );
  kv_meow128_update( &ctx[ 2 ], &block, ar[ 2 ], len );
  kv_meow128_final( &ctx[ 2 ], &block, &u[ 4 ], &u[ 5 ] );

  kv_meow128_init( &ctx[ 3 ], &block, u[ 6 ], u[ 7 ], len );
  kv_meow128_update( &ctx[ 3 ], &block, ar[ 3 ], len );
  kv_meow128_final( &ctx[ 3 ], &block, &u[ 6 ], &u[ 7 ] );

  n = len / 2;
  meow_vec_t vec0[ 2 ] = { { ar[ 0 ], n }, { ar[ 0 ] + n, len - n } };
  kv_hash_meow128_vec( vec0, 2, &v[ 0 ], &v[ 1 ] );
  meow_vec_t vec1[ 2 ] = { { ar[ 1 ], n }, { ar[ 1 ] + n, len - n } };
  kv_hash_meow128_vec( vec1, 2, &v[ 2 ], &v[ 3 ] );
  meow_vec_t vec2[ 2 ] = { { ar[ 2 ], n }, { ar[ 2 ] + n, len - n } };
  kv_hash_meow128_vec( vec2, 2, &v[ 4 ], &v[ 5 ] );
  meow_vec_t vec3[ 2 ] = { { ar[ 3 ], n }, { ar[ 3 ] + n, len - n } };
  kv_hash_meow128_vec( vec3, 2, &v[ 6 ], &v[ 7 ] );

  const void *par[ 8 ] = {
    ar[ 0 ], ar[ 1 ], ar[ 2 ], ar[ 3 ],
    ar[ 0 ], ar[ 1 ], ar[ 2 ], ar[ 3 ]
  };
  kv_hash_meow128_8_same_length_a( par, len, w );
  for ( n = 0; n < 8; n += 2 ) {
    if ( x[ n ] != y[ n ] ) printf( "2 same failed\n" );
    if ( x[ n ] != d[ n ] ) printf( "2 diff failed\n" );
    if ( x[ n ] != z[ n ] ) printf( "4 same failed\n" );
    if ( x[ n ] != e[ n ] ) printf( "4 diff failed\n" );
    if ( x[ n ] != u[ n ] ) printf( "upd same failed\n" );
    if ( x[ n ] != v[ n ] ) printf( "vec same failed\n" );
    if ( x[ n ] != w[ n ] ) printf( "lrg same failed\n" );
    if ( x[ n ] != w[ n+8 ] ) printf( "lrg same failed 1\n" );
    if ( x[ n+1 ] != y[ n+1 ] ) printf( "2 same failed 2\n" );
    if ( x[ n+1 ] != d[ n+1 ] ) printf( "2 diff failed 2\n" );
    if ( x[ n+1 ] != z[ n+1 ] ) printf( "4 same failed 2\n" );
    if ( x[ n+1 ] != e[ n+1 ] ) printf( "4 diff failed 2\n" );
    if ( x[ n+1 ] != u[ n+1 ] ) printf( "upd same failed 2\n" );
    if ( x[ n+1 ] != v[ n+1 ] ) printf( "vec same failed 2\n" );
    if ( x[ n+1 ] != w[ n+1 ] ) printf( "lrg same failed 2\n" );
    if ( x[ n+1 ] != w[ n+8+1 ] ) printf( "lrg same failed 3\n" );
  }
  for ( n = 0; n < 128; n++ )
    buf[ n ] = n;
  for ( n = 0; n < 128; n++ ) {
    x[ 0 ] = 10101; x[ 1 ] = 20202;
    y[ 0 ] = 10101; y[ 1 ] = 20202;
    kv_hash_meow128( buf, n, &x[ 0 ], &x[ 1 ] );
    kv_hash_meow128( &buf[ n ], 128 - n, &y[ 0 ], &y[ 1 ] );
    d[ 0 ] = 10101; d[ 1 ] = 20202;
    kv_hash_meow128_2_diff_length( &buf[ 0 ], n, &buf[ n ], 128 - n, d );
    if ( x[ 0 ] != d[ 0 ] || x[ 1 ] != d[ 1 ] ||
         y[ 0 ] != d[ 2 ] || y[ 1 ] != d[ 3 ] ) {
      printf( "part2 diff %lu\n", n );
    }
  }
  for ( n = 0; n < 128; n++ ) {
    for ( size_t m = n; m < 128; m++ ) {
      for ( size_t o = m; o < 128; o++ ) {
        x[ 0 ] = 10101; x[ 1 ] = 20202;
        y[ 0 ] = 10101; y[ 1 ] = 20202;
        z[ 0 ] = 10101; z[ 1 ] = 20202;
        u[ 0 ] = 10101; u[ 1 ] = 20202;
        kv_hash_meow128( buf, n, &x[ 0 ], &x[ 1 ] );
        kv_hash_meow128( &buf[ n ], m - n, &y[ 0 ], &y[ 1 ] );
        kv_hash_meow128( &buf[ m ], o - m, &z[ 0 ], &z[ 1 ] );
        kv_hash_meow128( &buf[ o ], 128 - o, &u[ 0 ], &u[ 1 ] );
        d[ 0 ] = 10101; d[ 1 ] = 20202;
        kv_hash_meow128_4_diff_length( &buf[ 0 ], n,
                                       &buf[ n ], m - n,
                                       &buf[ m ], o - m,
                                       &buf[ o ], 128 - o, d );
        if ( x[ 0 ] != d[ 0 ] || x[ 1 ] != d[ 1 ] ||
             y[ 0 ] != d[ 2 ] || y[ 1 ] != d[ 3 ] ||
             z[ 0 ] != d[ 4 ] || z[ 1 ] != d[ 5 ] ||
             u[ 0 ] != d[ 6 ] || u[ 1 ] != d[ 7 ] ) {
          printf( "part4 diff %lu %lu %lu\n", n, m, o );
        }
      }
    }
  }
#endif
  printf( "hash=%s with keylen=%u\n", name, keylen );
  printf( "timing iteration count = %lu\n\n", TEST_COUNT );

  for ( keycount = 16; keycount <= 1024 * 1024; keycount *= 2 ) {
    KeyBufAligned * kb = KeyBufAligned::new_array( NULL, keycount );
    const uint32_t ht_size = (uint32_t) ( (double) keycount / 0.75 );
    uint32_t * cov = (uint32_t *) ::malloc( sizeof( uint32_t ) * ht_size );

    k = 0;
    for ( i = 0; i < keycount; i++ ) {
      kb[ i ].zero();
      fill->nextKey( kb[ i ], keylen );
      k += kb[ i ].kb.keylen;
    }
#if defined( USE_KV_MEOW_HASH )
    if ( meow_x2 ) {
      t1 = current_monotonic_time_s();
      j = 0;
      for ( i = 0; i < TEST_COUNT; i += 2 ) {
        uint64_t h[ 4 ];
        h[ 0 ] = 0; h[ 1 ] = 0;
        kv_hash_meow128_2_same_length( kb[ j ].kb.u.buf,
                                       kb[ j+1 ].kb.u.buf,
                                       kb[ j ].kb.keylen, h );
        j = ( j + 2 ) % keycount;
      }
      t2 = current_monotonic_time_s();
      t2 -= t1;

      ::memset( cov, 0, ht_size * sizeof( cov[ 0 ] ) );
      for ( j = 0; j < keycount; j += 2 ) {
        uint64_t h[ 4 ];
        h[ 0 ] = 0; h[ 1 ] = 0;
        kv_hash_meow128_2_same_length( kb[ j ].kb.u.buf,
                                       kb[ j+1 ].kb.u.buf,
                                       kb[ j ].kb.keylen, h );
        cov[ h[ 1 ] % ht_size ]++;
        cov[ h[ 3 ] % ht_size ]++;
      }
    }
    else if ( meow_x2_diff ) {
      t1 = current_monotonic_time_s();
      j = 0;
      for ( i = 0; i < TEST_COUNT; i += 2 ) {
        uint64_t h[ 4 ];
        h[ 0 ] = 0; h[ 1 ] = 0;
        kv_hash_meow128_2_diff_length( kb[ j ].kb.u.buf,
                                       kb[ j ].kb.keylen,
                                       kb[ j+1 ].kb.u.buf,
                                       kb[ j+1 ].kb.keylen, h );
        j = ( j + 2 ) % keycount;
      }
      t2 = current_monotonic_time_s();
      t2 -= t1;

      ::memset( cov, 0, ht_size * sizeof( cov[ 0 ] ) );
      for ( j = 0; j < keycount; j += 2 ) {
        uint64_t h[ 4 ];
        h[ 0 ] = 0; h[ 1 ] = 0;
        kv_hash_meow128_2_diff_length( kb[ j ].kb.u.buf,
                                       kb[ j ].kb.keylen,
                                       kb[ j+1 ].kb.u.buf,
                                       kb[ j+1 ].kb.keylen, h );
        cov[ h[ 1 ] % ht_size ]++;
        cov[ h[ 3 ] % ht_size ]++;
      }
    }
    else if ( meow_x4 ) {
      t1 = current_monotonic_time_s();
      j = 0;
      for ( i = 0; i < TEST_COUNT; i += 4 ) {
        uint64_t h[ 8 ];
        h[ 0 ] = 0; h[ 1 ] = 0;
        kv_hash_meow128_4_same_length( kb[ j ].kb.u.buf,
                                       kb[ j+1 ].kb.u.buf,
                                       kb[ j+2 ].kb.u.buf,
                                       kb[ j+3 ].kb.u.buf,
                                       kb[ j ].kb.keylen, h );
        j = ( j + 4 ) % keycount;
      }
      t2 = current_monotonic_time_s();
      t2 -= t1;

      ::memset( cov, 0, ht_size * sizeof( cov[ 0 ] ) );
      for ( j = 0; j < keycount; j += 4 ) {
        uint64_t h[ 8 ];
        h[ 0 ] = 0; h[ 1 ] = 0;
        kv_hash_meow128_4_same_length( kb[ j ].kb.u.buf,
                                       kb[ j+1 ].kb.u.buf,
                                       kb[ j+2 ].kb.u.buf,
                                       kb[ j+3 ].kb.u.buf,
                                       kb[ j ].kb.keylen, h );
        cov[ h[ 1 ] % ht_size ]++;
        cov[ h[ 3 ] % ht_size ]++;
        cov[ h[ 5 ] % ht_size ]++;
        cov[ h[ 7 ] % ht_size ]++;
      }
    }
    else if ( meow_x4_diff ) {
      t1 = current_monotonic_time_s();
      j = 0;
      for ( i = 0; i < TEST_COUNT; i += 4 ) {
        uint64_t h[ 8 ];
        h[ 0 ] = 0; h[ 1 ] = 0;
        kv_hash_meow128_4_diff_length( kb[ j ].kb.u.buf,
                                       kb[ j ].kb.keylen,
                                       kb[ j+1 ].kb.u.buf,
                                       kb[ j+1 ].kb.keylen,
                                       kb[ j+2 ].kb.u.buf,
                                       kb[ j+2 ].kb.keylen,
                                       kb[ j+3 ].kb.u.buf,
                                       kb[ j+3 ].kb.keylen, h );
        j = ( j + 4 ) % keycount;
      }
      t2 = current_monotonic_time_s();
      t2 -= t1;

      ::memset( cov, 0, ht_size * sizeof( cov[ 0 ] ) );
      for ( j = 0; j < keycount; j += 4 ) {
        uint64_t h[ 8 ];
        h[ 0 ] = 0; h[ 1 ] = 0;
        kv_hash_meow128_4_diff_length( kb[ j ].kb.u.buf,
                                       kb[ j ].kb.keylen,
                                       kb[ j+1 ].kb.u.buf,
                                       kb[ j+1 ].kb.keylen,
                                       kb[ j+2 ].kb.u.buf,
                                       kb[ j+2 ].kb.keylen,
                                       kb[ j+3 ].kb.u.buf,
                                       kb[ j+3 ].kb.keylen, h );
        cov[ h[ 1 ] % ht_size ]++;
        cov[ h[ 3 ] % ht_size ]++;
        cov[ h[ 5 ] % ht_size ]++;
        cov[ h[ 7 ] % ht_size ]++;
      }
    }
    else if ( meow_x8 ) {
      t1 = current_monotonic_time_s();
      j = 0;
      for ( i = 0; i < TEST_COUNT; i += 8 ) {
        uint64_t h[ 16 ];
        h[ 0 ] = 0; h[ 1 ] = 0;
        kv_hash_meow128_8_same_length( kb[ j ].kb.u.buf,
                                       kb[ j+1 ].kb.u.buf,
                                       kb[ j+2 ].kb.u.buf,
                                       kb[ j+3 ].kb.u.buf,
                                       kb[ j+4 ].kb.u.buf,
                                       kb[ j+5 ].kb.u.buf,
                                       kb[ j+6 ].kb.u.buf,
                                       kb[ j+7 ].kb.u.buf,
                                       kb[ j ].kb.keylen, h );
        j = ( j + 8 ) % keycount;
      }
      t2 = current_monotonic_time_s();
      t2 -= t1;

      ::memset( cov, 0, ht_size * sizeof( cov[ 0 ] ) );
      for ( j = 0; j < keycount; j += 4 ) {
        uint64_t h[ 16 ];
        h[ 0 ] = 0; h[ 1 ] = 0;
        kv_hash_meow128_8_same_length( kb[ j ].kb.u.buf,
                                       kb[ j+1 ].kb.u.buf,
                                       kb[ j+2 ].kb.u.buf,
                                       kb[ j+3 ].kb.u.buf,
                                       kb[ j+4 ].kb.u.buf,
                                       kb[ j+5 ].kb.u.buf,
                                       kb[ j+6 ].kb.u.buf,
                                       kb[ j+7 ].kb.u.buf,
                                       kb[ j ].kb.keylen, h );
        cov[ h[ 1 ] % ht_size ]++;
        cov[ h[ 3 ] % ht_size ]++;
        cov[ h[ 5 ] % ht_size ]++;
        cov[ h[ 7 ] % ht_size ]++;
        cov[ h[ 9 ] % ht_size ]++;
        cov[ h[ 11 ] % ht_size ]++;
        cov[ h[ 13 ] % ht_size ]++;
        cov[ h[ 15 ] % ht_size ]++;
      }
    }
    else {
#endif
      t1 = current_monotonic_time_s();
      j = 0;
      for ( i = 0; i < TEST_COUNT; i++ ) {
        uint64_t h1 = 0, h2 = 0;
        func( kb[ j ].kb.u.buf, kb[ j ].kb.keylen, &h1, &h2 );
        j = ( j + 1 ) % keycount;
      }
      t2 = current_monotonic_time_s();
      t2 -= t1;

      ::memset( cov, 0, ht_size * sizeof( cov[ 0 ] ) );
      for ( j = 0; j < keycount; j++ ) {
        uint64_t h1 = 0, h2 = 0;
        func( kb[ j ].kb.u.buf, kb[ j ].kb.keylen, &h1, &h2 );
        cov[ h1 % ht_size ]++;
      }
#if defined( USE_KV_MEOW_HASH )
    }
#endif
    ::memset( map, 0, sizeof( map ) );
    for ( j = 0; j < ht_size; j++ ) {
      uint32_t x = cov[ j ];
      if ( x >= MAP_COUNT )
        x = MAP_COUNT - 1;
      map[ x ]++;
    }

    printf( "%lu keys, %.1f length, ", keycount,
            (double) k / (double) keycount );
    printf( "hash = %.1fns %.1f/s\n", t2 / (double) TEST_COUNT * 1000000000.0,
            (double) TEST_COUNT / t2 );
    for ( j = 0; j < MAP_COUNT; j++ ) {
      if ( map[ j ] != 0 )
        printf( "[%lu]: %.1f%% ", j,
                (double) map[ j ] * 100.0 / 
                (double) ht_size );
    }
    printf( "(coll @ %.1f%%)\n\n",
            (double) keycount * 100.0 / (double) ht_size );

    delete kb;
    ::free( cov );
  }
  return 0;
}
