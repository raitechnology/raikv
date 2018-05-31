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
  virtual void nextKey( KeyFragment &kb,  uint8_t keylen ) {}
  void operator delete( void *ptr ) {}
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
  void operator delete( void *ptr ) {}
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
  void operator delete( void *ptr ) {}
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
  void operator delete( void *ptr ) {}
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
  const char * name = "aes";
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
  printf( "content=%s ", fill->name );
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
#if defined( USE_KV_SPOOKY_HASH )
  if ( ::strcmp( name, "spooky" ) == 0 )
    func = kv_hash_spooky128;
  else
#endif
#if defined( USE_KV_MURMUR_HASH )
  if ( ::strcmp( name, "murmur" ) == 0 )
    func = kv_hash_murmur128;
#endif
  if ( func == NULL ) {
    fprintf( stderr, "hash %s not available\n", name );
    goto cmd_error;
  }
  if ( argc > 3 ) {
    keylen = atoi( argv[ 3 ] );
    if ( keylen == 0 )
      goto cmd_error;
  }
  printf( "hash=%s with keylen=%u\n", name, keylen );
  printf( "timing iteration count = %lu\n\n", TEST_COUNT );

  for ( keycount = 16; keycount <= 1024 * 1024; keycount *= 2 ) {
    KeyBufAligned * kb = KeyBufAligned::new_array( keycount );
    const uint32_t ht_size = (uint32_t) ( (double) keycount / 0.75 );
    uint32_t * cov = (uint32_t *) ::malloc( sizeof( uint32_t ) * ht_size );

    k = 0;
    for ( i = 0; i < keycount; i++ ) {
      kb[ i ].zero();
      fill->nextKey( kb[ i ], keylen );
      k += kb[ i ].kb.keylen;
    }

    t1 = current_monotonic_time_s();
    j = 0;
    for ( i = 0; i < TEST_COUNT; i++ ) {
      uint64_t h1 = 0, h2 = 0;
      kb[ j ].hash( h1, h2, func );
      j = ( j + 1 ) % keycount;
    }
    t2 = current_monotonic_time_s();
    t2 -= t1;

    ::memset( cov, 0, ht_size * sizeof( cov[ 0 ] ) );
    for ( j = 0; j < keycount; j++ ) {
      uint64_t h1 = 0, h2 = 0;
      kb[ j ].hash( h1, h2, func );
      cov[ h1 % ht_size ]++;
    }
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
