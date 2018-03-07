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
    if ( ++kb.buf[ --j ] <= '9' )
      break;
    kb.buf[ j ] = '0';
    if ( j == 0 ) {
      ::memmove( &kb.buf[ 1 ], kb.buf, kb.keylen );
      kb.buf[ 0 ] = '1';
      kb.keylen++;
      break;
    }
  }
}

struct Content {
  const char *name;
  Content( const char *n ) : name( n ) {}
  virtual void nextKey( KeyFragment &kb,  uint8_t keylen ) {}
};

struct RandContent : public Content {
  rand::xorshift1024star rand;
  RandContent() : Content( "rand" ) {
    this->rand.init();
  }

  virtual void nextKey( KeyFragment &kb,  uint8_t keylen ) {
    kb.keylen = this->rand.nextByte() % (keylen*2) + 1;
    for ( uint8_t j = 0; j < kb.keylen; j++ ) {
      kb.buf[ j ] = "abcdefghijklmnopqrstuvwxyz.:0123456789"[
        this->rand.nextByte() % 38 ];
    }
  }
};

struct IntContent : public Content {
  uint64_t counter;
  IntContent() : Content( "int" ), counter( 0 ) {}

  virtual void nextKey( KeyFragment &kb,  uint8_t keylen ) {
    kb.keylen = keylen;
    uint8_t j = 0;
    do {
      ::memcpy( &kb.buf[ j ], &this->counter, sizeof( this->counter ) );
      this->counter++;
      j += sizeof( this->counter );
    } while ( j < keylen );
  }
};

struct IncrContent : public Content {
  uint64_t counter;
  IncrContent() : Content( "incr" ), counter( 0 ) {}

  virtual void nextKey( KeyFragment &kb,  uint8_t keylen ) {
    if ( keylen < 2 ) keylen = 2;
    kb.keylen = keylen;
    uint8_t j = 0;
    do {
      kb.buf[ j++ ] = '0';
    } while ( j < keylen - 1 );
    kb.buf[ j ] = 0;
    for ( uint64_t k = this->counter++; ; ) {
      kb.buf[ --j ] += ( k % 10 );
      k /= 10;
      if ( j == 0 || k == 0 )
        break;
    }
    return;
  }
};

int
main( int argc, char *argv[] )
{
  static const uint32_t MAP_COUNT = 256;
  kv_hash64_func_t func   = kv_hash_murmur64_a;
  kv_hash128_func_t func2 = NULL;
  uint64_t i, j, k, keycount;
  uint32_t map[ MAP_COUNT ];
  uint16_t keylen = 32;
  double      t1, t2;
  IntContent  ifill;
  RandContent rfill;
  IncrContent xfill;
  Content   * fill;
  const uint64_t TEST_COUNT = 10000000;

  if ( argc == 1 ) {
    fprintf( stderr, "%s (int|rand|incr) (cm|ch|xxh|murmur) (keylen)\n",
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

  if ( argc > 2 && ::strcmp( argv[ 2 ], "cm" ) == 0 ) {
    func2 = kv_hash_citymur128;
    printf( "hash=citymur128 " );
  }
  else if ( argc > 2 && ::strcmp( argv[ 2 ], "xxh" ) == 0 ) {
    func = kv_hash_xxh64;
    printf( "hash=xxh64 " );
  }
  else if ( argc > 2 && ::strcmp( argv[ 2 ], "ch" ) == 0 ) {
    func = kv_hash_cityhash64;
    printf( "hash=cityhash64 " );
  }
  else {
    func = kv_hash_murmur64_a;
    printf( "hash=murmur " );
  }

  if ( argc > 3 )
    keylen = atoi( argv[ 3 ] );
  printf( "with keylen=%u\n", keylen );

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
    if ( func2 != NULL ) {
      for ( i = 0; i < TEST_COUNT; i++ ) {
        kb[ j ].hash128( func2 );
        j = ( j + 1 ) % keycount;
      }
    }
    else {
      for ( i = 0; i < TEST_COUNT; i++ ) {
        kb[ j ].hash( func );
        j = ( j + 1 ) % keycount;
      }
    }
    t2 = current_monotonic_time_s();
    t2 -= t1;

    ::memset( cov, 0, ht_size * sizeof( cov[ 0 ] ) );
    if ( func2 != NULL ) {
      for ( j = 0; j < keycount; j++ )
        cov[ kb[ j ].hash128( func2 ) % ht_size ]++;
    }
    else {
      for ( j = 0; j < keycount; j++ )
        cov[ kb[ j ].hash( func ) % ht_size ]++;
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

