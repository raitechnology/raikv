#include <stdio.h>
#include <stdint.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <string.h>
#include <stdlib.h>
#include <raikv/bit_set.h>

using namespace rai;
using namespace kv;

int
do_test( const uint32_t *test,  const uint32_t *test2,  size_t cnt )
{
  BitSpace bits, bits2, bits3;
  uint32_t b, i;
  int fail = 0;

  bits.add( test, cnt );
  bits2.add( test2, cnt );

  if ( bits.count() != cnt ) {
    printf( "bits failed count\n" );
    fail++;
  }
  if ( bits2.count() != cnt ) {
    printf( "bits2 failed count\n" );
    fail++;
  }
#if 0
  i = 0;
  for ( b = 60; bits.rotate( b ); ) {
    bits3.add( b );
    if ( ++i == cnt )
      break;
  }
  if ( bits != bits3 ) {
    printf( "rotate equal failed\n" );
    fail++;
  }
#endif
  i = 0;
  if ( bits.first( b ) ) {
    do {
      if ( b != test[ i++ ] ) {
        printf( "failed test\n" );
        fail++;
      }
    } while ( bits.next( b ) );
  }
  if ( i != cnt ) {
    printf( "failed count 2\n" );
    fail++;
  }
  bits3.and_bits( bits, bits2 );
  if ( bits3.first( b ) ) {
    do {
      if ( ! bits.is_member( b ) || ! bits2.is_member( b ) ) {
        printf( "and failed\n" );
        fail++;
      }
    } while ( bits3.next( b ) );
  }
  bits3.or_bits( bits, bits2 );
  if ( bits3.first( b ) ) {
    do {
      if ( ! bits.is_member( b ) && ! bits2.is_member( b ) ) {
        printf( "or failed\n" );
        fail++;
      }
    } while ( bits3.next( b ) );
  }
  return fail;
}

int
main( void )
{
  const uint32_t test[] = {
    11 , 12 , 19 , 31 , 32 , 65 ,
    66 , 67 , 79 , 80 , 85 , 86
  };
  const uint32_t test2[] = {
    31 , 32 , 65 , 66 , 68 , 69 ,
    70 , 72 , 79 , 80 , 85 , 86
  };
  int fail;

  fail = do_test( test, test2, sizeof( test ) / sizeof( test[ 0 ] ) );

  for ( uint8_t b = 2; b < 17; b++ ) {
    uint64_t mask = ( 1 << b ) - 1;
    UIntArraySpace ival( b );
    for ( size_t i = 0; i < 100000; i++ ) {
      ival.set( i, i % mask );
    }
    for ( size_t i = 0; i < 100000; i++ ) {
      if ( ival.get( i ) != ( i % mask ) ) {
        printf( "bits %u offset %" PRIu64 " not correct: %" PRIu64 "\n", b, i,
                ival.get( i ) );
        fail = 1;
      }
    }
    /*printf( "bits: %u\n", b );
    for ( size_t i = 0; i < 100; i++ ) {
      printf( "index = %" PRIu64 ", size = %" PRIu64 "\n", i, ival.index_word_size( i + 1 ) );
    }*/
  }
  uint64_t n[ 2 ] = { 0, 0 };
  BitSetT<uint64_t> x( n );
  uint32_t b, next = 0, max = sizeof( n ) * 8;
  while ( x.set_first( b, max ) ) {
    if ( next != b ) {
      printf( "%u is wrong set_first\n", b );
      fail = 1;
    }
    next = b + 1;
  }
  x.remove( 8 ); x.remove( 18 );
  if ( ! x.set_first( b, max ) || b != 8 ) {
    printf( "%u should be 8\n", b );
    fail = 1;
  }
  if ( ! x.set_first( b, max ) || b != 18 ) {
    printf( "%u should be 18\n", b );
    fail = 1;
  }
  n[ 0 ] = 0; n[ 1 ] = 0;
  x.add( 15 ); x.add( 19 ); x.add( 66 ); x.add( 78 );
  uint32_t j, count = x.count( max );
  if ( count != 4 ) {
    printf( "count = %u\n", count );
    fail = 1;
  }
  if ( ! x.index( j, 0, max ) || j != 15 ) {
    printf( "j = %u\n", j );
    fail = 1;
  }
  if ( ! x.index( j, 1, max ) || j != 19 ) {
    printf( "j = %u\n", j );
    fail = 1;
  }
  if ( ! x.index( j, 2, max ) || j != 66 ) {
    printf( "j = %u\n", j );
    fail = 1;
  }
  if ( ! x.index( j, 3, max ) || j != 78 ) {
    printf( "j = %u\n", j );
    fail = 1;
  }
  if ( x.index( j, 4, max ) ) {
    printf( "j = %u\n", j );
    fail = 1;
  }

  return fail;
}

