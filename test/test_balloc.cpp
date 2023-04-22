#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <raikv/balloc.h>

#define lx "%" PRIx64 " "
#define lu "%" PRIu64 " "
using namespace rai;
using namespace kv;

typedef Balloc<8, 1024> Balloc8x1024;

int
main( void )
{
  size_t size[] = { 100, 99, 8, 16, 22, 33, 55, 102, 22, 5, 16, 88, 96, 20, 9, 50 };
  const size_t nsizes = sizeof( size ) / sizeof( size[ 0 ] );
  Balloc8x1024 * ball;
  void *p[ nsizes ];
  size_t i, j;

  ball = new ( ::malloc( sizeof( Balloc8x1024 ) ) ) Balloc8x1024();
  printf( "ball size " lu "units " lu "\n", sizeof( ball ), ball->BA_COUNT );
  for ( i = 0; i < nsizes; i++ ) {
    p[ i ] = ball->alloc( size[ i ] );
    ::memset( p[ i ], i, size[ i ] );
  }
  for ( i = 0; i < ball->N; i++ ) {
    printf( lx, ball->free_size[ i ] );
  }
  printf( "alloced " lu "malloced " lu "\n", ball->alloced, ball->malloced );
  for ( i = 1; i < nsizes; i += 2 ) {
    p[ i ] = ball->resize( p[ i ], size[ i ], size[ i ] / 2 );
    size[ i ] /= 2;
    ::memset( p[ i ], i, size[ i ] );
  }
  for ( i = 0; i < ball->N; i++ ) {
    printf( lx, ball->free_size[ i ] );
  }
  printf( "alloced " lu "malloced " lu "(half)\n", ball->alloced, ball->malloced );

  for ( i = 0; i < nsizes; i++ ) {
    p[ i ] = ball->compact( p[ i ], size[ i ] );
  }
  for ( i = 0; i < ball->N; i++ ) {
    printf( lx, ball->free_size[ i ] );
  }
  printf( "alloced " lu "malloced " lu "(compact)\n", ball->alloced, ball->malloced );

  for ( i = 0; i < nsizes; i += 2 ) {
    p[ i ] = ball->resize( p[ i ], size[ i ], size[ i ] * 2 );
    size[ i ] *= 2;
    ::memset( p[ i ], i, size[ i ] );
  }
  for ( i = 0; i < ball->N; i++ ) {
    printf( lx, ball->free_size[ i ] );
  }
  printf( "alloced " lu "malloced " lu "(double)\n", ball->alloced, ball->malloced );

  for ( i = 0; i < nsizes; i++ ) {
    p[ i ] = ball->compact( p[ i ], size[ i ] );
  }
  for ( i = 0; i < ball->N; i++ ) {
    printf( lx, ball->free_size[ i ] );
  }
  printf( "alloced " lu "malloced " lu "(compact)\n", ball->alloced, ball->malloced );

  for ( i = 0; i < nsizes; i++ ) {
    for ( j = 0; j < size[ i ]; j++ )
      if ( ((uint8_t *) p[ i ])[ j ] != (uint8_t) i )
        printf( "failed size " lu "\n", i );
    ball->release( p[ i ], size[ i ] );
    p[ i ] = NULL;
  }
  for ( i = 0; i < ball->N; i++ ) {
    printf( lx, ball->free_size[ i ] );
  }
  printf( "alloced " lu "malloced " lu "(free)\n", ball->alloced, ball->malloced );

  for ( i = 0; i < nsizes; i++ ) {
    p[ i ] = ball->resize( p[ i ], 0, size[ i ] );
    ::memset( p[ i ], i, size[ i ] );
  }
  for ( i = 0; i < ball->N; i++ ) {
    printf( lx, ball->free_size[ i ] );
  }
  printf( "alloced " lu "malloced " lu "resize(0)\n", ball->alloced, ball->malloced );

  for ( i = 0; i < nsizes; i++ ) {
    p[ i ] = ball->resize( p[ i ], size[ i ], 0 );
  }
  for ( i = 0; i < ball->N; i++ ) {
    printf( lx, ball->free_size[ i ] );
  }
  printf( "alloced " lu "malloced " lu "resize(1)\n", ball->alloced, ball->malloced );

  return 0;
}
