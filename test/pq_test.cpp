#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <raikv/prio_queue.h>

using namespace rai;
using namespace kv;

int
cmpint( const void *p,  const void *q )
{
  return *(const int *) p - *(const int *) q;
}

int
main( int argc, char *argv[] )
{
  PrioQueue<int> queue( 128 );
  static const int N = 100;
  int ar[ N ];
  int j;

  printf( "adding %d numbers to queue, range 0 -> 224\n", N );
  for ( j = 0; j < N; j++ )
    queue.push( ar[ j ] = ( rand() % 224 ) );

  printf( "removing range 100 -> 200\n" );
  for ( j = 100; j < 200; j++ )
    while ( queue.remove( j ) )
      ;
  printf( "popping remaining numbers\n" );
  while ( ! queue.is_empty() ) {
    int x = queue.pop();
    printf( "%d ", x );
  }
  printf( "\n" );
  printf( "original set\n" );
  qsort( ar, N, sizeof( ar[ 0 ] ), cmpint );
  for ( j = 0; j < N; j++ )
    printf( "%d ", ar[ j ] );
  printf( "\n" );

  return 0;
}
