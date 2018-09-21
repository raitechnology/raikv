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

namespace rai {
namespace xx {
struct IntVal {
  int i;
  IntVal( const int j = 0 ) : i( j ) {}
  operator int() const { return i; }
  static bool is_greater( IntVal *x,  IntVal *y ) {
    return x->i > y->i;
  }
};
}
}

using namespace xx;

int
main( int, char ** )
{
  PrioQueue<IntVal *, IntVal::is_greater> queue( 128 );
  static const int N = 100;
  IntVal test[ N ];
  int ar[ N ];
  int j, k;

  printf( "adding %d numbers to queue, range 0 -> 224\n", N );
  for ( j = 0; j < N; j++ ) {
    ar[ j ] = ( rand() % 224 );
    test[ j ].i = ar[ j ];
    queue.push( &test[ j ] );
  }

  printf( "removing range 100 -> 200\n" );
  for ( j = 100; j < 200; j++ ) {
    for ( k = 0; k < N; k++ )
      if ( test[ k ].i == j )
        queue.remove( &test[ k ] );
  }
  printf( "popping remaining numbers\n" );
  while ( ! queue.is_empty() ) {
    IntVal *x = queue.pop();
    printf( "%d ", (int) *x );
  }
  printf( "\n" );
  printf( "original set\n" );
  qsort( ar, N, sizeof( ar[ 0 ] ), cmpint );
  for ( j = 0; j < N; j++ )
    printf( "%d ", ar[ j ] );
  printf( "\n" );

  return 0;
}
