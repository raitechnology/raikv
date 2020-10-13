#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <raikv/delta_coder.h>
#include <raikv/util.h>

using namespace rai;
using namespace kv;

void
print_values( uint32_t *values,  uint32_t cnt )
{
  printf( "%u: [", cnt );
  for ( size_t j = 0; j < cnt; j++ )
    printf( "%x, ", values[ j ] );
  printf( "]\n" );
}

int
main( int, char ** )
{
  static const uint32_t NVALS = 10000;
  uint32_t i, j, cnt, x;
  uint32_t values[ NVALS ], values2[ NVALS ], code[ NVALS ];
  uint64_t t1, t2;
  rand::xorshift1024star rng;
  DeltaCoder dc;

  rng.init();
  for ( i = 1; i < 16; i++ ) {
    values[ i - 1 ] = i * 3;
    cnt = dc.encode_stream( i, values, 0, code );
    printf( "%u -> %u: ", i, cnt );
    for ( j = 0; j < cnt; j++ ) {
      printf( "%x ", code[ j ] );
    }
    printf( " .. " );
    cnt = dc.decode_stream( cnt, code, 0, values2 );
    for ( j = 0; j < cnt; j++ ) {
      printf( "%u ", values2[ j ] );
    }
    printf( "\n          " );
    for ( x = 0; x < 45; x += 9 ) {
      printf( "{%u:%s} ", x,
              dc.test_stream( cnt, code, 0, x ) ? "t" : "f" );
    }
    printf( "\n" );
  }

  values[ 0 ] = rng.next() % 10;
  for ( i = 1; i < NVALS; i++ ) {
    values[ i ] = values[ i-1 ] + 1 + rng.next() % 8;
    /*if ( ( i % 25 ) == 0 )
      values[ i ] += rand() % 256000;*/
  }
  //print_values( values, NVALS );
  printf( "encode_stream_length = %u\n",
          dc.encode_stream_length( NVALS, values, 0 ) );

  t1 = kv::current_monotonic_time_ns();
  cnt = 0;
  for ( i = 0; i < 10; i++ )
    cnt += dc.encode_stream( NVALS, values, 0, code );
  t2 = kv::current_monotonic_time_ns();
  cnt /= 10;
  printf( "encoded: %u vals into %u codes (%.1fns/val)\n", NVALS, cnt,
          (double) ( t2 - t1 ) / (double) ( NVALS * 10 ) );
  //print_values( code, cnt );

  printf( "decode_stream_length = %u\n",
          dc.decode_stream_length( cnt, code ) );

  t1 = kv::current_monotonic_time_ns();
  i = 0;
  for ( j = 0; j < 10; j++ )
    i += dc.decode_stream( cnt, code, 0, values2 );
  t2 = kv::current_monotonic_time_ns();
  i /= 10;
  printf( "decoded: %u codes into %u vals (%.1fns/val)\n", cnt, i,
          (double) ( t2 - t1 ) / (double) ( NVALS * 10 ) );

  for ( j = 0; j < i; j++ ) {
    if ( values[ j ] != values2[ j ] ) {
      printf( "different (%u)!!\n", j );
      //print_values( values2, i );
    }
    assert( values[ j ] == values2[ j ] );
  }
  if ( i != NVALS )
    printf( "nvals != i (%u)\n", i );
  assert( i == NVALS );

  return 0;
}

