#include <stdio.h>
#include <stdint.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <raikv/zipf.h>
#include <raikv/util.h>

using namespace rai;
using namespace kv;

int
main( void )
{
  rand::xoroshiro128plus r;
  ZipfianGen<99,100,rand::xoroshiro128plus> zgen( 1000, r );
  uint64_t sum[ 1000 ], j = 0;
  int i;

  r.init();
  ::memset( sum, 0, sizeof( sum ) );
  /*double t = kv_current_monotonic_time_s();*/
  for ( i = 0; i < 1000; i++ ) {
    uint64_t j = zgen.next();
    sum[ j ]++;
  }
  /*t = kv_current_monotonic_time_s() - t;*/
  for ( i = 0; i < 1000; i++ )
    printf( "%d %" PRIu64 " %" PRIu64 "\n", i, j += sum[ i ], sum[ i ] );
  /*&printf( "%.3f\n", t * 100.0 );*/

  return 0;
}
