#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <raikv/cube_route.h>

using namespace rai;
using namespace kv;

int
main( int, char ** )
{
  CubeRoute128 bis;

  bis.zero();
  bis.w[ 0 ] = 0xffff123457ULL;
  bis.w[ 1 ] = 0x754fff0707ULL;
  bis.print_bits();
  bis.print_pop();
  bis.print_traverse( 0, 20, 0, 128 );

  return 0;
}

