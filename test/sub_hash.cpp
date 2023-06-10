#include <stdio.h>
#define __STDC_FORMAT_MACROS
#include <stdint.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

#include <raikv/key_hash.h>

int
main( int argc, char *argv[] )
{
  for ( int i = 1; i < argc; i++ ) {
    printf( "0x%08x %s\n", kv_crc_c( argv[ i ], ::strlen( argv[ i ] ), 0 ),
            argv[ i ] );
  }
  return 0;
}
