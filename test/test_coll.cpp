#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/uint_ht.h>
#include <raikv/key_hash.h>
#include <raikv/util.h>

using namespace rai;
using namespace kv;

int
main( int argc, char *argv[] )
{
  bool wild = ( argc > 1 && argv[ 1 ][ 0 ] == '-' && argv[ 1 ][ 1 ] == 'w' );
  char buf[ 32 ];
  UIntHashTab * ht = UIntHashTab::resize( NULL );
  for ( uint32_t i = 0 /*1371838*/; i < 2001000; i++ ) {
    size_t n = uint32_to_string( i, buf );
    if ( wild )
      buf[ n++ ] = '.';
    uint32_t h = kv_crc_c( buf, n, 0 ), val;
    size_t pos;
    if ( ht->find( h, pos, val ) ) {
      printf( "collision: %u %u (0x%x)\n", i, val, h );
    }
    else {
      ht->set_rsz( ht, h, pos, i );
    }
  }
  return 0;
}

