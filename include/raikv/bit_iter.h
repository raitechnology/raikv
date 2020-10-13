#ifndef __rai_raikv__bit_iter_h__
#define __rai_raikv__bit_iter_h__

namespace rai {
namespace kv {

struct BitIter64 {
  const uint64_t w;
  uint8_t i;

  BitIter64( uint64_t word ) : w( word ), i( 0 ) {}

  bool next( void ) {
    uint64_t x = this->w >> ++this->i;
    if ( ( x & 1 ) == 0 ) {
      if ( x == 0 )
        return false;
      this->i += __builtin_ffsl( x ) - 1;
    }
    return true;
  }

  bool first( void ) {
    if ( this->w == 0 )
      return false;
    this->i = 0;
    if ( ( this->w & 1 ) != 0 )
      return true;
    return this->next();
  }
};

}
}

#if 0
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

int
main( int argc, char *argv[] )
{
  BitIter64 bi( atoi( argv[ 1 ] ) );

  if ( bi.first() ) {
    do {
      printf( "%x ", ( 1U << bi.i) );
    } while ( bi.next() );
  }
  printf( " = %x\n", bi.w );
  return 0;
}
#endif

#endif
