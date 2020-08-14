#include <stdio.h>
#include <stdint.h>
#include <string.h>

#include <raikv/pipe_buf.h>

using namespace rai;
using namespace kv;

int
main( int argc, char ** )
{
  static const uint64_t LOOP_CNT = 10 * 1000 * 1000,
                        REC_SIZE = 32;
  SignalHandler sighndl;
  PipeBuf * ping = PipeBuf::open( "ping", argc == 1, 0666 ),
          * pong = PipeBuf::open( "pong", argc == 1, 0666 );

  union {
    uint8_t buf[ REC_SIZE ];
    uint64_t t;
  } x, y, z;
  uint64_t sum = 0, min_val = 0, max_val = 0, val, i;
  if ( ping == NULL || pong == NULL ) {
    fprintf( stderr,
             "start ping side first (argc == 1), pong next (args > 1)\n" );
    return 1;
  }

  sighndl.install();

  ::memset( &x, 0, sizeof( x ) );
  ::memset( &y, 0, sizeof( y ) );
  ::memset( &z, 0, sizeof( z ) );
  if ( argc == 1 ) {
    x.t = 0;
    while ( ! sighndl.signaled && ping->write( &x, REC_SIZE ) == 0 )
      kv_sync_pause();
    while ( ! sighndl.signaled && pong->read( &y, REC_SIZE ) == 0 )
      kv_sync_pause();

    x.t = kv_current_monotonic_time_ns();
    for ( i = 0; ! sighndl.signaled; i++ ) {
      while ( ! sighndl.signaled && ping->write( &x, REC_SIZE ) == 0 )
        kv_sync_pause();
      while ( ! sighndl.signaled && pong->read( &y, REC_SIZE ) == 0 )
        kv_sync_pause();
      if ( y.t == (uint64_t) -1 )
        break;
      x.t = kv_current_monotonic_time_ns();
      val = x.t - y.t;
      if ( max_val == 0 )
        min_val = max_val = val;
      else if ( val > max_val )
        max_val = val;
      else if ( val < min_val )
        min_val = val;
      sum += val;
      if ( i >= LOOP_CNT )
        x.t = (uint64_t) -1;
    }
    printf( "i %ld avg %lu, min %lu, max %lu\n", i, sum / LOOP_CNT,
             min_val, max_val );
  }
  else {
    for ( i = 0; ! sighndl.signaled; i++ ) {
      while ( ! sighndl.signaled && ping->read( &y, REC_SIZE ) == 0 )
        kv_sync_pause();
      while ( ! sighndl.signaled && pong->write( &y, REC_SIZE ) == 0 )
        kv_sync_pause();
      if ( y.t == (uint64_t) -1 )
        break;
    }
    printf( "i %ld\n", i );
  }
  ping->close();
  pong->close();

  return 0;
}
