#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#ifndef _MSC_VER
#include <unistd.h>
#else
#include <io.h>
#endif
#include <raikv/ev_net.h>
#include <raikv/logger.h>
#include <raikv/array_space.h>

#ifdef _MSC_VER
#define dup _dup
#define write _write
#endif

using namespace rai;
using namespace kv;

struct LogTest : public EvTimerCallback {
  Logger & log;
  int      out_fd,
           log_trunc;
  ArrayCount<char, 256> spc;
  LogTest( Logger &l ) : log( l ), out_fd( -1 ), log_trunc( 0 ) {}
  virtual bool timer_cb( uint64_t ,  uint64_t ) noexcept;
  void log_output( int stream,  uint64_t stamp,  size_t len,
                   const char *buf ) noexcept;
  void flush_output( void ) noexcept;
};

void
LogTest::log_output( int stream,  uint64_t stamp,  size_t len,
                     const char *buf ) noexcept
{
  this->log.update_timestamp( stamp );
  size_t sz = len + Logger::TSHDR_LEN;
  char * p  = this->spc.make( this->spc.count + sz );
  p = &p[ this->spc.count ];
  this->spc.count += sz;
  ::memcpy( p, this->log.ts, Logger::TSERR_OFF );
  p = &p[ Logger::TSERR_OFF ];
  *p++ = ( stream == 1 ? ' ' : '!' );
  *p++ = ' ';
  ::memcpy( p, buf, len );
}

void
LogTest::flush_output( void ) noexcept
{
  if ( this->spc.count > 0 ) {
    uint32_t len = (uint32_t) this->spc.count;
    if ( (uint32_t) ::write( this->out_fd, this->spc.ptr, len ) != len ||
         this->spc.count > (size_t) len )
      this->log_trunc = 1;
    this->spc.clear();
  }
}

bool
LogTest::timer_cb( uint64_t timer_id,  uint64_t ) noexcept
{
  if ( timer_id == 1 ) {
    uint64_t ival;
    rand::fill_urandom_bytes( &ival, sizeof( ival ) );
    printf( "hello %016" PRIx64 "\n", ival );
    return true;
  }
  char     out[ 4 * 1024 ],
           err[ 4 * 1024 ];
  size_t   out_len   = sizeof( out ),
           err_len   = sizeof( err );
  uint64_t out_stamp = 0,
           err_stamp = 0;
  bool     out_done  = false,
           err_done  = false,
           b         = false;
  if ( ! log.avail() )
    return true;
  while ( ! out_done || ! err_done ) {
    if ( ! out_done && out_stamp == 0 ) {
      out_stamp = log.read_stdout( out, out_len );
      if ( out_stamp == 0 )
        out_done = true;
    }
    if ( ! err_done && err_stamp == 0 ) {
      err_stamp = log.read_stderr( err, err_len );
      if ( err_stamp == 0 )
        err_done = true;
    }
    bool do_out = ! out_done, do_err = ! err_done;
    if ( do_out && do_err ) {
      if ( out_stamp < err_stamp )
        do_err = false;
      else
        do_out = false;
    }
    if ( do_out ) {
      if ( out_len > 1 || out[ 0 ] != '\n' )
        this->log_output( 1, out_stamp, out_len, out );
      out_stamp = 0; out_len = sizeof( out );
      b = true;
    }
    if ( do_err ) {
      if ( err_len > 1 || err[ 0 ] != '\n' )
        this->log_output( 2, err_stamp, err_len, err );
      err_stamp = 0; err_len = sizeof( err );
      b = true;
    }
  }
  if ( b )
    this->flush_output();
  return true;
}

int
main( int argc, char *argv[] )
{
  SignalHandler sighndl;
  EvPoll   poll;
  Logger & log = *Logger::create();
  LogTest  test( log );
  int idle_count = 0;

  poll.init( 5, false );
  sighndl.install();
  if ( argc <= 1 )
    poll.timer.add_timer_millis( test, 100, 0, 0 );
  else {
    if ( log.output_log_file( argv[ 1 ] ) != 0 ) {
      perror( argv[ 1 ] );
      return 1;
    }
  }
  test.out_fd = ::dup( 1 );
  log.start_ev( poll );
  poll.timer.add_timer_seconds( test, 1, 1, 0 );
  for (;;) {
    /* loop 5 times before quiting, time to flush writes */
    if ( poll.quit >= 5 && idle_count > 0 )
      break;
    /* dispatch network events */
    int idle = poll.dispatch();
    if ( idle == EvPoll::DISPATCH_IDLE )
      idle_count++;
    else
      idle_count = 0;
    /* wait for network events */
    poll.wait( idle_count > 2 ? 100 : 0 );
    if ( sighndl.signaled )
      poll.quit++;
  }
  return 0;
}

