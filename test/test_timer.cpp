#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/ev_net.h>

using namespace rai;
using namespace kv;

struct TimerTest : public EvTimerCallback {
  double last;
  virtual bool timer_cb( uint64_t ,  uint64_t ) noexcept {
    double now = current_realtime_s();
    printf( "timer %.6f\n", now - this->last );
    this->last = now;
    return true;
  }
};

int
main( void )
{
  SignalHandler sighndl;
  EvPoll poll;
  TimerTest test;
  int idle_count = 0;
  poll.init( 5, false );

  test.last = current_realtime_s();
  poll.timer.add_timer_seconds( test, 1, 0, 0 );
  sighndl.install();
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
    poll.wait( idle_count > 255 ? 100 : 0 );
    if ( sighndl.signaled )
      poll.quit++;
  }
  return 0;
}

