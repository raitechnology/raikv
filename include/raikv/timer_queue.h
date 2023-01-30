#ifndef __rai_raikv__timer_queue_h__
#define __rai_raikv__timer_queue_h__

#ifndef _MSC_VER
#define HAVE_TIMERFD
#endif
#include <raikv/ev_net.h>

namespace rai {
namespace kv {
/* a priority queue of these */
struct EvTimerEvent {
  int32_t  id;          /* owner of event (fd), negative is a EvTimerCallback */
  uint32_t ival;        /* interval with lower 2 bits containing units */
  uint64_t timer_id,    /* if multiple timer events for each owner */
           next_expire, /* next expiration time */
           event_id;    /* timer_id + event_id identifies the timer to owner */

  static bool is_greater( EvTimerEvent e1,  EvTimerEvent e2 ) {
    return e1.next_expire > e2.next_expire; /* expires later */
  }
  bool operator==( const EvTimerEvent &el ) const {
    return this->id == el.id && this->timer_id == el.timer_id &&
           this->event_id == el.event_id;
  }
};
/* callbacks cannot disappear between epochs, fd based connections that can
 * close between epochs should use fd based timers with unique timer ids */
/*struct EvTimerCallback {
  virtual bool timer_cb( uint64_t timer_id,  uint64_t event_id ) noexcept;
};*/

struct EvTimerQueue : public EvSocket {
  static const int64_t MAX_DELTA = 100 * 1000; /* 100 us */

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  kv::PrioQueue<EvTimerEvent, EvTimerEvent::is_greater> queue; /* q events */
  uint64_t           epoch, /* current epoch */
                     expires; /* next delta expires */
  EvTimerCallback ** cb;    /* callbacks when event.id < 0 */
  size_t             cb_sz, /* extent of cb[] */
                     cb_used; /* number of cb[] used */
  bool               processing_timers;

  EvTimerQueue( EvPoll &p );
  static EvTimerQueue *create_timer_queue( EvPoll &p ) noexcept;

  /* add timer that expires in ival units */
  bool add_timer_units( int32_t id,  uint32_t ival,  TimerUnits u,
                        uint64_t timer_id,  uint64_t event_id ) noexcept;
  /* add timer that expires in ival units */
  bool add_timer_cb( EvTimerCallback &tcb,  uint32_t ival,  TimerUnits u,
                     uint64_t timer_id,  uint64_t event_id ) noexcept;
  /* if timer_id exists remove and return true */
  bool remove_timer( int32_t id,  uint64_t timer_id,
                     uint64_t event_id ) noexcept;
  bool remove_timer_cb( EvTimerCallback &tcb,  uint64_t timer_id,
                        uint64_t event_id ) noexcept;
  void repost( EvTimerEvent &ev ) noexcept;
  bool set_timer( void ) noexcept;
#ifndef HAVE_TIMERFD
  /* limit how long to set poll timer, only necessary when no timerfd */
  bool is_timer_ready( uint64_t &us ) {
    if ( this->delta > 0 ) {
      uint64_t curr_ns = current_monotonic_time_ns(),
               us_left;
      if ( curr_ns >= this->expires ) {
        us = 0;
        return true;
      }
      if ( us != 0 ) {
        us_left = ( this->expires - curr_ns ) / 1000;
        if ( us < 0 || us_left < (uint64_t) us ) {
          if ( (us = (int) us_left) == 0 )
            us = 1;
        }
      }
    }
    return false;
  }
#endif
  /* limit how long to dispatch poll events */
  uint64_t busy_delta( uint64_t mono_ns ) {
    if ( this->sock_state == 0 && this->expires != 0 ) {
      if ( this->expires <= mono_ns )
        return 0;
      return mono_ns - this->expires;
    }
    return MAX_DELTA;
  }
  virtual void write( void ) noexcept;   /* no write method */
  virtual void read( void ) noexcept;    /* read empties timerfd */
  virtual void process( void ) noexcept; /* execute timer callbacks */
  virtual void release( void ) noexcept;
};

}
}
#endif
