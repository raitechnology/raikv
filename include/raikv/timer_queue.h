#ifndef __rai_raikv__timer_queue_h__
#define __rai_raikv__timer_queue_h__

#include <raikv/ev_net.h>

namespace rai {
namespace kv {

enum TimerUnits {
  IVAL_SECS   = 0,
  IVAL_MILLIS = 1,
  IVAL_MICROS = 2,
  IVAL_NANOS  = 3
};

struct EvTimerEvent {
  int      id;          /* owner of event (fd) */
  uint32_t ival;        /* interval with lower 2 bits containing units */
  uint64_t timer_id,    /* if multiple timer events for each owner */
           next_expire, /* next expiration time */
           event_id;

  static bool is_greater( EvTimerEvent e1,  EvTimerEvent e2 ) {
    return e1.next_expire > e2.next_expire; /* expires later */
  }
  bool operator==( const EvTimerEvent &el ) const {
    return this->id == el.id && this->timer_id == el.timer_id &&
           this->event_id == el.event_id;
  }
};

struct EvTimerQueue : public EvSocket {
  static const int64_t MAX_DELTA = 100 * 1000; /* 100 us */

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  kv::PrioQueue<EvTimerEvent, EvTimerEvent::is_greater> queue;
  uint64_t last, epoch, delta, real;

  EvTimerQueue( EvPoll &p );
  static EvTimerQueue *create_timer_queue( EvPoll &p ) noexcept;

  /* add timer that expires in ival seconds */
  bool add_timer_seconds( int id,  uint32_t ival,  uint64_t timer_id,
                          uint64_t event_id ) {
    return this->add_timer_units( id, ival, IVAL_SECS, timer_id, event_id );
  }
  /* add timer that expires in ival units */
  bool add_timer_units( int id,  uint32_t ival,  TimerUnits u,
                        uint64_t timer_id,  uint64_t event_id ) noexcept;
  /* if timer_id exists remove and return true */
  bool remove_timer( int id,  uint64_t timer_id,  uint64_t event_id ) noexcept;
  void repost( void ) noexcept;
  bool set_timer( void ) noexcept;

  uint64_t busy_delta( uint64_t curr_ns ) {
    if ( this->delta > 0 ) {
      uint64_t d = this->delta, sav = curr_ns;
      curr_ns -= this->real;
      /* coarse real time expires, adjust monotonic time */
      if ( d <= curr_ns ) {
        uint64_t mon_ns = kv::current_monotonic_time_ns();
        if ( mon_ns >= this->epoch + d )
          return 0;
        this->real -= ( this->epoch + d ) - mon_ns;
        curr_ns = sav - this->real;
      }
      curr_ns = d - curr_ns;
      if ( curr_ns < MAX_DELTA )
        return curr_ns;
    }
    return MAX_DELTA;
  }

  virtual void write( void ) noexcept final;
  virtual void read( void ) noexcept final;
  virtual void process( void ) noexcept final;
  virtual void release( void ) noexcept final;
};

}
}
#endif
