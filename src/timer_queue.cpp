#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
#include <unistd.h>
#include <sys/time.h>
#include <sys/timerfd.h>
#else
#include <raikv/win.h>
#endif
#include <raikv/timer_queue.h>
#include <raikv/util.h>

using namespace rai;
using namespace kv;

EvTimerQueue::EvTimerQueue( EvPoll &p )
            : EvSocket( p, p.register_type( "timer_queue" ) ),
              epoch( 0 ), cb( 0 ), cb_sz( 0 ),
              cb_used( 0 ), cb_free( 0 ), processing_timers( false )
{
#ifdef HAVE_TIMERFD
  this->sock_opts = OPT_READ_HI;
#else
  this->sock_opts = OPT_NO_POLL;
#endif
}

bool
EvTimerCallback::timer_cb( uint64_t, uint64_t ) noexcept
{
  return false;
}

EvTimerQueue *
EvTimerQueue::create_timer_queue( EvPoll &p ) noexcept
{
  int tfd;

#ifdef HAVE_TIMERFD
  tfd = ::timerfd_create( CLOCK_MONOTONIC, TFD_NONBLOCK );
#else
  tfd = p.get_null_fd();
#endif
  if ( tfd == -1 ) {
    perror( "timerfd_create() failed" );
    return NULL;
  } 
  
  void * m = aligned_malloc( sizeof( EvTimerQueue ) );
  if ( m == NULL ) {
    perror( "alloc timer queue" );
  fini:;
#ifdef HAVE_TIMERFD
    ::close( tfd );
#else
    ::wp_close_fd( tfd );
#endif
    return NULL;
  }
  EvTimerQueue * q = new ( m ) EvTimerQueue( p );
  q->PeerData::init_peer( p.get_next_id(), tfd, -1, NULL, "timer" );
  q->expires = 0;
  q->epoch   = current_monotonic_time_ns();
  if ( p.add_sock( q ) < 0 ) {
    printf( "failed to add timer %d\n", tfd );
    goto fini;
  }
  return q;
}

static const uint32_t to_ns[] = { 1000 * 1000 * 1000, 1000 * 1000, 1000, 1 };

bool
EvTimerQueue::add_timer_units( int32_t id,  uint32_t ival,  TimerUnits u,
                               uint64_t timer_id,  uint64_t event_id ) noexcept
{
  EvTimerEvent el;
  el.id          = id;
  el.ival        = ( ival << 2 ) | (uint32_t) u;
  el.timer_id    = timer_id;
  el.next_expire = current_monotonic_time_ns() +
                   ( (uint64_t) ival * (uint64_t) to_ns[ u ] );
  el.event_id    = event_id;
  if ( ( el.ival >> 2 ) != ival ) {
    fprintf( stderr, "invalid timer range %u\n", ival );
    return false;
  }
  if ( ! this->queue.push( el ) ) {
    fprintf( stderr, "timer queue alloc failed\n" );
    return false;
  }
  if ( ! this->processing_timers )
    this->idle_push( EV_PROCESS );
  return true;
}

bool
EvTimerQueue::add_timer_cb( EvTimerCallback &tcb,  uint32_t ival,  TimerUnits u,
                            uint64_t timer_id,  uint64_t event_id ) noexcept
{
  uint32_t id = this->cb_used;
  if ( id < this->cb_sz ) {
    if ( this->cb[ id ] != NULL ) {
      for ( id = this->cb_free; id < this->cb_sz; id++ ) {
        if ( this->cb[ id ] == NULL ) {
          this->cb_free = id + 1;
          break;
        }
      }
      if ( id == this->cb_sz )
        this->cb_free = this->cb_sz;
    }
  }
  if ( id == this->cb_sz ) {
    size_t new_sz = ( this->cb_sz == 0 ? 8 : (size_t) this->cb_sz * 2 );
    if ( ( new_sz >> 31 ) != 0 ) /* int range */
      return false;
    void * p = ::realloc( this->cb, sizeof( this->cb[ 0 ] ) * new_sz );
    if ( p == NULL )
      return false;
    this->cb = (EvTimerCallback **) p;
    ::memset( &this->cb[ this->cb_sz ], 0,
              sizeof( this->cb[ 0 ] ) * ( new_sz - this->cb_sz ) );
    this->cb_sz = new_sz;
  }
  if ( this->add_timer_units( -((int32_t)id+1), ival, u, timer_id, event_id ) ) {
    this->cb[ id ] = &tcb;
    this->cb_used++;
    return true;
  }
  return false;
}

bool
EvTimerQueue::remove_timer( int32_t id,  uint64_t timer_id,
                            uint64_t event_id ) noexcept
{
  EvTimerEvent el;
  el.id          = id;
  el.ival        = 0;
  el.timer_id    = timer_id;
  el.next_expire = 0;
  el.event_id    = event_id;
  return this->queue.remove( el );
}

bool
EvTimerQueue::remove_timer_cb( EvTimerCallback &tcb,  uint64_t timer_id,
                               uint64_t event_id ) noexcept
{
  size_t i, num_elems = this->queue.num_elems;
  EvTimerEvent * elem = this->queue.heap;

  for ( i = 0; i < num_elems; i++ ) {
    if ( elem[ i ].id < 0 &&
         timer_id == elem[ i ].timer_id &&
         event_id == elem[ i ].event_id ) {
      uint32_t id = -(elem[ i ].id+1);
      if ( this->cb[ id ] == &tcb ) {
        EvTimerEvent el;
        el.id          = elem[ i ].id;
        el.ival        = 0;
        el.timer_id    = timer_id;
        el.next_expire = 0;
        el.event_id    = event_id;
        this->cb[ id ] = NULL;
        if ( id < this->cb_free )
          this->cb_free = id;
        this->cb_used -= 1;
        return this->queue.remove( el );
      }
    }
  }
  return false;
}

void
EvTimerQueue::repost( EvTimerEvent &ev ) noexcept
{
  /* this will skip intervals until expires > epoch */
  uint64_t amt = (uint64_t) ( ev.ival >> 2 ) *
                 (uint64_t) to_ns[ ev.ival & 3 ];
  this->epoch = current_monotonic_time_ns();
  ev.next_expire += amt;
  if ( ev.next_expire <= this->epoch ) {
    ev.next_expire += amt;
    if ( ev.next_expire <= this->epoch ) {
      if ( amt > 0 ) {
        uint64_t delta = this->epoch - ev.next_expire;
        delta = ( delta / amt + (uint64_t) 1 ) * amt;
        ev.next_expire += delta;
      }
      else {
        ev.next_expire = this->epoch;
      }
    }
  }
  this->queue.push( ev );
}

void
EvTimerQueue::read( void ) noexcept
{
#ifdef HAVE_TIMERFD
  uint8_t buf[ 1024 ];
  ssize_t n;
  for (;;) {
    n = ::read( this->fd, buf, sizeof( buf ) );
    if ( n < 0 )
      break;
    this->read_ns = this->poll.now_ns;
  }
  if ( errno != EINTR ) {
    if ( errno != EAGAIN ) {
      perror( "raed timer" );
      this->popall();
      this->push( EV_CLOSE );
      return;
    }
  }
#endif
  this->pop3( EV_READ, EV_READ_LO, EV_READ_HI );
  this->push( EV_PROCESS );
}

bool
EvTimerQueue::set_timer( void ) noexcept
{
#ifdef HAVE_TIMERFD
  struct itimerspec ts;
  uint64_t delta = this->expires - this->epoch;
  ts.it_interval.tv_sec = 0;
  ts.it_interval.tv_nsec = 0;
  ts.it_value.tv_sec  = delta / (uint64_t) 1000000000;
  ts.it_value.tv_nsec = delta % (uint64_t) 1000000000;

  if ( this->fd == -1 ) /* shutdown */
    return false;
  if ( timerfd_settime( this->fd, 0, &ts, NULL ) < 0 ) {
    perror( "set timer" );
    return false;
  }
#endif
  return true;
}

void
EvTimerQueue::process( void ) noexcept
{
  bool rearm_timer;
  this->processing_timers = true;
  this->epoch = current_monotonic_time_ns();

  for (;;) {
    if ( this->queue.is_empty() ) {
      this->expires = 0;
      break;
    }
    EvTimerEvent ev = this->queue.heap[ 0 ];
    if ( ev.next_expire <= this->epoch ) { /* timers are ready to fire */
      this->queue.pop();
      if ( ev.id < 0 ) {
        uint32_t id = -(ev.id+1);
        rearm_timer =
          this->cb[ id ]->timer_cb( ev.timer_id, ev.event_id );
        if ( ! rearm_timer ) {
          this->cb[ id ] = NULL;
          this->cb_used -= 1;
          if ( id < this->cb_free )
            this->cb_free = id;
        }
      }
      else {
        rearm_timer = this->poll.timer_expire( ev );
      }
      if ( rearm_timer )
        this->repost( ev );    /* next timer interval */
    }
    else {
      this->expires = ev.next_expire;

      if ( ! this->set_timer() ) {
        this->processing_timers = false;
        return; /* probably need to exit, this retries later */
      }
      break;
    }
  }
  this->pop( EV_PROCESS ); /* all timers that expired are processed */
  this->processing_timers = false;
}

void EvTimerQueue::write( void ) noexcept {}
void EvTimerQueue::release( void ) noexcept {
  this->expires = 0;
}

/* start a timer event callback */
bool
TimerQueue::add_timer_double( EvTimerCallback &tcb,  double secs,
                              uint64_t timer_id,  uint64_t event_id ) noexcept
{
  TimerUnits u = IVAL_SECS;
  uint32_t   v = (uint32_t) secs;
  double     x = secs;
  while ( u < IVAL_NANOS && x * 1000.0 < 1000000000.0 ) {
    x *= 1000.0;
    v  = (uint32_t) x;
    u  = (TimerUnits) ( u + 1 );
  }
  return this->queue->add_timer_cb( tcb, v, u, timer_id, event_id );
}

bool
TimerQueue::add_timer_seconds( EvTimerCallback &tcb,  uint32_t secs,
                               uint64_t timer_id,  uint64_t event_id ) noexcept
{
  return this->queue->add_timer_cb( tcb, secs, IVAL_SECS, timer_id, event_id );
}

bool
TimerQueue::add_timer_millis( EvTimerCallback &tcb,  uint32_t msecs,
                              uint64_t timer_id,  uint64_t event_id ) noexcept
{
  return this->queue->add_timer_cb( tcb, msecs, IVAL_MILLIS, timer_id,
                                    event_id );
}

bool
TimerQueue::add_timer_micros( EvTimerCallback &tcb,  uint32_t usecs,
                              uint64_t timer_id,  uint64_t event_id ) noexcept
{
  return this->queue->add_timer_cb( tcb, usecs, IVAL_MICROS, timer_id,
                                    event_id );
}

bool
TimerQueue::add_timer_nanos( EvTimerCallback &tcb,  uint32_t nsecs,
                             uint64_t timer_id,  uint64_t event_id ) noexcept
{
  return this->queue->add_timer_cb( tcb, nsecs, IVAL_NANOS, timer_id, event_id);
}

bool
TimerQueue::add_timer_units( EvTimerCallback &tcb,  uint32_t val,
                             TimerUnits units,  uint64_t timer_id,
                             uint64_t event_id ) noexcept
{
  return this->queue->add_timer_cb( tcb, val, units, timer_id, event_id );
}

/* start a timer event fd */
bool
TimerQueue::add_timer_double( int32_t id,  double secs,  uint64_t timer_id,
                              uint64_t event_id ) noexcept
{
  TimerUnits u = IVAL_SECS;
  uint32_t   v = (uint32_t) secs;
  double     x = secs;
  while ( u < IVAL_NANOS && x * 1000.0 < 1000000000.0 ) {
    x *= 1000.0;
    v  = (uint32_t) x;
    u  = (TimerUnits) ( u + 1 );
  }
  return this->queue->add_timer_units( id, v, u, timer_id, event_id );
}

bool
TimerQueue::add_timer_seconds( int32_t id,  uint32_t secs,  uint64_t timer_id,
                               uint64_t event_id ) noexcept
{
  return this->queue->add_timer_units( id, secs, IVAL_SECS, timer_id, event_id);
}

bool
TimerQueue::add_timer_millis( int32_t id,  uint32_t msecs,  uint64_t timer_id,
                              uint64_t event_id ) noexcept
{
  return this->queue->add_timer_units( id, msecs, IVAL_MILLIS, timer_id,
                                       event_id );
}

bool
TimerQueue::add_timer_micros( int32_t id,  uint32_t usecs,  uint64_t timer_id,
                              uint64_t event_id ) noexcept
{
  return this->queue->add_timer_units( id, usecs, IVAL_MICROS, timer_id,
                                       event_id );
}

bool
TimerQueue::add_timer_nanos( int32_t id,  uint32_t nsecs,  uint64_t timer_id,
                             uint64_t event_id ) noexcept
{
  return this->queue->add_timer_units( id, nsecs, IVAL_NANOS, timer_id,
                                       event_id );
}

bool
TimerQueue::add_timer_units( int32_t id,  uint32_t val,  TimerUnits units,
                             uint64_t timer_id,  uint64_t event_id ) noexcept
{
  return this->queue->add_timer_units( id, val, units, timer_id, event_id );
}

bool
TimerQueue::remove_timer( int32_t id,  uint64_t timer_id,
                          uint64_t event_id ) noexcept
{
  return this->queue->remove_timer( id, timer_id, event_id );
}

bool
TimerQueue::remove_timer_cb( EvTimerCallback &tcb,  uint64_t timer_id,
                             uint64_t event_id ) noexcept
{
  return this->queue->remove_timer_cb( tcb, timer_id, event_id );
}

