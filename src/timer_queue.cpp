#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/timerfd.h>
#include <raikv/timer_queue.h>
#include <raikv/util.h>

using namespace rai;
using namespace kv;

EvTimerQueue::EvTimerQueue( EvPoll &p )
            : EvSocket( p, p.register_type( "timer_queue" ) ),
              last( 0 ), epoch( 0 ), delta( 0 ), real( 0 ),
              cb( 0 ), cb_sz( 0 ), cb_used( 0 )
{
  this->sock_opts = OPT_READ_HI;
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

  tfd = ::timerfd_create( CLOCK_MONOTONIC, TFD_NONBLOCK );
  if ( tfd == -1 ) {
    perror( "timerfd_create() failed" );
    return NULL;
  } 
  
  void * m = aligned_malloc( sizeof( EvTimerQueue ) );
  if ( m == NULL ) {
    perror( "alloc timer queue" );
    ::close( tfd );
    return NULL;
  }
  EvTimerQueue * q = new ( m ) EvTimerQueue( p );
  q->PeerData::init_peer( tfd, NULL, "timer" );
  q->last   = current_monotonic_time_ns();
  q->epoch  = q->last;
  q->delta  = 0;
  q->real   = p.current_coarse_ns();
  if ( p.add_sock( q ) < 0 ) {
    printf( "failed to add timer %d\n", tfd );
    ::close( tfd );
    return NULL;
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
  this->idle_push( EV_PROCESS );
  return true;
}

bool
EvTimerQueue::add_timer_cb( EvTimerCallback &tcb,  uint32_t ival,  TimerUnits u,
                            uint64_t timer_id,  uint64_t event_id ) noexcept
{
  int32_t id = (int32_t) this->cb_used;
  if ( this->cb_used < this->cb_sz ) {
    if ( this->cb[ this->cb_used ] != NULL ) {
      for ( id = 0; ; id++ ) {
        if ( this->cb[ id ] == NULL )
          break;
      }
    }
  }
  else {
    size_t new_sz = ( this->cb_sz == 0 ? 8 : this->cb_sz * 2 );
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
  if ( this->add_timer_units( -(id+1), ival, u, timer_id, event_id ) ) {
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

void
EvTimerQueue::repost( void ) noexcept
{
  EvTimerEvent el = this->queue.pop();
  /* this will skip intervals until expires > epoch */
  do {
    el.next_expire += (uint64_t) ( el.ival >> 2 ) *
                      (uint64_t) to_ns[ el.ival & 3 ];
  } while ( el.next_expire <= this->epoch );
  this->queue.push( el );
}

void
EvTimerQueue::read( void ) noexcept
{
  uint8_t buf[ 1024 ];
  ssize_t n;
  for (;;) {
    n = ::read( this->fd, buf, sizeof( buf ) );
    if ( n < 0 ) {
      if ( errno != EINTR ) {
        if ( errno != EAGAIN ) {
          perror( "raed timer" );
          this->popall();
          this->push( EV_CLOSE );
        }
        else {
          this->pop3( EV_READ, EV_READ_LO, EV_READ_HI );
          this->push( EV_PROCESS );
        }
      }
      break;
    }
  }
}

bool
EvTimerQueue::set_timer( void ) noexcept
{
  struct itimerspec ts;
  ts.it_interval.tv_sec = 0;
  ts.it_interval.tv_nsec = 0;
  ts.it_value.tv_sec  = this->delta / (uint64_t) 1000000000;
  ts.it_value.tv_nsec = this->delta % (uint64_t) 1000000000;

  if ( timerfd_settime( this->fd, 0, &ts, NULL ) < 0 ) {
    perror( "set timer" );
    return false;
  }
  return true;
}

void
EvTimerQueue::process( void ) noexcept
{
  bool rearm_timer;
  this->last  = this->epoch;
  this->epoch = current_monotonic_time_ns();
  this->real  = this->poll.current_coarse_ns();
  this->delta = 0;

  while ( ! this->queue.is_empty() ) {
    EvTimerEvent &ev = this->queue.heap[ 0 ];
    if ( ev.next_expire <= this->epoch ) { /* timers are ready to fire */
      if ( ev.id < 0 ) {
        rearm_timer =
          this->cb[ -(ev.id+1) ]->timer_cb( ev.timer_id, ev.event_id );
        if ( ! rearm_timer ) {
          this->cb[ -(ev.id+1) ] = NULL;
          this->cb_used -= 1;
        }
      }
      else {
        rearm_timer = this->poll.timer_expire( ev );
      }
      if ( rearm_timer )
        this->repost();    /* next timer interval */
      else
        this->queue.pop(); /* remove timer */
    }
    else {
      this->delta = ev.next_expire - this->epoch;
      /*printf( "delta %lu.%lu\n", this->delta / (uint64_t) 1000000000,
                                 this->delta % (uint64_t) 1000000000 );*/
      if ( ! this->set_timer() )
        return; /* probably need to exit, this retries later */
      break;
    }
  }
  this->pop( EV_PROCESS ); /* all timers that expired are processed */
}

void EvTimerQueue::write( void ) noexcept {}
void EvTimerQueue::release( void ) noexcept {}

/* start a timer event callback */
bool
TimerQueue::add_timer_seconds( EvTimerCallback &tcb,  uint32_t secs,
                               uint64_t timer_id, uint64_t event_id ) noexcept
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
/* start a timer event fd */
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
TimerQueue::remove_timer( int32_t id,  uint64_t timer_id,
                          uint64_t event_id ) noexcept
{
  return this->queue->remove_timer( id, timer_id, event_id );
}

uint64_t
TimerQueue::current_monotonic_time_ns( void ) noexcept
{
  return this->queue->epoch;
}

uint64_t
TimerQueue::current_time_ns( void ) noexcept
{
  return this->queue->real;
}

