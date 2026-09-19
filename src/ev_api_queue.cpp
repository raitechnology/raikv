/* thread-neutral api core: the pieces that must exist once per process
 * (see include/raikv/ev_api_queue.h) */
#include <raikv/ev_api_queue.h>

using namespace rai;
using namespace kv;

namespace rai {
namespace kv {
thread_local ApiDispatchTLS api_dispatch_tls = { 0 };

bool
ApiTimer::timer_cb( uint64_t,  uint64_t ) noexcept
{
  if ( this->cb == NULL )
    return false;
  if ( this->in_queue )
    return true;
  ApiQueue * q = this->api.get_queue( this->queue );
  if ( q == NULL )
    return false;
  pthread_mutex_lock( &q->mutex );
  this->in_queue = true;
  if ( q->push( this->id, this->cb, NULL, this->cl, NULL ) )
    ApiCore::queue_signal( *q );
  pthread_mutex_unlock( &q->mutex );
  return true;
}

void *
api_epoll_thread( void *arg ) noexcept
{
  ApiCore & api  = *(ApiCore *) arg;
  EvPoll  & poll = api.poll;
  api.ev_thr_id = pthread_self();
  int idle_count = 0;
  for (;;) {
    int idle = poll.dispatch();
    if ( idle == EvPoll::DISPATCH_IDLE )
      idle_count++;
    else
      idle_count = 0;
    poll.wait( idle_count > 10 ? 100 : 0 );
  }
  return NULL;
}

void *
api_disp_thread( void *arg ) noexcept
{
  ApiDispatcher & disp = *(ApiDispatcher *) arg;
  double t = ( disp.idle_timeout == API_WAIT_FOREVER ? 10.0
                                                     : disp.idle_timeout );
  while ( ! disp.quit ) {
    if ( disp.api.timed_dispatch_queue( disp.queue, t ) == API_INVALID_QUEUE )
      break;
  }
  pthread_mutex_lock( &disp.mutex );
  disp.done = true;
  pthread_cond_broadcast( &disp.cond );
  pthread_mutex_unlock( &disp.mutex );
  return NULL;
}

void *
api_disp_group_thread( void *arg ) noexcept
{
  ApiDispatcher & disp = *(ApiDispatcher *) arg;
  double t = ( disp.idle_timeout == API_WAIT_FOREVER ? 10.0
                                                     : disp.idle_timeout );
  while ( ! disp.quit ) {
    if ( disp.api.timed_dispatch_group( disp.queue, t ) ==
         API_INVALID_QUEUE_GROUP )
      break;
  }
  pthread_mutex_lock( &disp.mutex );
  disp.done = true;
  pthread_cond_broadcast( &disp.cond );
  pthread_mutex_unlock( &disp.mutex );
  return NULL;
}
}
}
