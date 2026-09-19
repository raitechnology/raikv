#ifndef __rai_raikv__ev_api_queue_h__
#define __rai_raikv__ev_api_queue_h__

/* Protocol-neutral core of a thread-safe "api library" over an EvPoll:
 *
 *  - ApiCore     one epoll thread (api_epoll_thread) owns the EvPoll; api
 *                calls from application threads cross to it through an
 *                ApiPipe carrying ApiOp records and block on a cond var until
 *                the op ran (exec()).  Objects are handed out as small integer
 *                ids through a typed registry (make / get / rem).
 *  - ApiQueue    an event queue an application thread drains with
 *                timed_dispatch_queue(); the epoll thread pushes events onto
 *                it (push + queue_signal).  Priorities, limit policy, hook,
 *                on-complete callback, queue groups, dispatcher threads.
 *  - ApiTimer    an EvTimerCallback that pushes a timer event on a queue.
 *
 * What an event *means* is the protocol library's business: ApiCore::
 * dispatch_event() is virtual, the queue only carries (id, cb, vcb, cl, msg).
 * sassrv's tibrv 7 api (rv7_api.cpp) and omm's api (omm_api.cpp) both sit
 * on this; the transport, listener and message types are theirs. */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <pthread.h>
#include <errno.h>
#include <fcntl.h>
#include <time.h>
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
#include <unistd.h>
#include <poll.h>
#else
#include <raikv/win.h>
#endif
#include <raikv/ev_net.h>
#include <raikv/dlinklist.h>
#include <raikv/util.h>

namespace rai {
namespace kv {

/* element types in the registry; protocol libraries may add their own from
 * API_ELEM_USER up (the tibrv api uses 8, 9 for the ft member / monitor) */
enum ApiElemType {
  API_ELEM_NONE        = 0,
  API_ELEM_TIMER       = 1,
  API_ELEM_IO          = 2,
  API_ELEM_LISTENER    = 3,
  API_ELEM_QUEUE       = 4,
  API_ELEM_QUEUE_GROUP = 5,
  API_ELEM_TRANSPORT   = 6,
  API_ELEM_DISPATCHER  = 7,
  API_ELEM_USER        = 8
};

enum ApiStatus {
  API_OK                  = 0,
  API_TIMEOUT             = 1,
  API_INVALID_QUEUE       = 2,
  API_INVALID_QUEUE_GROUP = 3,
  API_INVALID_EVENT       = 4,
  API_INVALID_DISPATCHER  = 5,
  API_INVALID_ARG         = 6,
  API_INIT_FAILURE        = 7
};

static const double API_WAIT_FOREVER = -1.0;
static const uint32_t API_INVALID_ID = 0;

/* absolute timespec `timeout` secs from now; < 0 uses default_timeout */
static inline struct timespec
api_ts_timeout( double timeout,  double default_timeout = 0 ) {
  struct timespec ts;
  if ( timeout < 0.0 )
    timeout = default_timeout;
  if ( timeout > 0.0 ) {
    clock_gettime( CLOCK_REALTIME, &ts );
    double frac, i;
    frac = modf( timeout, &i );
    ts.tv_sec  += (time_t) i;
    ts.tv_nsec += (long) ( frac * 1000000000.0 );
    if ( ts.tv_nsec >= 1000000000 ) {
      ts.tv_sec++;
      ts.tv_nsec -= 1000000000;
    }
  }
  else {
    ts.tv_sec  = 0;
    ts.tv_nsec = 0;
  }
  return ts;
}

/* bump allocator for queue events: two of these alternate per queue, the
 * idle one is reset when the list swaps (MDMsgMem shape, no raimd dep) */
struct ApiMemArena {
  struct Blk { Blk *next; size_t size, off; };
  Blk  * hd;
  size_t blk_size;

  ApiMemArena( size_t bsz = 16 * 1024 ) : hd( 0 ), blk_size( bsz ) {}
  ~ApiMemArena() { this->release(); }

  void *make( size_t sz ) {
    sz = ( sz + 15 ) & ~(size_t) 15;
    if ( this->hd == NULL || this->hd->off + sz > this->hd->size ) {
      size_t bsz = ( sz + sizeof( Blk ) > this->blk_size ? sz + sizeof( Blk )
                                                          : this->blk_size );
      Blk * b = (Blk *) ::malloc( bsz );
      b->next = this->hd;
      b->size = bsz;
      b->off  = ( sizeof( Blk ) + 15 ) & ~(size_t) 15;
      this->hd = b;
    }
    void * p = &((uint8_t *) this->hd)[ this->hd->off ];
    this->hd->off += sz;
    return p;
  }
  template<class T> void alloc( size_t sz,  T **ptr ) {
    *ptr = (T *) this->make( sz );
  }
  /* grow *ptr from osz to nsz bytes, in place when it was the last make() */
  template<class T> void extend( size_t osz,  size_t nsz,  T **ptr ) {
    uint8_t * p = (uint8_t *) *ptr;
    size_t    o = ( osz + 15 ) & ~(size_t) 15,
              n = ( nsz + 15 ) & ~(size_t) 15;
    if ( this->hd != NULL && p + o == &((uint8_t *) this->hd)[ this->hd->off ] &&
         this->hd->off - o + n <= this->hd->size ) {
      this->hd->off += n - o;
      return;
    }
    void * q = this->make( nsz );
    ::memcpy( q, p, osz );
    *ptr = (T *) q;
  }
  /* keep the first (largest, most recent) block, free the rest */
  void reuse( void ) {
    if ( this->hd != NULL ) {
      Blk * b = this->hd->next;
      while ( b != NULL ) {
        Blk * n = b->next;
        ::free( b );
        b = n;
      }
      this->hd->next = NULL;
      this->hd->off  = ( sizeof( Blk ) + 15 ) & ~(size_t) 15;
    }
  }
  void release( void ) {
    while ( this->hd != NULL ) {
      Blk * n = this->hd->next;
      ::free( this->hd );
      this->hd = n;
    }
  }
};

/* ---- registry ------------------------------------------------------------ */

struct ApiElem {
  uint32_t id, type;
  void   * ptr;
};

/* ---- pipe to the epoll thread ------------------------------------------- */

struct ApiPipe;
/* an operation run on the epoll thread; the caller holds *mutex, exec()s
 * (blocks on *cond until complete), then unlocks */
struct ApiOp {
  pthread_mutex_t * mutex;
  pthread_cond_t  * cond;
  bool            * complete;
  ApiOp( pthread_mutex_t *m,  pthread_cond_t *c )
    : mutex( m ), cond( c ), complete( 0 ) {}
  /* ops live on the caller's stack; class delete keeps the mingw build
   * (no libstdc++ in the dlls) from needing the global operator delete */
  void operator delete( void * ) {}
  virtual ~ApiOp() {}
  virtual void run( ApiPipe &pipe ) noexcept = 0;
};

struct ApiPipe : public EvConnection {
  int write_fd;

  void * operator new( size_t, void *ptr ) { return ptr; }
  ApiPipe( EvPoll &poll,  int wfd,  const char *type_name = "api_pipe" ) :
    EvConnection( poll, poll.register_type( type_name ) ), write_fd( wfd ) {}

  bool start( int rfd,  const char *name ) noexcept {
    this->PeerData::init_peer( this->poll.get_next_id(), rfd, -1, NULL, name );
    return this->poll.add_sock( this ) == 0;
  }
  /* application thread side: post op, wait for the epoll thread to run it;
   * the caller holds *op.mutex */
  void exec( ApiOp &op ) noexcept {
    ApiOp   * ptr = &op;
    uint8_t * p   = (uint8_t *) &ptr,
            * e   = &p[ sizeof( ptr ) ];
    bool      complete = false;
    op.complete = &complete;
    for (;;) {
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
      int n = (int) ::write( this->write_fd, p, e - p );
#else
      struct iovec iov = { p, (size_t) ( e - p ) };
      int n = (int) ::wp_send( this->write_fd, &iov, 1 );
#endif
      if ( n > 0 ) {
        p += n;
        if ( p == e )
          break;
      }
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
      struct pollfd fds = { this->write_fd, POLLOUT, POLLOUT };
      ::poll( &fds, 1, 10 );
#else
      ::Sleep( 1 ); /* loopback socketpair, rarely full */
#endif
    }
    while ( ! *op.complete )
      pthread_cond_wait( op.cond, op.mutex );
    op.complete = NULL;
  }
  /* epoll thread side */
  virtual void process( void ) noexcept {
    for (;;) {
      size_t buflen = this->len - this->off;
      if ( buflen < sizeof( ApiOp * ) ) {
        this->pop( EV_PROCESS );
        return;
      }
      ApiOp * op;
      ::memcpy( &op, &this->recv[ this->off ], sizeof( op ) );
      this->off += sizeof( op );
      op->run( *this );
      pthread_mutex_lock( op->mutex );
      *op->complete = true;
      pthread_cond_broadcast( op->cond );
      pthread_mutex_unlock( op->mutex );
    }
  }
  virtual void release( void ) noexcept {}
};

/* ---- queues -------------------------------------------------------------- */

struct ApiCore;
struct ApiQueueGroup;

/* one dispatchable event: a callback with a message (listener) or without
 * (timer); vcb events coalesce consecutive messages for the same id */
struct ApiQueueEvent {
  ApiQueueEvent * next, * back;
  void          * msg, ** vec;
  void          * cb,   /* protocol event callback, cast by dispatch_event */
                * vcb;  /* protocol vector callback */
  const void    * cl;
  uint32_t        id,   /* the event (listener / timer) id */
                  cnt;

  void * operator new( size_t, void *ptr ) { return ptr; }
  ApiQueueEvent( uint32_t i,  void *e,  void *v,  const void *c,  void *m )
    : next( 0 ), back( 0 ), msg( m ), vec( 0 ), cb( e ), vcb( v ), cl( c ),
      id( i ), cnt( 1 ) {}
};
typedef DLinkList< ApiQueueEvent > ApiQueueEventList;

typedef void (*ApiQueueHook)( uint32_t queue_id,  void *closure );
typedef void (*ApiQueueOnComplete)( uint32_t queue_id,  void *closure );

enum ApiQueueLimitPolicy {
  API_QUEUE_DISCARD_NONE  = 0,
  API_QUEUE_DISCARD_NEW   = 1,
  API_QUEUE_DISCARD_FIRST = 2,
  API_QUEUE_DISCARD_LAST  = 3
};

struct ApiQueue {
  ApiCore            & api;
  ApiQueue           * next, * back;   /* queue group links */
  uint32_t             id,
                       priority,
                       count;
  ApiQueueHook         hook;
  void               * hook_cl;
  char               * name;
  ApiQueueLimitPolicy  policy;
  uint32_t             max_ev,
                       discard;
  pthread_mutex_t      mutex;
  pthread_cond_t       cond;
  ApiQueueEventList    list;
  ApiMemArena          mem_x[ 2 ];
  uint8_t              mptr;
  bool                 done;
  ApiQueueOnComplete   cb;
  const void         * cl;
  ApiQueueGroup      * grp;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  ApiQueue( ApiCore &a,  uint32_t i ) : api( a ), next( 0 ), back( 0 ),
      id( i ), priority( 0 ), count( 0 ), hook( 0 ), hook_cl( 0 ), name( 0 ),
      policy( API_QUEUE_DISCARD_NONE ), max_ev( 0 ), discard( 0 ), mptr( 0 ),
      done( false ), cb( 0 ), cl( 0 ), grp( 0 ) {
    pthread_mutex_init( &this->mutex, NULL );
    pthread_cond_init( &this->cond, NULL );
  }
  /* caller holds mutex; true when the queue went empty -> non-empty (signal) */
  bool push( uint32_t id,  void *cb,  void *vcb,  const void *cl,
             void *msg ) noexcept {
    if ( vcb != NULL && ! this->list.is_empty() && id == this->list.tl->id ) {
      ApiQueueEvent * e = this->list.tl;
      if ( e->cnt == 1 ) {
        size_t sz = 4 * sizeof( void * );
        this->mem_x[ this->mptr ].alloc( sz, &e->vec );
        e->vec[ 0 ] = e->msg;
        e->vec[ 1 ] = msg;
        e->cnt = 2;
      }
      else {
        if ( ( e->cnt & 3 ) == 0 ) {
          size_t osz = e->cnt * sizeof( void * ),
                 nsz = ( e->cnt + 4 ) * sizeof( void * );
          this->mem_x[ this->mptr ].extend( osz, nsz, &e->vec );
        }
        e->vec[ e->cnt++ ] = msg;
      }
    }
    else {
      this->list.push_tl(
        new ( this->mem_x[ this->mptr ].make( sizeof( ApiQueueEvent ) ) )
          ApiQueueEvent( id, cb, vcb, cl, msg ) );
      if ( this->count++ == 0 )
        return true;
    }
    return false;
  }
  /* caller holds mutex: detach the pending events for dispatch */
  ApiQueueEventList take_list( void ) noexcept {
    ApiQueueEventList list2 = this->list;
    this->list.init();
    this->mptr = ( this->mptr + 1 ) % 2;
    this->mem_x[ this->mptr ].reuse();
    this->count = 0;
    return list2;
  }
  ApiStatus finish_queue( void ) noexcept {
    if ( this->done && this->cb != NULL ) {
      pthread_mutex_lock( &this->mutex );
      if ( this->cb != NULL ) {
        this->cb( this->id, (void *) this->cl );
        this->cb = NULL;
      }
      pthread_mutex_unlock( &this->mutex );
    }
    return API_OK;
  }
};
typedef DLinkList< ApiQueue > ApiQueueList;

struct ApiQueueGroup {
  ApiCore       & api;
  ApiQueueList    list;
  uint32_t        id;
  pthread_mutex_t mutex;
  pthread_cond_t  cond;
  uint32_t        count;
  bool            update,
                  done;
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  ApiQueueGroup( ApiCore &a,  uint32_t i ) : api( a ), id( i ), count( 0 ),
      update( false ), done( false ) {
    pthread_mutex_init( &this->mutex, NULL );
    pthread_cond_init( &this->cond, NULL );
  }
};

struct ApiDispatcher {
  ApiCore       & api;
  uint32_t        id,
                  queue;         /* queue or queue group id */
  double          idle_timeout;
  char          * name;
  bool            quit,
                  done,
                  is_queue,
                  is_queue_group;
  pthread_mutex_t mutex;
  pthread_cond_t  cond;
  pthread_t       thr_id;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  ApiDispatcher( ApiCore &a,  uint32_t i ) : api( a ), id( i ),
      queue( 0 ), idle_timeout( 0 ), name( 0 ),
      quit( false ), done( false ), is_queue( false ), is_queue_group( false ),
      thr_id( 0 ) {
    pthread_mutex_init( &this->mutex, NULL );
    pthread_cond_init( &this->cond, NULL );
  }
};

/* a timer event source: fires on the epoll thread, queues an event */
struct ApiTimer : public EvTimerCallback {
  ApiCore    & api;
  uint32_t     id,
               queue;
  void       * cb;    /* protocol timer callback */
  const void * cl;
  double       ival;
  bool         in_queue;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  ApiTimer( ApiCore &a,  uint32_t i ) : api( a ), id( i ), queue( 0 ),
      cb( 0 ), cl( 0 ), ival( 0 ), in_queue( false ) {}
  virtual bool timer_cb( uint64_t timer_id,  uint64_t event_id ) noexcept;
  virtual ~ApiTimer() {}
};

/* per thread dispatch nesting: dispatch_end() runs when the outermost
 * dispatch pass on this thread finishes (nested dispatch from a callback) */
struct ApiDispatchTLS {
  uint32_t depth;
};
extern thread_local ApiDispatchTLS api_dispatch_tls;

void *api_epoll_thread( void *arg ) noexcept;
void *api_disp_thread( void *arg ) noexcept;
void *api_disp_group_thread( void *arg ) noexcept;

/* ---- the core ------------------------------------------------------------ */

struct ApiCore {
  EvPoll          poll;
  uint32_t        next_id, free_id, map_size;
  ApiElem       * map;
  pthread_mutex_t map_mutex;
  pthread_cond_t  cond;
  ApiPipe       * ev_read;
  int             pfd[ 2 ];
  pthread_t       ev_thr_id;       /* the epoll thread, set at its start */

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { aligned_free( ptr ); }
  ApiCore( uint32_t first_id = 11 ) : next_id( first_id ), free_id( 0 ),
      map_size( 0 ), map( 0 ), ev_read( 0 ), ev_thr_id( pthread_self() ) {
    this->pfd[ 0 ] = this->pfd[ 1 ] = -1;
    pthread_mutex_init( &this->map_mutex, NULL );
    pthread_cond_init( &this->cond, NULL );
  }
  virtual ~ApiCore() {}

  /* protocol hooks */
  virtual void dispatch_event( ApiQueueEvent &ev ) noexcept = 0;
  virtual void dispatch_end( void ) noexcept {} /* outermost pass finished */

  bool on_ev_thread( void ) const noexcept {
    return pthread_equal( pthread_self(), this->ev_thr_id ) != 0;
  }

  /* --- registry --- */
  /* construct T( *this, id ) and register it; add = extra trailing bytes;
   * id = 0 allocates one */
  template<class T, class API>
  T *make( API &api,  uint32_t type,  size_t add = 0,  uint32_t id = 0,
           bool aligned = false ) {
    void * mem = aligned ? aligned_malloc( sizeof( T ) + add )
                         : ::malloc( sizeof( T ) + add );
    pthread_mutex_lock( &this->map_mutex );
    if ( id == 0 ) {
      if ( this->free_id != 0 ) {
        for (;;) {
          id = this->free_id++;
          if ( id >= this->next_id ) {
            id = this->next_id++;
            this->free_id = 0;
            break;
          }
          if ( this->map[ id ].ptr == NULL )
            break;
        }
      }
      else {
        id = this->next_id++;
      }
    }
    T *p = new ( mem ) T( api, id );
    if ( id >= this->map_size ) {
      uint32_t nsz = this->map_size;
      while ( nsz <= id )
        nsz += 16;
      this->map = (ApiElem *) ::realloc( this->map, nsz * sizeof( ApiElem ) );
      ::memset( &this->map[ this->map_size ], 0,
                ( nsz - this->map_size ) * sizeof( ApiElem ) );
      this->map_size = nsz;
    }
    this->map[ id ].id   = id;
    this->map[ id ].type = type;
    this->map[ id ].ptr  = p;
    pthread_mutex_unlock( &this->map_mutex );
    return p;
  }
  template<class T>
  T *get( uint32_t id,  uint32_t type ) {
    pthread_mutex_lock( &this->map_mutex );
    bool b = ( id < this->map_size && id == this->map[ id ].id &&
               type == this->map[ id ].type );
    T  * p = (T *) ( b ? this->map[ id ].ptr : NULL );
    pthread_mutex_unlock( &this->map_mutex );
    return p;
  }
  template<class T>
  T *rem( uint32_t id,  uint32_t type ) {
    pthread_mutex_lock( &this->map_mutex );
    bool b = ( id < this->map_size && id == this->map[ id ].id &&
               type == this->map[ id ].type );
    T  * p = NULL;
    if ( b ) {
      p = (T *) this->map[ id ].ptr;
      this->map[ id ].ptr = NULL;
    }
    if ( this->free_id == 0 || id < this->free_id )
      this->free_id = id;
    pthread_mutex_unlock( &this->map_mutex );
    return p;
  }
  static void set_string( char *&str,  const char *value ) {
    if ( str != NULL ) { ::free( str ); str = NULL; }
    if ( value != NULL ) { str = ::strdup( value ); }
  }

  /* --- open: pipe + poll; the caller then constructs its ApiPipe (or a
   * subclass) with pfd[ 1 ], start()s it on pfd[ 0 ], sets ev_read, adds its
   * own sockets, and calls start_ev_thread() --- */
  ApiStatus open_pipe( int numfds = 128 ) noexcept {
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
    if ( pipe2( this->pfd, O_CLOEXEC ) != 0 )
      return API_INIT_FAILURE;
    fcntl( this->pfd[ 0 ], F_SETFL, O_NONBLOCK |
           fcntl( this->pfd[ 0 ], F_GETFL ) );
#else
    if ( wp_socketpair( this->pfd ) != 0 ) /* pipe substitute, non-blocking */
      return API_INIT_FAILURE;
#endif
    this->poll.init( numfds, false );
    return API_OK;
  }
  /* default pipe when the protocol has no ops of its own */
  ApiStatus open_default_pipe( const char *name = "api_pipe" ) noexcept {
    ApiPipe * p = new ( aligned_malloc( sizeof( ApiPipe ) ) )
                  ApiPipe( this->poll, this->pfd[ 1 ] );
    if ( ! p->start( this->pfd[ 0 ], name ) )
      return API_INIT_FAILURE;
    this->ev_read = p;
    return API_OK;
  }
  void start_ev_thread( void ) noexcept {
    pthread_t id;
    pthread_attr_t attr;
    pthread_attr_init( &attr );
    pthread_attr_setdetachstate( &attr, 1 );
    pthread_create( &id, &attr, api_epoll_thread, this );
  }

  /* --- queues --- */
  ApiQueue *create_queue( uint32_t id = 0 ) noexcept {
    return this->make<ApiQueue>( *this, API_ELEM_QUEUE, 0, id );
  }
  ApiQueue *get_queue( uint32_t q ) noexcept {
    return this->get<ApiQueue>( q, API_ELEM_QUEUE );
  }
  /* wake the dispatcher of q (or its group); caller holds q->mutex */
  static void queue_signal( ApiQueue &q ) noexcept {
    ApiQueueGroup * g = q.grp;
    if ( g == NULL )
      pthread_cond_broadcast( &q.cond );
    else {
      pthread_mutex_lock( &g->mutex );
      pthread_cond_broadcast( &g->cond );
      pthread_mutex_unlock( &g->mutex );
    }
  }
  /* push an event on queue id q and signal; false when q is gone */
  bool queue_push( uint32_t q,  uint32_t id,  void *cb,  void *vcb,
                   const void *cl,  void *msg ) noexcept {
    ApiQueue * queue = this->get_queue( q );
    if ( queue == NULL )
      return false;
    pthread_mutex_lock( &queue->mutex );
    if ( queue->push( id, cb, vcb, cl, msg ) )
      queue_signal( *queue );
    pthread_mutex_unlock( &queue->mutex );
    return true;
  }
  void dispatch_list( ApiQueueEventList &list2 ) noexcept {
    api_dispatch_tls.depth++;
    while ( ! list2.is_empty() )
      this->dispatch_event( *list2.pop_hd() );
    if ( --api_dispatch_tls.depth == 0 )
      this->dispatch_end();
  }
  ApiStatus timed_dispatch_queue( uint32_t q,  double timeout ) noexcept {
    ApiQueue * queue = this->get_queue( q );
    if ( queue == NULL || queue->done )
      return API_INVALID_QUEUE;
    pthread_mutex_lock( &queue->mutex );
    while ( queue->list.is_empty() ) {
      struct timespec ts = api_ts_timeout( timeout, 1.0 );
      pthread_cond_timedwait( &queue->cond, &queue->mutex, &ts );
      if ( timeout != API_WAIT_FOREVER || queue->done )
        break;
    }
    if ( queue->list.is_empty() ) {
      pthread_mutex_unlock( &queue->mutex );
      if ( queue->done )
        return queue->finish_queue();
      return API_TIMEOUT;
    }
    ApiQueueEventList list2 = queue->take_list();
    pthread_mutex_unlock( &queue->mutex );
    this->dispatch_list( list2 );
    if ( queue->done )
      return queue->finish_queue();
    return API_OK;
  }
  ApiStatus timed_dispatch_one_event( uint32_t q,  double timeout ) noexcept {
    ApiQueue * queue = this->get_queue( q );
    if ( queue == NULL || queue->done )
      return API_INVALID_QUEUE;
    pthread_mutex_lock( &queue->mutex );
    while ( queue->list.is_empty() ) {
      struct timespec ts = api_ts_timeout( timeout, 1.0 );
      pthread_cond_timedwait( &queue->cond, &queue->mutex, &ts );
      if ( timeout != API_WAIT_FOREVER || queue->done )
        break;
    }
    if ( queue->list.is_empty() ) {
      pthread_mutex_unlock( &queue->mutex );
      if ( queue->done )
        return queue->finish_queue();
      return API_TIMEOUT;
    }
    ApiQueueEvent * ev = queue->list.pop_hd();
    queue->count--;
    if ( queue->list.is_empty() ) {
      queue->mptr = ( queue->mptr + 1 ) % 2;
      queue->mem_x[ queue->mptr ].reuse();
    }
    pthread_mutex_unlock( &queue->mutex );
    ApiQueueEventList list2;
    list2.push_tl( ev );
    this->dispatch_list( list2 );
    if ( queue->done )
      return queue->finish_queue();
    return API_OK;
  }
  ApiStatus destroy_queue( uint32_t q,  ApiQueueOnComplete cb,
                           const void *cl ) noexcept {
    ApiQueue * queue = this->get_queue( q );
    if ( queue == NULL || queue->done )
      return API_INVALID_QUEUE;
    queue->done = true;
    if ( pthread_mutex_trylock( &queue->mutex ) == 0 ) {
      if ( cb != NULL )
        cb( q, (void *) cl );
      pthread_mutex_unlock( &queue->mutex );
    }
    else {
      queue->cb = cb;
      queue->cl = cl;
    }
    return API_OK;
  }
  ApiStatus get_queue_count( uint32_t q,  uint32_t &num ) noexcept {
    ApiQueue * queue = this->get_queue( q );
    if ( queue == NULL )
      return API_INVALID_QUEUE;
    pthread_mutex_lock( &queue->mutex );
    num = queue->count;
    pthread_mutex_unlock( &queue->mutex );
    return API_OK;
  }

  /* --- queue groups --- */
  static int cmp_queue_prio( const ApiQueue &x,  const ApiQueue &y ) {
    if ( x.priority > y.priority ) return -1;
    if ( x.priority == y.priority ) return 0;
    return 1;
  }
  ApiQueueGroup *create_queue_group( void ) noexcept {
    return this->make<ApiQueueGroup>( *this, API_ELEM_QUEUE_GROUP );
  }
  ApiQueueGroup *get_queue_group( uint32_t g ) noexcept {
    return this->get<ApiQueueGroup>( g, API_ELEM_QUEUE_GROUP );
  }
  ApiStatus timed_dispatch_group( uint32_t grp,  double timeout ) noexcept {
    ApiQueueGroup * g = this->get_queue_group( grp );
    ApiQueue      * queue;
    if ( g == NULL || g->done )
      return API_INVALID_QUEUE_GROUP;
    pthread_mutex_lock( &g->mutex );
    if ( g->update ) {
      g->list.sort<cmp_queue_prio>();
      g->update = false;
    }
    bool all_done;
    for (;;) {
      all_done = true;
      for ( queue = g->list.hd; queue != NULL; queue = queue->next ) {
        if ( queue->count > 0 )
          break;
        all_done &= queue->done;
      }
      if ( queue == NULL ) {
        struct timespec ts = api_ts_timeout( timeout, 1.0 );
        pthread_cond_timedwait( &g->cond, &g->mutex, &ts );
      }
      if ( queue != NULL || timeout != API_WAIT_FOREVER || all_done )
        break;
    }
    for ( queue = g->list.hd; queue != NULL; queue = queue->next )
      if ( queue->count > 0 )
        break;
    pthread_mutex_unlock( &g->mutex );
    if ( queue == NULL ) {
      if ( all_done ) {
        for ( queue = g->list.hd; queue != NULL; queue = queue->next )
          queue->finish_queue();
        return API_OK;
      }
      return API_TIMEOUT;
    }
    pthread_mutex_lock( &queue->mutex );
    ApiQueueEventList list2;
    if ( queue->grp == g )
      list2 = queue->take_list();
    pthread_mutex_unlock( &queue->mutex );
    this->dispatch_list( list2 );
    return API_OK;
  }
  ApiStatus destroy_queue_group( uint32_t grp ) noexcept {
    ApiQueueGroup * g = this->get_queue_group( grp );
    if ( g == NULL || g->done )
      return API_INVALID_QUEUE_GROUP;
    g->done = true;
    return API_OK;
  }
  ApiStatus add_queue_group( uint32_t grp,  uint32_t q ) noexcept {
    ApiQueueGroup * g     = this->get_queue_group( grp );
    ApiQueue      * queue = this->get_queue( q );
    if ( queue == NULL || queue->done )
      return API_INVALID_QUEUE;
    if ( g == NULL || g->done )
      return API_INVALID_QUEUE_GROUP;
    pthread_mutex_lock( &queue->mutex );
    pthread_mutex_lock( &g->mutex );
    queue->grp = g;
    g->list.push_tl( queue );
    if ( g->count++ > 0 )
      g->list.sort<cmp_queue_prio>();
    g->update = false;
    if ( queue->count > 0 )
      pthread_cond_broadcast( &g->cond );
    pthread_mutex_unlock( &g->mutex );
    pthread_mutex_unlock( &queue->mutex );
    return API_OK;
  }
  ApiStatus remove_queue_group( uint32_t grp,  uint32_t q ) noexcept {
    ApiQueueGroup * g     = this->get_queue_group( grp );
    ApiQueue      * queue = this->get_queue( q );
    if ( queue == NULL || queue->done )
      return API_INVALID_QUEUE;
    if ( g == NULL || g->done )
      return API_INVALID_QUEUE_GROUP;
    pthread_mutex_lock( &queue->mutex );
    pthread_mutex_lock( &g->mutex );
    queue->grp = NULL;
    g->list.pop( queue );
    g->count--;
    pthread_mutex_unlock( &g->mutex );
    pthread_mutex_unlock( &queue->mutex );
    return API_OK;
  }

  /* --- timers (the epoll timer queue is touched on the epoll thread) --- */
  struct TimerOp : public ApiOp {
    ApiTimer & t;
    int        what; /* 0 create, 1 destroy, 2 reset */
    TimerOp( ApiTimer &tm,  int w,  pthread_mutex_t *m,  pthread_cond_t *c )
      : ApiOp( m, c ), t( tm ), what( w ) {}
    virtual void run( ApiPipe &pipe ) noexcept {
      TimerQueue & timer_q = pipe.poll.timer;
      if ( this->what != 0 )
        timer_q.remove_timer_cb( this->t, this->t.id, 0 );
      if ( this->what != 1 )
        timer_q.add_timer_double( this->t, this->t.ival, this->t.id, 0 );
    }
  };
  ApiTimer *get_timer( uint32_t id ) noexcept {
    return this->get<ApiTimer>( id, API_ELEM_TIMER );
  }
  /* run a timer op on the epoll thread, serialized on the timer's queue */
  ApiStatus timer_op( ApiTimer &t,  int what ) noexcept {
    ApiQueue * q = this->get_queue( t.queue );
    if ( q == NULL )
      return API_INVALID_QUEUE;
    TimerOp op( t, what, &q->mutex, &q->cond );
    pthread_mutex_lock( &q->mutex );
    this->ev_read->exec( op );
    pthread_mutex_unlock( &q->mutex );
    return API_OK;
  }
  ApiStatus create_timer( uint32_t &event,  uint32_t queue,  void *cb,
                          double ival,  const void *closure ) noexcept {
    event = API_INVALID_ID;
    if ( this->get_queue( queue ) == NULL )
      return API_INVALID_QUEUE;
    if ( ival < 0.0 || ival != ival )      /* negative or NaN; 0.0 is legal */
      return API_INVALID_ARG;
    ApiTimer * t = this->make<ApiTimer>( *this, API_ELEM_TIMER );
    t->queue = queue;
    t->cb    = cb;
    t->cl    = closure;
    t->ival  = ival;
    ApiStatus s = this->timer_op( *t, 0 );
    if ( s == API_OK )
      event = t->id;
    return s;
  }
  /* remove from the epoll timer queue and the registry; the caller frees */
  ApiTimer *destroy_timer( uint32_t event ) noexcept {
    ApiTimer * t = this->rem<ApiTimer>( event, API_ELEM_TIMER );
    if ( t != NULL ) {
      this->timer_op( *t, 1 );
      t->cb = NULL;
    }
    return t;
  }
  ApiStatus reset_timer_interval( uint32_t event,  double ival ) noexcept {
    if ( ival < 0.0 || ival != ival )
      return API_INVALID_ARG;
    ApiTimer * t = this->get_timer( event );
    if ( t == NULL )
      return API_INVALID_EVENT;
    t->ival = ival;
    return this->timer_op( *t, 2 );
  }
  ApiStatus get_timer_interval( uint32_t event,  double &ival ) noexcept {
    ApiTimer * t = this->get_timer( event );
    if ( t == NULL )
      return API_INVALID_EVENT;
    ival = t->ival;
    return API_OK;
  }

  /* --- dispatcher threads --- */
  ApiDispatcher *create_dispatcher( uint32_t able,  double idle_timeout ) noexcept {
    ApiDispatcher * d = this->make<ApiDispatcher>( *this, API_ELEM_DISPATCHER );
    d->queue        = able;
    d->idle_timeout = idle_timeout;
    pthread_attr_t attr;
    pthread_attr_init( &attr );
    pthread_attr_setdetachstate( &attr, 1 );
    if ( this->get_queue( able ) != NULL ) {
      d->is_queue = true;
      pthread_create( &d->thr_id, &attr, api_disp_thread, d );
    }
    else if ( this->get_queue_group( able ) != NULL ) {
      d->is_queue_group = true;
      pthread_create( &d->thr_id, &attr, api_disp_group_thread, d );
    }
    return d;
  }
  ApiDispatcher *get_dispatcher( uint32_t disp ) noexcept {
    return this->get<ApiDispatcher>( disp, API_ELEM_DISPATCHER );
  }
  ApiStatus join_dispatcher( uint32_t disp ) noexcept {
    ApiDispatcher * d = this->get_dispatcher( disp );
    if ( d == NULL )
      return API_INVALID_DISPATCHER;
    bool wait_for_done = false;
    if ( d->is_queue ) {
      ApiQueue * q = this->get_queue( d->queue );
      bool q_locked = ( q != NULL && pthread_mutex_trylock( &q->mutex ) == 0 );
      d->quit = true;
      if ( q_locked ) {
        pthread_cond_broadcast( &q->cond );
        pthread_mutex_unlock( &q->mutex );
        wait_for_done = true;
      }
    }
    else if ( d->is_queue_group ) {
      ApiQueueGroup * g = this->get_queue_group( d->queue );
      bool q_locked = ( g != NULL && pthread_mutex_trylock( &g->mutex ) == 0 );
      d->quit = true;
      if ( q_locked ) {
        pthread_cond_broadcast( &g->cond );
        pthread_mutex_unlock( &g->mutex );
        wait_for_done = true;
      }
    }
    if ( wait_for_done && ! pthread_equal( pthread_self(), d->thr_id ) ) {
      pthread_mutex_lock( &d->mutex );
      while ( ! d->done )
        pthread_cond_wait( &d->cond, &d->mutex );
      pthread_mutex_unlock( &d->mutex );
    }
    return API_OK;
  }
};

}
}
#endif
