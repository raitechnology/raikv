#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <fcntl.h>
#include <errno.h>
#ifndef _MSC_VER
#include <unistd.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/un.h>
#else
#include <raikv/win.h>
#endif
#include <raikv/ev_net.h>
#include <raikv/ev_key.h>
#include <raikv/ev_publish.h>
#include <raikv/kv_pubsub.h>
#include <raikv/timer_queue.h>
#include <raikv/pattern_cvt.h>

using namespace rai;
using namespace kv;
uint32_t rai::kv::kv_pub_debug;

bool rai::kv::ev_would_block( int err ) noexcept {
  return ( err == EINTR || err == EAGAIN || err == EWOULDBLOCK ||
           err == EINPROGRESS );
}

RoutePDB::RoutePDB( EvPoll &p ) noexcept
  : RoutePublish( p, "default", -1, -1 ), timer( p.timer ) {}

RoutePublish::RoutePublish( EvPoll &p,  const char *svc,  uint32_t svc_num,
                            uint32_t rte_id ) noexcept
            : RouteDB( p.g_bloom_db ), poll( p ), map( 0 ), pubsub( 0 ),
              keyspace( 0 ), service_name( svc ), svc_id( svc_num ),
              route_id( rte_id ), ctx_id( (uint32_t) -1 ),
              dbx_id( (uint32_t) -1 ), key_flags( 0 ) {}

uint8_t
EvPoll::register_type( const char *s ) noexcept
{
  uint8_t t = (uint8_t) kv_crc_c( s, ::strlen( s ), 0 );
  if ( t == 0 ) t = 1;
  for ( int i = 0; i < 256; i++ ) {
    const char *x = this->sock_type_str[ t ];
    if ( x == NULL ) {
      this->sock_type_str[ t ] = s;
      return t;
    }
    if ( ::strcmp( x, s ) == 0 )
      return t;
    if ( t == 0xff )
      t = 1;
    else
      t++;
  }
  /* should be unique */
  fprintf( stderr, "No types left %s\n", s );
  exit( 1 );
  return 0;
}

struct DbgRouteNotify : public RouteNotify {
  void * operator new( size_t, void *ptr ) { return ptr; }
  DbgRouteNotify( RoutePublish &p ) : RouteNotify( p ) {}
};

int
EvPoll::init( int numfds,  bool prefetch ) noexcept
{
  uint32_t n   = align<uint32_t>( numfds, 2 ); /* 64 bit boundary */
  uint32_t mfd = EvPoll::ALLOC_INCR;
  size_t   sz  = sizeof( this->ev[ 0 ] ) * n;
  if ( prefetch )
    this->prefetch_queue = EvPrefetchQueue::create();
  this->init_ns = current_realtime_coarse_ns();
  this->now_ns  = this->init_ns;

  if ( (this->efd = ::epoll_create( n )) < 0 ) {
    perror( "epoll" );
    return -1;
  }
  this->nfds = n;
  this->ev   = (struct epoll_event *) ::malloc( sz );
  this->sock = (EvSocket **) ::malloc( mfd * sizeof( this->sock[ 0 ] ) );
  if ( this->ev == NULL || this->sock == NULL ) {
    perror( "malloc" );
    return -1;
  }
  for ( uint32_t i = 0; i < mfd; i++ )
    this->sock[ i ] = NULL;
  this->maxfd = mfd - 1;
  this->timer.queue = EvTimerQueue::create_timer_queue( *this );
  if ( this->timer.queue == NULL )
    return -1;
#if 0
  void * p = ::malloc( sizeof( DbgRouteNotify ) );
  RouteNotify *x = new ( p ) DbgRouteNotify( this->sub_route );
  this->sub_route.add_route_notify( *x );
#endif
  return 0;
}

int
RoutePublish::init_shm( EvShm &shm ) noexcept
{
  this->map    = shm.map;
  this->ctx_id = shm.ctx_id;
  this->dbx_id = shm.dbx_id;
  if ( this->map != NULL && ! shm.map->hdr.ht_read_only ) {
    uint64_t map_init = shm.map->hdr.create_stamp;
    if ( (this->pubsub = KvPubSub::create( *this, shm.ipc_name,
                                           map_init, shm.ctx_name )) == NULL ) {
      fprintf( stderr, "Unable to open kv pub sub\n" );
      return -1;
    }
#if 0
    void * p = ::malloc( sizeof( RedisKeyspaceNotify ) );
    if ( p == NULL ) {
      perror( "malloc" );
      return -1;
    }
    this->keyspace = new ( p ) RedisKeyspaceNotify( *this );
    this->add_route_notify( *this->keyspace );
#endif
  }
  else if ( shm.ipc_name != NULL ) {
    if ( (this->pubsub =
          KvPubSub::create( *this, shm.ipc_name, 0, shm.ctx_name )) == NULL ) {
      fprintf( stderr, "Unable to open kv pub sub\n" );
      return -1;
    }
  }
  return 0;
}

void
EvPoll::add_write_poll( EvSocket *s ) noexcept
{
  struct epoll_event event;
  s->set_poll( IN_EPOLL_WRITE );
  ::memset( &event, 0, sizeof( struct epoll_event ) );
  event.data.fd = s->fd;
  event.events  = EPOLLOUT | EPOLLRDHUP/* | EPOLLET*/;
  if ( ::epoll_ctl( this->efd, EPOLL_CTL_MOD, s->fd, &event ) < 0 ) {
    s->set_sock_err( EV_ERR_WRITE_POLL, errno );
    s->set_poll( IN_EPOLL_READ );
    s->popall();
    s->idle_push( EV_CLOSE );
    return;
  }
  this->wr_count++;
  if ( this->wr_timeout_ns != 0 ||
       ( this->conn_timeout_ns != 0 && s->bytes_sent + s->bytes_recv == 0 ) )
    this->push_write_queue( s );
}

void
EvPoll::remove_write_poll( EvSocket *s ) noexcept
{
  struct epoll_event event;
  this->remove_write_queue( s );
  s->set_poll( IN_EPOLL_READ );
  s->pop( EV_WRITE_POLL );
  s->idle_push( EV_WRITE_HI );
  this->wr_count--;
  ::memset( &event, 0, sizeof( struct epoll_event ) );
  event.data.fd = s->fd;
  event.events  = EPOLLIN | EPOLLRDHUP | EPOLLET;
  if ( ::epoll_ctl( this->efd, EPOLL_CTL_MOD, s->fd, &event ) < 0 ) {
    s->set_sock_err( EV_ERR_READ_POLL, errno );
    s->popall();
    s->idle_push( EV_CLOSE );
  }
}

int
EvPoll::wait( int ms ) noexcept
{
  EvSocket *s;
  int n, m = 0;
  EvState do_event;
#ifndef HAVE_TIMERFD
  uint64_t us = ms * 1000;
  if ( this->timer.queue->is_timer_ready( us ) ) {
    this->timer.queue->idle_push( EV_PROCESS );
    return 1;
  }
  n = ::wp_epoll_wait( this->efd, this->ev, this->nfds, us );
#else
  n = ::epoll_wait( this->efd, this->ev, this->nfds, ms );
#endif
  if ( n < 0 ) {
    if ( errno == EINTR )
      return 0;
    perror( "epoll_wait" );
    return -1;
  }
  for ( int i = 0; i < n; i++ ) {
    s = this->sock[ this->ev[ i ].data.fd ];

    if ( s->test( EV_WRITE_POLL ) ) {
      if ( ( this->ev[ i ].events & EPOLLOUT ) != 0 ) {
        this->remove_write_poll( s ); /* move back to read poll and try write */
        continue;
      }
      this->remove_write_queue( s ); /* a EPOLLERR, can't write, close it */
      do_event = EV_CLOSE;
    }
    else {
      if ( ( this->ev[ i ].events & EPOLLIN ) != 0 )
        do_event = EV_READ;
      else if ( ( this->ev[ i ].events & ( EPOLLERR | EPOLLHUP ) ) != 0 ) {
        /* should make this a state, happens with udp connect fails *
        int err = 0;
        socklen_t errlen = sizeof( err );
        if ( ::getsockopt( this->ev[ i ].data.fd, SOL_SOCKET, SO_ERROR,
                           &err, &errlen ) == 0 ) {
          printf( "so_err %d/%s\n", err, strerror( err ) );
        }
        if ( s->sock_base == EV_UDP_BASE )
          goto skip_event;
        else */
        do_event = EV_READ;
      }
      else
        do_event = EV_READ;
    }
    if ( do_event == EV_READ ) {
      if ( s->test_opts( OPT_READ_HI ) )
        do_event = EV_READ_HI;
      s->idle_push( do_event );
    }
    else {
      s->popall();
      s->idle_push( EV_CLOSE ); /* maybe print err if bytes unsent */
    }
  }
  /* check if write pollers are timing out */
  if ( ! this->ev_write.is_empty() ) {
    uint64_t ns = this->current_coarse_ns();
    for (;;) {
      s = this->ev_write.heap[ 0 ];
      if ( ns <= s->PeerData::active_ns ) /* XXX should use monotonic time */
        break;
      uint64_t delta = ns - s->PeerData::active_ns;
      /* XXX this may hold the connect timeouts longer than they should */
      if ( delta > this->wr_timeout_ns ||
           ( delta > this->conn_timeout_ns &&
             s->bytes_sent + s->bytes_recv == 0 ) ) {
        this->remove_write_poll( s );
        this->idle_close( s, delta );
        m++;
        if ( this->ev_write.is_empty() )
          break;
      }
      else {
        break;
      }
    }
  }
  return n + m; /* returns the number of new events */
}
/* allocate an fd for a null sockets which are used for event based objects
 * that wait on timers and/or subscriptions */
int
EvPoll::get_null_fd( void ) noexcept
{
#ifndef _MSC_VER
  if ( this->null_fd < 0 ) {
    this->null_fd = ::open( "/dev/null", O_RDWR | O_NONBLOCK );
    return this->null_fd;
  }
  return ::dup( this->null_fd );
#else
  return ::wp_make_null_fd();
#endif
}
/* enable epolling of sock fd */
int
EvPoll::add_sock( EvSocket *s ) noexcept
{
  if ( s->fd < 0 ) /* must be a valid fd */
    return -EV_ERR_BAD_FD;
  /* make enough space for fd */
  if ( (uint32_t) s->fd > this->maxfd ) {
    uint32_t mfd = align<uint32_t>( s->fd + 1, EvPoll::ALLOC_INCR );
    EvSocket **tmp;
    tmp = (EvSocket **)
          ::realloc( this->sock, mfd * sizeof( this->sock[ 0 ] ) );
    if ( tmp == NULL )
      return s->set_sock_err( EV_ERR_ALLOC, errno );
    for ( uint32_t i = this->maxfd + 1; i < mfd; i++ )
      tmp[ i ] = NULL;
    this->sock  = tmp;
    this->maxfd = mfd - 1;
  }
  if ( ! s->test_opts( OPT_NO_POLL ) ) {
    /* add to poll set */
    struct epoll_event event;
    s->set_poll( IN_EPOLL_READ );
    ::memset( &event, 0, sizeof( struct epoll_event ) );
    event.data.fd = s->fd;
    event.events  = EPOLLIN | EPOLLRDHUP | EPOLLET;
    if ( ::epoll_ctl( this->efd, EPOLL_CTL_ADD, s->fd, &event ) < 0 ) {
      return s->set_sock_err( EV_ERR_READ_POLL, errno );
    }
  }
  this->sock[ s->fd ] = s;
  this->fdcnt++;
  /* add to active list */
  s->set_list( IN_ACTIVE_LIST );
  this->active_list.push_tl( s );
  /* if sock starts in write mode, add it to the queue */
  s->prio_cnt = this->prio_tick;
  if ( s->sock_state != 0 )
    this->push_event_queue( s );
  uint64_t ns = this->current_coarse_ns();
  s->start_ns   = ns;
  s->active_ns  = ns;
  s->id         = ++this->next_id;
  s->init_stats();
  return 0;
}
/* remove a sock fd from epolling */
void
EvPoll::remove_sock( EvSocket *s ) noexcept
{
  struct epoll_event event;
  if ( s->fd < 0 )
    return;
  if ( s->in_list( IN_FREE_LIST ) ) {
    fprintf( stderr, "!!! removing free list sock\n" );
    return;
  }
  /* remove poll set */
  if ( (uint32_t) s->fd <= this->maxfd && this->sock[ s->fd ] == s ) {
    if ( ! s->test_opts( OPT_NO_POLL ) ) { /* if is a epoll sock */
      if ( s->in_poll( IN_EPOLL_WRITE ) )
        this->wr_count--;
      s->set_poll( IN_NO_LIST );
      ::memset( &event, 0, sizeof( struct epoll_event ) );
      event.data.fd = s->fd;
      event.events  = 0;
      if ( ::epoll_ctl( this->efd, EPOLL_CTL_DEL, s->fd, &event ) < 0 )
        s->set_sock_err( EV_ERR_READ_POLL, errno );
    }
    this->sock[ s->fd ] = NULL;
    this->fdcnt--;
  }
  /* close if wants */
  if ( ! s->test_opts( OPT_NO_CLOSE ) && s->fd != this->null_fd ) {
    int status;
#ifndef _MSC_VER
    status = ::close( s->fd );
#else
    status = ::wp_close_fd( s->fd );
#endif
    if ( status != 0 )
      s->set_sock_err( EV_ERR_CLOSE, errno );
  }
  /* remove from queues, if in them */
  this->remove_event_queue( s );
  this->remove_write_queue( s );
  s->popall();

  if ( s->in_list( IN_ACTIVE_LIST ) ) {
    s->client_stats( this->peer_stats );
    s->set_list( IN_NO_LIST );
    this->active_list.pop( s );
  }
  /* release memory buffers */
  s->release();
  s->fd = -1;
}

void
EvPoll::idle_close( EvSocket *s,  uint64_t ns ) noexcept
{
  s->set_sock_err( EV_ERR_WRITE_TIMEOUT, (uint16_t) ( ns / 1000000000ULL ) );
  /* log closed fd */
  this->remove_write_queue( s );
  s->popall();
  s->idle_push( EV_CLOSE );
}

void *
EvPoll::alloc_sock( size_t sz ) noexcept
{
  size_t    need = kv::align<size_t>( sz, 64 );
  uint8_t * p    = (uint8_t *) this->sock_mem;;

  if ( need > this->sock_mem_left ) {
    if ( need > 128 * 1024 )
      return aligned_malloc( need );
    this->sock_mem = aligned_malloc( 1024 * 1024 );
    if ( this->sock_mem == NULL ) {
      this->sock_mem_left = 0;
      ::perror( "alloc_sock: no mem" );
      return NULL;
    }
    this->sock_mem_left = 1024 * 1024;
    p = (uint8_t *) this->sock_mem;
  }
  this->sock_mem = &p[ need ];
  this->sock_mem_left -= need;
  return p;
}

const char *
EvSocket::state_string( EvState state ) noexcept
{
  switch ( state ) {
    case EV_READ_HI:    return "read_hi";
    case EV_CLOSE:      return "close";
    case EV_WRITE_POLL: return "write_poll";
    case EV_WRITE_HI:   return "write_hi";
    case EV_READ:       return "read";
    case EV_PROCESS:    return "process";
    case EV_PREFETCH:   return "prefetch";
    case EV_WRITE:      return "write";
    case EV_SHUTDOWN:   return "shutdown";
    case EV_READ_LO:    return "read_lo";
    case EV_BUSY_POLL:  return "busy_poll";
  }
  return "unknown_state";
}
#ifdef EV_NET_DBG
void
EvStateDbg::print_dbg( void )
{
  EvSocket & sock = static_cast<EvSocket &>( *this );
  for ( int i = 0; i < 16; i++ ) {
    if ( this->current.cnt[ i ] != this->last.cnt[ i ] ) {
      uint64_t diff = this->current.cnt[ i ] - this->last.cnt[ i ];
      this->last.cnt[ i ] = this->current.cnt[ i ];
      printf( "%s %s: %" PRIu64 "\n",
        sock.kind, EvSocket::state_string( (EvState) i ), diff );
    }
  }
}
#endif
/* vtable dispatch */
const char *
EvSocket::type_string( void ) noexcept
{
  const char *x = this->poll.sock_type_str[ this->sock_type ];
  if ( x != NULL )
    return x;
  if ( this->sock_type == 0 )
    return "ev_socket_0";
  return "ev_socket";
}

void
EvSocket::dbg( const char *where ) noexcept
{
  fprintf( stderr, "dbg: %s at %s\n", this->type_string(), where );
}

bool
EvSocket::busy_poll( void ) noexcept
{
  uint64_t nb = this->bytes_recv;
  this->read();
  return nb != this->bytes_recv; /* if progress */
}

void
EvSocket::process_shutdown( void ) noexcept
{
  /* transition from shutdown to close */
  this->pushpop( EV_CLOSE, EV_SHUTDOWN );
}

void
EvSocket::process_close( void ) noexcept
{
  /* after this, close fd */
  this->poll.remove_sock( this );
  this->poll.push_free_list( this );
}

/* thsee are optional, but not necessary for a working protocol */
/* timer, return true to rearm same interval */
bool EvSocket::timer_expire( uint64_t, uint64_t ) noexcept { return false; }
/* pub sub, subject resolution, deliver publish message */
bool EvSocket::hash_to_sub( uint32_t, char *, size_t & ) noexcept { return false; }
uint8_t EvSocket::is_subscribed( const NotifySub & ) noexcept { return EV_NOT_SUBSCRIBED; }
uint8_t EvSocket::is_psubscribed( const NotifyPattern & ) noexcept { return EV_NOT_SUBSCRIBED; }
bool EvSocket::on_msg( EvPublish & ) noexcept { return true; }
/* key prefetch, requires a batch of keys to hide latency */
void EvSocket::key_prefetch( EvKeyCtx & ) noexcept {}
int  EvSocket::key_continue( EvKeyCtx & ) noexcept { return 0; }

EvListen::EvListen( EvPoll &p,  const char *lname,  const char *name )
    : EvSocket( p, p.register_type( lname ), EV_LISTEN_BASE ), accept_cnt( 0 ),
      notify( 0 ), accept_sock_type( p.register_type( name ) )
{
  this->timer_id = (uint64_t) this->accept_sock_type << 56;
}

void
EvListen::read( void ) noexcept
{
  if ( this->accept() != NULL )
    this->accept_cnt++;
}

/* listeners don't do these */
void EvListen::write( void ) noexcept {}   /* nothing to write */
void EvListen::process( void ) noexcept {} /* nothing to process */
void EvListen::release( void ) noexcept {} /* no buffers */

void
EvListen::reset_read_poll( void ) noexcept
{
  this->pop3( EV_READ, EV_READ_LO, EV_READ_HI );
#ifdef _MSC_VER
  /* reset EPOLLET */
  struct epoll_event event;
  event.data.fd = this->fd;
  event.events  = EPOLLIN | EPOLLRDHUP | EPOLLET;
  ::epoll_ctl( this->poll.efd, EPOLL_CTL_MOD, this->fd, &event );
#endif
}

void
EvPoll::drain_prefetch( void ) noexcept
{
  EvPrefetchQueue & pq = *this->prefetch_queue;
  EvKeyCtx * ctx[ PREFETCH_SIZE ];
  EvSocket * s;
  size_t i, j, sz, cnt = 0;

  sz = PREFETCH_SIZE;
  if ( sz > pq.count() )
    sz = pq.count();
  for ( i = 0; i < sz; i++ ) {
    ctx[ i ] = pq.pop();
    EvKeyCtx & k = *ctx[ i ];
    s = k.owner;
    s->key_prefetch( k );
  }
  /*this->prefetch_cnt[ sz ]++;*/
  i &= ( PREFETCH_SIZE - 1 );
  for ( j = 0; ; ) {
    EvKeyCtx & k = *ctx[ j ];
    s = k.owner;
    switch( s->key_continue( k ) ) {
      default:
      case EK_SUCCESS:
        s->msgs_sent++;
        s->process();
        if ( s->test( EV_PREFETCH ) != 0 ) {
          s->pop( EV_PREFETCH ); /* continue prefetching */
        }
        else { /* push back into queue if has an event for read or write */
          if ( s->sock_state != 0 )
            this->push_event_queue( s );
        }
        break;
      case EK_DEPENDS:   /* incomplete, depends on another key */
        pq.push( &k );
        break;
      case EK_CONTINUE:  /* key complete, more keys to go */
        break;
    }
    cnt++;
    if ( --sz == 0 && pq.is_empty() ) {
      /*this->prefetch_cnt[ 0 ] += cnt;*/
      return;
    }
    j = ( j + 1 ) & ( PREFETCH_SIZE - 1 );
    if ( ! pq.is_empty() ) {
      do {
        ctx[ i ] = pq.pop();
        EvKeyCtx & k = *ctx[ i ];
        s = k.owner;
        s->key_prefetch( k );
        /*ctx[ i ]->prefetch();*/
        i = ( i + 1 ) & ( PREFETCH_SIZE - 1 );
      } while ( ++sz < PREFETCH_SIZE && ! pq.is_empty() );
      /*this->prefetch_cnt[ sz ]++;*/
    }
  }
}

uint64_t
EvPoll::current_coarse_ns( void ) noexcept
{
  /*if ( this->sub_route.map != NULL && ! this->sub_route.map->hdr.ht_read_only )
    this->now_ns = this->sub_route.map->hdr.current_stamp;
  else*/
  this->now_ns = current_realtime_coarse_ns();
  return this->now_ns;
}

uint64_t
EvPoll::create_ns( void ) const noexcept
{
  if ( this->sub_route.map != NULL && ! this->sub_route.map->hdr.ht_read_only )
    return this->sub_route.map->hdr.create_stamp;
  return this->init_ns;
}

int
EvPoll::dispatch( void ) noexcept
{
  EvSocket * s;
  uint64_t start   = this->prio_tick,
           curr_ns = this->current_coarse_ns(),
           busy_ns = this->timer.queue->busy_delta( curr_ns ),
           used_ns = 0;
  int      ret     = DISPATCH_IDLE;
  EvState  state;

  if ( this->quit )
    this->process_quit();
  for (;;) {
  next_tick:;
    if ( start + 300 < this->prio_tick ) { /* run poll() at least every 300 */
      ret |= POLL_NEEDED | DISPATCH_BUSY;
      return ret;
    }
    if ( used_ns >= busy_ns ) { /* if a timer may expire, run poll() */
      if ( used_ns > 0 ) {
        /* the real time is updated at coarse intervals (10 to 100 us) */
        curr_ns = this->current_coarse_ns();
        /* how much busy work before timer expires */
        busy_ns = this->timer.queue->busy_delta( curr_ns );
      }
      if ( busy_ns == 0 ) {
        /* if timer is already an event (in_queue=1), dispatch that first */
        if ( ! this->timer.queue->in_queue( IN_EVENT_QUEUE ) ) {
          ret |= POLL_NEEDED;
          if ( start != this->prio_tick )
            ret |= DISPATCH_BUSY;
          return ret;
        }
      }
      used_ns = 0;
    }
    used_ns += 300; /* guess 300 ns is upper bound for dispatching event */
    if ( this->ev_queue.is_empty() ) {
      if ( this->prefetch_pending > 0 ) {
      do_prefetch:;
        this->prefetch_pending = 0;
        this->drain_prefetch(); /* run prefetch */
        if ( ! this->ev_queue.is_empty() )
          goto next_tick;
      }

      if ( start != this->prio_tick )
        ret |= DISPATCH_BUSY;
      return ret;
    }
    s     = this->ev_queue.heap[ 0 ];
    state = s->get_dispatch_state();
    EV_DBG_DISPATCH( s, state );
    this->prio_tick++;
    if ( state > EV_PREFETCH && this->prefetch_pending > 0 )
      goto do_prefetch;
    s->set_queue( IN_NO_QUEUE );
    this->ev_queue.pop();
    /*printf( "fd %d type %s state %s\n",
      s->fd, sock_type_string( s->type ), state_string( (EvState) state ) );*/
    /*printf( "dispatch %u %u (%x)\n", s->type, state, s->sock_state );*/
    switch ( state ) {
      case EV_READ:
      case EV_READ_LO:
      case EV_READ_HI:
        s->active_ns = curr_ns;
        s->read();
        break;
      case EV_PROCESS:
        s->process();
        break;
      case EV_PREFETCH:
        s->pop( EV_PREFETCH );
        this->prefetch_pending++;
        goto next_tick; /* skip putting s back into event queue */
      case EV_WRITE:
      case EV_WRITE_HI:
      case EV_WRITE_POLL:
        s->write();
        break;
      case EV_SHUTDOWN:
        s->process_shutdown();
        break;
      case EV_CLOSE:
        s->popall();
        s->process_close();
        goto next_tick; /* sock is no longer valid */
      case EV_BUSY_POLL:
        ret |= BUSY_POLL;
        if ( ! s->busy_poll() ) { /* if no progress made */
          s->prio_cnt = this->prio_tick;
          this->push_event_queue( s );
          if ( start + 1 != this->prio_tick )
            ret |= DISPATCH_BUSY; /* some progress made */
          return ret;
        }
        /* otherwise continue busy */
        break;
    }
    if ( s->sock_state != 0 ) {
      if ( s->test( EV_WRITE_HI ) ) {
        if ( s->test( EV_WRITE_POLL ) ) {
          ret |= POLL_NEEDED;
          this->add_write_poll( s );
          continue; /* don't put into event queue */
        }
        ret |= WRITE_PRESSURE;
      }
      s->prio_cnt = this->prio_tick;
      this->push_event_queue( s );
    }
  }
}

namespace rai {
namespace kv {
struct ForwardBase {
  RoutePublish & sub_route;
  uint32_t       total;
  ForwardBase( RoutePublish &p ) : sub_route( p ), total( 0 ) {}
  void debug_subject( EvPublish &pub, EvSocket *s, const char *w ) {
    printf( "%s(%.*s,%x,%x) %s -> %s.%s(%u)\n", w,
            (int) pub.subject_len, pub.subject, pub.subj_hash,
            kv_crc_c( pub.msg, pub.msg_len, 0 ),
            this->sub_route.service_name,
            s->name[ 0 ] ? s->name : s->peer_address.buf, s->kind, s->fd );
  }
  void debug_total( EvPublish &pub ) {
    if ( this->total == 0 ) {
      printf( "no routes for %.*s\n", (int) pub.subject_len, pub.subject );
    }
  }
};
struct ForwardAll : public ForwardBase {
  ForwardAll( RoutePublish &p ) : ForwardBase( p ) {}
  bool on_msg( uint32_t fd,  EvPublish &pub ) {
    EvSocket * s;
    if ( fd <= this->sub_route.poll.maxfd &&
         (s = this->sub_route.poll.sock[ fd ]) != NULL ) {
      this->total++;
      if ( kv_pub_debug )
        this->debug_subject( pub, s, "fwd_all" );
      return s->on_msg( pub );
    }
    return true;
  }
};
struct ForwardSome : public ForwardBase {
  uint32_t * routes,
             rcnt;
  ForwardSome( RoutePublish &p,  uint32_t *rtes,  uint32_t cnt )
    : ForwardBase( p ), routes( rtes ), rcnt( cnt ) {}
  bool on_msg( uint32_t fd,  EvPublish &pub ) {
    EvSocket * s;
    uint32_t off = bsearch_route( fd, this->routes, this->rcnt );
    if ( off == this->rcnt || this->routes[ off ] != fd ) {
      if ( fd <= this->sub_route.poll.maxfd &&
           (s = this->sub_route.poll.sock[ fd ]) != NULL ) {
        this->total++;
        if ( kv_pub_debug )
          this->debug_subject( pub, s, "fwd_som" );
        return s->on_msg( pub );
      }
    }
    return true;
  }
};
struct ForwardSet : public ForwardBase {
  const BitSpace & fdset;
  ForwardSet( RoutePublish &p,  const BitSpace &b )
    : ForwardBase( p ), fdset( b ) {}
  bool on_msg( uint32_t fd,  EvPublish &pub ) {
    EvSocket * s;
    if ( this->fdset.is_member( fd ) && fd <= this->sub_route.poll.maxfd &&
         (s = this->sub_route.poll.sock[ fd ]) != NULL ) {
      this->total++;
      if ( kv_pub_debug )
        this->debug_subject( pub, s, "fwd_set" );
      return s->on_msg( pub );
    }
    return true;
  }
};
struct ForwardExcept : public ForwardBase {
  const BitSpace & fdexcept;
  ForwardExcept( RoutePublish &p,  const BitSpace &b )
    : ForwardBase( p ), fdexcept( b ) {}
  bool on_msg( uint32_t fd,  EvPublish &pub ) {
    EvSocket * s;
    if ( ! this->fdexcept.is_member( fd ) && fd <= this->sub_route.poll.maxfd &&
         (s = this->sub_route.poll.sock[ fd ]) != NULL ) {
      this->total++;
      if ( kv_pub_debug )
        this->debug_subject( pub, s, "fwd_exc" );
      return s->on_msg( pub );
    }
    return true;
  }
};
struct ForwardNotFd : public ForwardBase {
  uint32_t not_fd;
  ForwardNotFd( RoutePublish &p,  uint32_t fd )
    : ForwardBase( p ), not_fd( fd ) {}
  bool on_msg( uint32_t fd,  EvPublish &pub ) {
    EvSocket * s;
    if ( fd != this->not_fd && fd <= this->sub_route.poll.maxfd &&
         (s = this->sub_route.poll.sock[ fd ]) != NULL ) {
      this->total++;
      if ( kv_pub_debug )
        this->debug_subject( pub, s, "fwd_not" );
      return s->on_msg( pub );
    }
    return true;
  }
};
struct ForwardNotFd2 : public ForwardBase {
  uint32_t not_fd, not_fd2;
  ForwardNotFd2( RoutePublish &p,  uint32_t fd,  uint32_t fd2 )
    : ForwardBase( p ), not_fd( fd ), not_fd2( fd2 ) {}
  bool on_msg( uint32_t fd,  EvPublish &pub ) {
    EvSocket * s;
    if ( fd != this->not_fd && fd != this->not_fd2 &&
         fd <= this->sub_route.poll.maxfd &&
         (s = this->sub_route.poll.sock[ fd ]) != NULL ) {
      this->total++;
      if ( kv_pub_debug )
        this->debug_subject( pub, s, "fwd_nt2" );
      return s->on_msg( pub );
    }
    return true;
  }
};
}
}
/* different publishers for different size route matches, one() is the most
 * common, but multi / queue need to be used with multiple routes */
template<class Forward>
static bool
publish_one( EvPublish &pub,  RoutePublishData &rpd,  Forward &fwd ) noexcept
{
  uint32_t * routes = rpd.routes;
  uint32_t   rcount = rpd.rcount;
  uint32_t   hash[ 1 ];
  uint8_t    prefix[ 1 ];
  bool       flow_good = true;

  pub.hash       = hash;
  pub.prefix     = prefix;
  pub.prefix_cnt = 1;
  hash[ 0 ]      = rpd.hash;
  prefix[ 0 ]    = rpd.prefix;
  for ( uint32_t i = 0; i < rcount; i++ ) {
    flow_good &= fwd.on_msg( routes[ i ], pub );
  }
  if ( kv_pub_debug )
    fwd.debug_total( pub );
  return flow_good;
}

template<class Forward>
static bool
publish_multi_64( EvPublish &pub,  RoutePublishData *rpd,  uint8_t n,
                  Forward &fwd ) noexcept
{
  uint64_t bits = 0;
  uint8_t  rpd_fds[ MAX_RTE ],
           prefix[ MAX_RTE ];
  uint32_t hash[ MAX_RTE ], /* limit is number of rpd elements */
           min_route, i, cnt;
  bool     flow_good = true;

  for ( i = 0; i < n; i++ ) {
    uint32_t fd  = rpd[ i ].routes[ 0 ];
    bits        |= (uint64_t) 1 << fd;
    rpd_fds[ i ] = fd;
  }
  pub.hash   = hash;
  pub.prefix = prefix;
  for (;;) {
    if ( bits == 0 )
      return flow_good; /* if no routes left */

    min_route = kv_ffsl( bits ) - 1;
    bits     &= ~( (uint64_t) 1 << min_route );
    cnt       = 0;
    for ( i = 0; i < n; i++ ) {
      /* accumulate hashes going to min_route */
      if ( min_route == rpd_fds[ i ] ) {
        rpd[ i ].routes++;
        if ( --rpd[ i ].rcount != 0 ) {
          uint32_t fd  = rpd[ i ].routes[ 0 ];
          bits        |= (uint64_t) 1 << fd;
          rpd_fds[ i ] = fd;
        }
        else {
          rpd_fds[ i ] = 64;
        }
        hash[ cnt ]   = rpd[ i ].hash;
        prefix[ cnt ] = rpd[ i ].prefix;
        cnt++;
      }
    }
    /* send hashes to min_route */
    pub.prefix_cnt = cnt;
    flow_good &= fwd.on_msg( min_route, pub );
  }
  if ( kv_pub_debug )
    fwd.debug_total( pub );
}

template<uint8_t N, class Forward>
static bool
publish_multi( EvPublish &pub,  RoutePublishData *rpd,
               Forward &fwd ) noexcept
{
  uint32_t min_route,
           hash[ N ]; /* limit is number of rpd elements */
  uint8_t  prefix[ N ],
           i, cnt;
  bool     flow_good = true;

  pub.hash   = hash;
  pub.prefix = prefix;
  for (;;) {
    for ( i = 0; i < N; ) {
      if ( rpd[ i++ ].rcount > 0 ) {
        min_route = rpd[ i - 1 ].routes[ 0 ];
        goto have_one_route;
      }
    }
    return flow_good; /* if no routes left */

  have_one_route:; /* if at least one route, find minimum route number */
    for ( ; i < N; i++ ) {
      if ( rpd[ i ].rcount > 0 && rpd[ i ].routes[ 0 ] < min_route )
        min_route = rpd[ i ].routes[ 0 ];
    }
    /* accumulate hashes going to min_route */
    cnt = 0;
    for ( i = 0; i < N; i++ ) {
      if ( rpd[ i ].rcount > 0 && rpd[ i ].routes[ 0 ] == min_route ) {
        rpd[ i ].routes++;
        rpd[ i ].rcount--;
        hash[ cnt ]   = rpd[ i ].hash;
        prefix[ cnt ] = rpd[ i ].prefix;
        cnt++;
      }
    }
    /* send hashes to min_route */
    pub.prefix_cnt = cnt;
    flow_good &= fwd.on_msg( min_route, pub );
  }
  if ( kv_pub_debug )
    fwd.debug_total( pub );
}

/* same as above with a prio queue heap instead of linear search */
template<class Forward>
static bool
publish_queue( EvPublish &pub,  RoutePublishQueue &queue,
               Forward &fwd ) noexcept
{
  RoutePublishData * rpd = queue.pop();
  uint32_t           min_route;
  uint32_t           hash[ 65 ];
  bool               flow_good = true;
  uint8_t            cnt;
  uint8_t            prefix[ 65 ];

  pub.hash   = hash;
  pub.prefix = prefix;
  while ( rpd != NULL ) {
    min_route = rpd->routes[ 0 ];
    rpd->routes++;
    rpd->rcount--;
    cnt = 1;
    hash[ 0 ]   = rpd->hash;
    prefix[ 0 ] = rpd->prefix;
    if ( rpd->rcount > 0 )
      queue.push( rpd );
    for (;;) {
      if ( queue.is_empty() ) {
        rpd = NULL;
        break;
      }
      rpd = queue.pop();
      if ( min_route != rpd->routes[ 0 ] )
        break;
      rpd->routes++;
      rpd->rcount--;
      hash[ cnt ]   = rpd->hash;
      prefix[ cnt ] = rpd->prefix;
      cnt++;
      if ( rpd->rcount > 0 )
        queue.push( rpd );
    }
    pub.prefix_cnt = cnt;
    flow_good &= fwd.on_msg( min_route, pub );
  }
  if ( kv_pub_debug )
    fwd.debug_total( pub );
  return flow_good;
}

template<class Forward>
static bool
forward_message( EvPublish &pub,  RoutePublish &sub_route,
                 Forward &fwd ) noexcept
{
  RoutePublishCache cache( sub_route, pub.subject, pub.subject_len,
                           pub.subj_hash, pub.shard );
  /* likely cases <= 3 wildcard matches, most likely <= 1 match */
  if ( cache.n == 0 )
    return true;
  if ( cache.n == 1 )
    return publish_one<Forward>( pub, cache.rpd[ 0 ], fwd );
  if ( sub_route.poll.maxfd < 64 )
    return publish_multi_64<Forward>( pub, cache.rpd, cache.n, fwd );
  if ( cache.n == 2 )
    return publish_multi<2, Forward>( pub, cache.rpd, fwd );

  for ( uint8_t i = 0; i < cache.n; i++ )
    sub_route.poll.pub_queue.push( &cache.rpd[ i ] );
  return publish_queue<Forward>( pub, sub_route.poll.pub_queue, fwd );
}

/* match subject against route db and forward msg to fds subscribed, route
 * db contains both exact matches and wildcard prefix matches */
bool
RoutePublish::forward_msg( EvPublish &pub ) noexcept
{
  ForwardAll fwd( *this );
  return forward_message<ForwardAll>( pub, *this, fwd );
}
bool
RoutePublish::forward_with_cnt( EvPublish &pub,  uint32_t &rcnt ) noexcept
{
  ForwardAll fwd( *this );
  bool b = forward_message<ForwardAll>( pub, *this, fwd );
  rcnt = fwd.total;
  return b;
}
bool
RoutePublish::forward_except( EvPublish &pub,
                              const BitSpace &fdexcept ) noexcept
{
  ForwardExcept fwd( *this, fdexcept );
  return forward_message<ForwardExcept>( pub, *this, fwd );
}
bool
RoutePublish::forward_except_with_cnt( EvPublish &pub, const BitSpace &fdexcept,
                                       uint32_t &rcnt ) noexcept
{
  ForwardExcept fwd( *this, fdexcept );
  bool b = forward_message<ForwardExcept>( pub, *this, fwd );
  rcnt = fwd.total;
  return b;
}
bool
RoutePublish::forward_set_with_cnt( EvPublish &pub, const BitSpace &fdset,
                                    uint32_t &rcnt ) noexcept
{
  ForwardSet fwd( *this, fdset );
  bool b = forward_message<ForwardSet>( pub, *this, fwd );
  rcnt = fwd.total;
  return b;
}
#if 0
/* match subject against route db and forward msg to fds subscribed, route
 * db contains both exact matches and wildcard prefix matches */
bool
RoutePublish::forward_msg( EvPublish &pub,  uint32_t *rcount_total,
                           uint8_t pref_cnt,  KvPrefHash *ph ) noexcept
{
  RoutePublishCache cache( *this, pub.subject, pub.subject_len,
                           pub.subj_hash, pref_cnt, ph );
  ForwardAll fwd( *this );
  bool b = true;
  if ( cache.n != 0 ) {
    if ( cache.n == 1 )
      b = publish_one<ForwardAll>( pub, cache.rpd[ 0 ], fwd );
    else if ( cache.n == 2 )
      b = publish_multi<2, ForwardAll>( pub, cache.rpd, fwd );
    else {
      for ( uint8_t i = 0; i < cache.n; i++ )
        this->poll.pub_queue.push( &cache.rpd[ i ] );
      b = publish_queue<ForwardAll>( pub, this->poll.pub_queue, fwd );
    }
  }
  if ( rcount_total != NULL )
    *rcount_total = fwd.total;
  return b;
}
#endif
/* publish to some destination */
bool
RoutePublish::forward_some( EvPublish &pub,  uint32_t *routes,
                            uint32_t rcnt ) noexcept
{
  ForwardSome fwd_some( *this, routes, rcnt );
  return forward_message<ForwardSome>( pub, *this, fwd_some );
}
/* publish except to fd */
bool
RoutePublish::forward_not_fd( EvPublish &pub,  uint32_t not_fd ) noexcept
{
  ForwardNotFd fwd_not( *this, not_fd );
  return forward_message<ForwardNotFd>( pub, *this, fwd_not );
}
/* publish except to fd */
bool
RoutePublish::forward_not_fd2( EvPublish &pub,  uint32_t not_fd,
                               uint32_t not_fd2 ) noexcept
{
  ForwardNotFd2 fwd_not2( *this, not_fd, not_fd2 );
  return forward_message<ForwardNotFd2>( pub, *this, fwd_not2 );
}
/* publish to destinations, no route matching */
bool
RoutePublish::forward_all( EvPublish &pub,  uint32_t *routes,
                           uint32_t rcnt ) noexcept
{
  bool    flow_good = true;
  uint8_t prefix    = SUB_RTE;

  pub.hash       = &pub.subj_hash;
  pub.prefix     = &prefix;
  pub.prefix_cnt = 1;

  for ( uint32_t i = 0; i < rcnt; i++ ) {
    EvSocket * s;
    uint32_t   fd = routes[ i ];
    if ( fd <= this->poll.maxfd && (s = this->poll.sock[ fd ]) != NULL ) {
      flow_good &= s->on_msg( pub );
    }
  }
  return flow_good;
}

bool
RoutePublish::forward_set( EvPublish &pub,  const BitSpace &fdset ) noexcept
{
  uint32_t cnt       = 0;
  bool     flow_good = true;
  uint8_t  prefix    = SUB_RTE;

  pub.hash       = &pub.subj_hash;
  pub.prefix     = &prefix;
  pub.prefix_cnt = 1;

  for ( size_t i = 0; i < fdset.size; i++ ) {
    uint32_t fd   = (uint32_t) ( i * fdset.WORD_BITS );
    uint64_t word = fdset.ptr[ i ];
    for ( ; word != 0; fd++ ) {
      if ( ( word & 1 ) != 0 ) {
        EvSocket * s;
        if ( fd > this->poll.maxfd )
          break;
        if ( (s = this->poll.sock[ fd ]) != NULL ) {
          flow_good &= s->on_msg( pub );
          if ( kv_pub_debug ) {
            printf( "fwd_set %u\n", fd );
            cnt++;
          }
        }
      }
      word >>= 1;
    }
  }
  if ( kv_pub_debug && cnt == 0 )
    printf( "fwd_set empty\n" );

  return flow_good;
}

bool
RoutePublish::forward_set_not_fd( EvPublish &pub,  const BitSpace &fdset,
                                  uint32_t not_fd ) noexcept
{
  uint32_t cnt       = 0;
  bool     flow_good = true;
  uint8_t  prefix    = SUB_RTE;

  pub.hash       = &pub.subj_hash;
  pub.prefix     = &prefix;
  pub.prefix_cnt = 1;

  for ( size_t i = 0; i < fdset.size; i++ ) {
    uint32_t fd   = (uint32_t) ( i * fdset.WORD_BITS );
    uint64_t word = fdset.ptr[ i ];
    for ( ; word != 0; fd++ ) {
      if ( ( word & 1 ) != 0 && fd != not_fd ) {
        EvSocket * s;
        if ( fd > this->poll.maxfd )
          break;
        if ( (s = this->poll.sock[ fd ]) != NULL ) {
          flow_good &= s->on_msg( pub );
          if ( kv_pub_debug ) {
            printf( "fwd_not_%u %u\n", not_fd, fd );
            cnt++;
          }
        }
      }
      word >>= 1;
    }
  }
  if ( kv_pub_debug && cnt == 0 )
    printf( "fwd_not_%u empty\n", not_fd );
  return flow_good;
}

/* publish to one destination */
bool
RoutePublish::forward_to( EvPublish &pub,  uint32_t fd ) noexcept
{
  uint8_t  n = 0;
  uint32_t phash[ MAX_RTE ];
  uint8_t  prefix[ MAX_RTE ];

  if ( this->is_sub_member( pub.subj_hash, fd ) ) {
    phash[ 0 ]  = pub.subj_hash;
    prefix[ 0 ] = SUB_RTE;
    n++;
  }
  const char * key[ MAX_PRE ];
  size_t       keylen[ MAX_PRE ];
  uint32_t     hash[ MAX_PRE ];
  uint8_t      k = 0, x;

  x = this->setup_prefix_hash( pub.subject, pub.subject_len, key, keylen, hash);
  /* prefix len 0 matches all */
  if ( x > 0 && keylen[ 0 ] == 0 ) {
    if ( this->is_member( 0, hash[ 0 ], fd ) ) {
      phash[ n ]  = hash[ 0 ];
      prefix[ n ] = 0;
      n++;
    }
    k = 1;
  }
  /* the rest of the prefixes are hashed */
  if ( k < x ) {
    kv_crc_c_array( (const void **) &key[ k ], &keylen[ k ], &hash[ k ], x-k );
    do {
      if ( this->is_member( (uint16_t) keylen[ k ], hash[ k ], fd ) ) {
        phash[ n ]  = hash[ k ];
        prefix[ n ] = (uint8_t) keylen[ k ];
        n++;
      }
    } while ( ++k < x );
  }
  pub.hash       = phash;
  pub.prefix     = prefix;
  pub.prefix_cnt = n;

  EvSocket * s;
  bool       b = true;
  if ( fd <= this->poll.maxfd && (s = this->poll.sock[ fd ]) != NULL ) {
    b = s->on_msg( pub );
    if ( kv_pub_debug ) {
      printf( "fwd_to_%u ok\n", fd );
    }
  }
  else if ( kv_pub_debug ) {
    printf( "fwd_to_%u empty\n", fd );
  }
  return b;
}

/* convert a hash into a subject string, this may have collisions */
bool
RoutePublish::hash_to_sub( uint32_t r,  uint32_t h,  char *key,
                           size_t &keylen ) noexcept
{
  EvSocket *s;
  bool b = false;
  if ( r <= this->poll.maxfd && (s = this->poll.sock[ r ]) != NULL )
    b = s->hash_to_sub( h, key, keylen );
  return b;
}
#if 0
void
RoutePublish::resolve_collisions( NotifySub &sub,  RouteRef &rte ) noexcept
{
  for ( uint32_t i = 0; i < rte.rcnt; i++ ) {
    uint32_t r = rte.routes[ i ];
    if ( r != sub.src_fd && r <= this->poll.maxfd ) {
      EvSocket *s;
      if ( (s = this->poll.sock[ r ]) != NULL ) {
        uint8_t v = s->is_subscribed( sub );
        if ( ( v & EV_COLLISION ) != 0  )
          sub.hash_collision = 1;
        if ( ( v & EV_NOT_SUBSCRIBED ) != 0 )
          sub.sub_count--;
      }
    }
  }
}

void
RoutePublish::resolve_pcollisions( NotifyPattern &pat,  RouteRef &rte ) noexcept
{
  for ( uint32_t i = 0; i < rte.rcnt; i++ ) {
    uint32_t r = rte.routes[ i ];
    if ( r != pat.src_fd && r <= this->poll.maxfd ) {
      EvSocket *s;
      if ( (s = this->poll.sock[ r ]) != NULL ) {
        uint8_t v = s->is_psubscribed( pat );
        if ( ( v & EV_COLLISION ) != 0  )
          pat.hash_collision = 1;
        if ( ( v & EV_NOT_SUBSCRIBED ) != 0 )
          pat.sub_count--;
      }
    }
  }
}
#endif
#if 0
/* modify keyspace route */
void
RedisKeyspaceNotify::update_keyspace_route( uint32_t &val,  uint16_t bit,
                                            int add,  uint32_t fd ) noexcept
{
  RouteRef rte( this->sub_route, this->sub_route.zip.route_spc[ 0 ] );
  uint32_t rcnt = 0, xcnt;
  uint16_t & key_flags = this->sub_route.key_flags;

  /* if bit is set, then val has routes */
  if ( ( key_flags & bit ) != 0 )
    rcnt = rte.decompress( val, add > 0 );
  /* if unsub or sub */
  if ( add < 0 )
    xcnt = rte.remove( fd );
  else
    xcnt = rte.insert( fd );
  /* if route changed */
  if ( xcnt != rcnt ) {
    /* if route deleted */
    if ( xcnt == 0 ) {
      val = 0;
      key_flags &= ~bit;
    }
    /* otherwise added */
    else {
      key_flags |= bit;
      val = rte.compress();
    }
    rte.deref();
  }
}
/* track number of subscribes to keyspace subjects to enable them */
void
RedisKeyspaceNotify::update_keyspace_count( const char *sub,  size_t len,
                                            int add,  uint32_t fd ) noexcept
{
  /* keyspace subjects are special, since subscribing to them can create
   * some overhead */
  static const char kspc[] = "__keyspace@",
                    kevt[] = "__keyevent@",
                    lblk[] = "__listblkd@",
                    zblk[] = "__zsetblkd@",
                    sblk[] = "__strmblkd@",
                    moni[] = "__monitor_@";
  if ( ::memcmp( kspc, sub, len ) == 0 ) /* len <= 11, could match multiple */
    this->update_keyspace_route( this->keyspace, EKF_KEYSPACE_FWD, add, fd );
  if ( ::memcmp( kevt, sub, len ) == 0 )
    this->update_keyspace_route( this->keyevent, EKF_KEYEVENT_FWD, add, fd );
  if ( ::memcmp( lblk, sub, len ) == 0 )
    this->update_keyspace_route( this->listblkd, EKF_LISTBLKD_NOT, add, fd );
  if ( ::memcmp( zblk, sub, len ) == 0 )
    this->update_keyspace_route( this->zsetblkd, EKF_ZSETBLKD_NOT, add, fd );
  if ( ::memcmp( sblk, sub, len ) == 0 )
    this->update_keyspace_route( this->strmblkd, EKF_STRMBLKD_NOT, add, fd );
  if ( ::memcmp( moni, sub, len ) == 0 )
    this->update_keyspace_route( this->monitor , EKF_MONITOR     , add, fd );

  /*printf( "%.*s %d key_flags %x\n", (int) len, sub, add, this->key_flags );*/
}
/* client subscribe, notify to kv pubsub */
void
RedisKeyspaceNotify::on_sub( NotifySub &sub ) noexcept
{
  if ( sub.subject_len > 11 )
    this->update_keyspace_count( sub.subject, 11, 1, sub.src_fd );
}

void
RedisKeyspaceNotify::on_unsub( NotifySub &sub ) noexcept
{
  if ( sub.subject_len > 11 )
    this->update_keyspace_count( sub.subject, 11, -1, sub.src_fd );
}
/* client pattern subscribe, notify to kv pubsub */
void
RedisKeyspaceNotify::on_psub( NotifyPattern &pat ) noexcept
{
  size_t pre_len = ( pat.cvt.prefixlen < 11 ? pat.cvt.prefixlen : 11 );
  this->update_keyspace_count( pat.pattern, pre_len, 1, pat.src_fd );
}

void
RedisKeyspaceNotify::on_punsub( NotifyPattern &pat ) noexcept
{
  size_t pre_len = ( pat.cvt.prefixlen < 11 ? pat.cvt.prefixlen : 11 );
  this->update_keyspace_count( pat.pattern, pre_len, -1, pat.src_fd );
}

void
RedisKeyspaceNotify::on_reassert( uint32_t ,  RouteVec<RouteSub> &,
                                  RouteVec<RouteSub> & ) noexcept
{
}
#endif
void
RoutePublish::notify_reassert( uint32_t fd, RouteVec<RouteSub> &sub_db,
                               RouteVec<RouteSub> &pat_db ) noexcept
{
  for ( RouteNotify *p = this->notify_list.hd; p != NULL; p = p->next ) {
    p->on_reassert( fd, sub_db, pat_db );
  }
}
/* defined for vtable to avoid pure virtuals */
void
RouteNotify::on_sub( NotifySub &sub ) noexcept
{
  printf( "on_sub( sub=%.*s, rep=%.*s, h=0x%x, fd=%u, cnt=%u, col=%u, %c )\n",
          sub.subject_len, sub.subject, sub.reply_len, sub.reply,
          sub.subj_hash, sub.src_fd, sub.sub_count, sub.hash_collision,
          sub.src_type );
}
void
RouteNotify::on_unsub( NotifySub &sub ) noexcept
{
  printf( "on_unsub( sub=%.*s, h=0x%x, fd=%u, cnt=%u, col=%u, %c )\n",
          sub.subject_len, sub.subject,
          sub.subj_hash, sub.src_fd, sub.sub_count, sub.hash_collision,
          sub.src_type );
}
void
RouteNotify::on_psub( NotifyPattern &pat ) noexcept
{
  printf( "on_psub( sub=%.*s, rep=%.*s, h=0x%x, fd=%u, cnt=%u, col=%u, %c )\n",
          pat.pattern_len, pat.pattern, pat.reply_len, pat.reply,
          pat.prefix_hash, pat.src_fd, pat.sub_count, pat.hash_collision,
          pat.src_type );
}
void
RouteNotify::on_punsub( NotifyPattern &pat ) noexcept
{
  printf( "on_punsub( sub=%.*s, h=0x%x, fd=%u, cnt=%u, col=%u, %c )\n",
          pat.pattern_len, pat.pattern,
          pat.prefix_hash, pat.src_fd, pat.sub_count,
          pat.hash_collision, pat.src_type );
}
void
RouteNotify::on_reassert( uint32_t, RouteVec<RouteSub> &,
                          RouteVec<RouteSub> & ) noexcept
{
}
void
RouteNotify::on_bloom_ref( BloomRef & ) noexcept
{
}
void
RouteNotify::on_bloom_deref( BloomRef & ) noexcept
{
}

/* shutdown and close all open socks */
void
EvPoll::process_quit( void ) noexcept
{
  if ( this->quit ) {
    EvSocket *s = this->active_list.hd;
    if ( s == NULL ) { /* no more sockets open */
      this->quit = 5;
      return;
    }
    /* wait for socks to flush data for up to 5 interations */
    do {
      if ( this->quit >= 5 ) {
        this->remove_event_queue( s );
        if ( s->sock_state != 0 )
          s->popall();
        s->push( EV_CLOSE );
        this->push_event_queue( s );
      }
      else if ( ! s->test( EV_SHUTDOWN | EV_CLOSE ) ) {
        s->idle_push( EV_SHUTDOWN );
      }
    } while ( (s = (EvSocket *) s->next) != NULL );
    this->quit++;
  }
}
bool
PeerAddrStr::set_sock_addr( int fd ) noexcept
{
  struct sockaddr_storage addr;
  socklen_t addrlen = sizeof( addr );
  if ( ::getsockname( fd, (struct sockaddr *) &addr, &addrlen ) == 0 ) {
    this->set_addr( (struct sockaddr *) &addr );
    return true;
  }
  this->set_addr( NULL );
  return false;
}
bool
PeerAddrStr::set_peer_addr( int fd ) noexcept
{
  struct sockaddr_storage addr;
  socklen_t addrlen = sizeof( addr );
  if ( ::getpeername( fd, (struct sockaddr *) &addr, &addrlen ) == 0 ) {
    this->set_addr( (struct sockaddr *) &addr );
    return true;
  }
  this->set_addr( NULL );
  return false;
}
/* convert sockaddr into a string and set peer_address[] */
void
PeerAddrStr::set_addr( const sockaddr *sa ) noexcept
{
  const size_t maxlen = sizeof( this->buf );
  char         tmp_buf[ maxlen ],
             * s = tmp_buf,
             * t = NULL;
  const char * p;
  size_t       len;
  in_addr    * in;
  in6_addr   * in6;
  uint16_t     in_port;

  if ( sa != NULL ) {
    switch ( sa->sa_family ) {
      case AF_INET6:
        in6     = &((struct sockaddr_in6 *) sa)->sin6_addr;
        in_port = ((struct sockaddr_in6 *) sa)->sin6_port;
        /* check if ::ffff: prefix */
        if ( ((uint64_t *) in6)[ 0 ] == 0 &&
             ((uint16_t *) in6)[ 4 ] == 0 &&
             ((uint16_t *) in6)[ 5 ] == 0xffffU ) {
          in = &((in_addr *) in6)[ 3 ];
          goto do_af_inet;
        }
        p = inet_ntop( AF_INET6, in6, &s[ 1 ], maxlen - 9 );
        if ( p == NULL )
          break;
        /* make [ip6]:port */
        len = ::strlen( &s[ 1 ] ) + 1;
        t   = &s[ len ];
        s[ 0 ] = '[';
        t[ 0 ] = ']';
        t[ 1 ] = ':';
        len = uint64_to_string( ntohs( in_port ), &t[ 2 ] );
        t   = &t[ 2 + len ];
        break;

      case AF_INET:
        in      = &((struct sockaddr_in *) sa)->sin_addr;
        in_port = ((struct sockaddr_in *) sa)->sin_port;
      do_af_inet:;
        p = inet_ntop( AF_INET, in, s, maxlen - 7 );
        if ( p == NULL )
          break;
        /* make ip4:port */
        len = ::strlen( s );
        t   = &s[ len ];
        t[ 0 ] = ':';
        len = uint64_to_string( ntohs( in_port ), &t[ 1 ] );
        t   = &t[ 1 + len ];
        break;
#ifndef _MSC_VER
      case AF_LOCAL:
        len = ::strnlen( ((struct sockaddr_un *) sa)->sun_path,
                         sizeof( ((struct sockaddr_un *) sa)->sun_path ) );
        if ( len > maxlen - 1 )
          len = maxlen - 1;
        ::memcpy( s, ((struct sockaddr_un *) sa)->sun_path, len );
        t = &s[ len ];
        break;
#endif
      default:
        break;
    }
  }
  if ( t != NULL ) {
    /* set strlen */
    set_strlen64( this->buf, tmp_buf, t - s );
  }
  else {
    set_strlen64( this->buf, NULL, 0 );
  }
}

bool
EvSocket::match( PeerMatchArgs &ka ) noexcept
{
  return EvSocket::client_match( *this, &ka, NULL );
}

int
EvSocket::client_list( char *buf,  size_t buflen ) noexcept
{
  /* id=1082 addr=[::1]:43362 fd=8 name= age=1 idle=0 flags=N */
  static const uint64_t ONE_NS = 1000000000;
  uint64_t cur_time_ns = this->poll.current_coarse_ns();
  /* list: 'id=1082 addr=[::1]:43362 fd=8 name= age=1 idle=0 flags=N db=0
   * sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=32768 obl=0 oll=0 omem=0
   * events=r cmd=client\n'
   * id=unique id, addr=peer addr, fd=sock, age=time
   * conn, idle=time idle, flags=mode, db=cur db, sub=channel subs,
   * psub=pattern subs, multi=cmds qbuf=query buf size, qbuf-free=free
   * qbuf, obl=output buf len, oll=outut list len, omem=output mem usage,
   * events=sock rd/wr, cmd=last cmd issued */
  return ::snprintf( buf, buflen,
    "id=%" PRIu64 " addr=%.*s fd=%d name=%.*s kind=%s age=%" PRId64 " idle=%" PRId64 " ",
    this->PeerData::id,
    (int) this->PeerData::get_peer_address_strlen(),
    this->PeerData::peer_address.buf,
    this->PeerData::fd,
    (int) this->PeerData::get_name_strlen(), this->PeerData::name,
    this->PeerData::kind,
    ( cur_time_ns - this->PeerData::start_ns ) / ONE_NS,
    ( cur_time_ns - this->PeerData::active_ns ) / ONE_NS );
}

bool
EvSocket::client_match( PeerData &pd,  PeerMatchArgs *ka,  ... ) noexcept
{
#ifdef _MSC_VER
#define strncasecmp _strnicmp
#endif
  /* match filters, if any don't match return false */
  if ( ka->id != 0 )
    if ( (uint64_t) ka->id != pd.id ) /* match id */
      return false;
  if ( ka->ip_len != 0 ) /* match ip address string */
    if ( ka->ip_len != pd.get_peer_address_strlen() ||
         ::memcmp( ka->ip, pd.peer_address.buf, ka->ip_len ) != 0 )
      return false;
  if ( ka->type_len != 0 ) {
    va_list    args;
    size_t     k, sz;
    const char * str;
    va_start( args, ka );
    for ( k = 1; ; k++ ) {
      str = va_arg( args, const char * );
      if ( str == NULL ) {
        k = 0; /* no match */
        break;
      }
      sz = va_arg( args, size_t );
      if ( sz == ka->type_len &&
           ::strncasecmp( ka->type, str, sz ) == 0 )
        break; /* match */
    }
    va_end( args );
    if ( k != 0 )
      return true;
    /* match the kind */
    if ( pd.kind != NULL && ka->type_len == ::strlen( pd.kind ) &&
         ::strncasecmp( ka->type, pd.kind, ka->type_len ) == 0 )
      return true;
    return false;
  }
  return true;
}

bool
EvSocket::client_kill( void ) noexcept
{
  /* if already shutdown, close up immediately */
  if ( this->test( EV_SHUTDOWN ) != 0 ) {
    this->poll.remove_event_queue( this );
    this->poll.remove_write_queue( this );
    if ( this->sock_state != 0 )
      this->popall();
    this->idle_push( EV_CLOSE );
  }
  else { /* close after writing pending data */
    this->idle_push( EV_SHUTDOWN );
  }
  return true;
}

bool
EvConnection::match( PeerMatchArgs &ka ) noexcept
{
  return EvSocket::client_match( *this, &ka, "tcp", 3, NULL );
}

int
EvConnection::client_list( char *buf,  size_t buflen ) noexcept
{
  int i = this->EvSocket::client_list( buf, buflen );
  if ( i >= 0 ) {
    i += ::snprintf( &buf[ i ], buflen - (size_t) i,
      "rbuf=%u rsz=%u imsg=%" PRIu64 " br=%" PRIu64 " wbuf=%" PRIu64 " wsz=%" PRIu64 " omsg=%" PRIu64 " bs=%" PRIu64 " ",
      this->len - this->off, this->recv_size, this->msgs_recv, this->bytes_recv,
      this->wr_pending,
      this->tmp.fast_len + this->tmp.block_cnt * this->tmp.alloc_size,
      this->msgs_sent, this->bytes_sent );
  }
  return i;
}

void
EvSocket::client_stats( PeerStats &ps ) noexcept
{
  ps.bytes_recv += this->bytes_recv;
  ps.bytes_sent += this->bytes_sent;
  ps.msgs_recv  += this->msgs_recv;
  ps.msgs_sent  += this->msgs_sent;
}

void
EvSocket::retired_stats( PeerStats &ps ) noexcept
{
  ps.bytes_recv += this->poll.peer_stats.bytes_recv;
  ps.bytes_sent += this->poll.peer_stats.bytes_sent;
  ps.msgs_recv  += this->poll.peer_stats.msgs_recv;
  ps.msgs_sent  += this->poll.peer_stats.msgs_sent;
  ps.accept_cnt += this->poll.peer_stats.accept_cnt;
}

bool
EvListen::match( PeerMatchArgs &ka ) noexcept
{
  return EvSocket::client_match( *this, &ka, "listen", 6, NULL );
}

int
EvListen::client_list( char *buf,  size_t buflen ) noexcept
{
  int i = this->EvSocket::client_list( buf, buflen );
  if ( i >= 0 ) {
    i += ::snprintf( &buf[ i ], buflen - (size_t) i,
                     "acpt=%" PRIu64 " ", this->accept_cnt );
  }
  return i;
}

void
EvListen::client_stats( PeerStats &ps ) noexcept
{
  ps.accept_cnt += this->accept_cnt;
}

bool
EvUdp::match( PeerMatchArgs &ka ) noexcept
{
  return EvSocket::client_match( *this, &ka, "udp", 3, NULL );
}

int
EvUdp::client_list( char *buf,  size_t buflen ) noexcept
{
  int i = this->EvSocket::client_list( buf, buflen );
  if ( i >= 0 ) {
    i += ::snprintf( &buf[ i ], buflen - (size_t) i,
                     "imsg=%" PRIu64 " omsg=%" PRIu64 " br=%" PRIu64 " bs=%" PRIu64 " ",
                     this->msgs_recv, this->msgs_sent,
                     this->bytes_recv, this->bytes_sent );
  }
  return i;
}

PeerData *
PeerMatchIter::first( void ) noexcept
{
  /* go to the front */
  for ( this->p = &this->me; this->p->back != NULL; this->p = this->p->back )
    ;
  return this->next(); /* match the next */
}

PeerData *
PeerMatchIter::next( void ) noexcept
{
  while ( this->p != NULL ) { /* while peers to match */
    PeerData & x = *this->p;
    this->p = x.next;
    if ( ( &x == &this->me && this->ka.skipme ) || /* match peer */
         ! x.match( this->ka ) )
      continue;
    return &x;
  }
  return NULL;
}
/* dispatch a timer firing */
bool
EvPoll::timer_expire( EvTimerEvent &ev ) noexcept
{
  EvSocket *s;
  bool b = false;
  if ( (uint32_t) ev.id <= this->maxfd && (s = this->sock[ ev.id ]) != NULL )
    b = s->timer_expire( ev.timer_id, ev.event_id );
  return b;
}
/* fill up recv buffers */
void
EvConnection::read( void ) noexcept
{
  this->adjust_recv();
  for (;;) {
    if ( this->len < this->recv_size ) {
      ssize_t nbytes;
#ifndef _MSC_VER
      nbytes = ::read( this->fd, &this->recv[ this->len ],
                       this->recv_size - this->len );
#else
      nbytes = ::wp_read( this->fd, &this->recv[ this->len ],
                          this->recv_size - this->len );
#endif
      if ( nbytes > 0 ) {
        this->len += (uint32_t) nbytes;
        this->bytes_recv += nbytes;
        this->push( EV_PROCESS );
        /* if buf almost full, switch to low priority read */
        if ( this->len >= this->recv_highwater )
          this->pushpop( EV_READ_LO, EV_READ );
        else
          this->pushpop( EV_READ, EV_READ_LO );
        return;
      }
      /* wait for epoll() to set EV_READ again */
      this->pop3( EV_READ, EV_READ_LO, EV_READ_HI );
#ifdef _MSC_VER
      /* reset EPOLLET */
      struct epoll_event event;
      event.data.fd = this->fd;
      event.events  = EPOLLIN | EPOLLRDHUP | EPOLLET;
      ::epoll_ctl( this->poll.efd, EPOLL_CTL_MOD, this->fd, &event );
#endif
      if ( nbytes < 0 ) {
        if ( ! ev_would_block( errno ) ) {
          if ( errno != ECONNRESET )
            this->set_sock_err( EV_ERR_BAD_READ, errno );
          else
            this->set_sock_err( EV_ERR_READ_RESET, errno );
          this->popall();
          this->push( EV_CLOSE );
        }
      }
      else if ( nbytes == 0 )
        this->push( EV_SHUTDOWN ); /* close after process and writes */
      /*else if ( this->test( EV_WRITE ) )
        this->pushpop( EV_WRITE_HI, EV_WRITE );*/
      return;
    }
    /* allow draining of existing buf before resizing */
    if ( this->test( EV_READ ) ) {
      this->pushpop( EV_READ_LO, EV_READ );
      return;
    }
    /* process was not able to drain read buf */
    if ( ! this->resize_recv_buf() )
      return;
  }
}
/* if msg is too large for existing buffers, resize it */
bool
EvConnection::resize_recv_buf( void ) noexcept
{
  size_t newsz = this->recv_size * 2;
  if ( newsz != (size_t) (uint32_t) newsz )
    return false;
  void * ex_recv_buf = ::malloc( newsz );
  if ( ex_recv_buf == NULL )
    return false;
  ::memcpy( ex_recv_buf, &this->recv[ this->off ], this->len );
  this->len -= this->off;
  this->off  = 0;
  if ( this->recv != this->recv_buf )
    ::free( this->recv );
  this->recv = (char *) ex_recv_buf;
  this->recv_size = (uint32_t) newsz;
  return true;
}
/* write data to sock fd */
void
EvConnection::write( void ) noexcept
{
  StreamBuf & strm = *this;
  if ( strm.sz > 0 )
    strm.flush();
  else if ( strm.wr_pending == 0 ) {
    this->pop3( EV_WRITE, EV_WRITE_HI, EV_WRITE_POLL );
    this->push( EV_READ_LO );
    return;
  }

  ssize_t nbytes;
  size_t  nb = 0;
#ifndef _MSC_VER
  struct msghdr h;
  ::memset( &h, 0, sizeof( h ) );
  h.msg_iov    = strm.iov;
  h.msg_iovlen = strm.idx;
  if ( h.msg_iovlen == 1 ) {
    nbytes = ::send( this->fd, h.msg_iov[ 0 ].iov_base,
                     h.msg_iov[ 0 ].iov_len, MSG_NOSIGNAL );
  }
  else {
    nbytes = ::sendmsg( this->fd, &h, MSG_NOSIGNAL );
    while ( nbytes < 0 && errno == EMSGSIZE ) {
      if ( (h.msg_iovlen /= 2) == 0 )
        break;
      nbytes = ::sendmsg( this->fd, &h, MSG_NOSIGNAL );
    }
  }
#else
  nbytes = ::wp_send( this->fd, &strm.iov[ 0 ], strm.idx );
#endif
  if ( nbytes > 0 ) {
    strm.wr_pending -= nbytes;
    this->bytes_sent += nbytes;
    nb += nbytes;
    if ( strm.wr_pending == 0 ) {
      strm.reset();
      this->pop3( EV_WRITE, EV_WRITE_HI, EV_WRITE_POLL );
      this->push( EV_READ_LO );
    }
    else {
      size_t woff = 0;
      for (;;) {
        size_t iov_len = strm.iov[ woff ].iov_len;
        if ( (size_t) nbytes >= iov_len ) {
          nbytes -= iov_len;
          woff++;
          if ( nbytes == 0 )
            break;
        }
        else {
          char *base = (char *) strm.iov[ woff ].iov_base;
          strm.iov[ woff ].iov_len -= nbytes;
          strm.iov[ woff ].iov_base = &base[ nbytes ];
          break;
        }
      }
      if ( woff > 0 ) {
        ::memmove( &strm.iov[ 0 ], &this->iov[ woff ],
                   sizeof( this->iov[ 0 ] ) * ( this->idx - woff ) );
        this->idx -= woff;
      }
    }
    return;
  }
  if ( nbytes == 0 || ( nbytes < 0 && ! ev_would_block( errno ) ) ) {
    if ( nbytes < 0 && errno == ENOTCONN && this->bytes_sent == 0 ) {
      this->push( EV_WRITE_HI );
      this->push( EV_WRITE_POLL );
      return;
    }
    else {
      if ( nbytes < 0 && errno != ECONNRESET && errno != EPIPE )
        this->set_sock_err( EV_ERR_BAD_WRITE, errno );
      else
        this->set_sock_err( EV_ERR_WRITE_RESET, errno );
      this->popall();
      this->push( EV_CLOSE );
      return;
    }
  }
  if ( this->test( EV_WRITE_HI ) )
    this->push( EV_WRITE_POLL );
  else
    this->push( EV_WRITE_HI );
}
/* use mmsg for udp sockets */
bool
EvDgram::alloc_mmsg( void ) noexcept
{
  static const size_t gsz = sizeof( struct sockaddr_storage ) +
                            sizeof( struct iovec ),
                      psz = 64 * 1024 + gsz;
  StreamBuf      & strm     = *this;
  uint32_t         i,
                   new_size;
  struct mmsghdr * sav      = this->in_mhdr;

  if ( this->in_nsize <= this->in_size ) {
    if ( this->in_nsize > 1024 || this->in_nsize * 2 <= this->in_size )
      return false;
    this->in_nsize *= 2;
  }
  new_size = this->in_nsize - this->in_size;
  /* allocate new_size buffers, and in_nsize headers */
  this->in_mhdr = (struct mmsghdr *) strm.alloc_temp( psz * new_size +
                                    sizeof( struct mmsghdr ) * this->in_nsize );
  if ( this->in_mhdr == NULL )
    return false;
  i = this->in_size;
  /* if any existing buffers exist, the pointers will be copied to the head */
  if ( i > 0 )
    ::memcpy( this->in_mhdr, sav, sizeof( sav[ 0 ] ) * i );
  /* initialize the rest of the buffers at the tail */
  void *p = (void *) &this->in_mhdr[ this->in_nsize ]/*,
       *g = (void *) &((uint8_t *) p)[ gsz * this->in_size ]*/ ;
  for ( ; i < this->in_nsize; i++ ) {
    this->in_mhdr[ i ].msg_hdr.msg_name    = (struct sockaddr *) p;
    this->in_mhdr[ i ].msg_hdr.msg_namelen = sizeof( struct sockaddr_storage );
    p = &((uint8_t *) p)[ sizeof( struct sockaddr_storage ) ];

    this->in_mhdr[ i ].msg_hdr.msg_iov    = (struct iovec *) p;
    this->in_mhdr[ i ].msg_hdr.msg_iovlen = 1;
    p = &((uint8_t *) p)[ sizeof( struct iovec ) ];

    this->in_mhdr[ i ].msg_hdr.msg_iov[ 0 ].iov_base = p;
    this->in_mhdr[ i ].msg_hdr.msg_iov[ 0 ].iov_len  = 64 * 1024;
    p = &((uint8_t *) p)[ 64 * 1024 ];

    this->in_mhdr[ i ].msg_hdr.msg_control    = NULL;
    this->in_mhdr[ i ].msg_hdr.msg_controllen = 0;
    this->in_mhdr[ i ].msg_hdr.msg_flags      = 0;

    this->in_mhdr[ i ].msg_len = 0;
  }
  /* in_nsize can expand if we need more udp frames */
  this->in_size = this->in_nsize;
  return true;
}

int
EvDgram::discard_pkt( void ) noexcept
{
  uint8_t buf[ 64 * 1024 +
               sizeof( struct sockaddr_storage ) +
               sizeof( struct iovec ) ],
        * p = buf;
  msghdr  hdr;
  ssize_t nbytes;
  hdr.msg_name    = (struct sockaddr_storage *) (void *) p;
  hdr.msg_namelen = sizeof( sockaddr_storage );
  p += sizeof( struct sockaddr_storage );
  hdr.msg_iov        = (struct iovec *) p;
  hdr.msg_iovlen     = 1;
  p += sizeof( struct iovec );
  hdr.msg_iov[ 0 ].iov_base = p;
  hdr.msg_iov[ 0 ].iov_len  = 64 * 1024;
  hdr.msg_control    = NULL;
  hdr.msg_controllen = 0;
  hdr.msg_flags      = 0;
#ifndef _MSC_VER
  nbytes = ::recvmsg( this->fd, &hdr, 0 );
#else
  nbytes = ::wp_recvmsg( this->fd, &hdr );
#endif
  if ( nbytes > 0 )
    fprintf( stderr, "Discard %u bytes in_nmsgs %u in_size %u\n",
             (uint32_t) nbytes, this->in_nmsgs, this->in_size );
  return (int) nbytes;
}

/* read udp packets */
void
EvDgram::read( void ) noexcept
{
  int     nmsgs  = 0;
  ssize_t nbytes = 0;

  if ( this->in_nmsgs == this->in_size && ! this->alloc_mmsg() ) {
    nbytes = this->discard_pkt();
  }
#if ! defined( NO_RECVMMSG ) && ! defined( _MSC_VER )
  else if ( this->in_nmsgs + 1 < this->in_size ) {
    nmsgs = ::recvmmsg( this->fd, &this->in_mhdr[ this->in_nmsgs ],
                        this->in_size - this->in_nmsgs, 0, NULL );
  }
  else {
    nbytes = ::recvmsg( this->fd, &this->in_mhdr[ this->in_nmsgs ].msg_hdr, 0 );
    if ( nbytes > 0 ) {
      this->in_mhdr[ this->in_nmsgs ].msg_len = nbytes;
      nmsgs = 1;
    }
  }
#else
  else {
    while ( this->in_nmsgs + nmsgs < this->in_size ) {
#ifndef _MSC_VER
      nbytes = ::recvmsg( this->fd,
                          &this->in_mhdr[ this->in_nmsgs + nmsgs ].msg_hdr, 0 );
#else
      nbytes = ::wp_recvmsg( this->fd,
                             &this->in_mhdr[ this->in_nmsgs + nmsgs ].msg_hdr );
#endif
      if ( nbytes > 0 ) {
        this->in_mhdr[ this->in_nmsgs + nmsgs ].msg_len = (uint32_t) nbytes;
        nmsgs++;
        continue;
      }
      break;
    }
  }
#endif
  if ( nmsgs > 0 ) {
    uint32_t j = this->in_nmsgs;
    this->in_nmsgs += nmsgs;
    for ( int i = 0; i < nmsgs; i++ )
      this->bytes_recv += this->in_mhdr[ j++ ].msg_len;
    /* keep recvmmsg packet recv count to 8 at a time, that is close to
     * the sweet spot of latency vs bandwidth */
    this->in_nsize = ( ( this->in_nmsgs < 8 ) ? this->in_nmsgs + 1 : 8 );
    this->push( EV_PROCESS );
    this->pushpop( EV_READ_LO, EV_READ );
    return;
  }
  this->in_nsize = 1;
  /* wait for epoll() to set EV_READ again */
  this->pop3( EV_READ, EV_READ_LO, EV_READ_HI );
#ifdef _MSC_VER
  /* reset EPOLLET */
  struct epoll_event event;
  event.data.fd = this->fd;
  event.events  = EPOLLIN | EPOLLRDHUP | EPOLLET;
  ::epoll_ctl( this->poll.efd, EPOLL_CTL_MOD, this->fd, &event );
#endif
  if ( ( nmsgs < 0 || nbytes < 0 ) && errno != EINTR ) {
    if ( ! ev_would_block( errno ) ) {
      if ( errno != ECONNRESET )
        this->set_sock_err( EV_ERR_BAD_READ, errno );
      else
        this->set_sock_err( EV_ERR_READ_RESET, errno );
      this->popall();
      this->push( EV_CLOSE );
    }
  }
}
/* write udp packets */
void
EvDgram::write( void ) noexcept
{
  int nmsgs = 0;
#if ! defined( NO_SENDMMSG ) && ! defined( _MSC_VER )
  if ( this->out_nmsgs > 1 ) {
    nmsgs = ::sendmmsg( this->fd, this->out_mhdr, this->out_nmsgs, 0 );
    if ( nmsgs > 0 ) {
      for ( uint32_t i = 0; i < this->out_nmsgs; i++ )
        this->bytes_sent += this->out_mhdr[ i ].msg_len;
      this->clear_buffers();
      this->pop3( EV_WRITE, EV_WRITE_HI, EV_WRITE_POLL );
      return;
    }
  }
  else
#endif
  {
    for ( uint32_t i = 0; i < this->out_nmsgs; i++ ) {
      ssize_t nbytes;
#ifndef _MSC_VER
      nbytes = ::sendmsg( this->fd, &this->out_mhdr[ i ].msg_hdr, 0 );
#else
      nbytes = ::wp_sendmsg( this->fd, &this->out_mhdr[ i ].msg_hdr );
#endif
      if ( nbytes > 0 )
        this->bytes_sent += nbytes;
      if ( nbytes < 0 ) {
        nmsgs = -1; /* check errno below */
        break;
      }
    }
  }
  if ( nmsgs < 0 && ! ev_would_block( errno ) ) {
    if ( errno != ECONNRESET && errno != EPIPE )
      this->set_sock_err( EV_ERR_BAD_WRITE, errno );
    else
      this->set_sock_err( EV_ERR_WRITE_RESET, errno );
    /* normal disconnect */
    this->popall();
    this->push( EV_CLOSE );
  }
  /* if msgs were unsent, this drops them */
  this->clear_buffers();
  this->pop3( EV_WRITE, EV_WRITE_HI, EV_WRITE_POLL );
}
/* if some alloc failed, kill the client */
void
EvSocket::close_error( uint16_t serr,  uint16_t err ) noexcept
{
  this->set_sock_err( serr, err );
  this->popall();
  this->idle_push( EV_CLOSE );
}

int
EvSocket::set_sock_err( uint16_t serr,  uint16_t err ) noexcept
{
  this->sock_err   = serr;
  this->sock_errno = err;
  if ( serr + err != 0 )
    if ( ( this->sock_opts & OPT_VERBOSE ) != 0 )
      this->print_sock_error();
  return -(int) serr;
}

const char *
EvSocket::err_string( EvSockErr err ) noexcept
{
  switch ( err ) {
    case EV_ERR_NONE:          return "ERR_NONE";
    case EV_ERR_CLOSE:         return "ERR_CLOSE, fd failed to close";
    case EV_ERR_WRITE_TIMEOUT: return "ERR_WRITE_TIMEOUT, no progress writing";
    case EV_ERR_BAD_WRITE:     return "ERR_BAD_WRITE, write failed";
    case EV_ERR_WRITE_RESET:   return "ERR_WRITE_RESET, write connection reset";
    case EV_ERR_BAD_READ:      return "ERR_BAD_READ, read failed";
    case EV_ERR_READ_RESET:    return "ERR_READ_RESET, read connection reset";
    case EV_ERR_WRITE_POLL:    return "ERR_WRITE_POLL, poll failed to mod write";
    case EV_ERR_READ_POLL:     return "ERR_READ_POLL, epoll failed to mod read";
    case EV_ERR_ALLOC:         return "ERR_ALLOC, alloc sock data failed";
    case EV_ERR_GETADDRINFO:   return "ERR_GETADDRINFO, addr resolve failed";
    case EV_ERR_BIND:          return "ERR_BIND, bind addr failed";
    case EV_ERR_CONNECT:       return "ERR_CONNECT, connect addr failed";
    case EV_ERR_BAD_FD:        return "ERR_BAD_FD, fd invalid";
    case EV_ERR_SOCKET:        return "ERR_SOCKET, socket create failed";
    case EV_ERR_MULTI_IF:      return "EV_ERR_MULTI_IF, set multicast interface";
    case EV_ERR_ADD_MCAST:     return "EV_ERR_ADD_MCAST, join multicast network";
    default:                   return NULL;
  }
}

const char *
EvSocket::sock_error_string( void ) noexcept
{
  return EvSocket::err_string( (EvSockErr) this->sock_err );
}

size_t
EvSocket::print_sock_error( char *out,  size_t outlen ) noexcept
{
  const char *s = NULL,
             *e = NULL;
  char ebuf[ 16 ];
  if ( this->sock_err != 0 ) {
    s = this->sock_error_string();
    if ( s == NULL ) {
      ::snprintf( ebuf, sizeof( ebuf ), "ERR_%u", this->sock_err );
      s = ebuf;
    }
  }
  if ( this->sock_errno != 0 ) {
    if ( this->sock_err != EV_ERR_WRITE_TIMEOUT )
      e = ::strerror( this->sock_errno );
  }
  char   buf[ 1024 ],
       * o    = buf;
  size_t off  = 0,
         olen = sizeof( buf );
  if ( out != NULL ) {
    o   = out;
    olen = outlen;
  }
  off = ::snprintf( o, olen, "Sock" );
  if ( s != NULL )
    off += ::snprintf( &o[ off ], olen - off, " error=%u/%s",
                       this->sock_err, s );
  off += ::snprintf( &o[ off ], olen - off, " fd=%d state=%x name=%s peer=%s",
               this->fd, this->sock_state, this->name, this->peer_address.buf );
  if ( e != NULL )
    off += ::snprintf( &o[ off ], olen - off, " errno=%u/%s",
                       this->sock_errno, e );
  else if ( this->sock_err == EV_ERR_WRITE_TIMEOUT )
    off += ::snprintf( &o[ off ], olen - off, " %u seconds",
                       this->sock_errno );
  if ( out == NULL )
    ::fprintf( stderr, "%.*s\n", (int) off, buf );
  return off;
}

void EvConnectionNotify::on_connect( EvSocket & ) noexcept {}
void EvConnectionNotify::on_shutdown( EvSocket &,  const char *,
                                      size_t ) noexcept {}

/* when a sock is not dispatch()ed, it may need to be rearranged in the queue
 * for correct priority */
void
EvSocket::idle_push( EvState s ) noexcept
{
  /* if no state, not currently in the queue */
  if ( ! this->in_poll( IN_EPOLL_WRITE ) ) {
    if ( ! this->in_queue( IN_EVENT_QUEUE ) ) {
    do_push:;
      this->push( s );
      this->prio_cnt = this->poll.prio_tick;
      this->poll.push_event_queue( this );
    }
    /* check if added state requires queue to be rearranged */
    else {
      int x1 = kv_ffsw( this->sock_state ),
          x2 = kv_ffsw( this->sock_state | ( 1 << s ) );
      /* new state has higher priority than current state, reorder queue */
      if ( x1 > x2 ) {
        this->poll.remove_event_queue( this );
        goto do_push; /* pop, then push */
      }
      /* otherwise, current state >= the new state, in the correct order */
      else {
        this->push( s );
      }
    }
  }
  else { /* waiting on write */
    this->push( s );
  }
}

bool
EvPrefetchQueue::more_queue( void ) noexcept
{
  void * p;
  size_t sz  = this->ar_size,
         nsz = sz * 2;

  if ( this->ar == this->ini ) {
    p = ::malloc( sizeof( this->ar[ 0 ] ) * nsz );
    if ( p != NULL ) {
      ::memcpy( p, this->ini, sizeof( this->ini ) );
      ::memset( this->ini, 0, sizeof( this->ini ) );
    }
  }
  else {
    p = ::realloc( this->ar, sizeof( this->ar[ 0 ] ) * nsz );
  }
  if ( p == NULL )
    return false;
  this->ar = (EvKeyCtx **) p;
  this->ar_size = nsz;
  ::memset( &this->ar[ sz ], 0, sizeof( this->ar[ 0 ] ) * sz );

  size_t j = this->hd & ( nsz - 1 ),
         k = this->hd & ( sz - 1 );
  /* first case, head moves to the new area:
   * [ k k k hd k k k k ] -> [ k k k 0 0 0 0 0 0 0 0 hd k k k k ]
   *                                 |xxxxxxx|  -->  |--------| */
  if ( j >= sz ) {
    while ( j < nsz ) {
      this->ar[ j ] = this->ar[ k ];
      this->ar[ k ] = NULL;
      j++; k++;
    }
  }
  /* send case, tail moves to the new area:
   * [ k k k hd k k k k ] -> [ 0 0 0 hd k k k k k k k 0 0 0 0 0 ]
   *                           |xxx|      ->    |---| */
  else {
    j = sz;
    for ( size_t i = 0; i < k; ) {
      this->ar[ j ] = this->ar[ i ];
      this->ar[ i ] = NULL;
      i++; j++;
    }
  }
  return true;
}
