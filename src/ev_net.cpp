#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <fcntl.h>
#include <errno.h>
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
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
#include <raikv/kv_pubsub.h>
#include <raikv/timer_queue.h>

using namespace rai;
using namespace kv;

EvPoll::EvPoll() noexcept
  : sock( 0 ), ev( 0 ), prefetch_queue( 0 ), prio_tick( 0 ),
    wr_timeout_ns( DEFAULT_NS_TIMEOUT ),
    conn_timeout_ns( DEFAULT_NS_CONNECT_TIMEOUT ),
    so_keepalive_ns( DEFAULT_NS_TIMEOUT ),
    next_id( 0 ), now_ns( 0 ), init_ns( 0 ), mono_ns( 0 ),
    coarse_ns( 0 ), coarse_mono( 0 ),
    fdcnt( 0 ), wr_count( 0 ), maxfd( 0 ), nfds( 0 ),
    send_highwater( StreamBuf::SND_BUFSIZE - 256 ),
    recv_highwater( DEFAULT_RCV_BUFSIZE - 256 ),
    efd( -1 ), null_fd( -1 ), quit( 0 ),
    prefetch_pending( 0 ), sub_route( *this ), sock_mem( 0 ),
    sock_mem_left( 0 ), free_buf( 0 )
{
  ::memset( this->sock_type_str, 0, sizeof( this->sock_type_str ) );
  ::memset( this->state_ns, 0, sizeof( this->state_ns ) );
  ::memset( this->state_cnt, 0, sizeof( this->state_cnt ) );
#if defined( _MSC_VER ) || defined( __MINGW32__ )
  ws_global_init();
#endif
}

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
  this->init_ns     = current_realtime_ns();
  this->mono_ns     = current_monotonic_time_ns();
  this->now_ns      = this->init_ns;
  this->coarse_ns   = this->init_ns;
  this->coarse_mono = this->mono_ns;

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
    s->popall();
    s->set_poll( IN_NO_LIST );
    event.events = 0;
    ::epoll_ctl( this->efd, EPOLL_CTL_DEL, s->fd, &event );
    s->idle_push( EV_CLOSE );
    return;
  }
  this->wr_count++;
  s->sock_wrpoll++;
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
    s->set_poll( IN_NO_LIST );
    event.events = 0;
    ::epoll_ctl( this->efd, EPOLL_CTL_DEL, s->fd, &event );
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
  if ( this->timer.queue->is_timer_ready( us, this->mono_ns ) ) {
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

    if ( s->in_poll( IN_EPOLL_WRITE ) ) {
      if ( ( this->ev[ i ].events & EPOLLOUT ) != 0 ) {
        if ( ( this->ev[ i ].events & ( EPOLLERR | EPOLLHUP ) ) == 0 ) {
          this->remove_write_poll( s ); /* move back to read poll, try write */
          continue;
        }
      }
      this->remove_poll( s ); /* a EPOLLERR, can't write, close it */
      this->remove_event_queue( s );
      this->remove_write_queue( s );
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
      if ( ! this->check_write_poll_timeout( s, ns ) )
        break;
      m++;
      if ( this->ev_write.is_empty() )
        break;
    }
  }
  return n + m; /* returns the number of new events */
}

bool
EvPoll::check_write_poll_timeout( EvSocket *s,  uint64_t ns ) noexcept
{
  uint64_t delta = ns - s->PeerData::active_ns;
  /* XXX this may hold the connect timeouts longer than they should */
  if ( ns <= s->PeerData::active_ns ) { /* XXX should use monotonic time */
    if ( ns < s->PeerData::active_ns ) {
      this->remove_write_queue( s );
      s->PeerData::active_ns = ns;
      this->push_write_queue( s );
    }
    return false;
  }
  if ( delta > this->wr_timeout_ns ||
       ( delta > this->conn_timeout_ns &&
         s->bytes_sent + s->bytes_recv == 0 ) ) {
    this->remove_write_poll( s );
    this->idle_close( s, delta );
    return true;
  }
  return false;
}

/* allocate an fd for a null sockets which are used for event based objects
 * that wait on timers and/or subscriptions */
int
EvPoll::get_null_fd( void ) noexcept
{
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
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
  s->active_ns  = this->current_coarse_ns();
  s->read_ns    = s->active_ns;
  s->init_stats();
  return 0;
}

uint64_t
EvPoll::get_next_id( void ) noexcept
{
  uint64_t ns = this->current_coarse_ns();
  if ( ns <= this->next_id ) {
    ns = this->next_id+1;
    /*while ( this->current_coarse_ns() < ns )
      ;*/
  }
  this->next_id = ns;
  return ns;
}

void
EvPoll::remove_poll( EvSocket *s ) noexcept
{
  if ( ! s->test_opts( OPT_NO_POLL ) ) { /* if is a epoll sock */
    if ( ! s->in_poll( IN_NO_LIST ) ) {
      if ( s->in_poll( IN_EPOLL_WRITE ) )
        this->wr_count--;
      s->set_poll( IN_NO_LIST );
      struct epoll_event event;
      ::memset( &event, 0, sizeof( struct epoll_event ) );
      event.data.fd = s->fd;
      event.events  = 0;
      ::epoll_ctl( this->efd, EPOLL_CTL_DEL, s->fd, &event );
    }
  }
}

void
EvPoll::remove_sock( EvSocket *s ) noexcept
{
  if ( s->fd < 0 )
    return;
  if ( s->in_list( IN_FREE_LIST ) ) {
    fprintf( stderr, "!!! removing free list sock\n" );
    return;
  }
  /* remove poll set */
  if ( (uint32_t) s->fd <= this->maxfd && this->sock[ s->fd ] == s ) {
    this->remove_poll( s );
    this->sock[ s->fd ] = NULL;
    this->fdcnt--;
  }
  /* close if wants */
  if ( ! s->test_opts( OPT_NO_CLOSE ) && s->fd != this->null_fd ) {
    int status;
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
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
  if ( ns != 0 )
    s->set_sock_err( EV_ERR_WRITE_TIMEOUT, (uint16_t) ( ns / 1000000000ULL ) );
  this->remove_poll( s );
  this->remove_event_queue( s );
  this->remove_write_queue( s );
  s->popall();
  s->idle_push( EV_CLOSE );
}

void *
EvPoll::alloc_sock( size_t sz ) noexcept
{
  size_t    need = align<size_t>( sz, 64 );
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
    case EV_NO_STATE:   return "no_state";
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
  if ( ! this->wait_empty() )
    this->notify_ready();
  /* after this, close fd */
  this->poll.remove_sock( this );
  this->poll.push_free_list( this );
}

  /* get service, if none, return false */
bool
EvSocket::get_service( void *host,  uint16_t &svc ) const noexcept
{
  (void) host;
  svc = 0;
  return false;
}
/* assign a session to connection */
bool
EvSocket::set_session( const char session[ MAX_SESSION_LEN ] ) noexcept
{
  (void) session[ 0 ];
  return false;
}

size_t
EvSocket::get_userid( char userid[ MAX_USERID_LEN ] ) const noexcept
{
  userid[ 0 ] = '\0';
  return 0;
}
/* get session name */
size_t
EvSocket::get_session( uint16_t,  char session[ MAX_SESSION_LEN ] ) const noexcept
{
  session[ 0 ] = '\0';
  return 0;
}
/* get session name */
size_t
EvSocket::get_subscriptions( uint16_t, SubRouteDB & ) noexcept
{
  return 0;
}
size_t
EvSocket::get_patterns( uint16_t, int, SubRouteDB & ) noexcept
{
  return 0;
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
  if ( this->accept() != NULL ) {
    this->accept_cnt++;
    this->read_ns = this->poll.now_ns;
  }
}

/* listeners don't do these */
void EvListen::write( void ) noexcept {}   /* nothing to write */
void EvListen::process( void ) noexcept {} /* nothing to process */
void EvListen::release( void ) noexcept {} /* no buffers */

void
EvListen::reset_read_poll( void ) noexcept
{
  this->pop3( EV_READ, EV_READ_LO, EV_READ_HI );
#if defined( _MSC_VER ) || defined( __MINGW32__ )
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

void
EvPoll::update_time_ns( void ) noexcept
{
  uint64_t r_ns = current_realtime_coarse_ns(),
           m_ns = current_monotonic_time_ns();
  if ( this->coarse_ns == r_ns ) {
    r_ns += ( m_ns - this->coarse_mono );
  }
  else {
    this->coarse_ns   = r_ns;
    this->coarse_mono = m_ns;
  }
  this->now_ns  = r_ns;
  this->mono_ns = m_ns;
}

uint64_t
EvPoll::current_coarse_ns( void ) noexcept
{
  this->update_time_ns();
  return this->now_ns;
}

uint64_t
EvPoll::current_mono_ns( void ) noexcept
{
  this->update_time_ns();
  return this->mono_ns;
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
           next_ns = this->current_mono_ns(),
           busy_ns = this->timer.queue->busy_delta( next_ns ),
           mark_ns = next_ns,
           used_ns = 0;
  int      ret     = DISPATCH_IDLE,
           next_state;
  EvState  state   = EV_NO_STATE;

  if ( this->quit )
    this->process_quit();
  for (;;) {
  next_tick:;
    if ( state != EV_NO_STATE ) {
      next_ns = this->current_mono_ns();
      this->state_ns[ state ] += next_ns - mark_ns;
      this->state_cnt[ state ]++;
      used_ns += next_ns - mark_ns;
      mark_ns  = next_ns;
      state    = EV_NO_STATE;
    }
    if ( start + 300 < this->prio_tick ) { /* run poll() at least every 300 */
      ret |= POLL_NEEDED | DISPATCH_BUSY;
      return ret;
    }
    if ( used_ns >= busy_ns ) { /* if a timer may expire, run poll() */
      if ( used_ns > 0 ) {
        /* the real time is updated at coarse intervals (10 to 100 us) */
        next_ns = this->current_mono_ns();
        /* how much busy work before timer expires */
        busy_ns = this->timer.queue->busy_delta( next_ns );
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
    s = this->ev_queue.heap[ 0 ];
    next_state = s->get_dispatch_state();

    EV_DBG_DISPATCH( s, next_state );
    this->prio_tick++;
    if ( next_state > EV_PREFETCH && this->prefetch_pending > 0 )
      goto do_prefetch;
    s->set_queue( IN_NO_QUEUE );
    this->ev_queue.pop();
    if ( next_state < 0 )
      continue;
    state = (EvState) next_state;
    switch ( state ) {
      case EV_NO_STATE:
        break;
      case EV_READ:
      case EV_READ_LO:
      case EV_READ_HI:
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
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
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
  if ( buflen == 0 )
    return 0;
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
  int n = ::snprintf( buf, buflen,
    "id=%" PRIu64 "addr=%.*s fd=%d name=%.*s kind=%s age=%" PRId64 " idle=%" PRId64 " rd=%" PRId64 " ",
    this->PeerData::start_ns,
    (int) this->PeerData::get_peer_address_strlen(),
    this->PeerData::peer_address.buf,
    this->PeerData::fd,
    (int) this->PeerData::get_name_strlen(), this->PeerData::name,
    this->PeerData::kind,
    ( cur_time_ns - this->PeerData::start_ns ) / ONE_NS,
    ( cur_time_ns - this->PeerData::active_ns ) / ONE_NS,
    ( cur_time_ns - this->PeerData::read_ns ) / ONE_NS );
  return min_int( n, (int) buflen - 1 );
}

bool
EvSocket::client_match( PeerData &pd,  PeerMatchArgs *ka,  ... ) noexcept
{
  /* match filters, if any don't match return false */
  if ( ka->id != 0 )
    if ( (uint64_t) ka->id != pd.start_ns ) /* match id */
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
    this->poll.idle_close( this, 0 );
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
  if ( buflen == 0 )
    return 0;
  int i = this->EvSocket::client_list( buf, buflen );
  if ( i >= 0 && (size_t) i < buflen - 1 ) {
    i += ::snprintf( &buf[ i ], buflen - (size_t) i,
      "rbuf=%u rsz=%u imsg=%" PRIu64 " br=%" PRIu64 " wbuf=%" PRIu64 " wsz=%" PRIu64 " omsg=%" PRIu64 " bs=%" PRIu64 " ",
      this->len - this->off, this->recv_size, this->msgs_recv, this->bytes_recv,
      this->wr_pending,
      this->tmp.fast_len + this->tmp.block_cnt * this->tmp.alloc_size,
      this->msgs_sent, this->bytes_sent );
  }
  return min_int( i, (int) buflen - 1 );
}

void
EvSocket::client_stats( PeerStats &ps ) noexcept
{
  ps.bytes_recv += this->bytes_recv;
  ps.bytes_sent += this->bytes_sent;
  ps.msgs_recv  += this->msgs_recv;
  ps.msgs_sent  += this->msgs_sent;
  if ( this->active_ns > ps.active_ns )
    ps.active_ns = this->active_ns;
  if ( this->read_ns > ps.read_ns )
    ps.read_ns = this->read_ns;
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
  if ( buflen == 0 )
    return 0;
  int i = this->EvSocket::client_list( buf, buflen );
  if ( i >= 0 && i < (int) buflen - 1 ) {
    i += ::snprintf( &buf[ i ], buflen - (size_t) i,
                     "acpt=%" PRIu64 " ", this->accept_cnt );
  }
  return min_int( i, (int) buflen - 1 );
}

void
EvListen::client_stats( PeerStats &ps ) noexcept
{
  ps.accept_cnt += this->accept_cnt;
  if ( this->read_ns > ps.read_ns )
    ps.read_ns = this->read_ns;
}

bool
EvUdp::match( PeerMatchArgs &ka ) noexcept
{
  return EvSocket::client_match( *this, &ka, "udp", 3, NULL );
}

int
EvUdp::client_list( char *buf,  size_t buflen ) noexcept
{
  if ( buflen == 0 )
    return 0;
  int i = this->EvSocket::client_list( buf, buflen );
  if ( i >= 0 && i < (int) buflen - 1 ) {
    i += ::snprintf( &buf[ i ], buflen - (size_t) i,
                     "imsg=%" PRIu64 " omsg=%" PRIu64 " br=%" PRIu64 " bs=%" PRIu64 " ",
                     this->msgs_recv, this->msgs_sent,
                     this->bytes_recv, this->bytes_sent );
  }
  return min_int( i, (int) buflen - 1 );
}

EvSocket *
PeerMatchIter::first( void ) noexcept
{
  /* go to the front */
  for ( this->p = &this->me; this->p->back != NULL; this->p = this->p->back )
    ;
  return this->next(); /* match the next */
}

EvSocket *
PeerMatchIter::next( void ) noexcept
{
  while ( this->p != NULL ) { /* while peers to match */
    EvSocket & x = *this->p;
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
  if ( this->off > 0 )
    this->adjust_recv();
  for (;;) {
    if ( this->len < this->recv_size ) {
      ssize_t nbytes;
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
      nbytes = ::read( this->fd, &this->recv[ this->len ],
                       this->recv_size - this->len );
#else
      nbytes = ::wp_read( this->fd, &this->recv[ this->len ],
                          this->recv_size - this->len );
#endif
      if ( nbytes > 0 ) {
        this->len += (uint32_t) nbytes;
        this->bytes_recv += nbytes;
        this->recv_count++;
        this->read_ns = this->poll.now_ns;
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
#if defined( _MSC_VER ) || defined( __MINGW32__ )
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
    if ( this->test( EV_READ ) && this->off < this->len ) {
      this->pushpop( EV_READ_LO, EV_READ );
      return;
    }
    /* process was not able to drain read buf */
    if ( ! this->resize_recv_buf( 0 ) ) {
      this->popall();
      this->push( EV_CLOSE );
      return;
    }
  }
}
/* if msg is too large for existing buffers, resize it */
bool
EvConnection::resize_recv_buf( size_t new_size ) noexcept
{
  size_t new_len = this->len - this->off;

  if ( new_size < this->recv_size * 2 ) {
    size_t avail = this->recv_size - new_len;
    if ( avail < this->recv_size / 2 )
      new_size = (size_t) this->recv_size * 2;
  }
  new_size = align<size_t>( new_size, 256 );
  if ( new_size > (size_t) 0xffffffffU ) {
    this->set_sock_err( EV_ERR_ALLOC, 0 );
    return false;
  }
  void * ex_recv_buf;
  if ( new_size <= sizeof( this->recv_buf ) ) {
    ex_recv_buf = this->recv_buf;
    new_size    = sizeof( this->recv_buf );
  }
  else {
    ex_recv_buf = EvPoll::ev_poll_alloc( this, new_size );
    if ( ex_recv_buf == NULL ) {
      this->set_sock_err( EV_ERR_ALLOC, errno );
      return false;
    }
  }
  if ( new_len > 0 )
    ::memmove( ex_recv_buf, &this->recv[ this->off ], new_len );
  this->off = 0;
  this->len = (uint32_t) new_len;
  if ( this->recv != this->recv_buf ) {
    if ( this->zref_index != 0 ) {
      this->poll.zero_copy_deref( this->zref_index, true );
      this->zref_index = 0;
    }
    else {
      this->poll.poll_free( this->recv, this->recv_size );
    }
  }
  this->recv = (char *) ex_recv_buf;
  this->recv_size = new_size;
  if ( new_size > this->recv_max )
    this->recv_max = (uint32_t) new_size;
  return true;
}

uint32_t
EvPoll::zero_copy_ref( uint32_t src_route,  const void *msg,
                       size_t msg_len ) noexcept
{
  if ( src_route > this->maxfd || this->sock[ src_route ] == NULL ||
       this->sock[ src_route ]->sock_base != EV_CONNECTION_BASE )
    return 0;
  EvConnection & conn = *(EvConnection *) this->sock[ src_route ];

  if ( conn.recv == conn.recv_buf ||
       (char *) msg < conn.recv ||
       &((char *) msg)[ msg_len ] > &conn.recv[ conn.len ] )
    return 0;

  bool is_new = false;
  if ( conn.zref_index == 0 ) {
    conn.zref_index = this->zref.count + 1;
    is_new = true;
  }
  ZeroRef & zr = this->zref[ conn.zref_index - 1 ];
  if ( is_new ) {
    zr.buf       = conn.recv;
    zr.ref_count = 1;
    zr.owner     = src_route;
    zr.buf_size  = conn.recv_size;
    conn.zref_count++;
  }
  zr.ref_count++;
  return conn.zref_index;
}

void
EvPoll::zero_copy_deref( uint32_t zref_index,  bool owner ) noexcept
{
  if ( zref_index - 1 >= this->zref.count )
    return;
  ZeroRef & zr = this->zref.ptr[ zref_index - 1 ];

  if ( owner )
    zr.owner = (uint32_t) -1;
  if ( --zr.ref_count != 0 ) {
    uint32_t src_route = zr.owner;
    if ( zr.ref_count == 1 && src_route != (uint32_t) -1 ) {
      if ( src_route <= this->maxfd && this->sock[ src_route ] != NULL &&
           this->sock[ src_route ]->sock_base == EV_CONNECTION_BASE ) {
        EvConnection & conn = *(EvConnection *) this->sock[ src_route ];
        if ( conn.recv == zr.buf && conn.off == conn.len &&
             conn.pending() == 0 ) {
          conn.zref_index = 0;
          conn.reset_recv();
          goto release_buf;
        }
      }
    }
    return;
  }
release_buf:;
  this->poll_free( zr.buf, zr.buf_size );
  if ( zref_index == this->zref.count ) {
    this->zref.count--;
    while ( this->zref.count > 0 &&
            this->zref.ptr[ this->zref.count - 1 ].ref_count == 0 )
      this->zref.count--;
  }
}

void *
EvPoll::ev_poll_alloc( void *cl,  size_t size ) noexcept
{
  EvSocket & sock = *(EvSocket *) cl;
  EvPoll   & poll = sock.poll;
  if ( size <= FREE_BUF_MAX_SIZE ) {
    if ( poll.free_buf == NULL )
      poll.free_buf = new ( ::malloc( sizeof( Balloc16k_2m ) ) ) Balloc16k_2m();
    void * ptr = poll.free_buf->try_alloc( size );
    if ( ptr != NULL )
      return ptr;
  }
  if ( sock.sock_base == EV_CONNECTION_BASE )
    ((EvConnection &) sock).malloc_count++;
  return ::malloc( size );
}

void
EvPoll::poll_free( void *ptr,  size_t size ) noexcept
{
  if ( this->free_buf != NULL && this->free_buf->in_block( ptr ) ) {
    this->free_buf->release( ptr, size );
    return;
  }
  ::free( ptr );
}

void
EvPoll::ev_poll_free( void *cl,  void *ptr,  size_t size ) noexcept
{
  EvSocket & sock = *(EvSocket *) cl;
  sock.poll.poll_free( ptr, size );
}

uint32_t
EvPoll::zero_copy_ref_count( uint32_t zref_index ) noexcept
{
  ZeroRef & zr = this->zref[ zref_index - 1 ];

  uint32_t count = zr.ref_count;
  if ( zr.owner != (uint32_t) -1 )
    count--;
  return count;
}

/* write data to sock fd */
void
EvConnection::write( void ) noexcept
{
  StreamBuf & strm    = *this;
  bool        is_high = this->test( EV_WRITE_HI );
  if ( strm.sz > 0 )
    strm.flush();
  else if ( strm.wr_pending == 0 ) {
write_finished:;
    this->pop3( EV_WRITE, EV_WRITE_HI, EV_WRITE_POLL );
    this->push( EV_READ_LO );
    if ( is_high ) {
write_notify:;
      if ( ! this->wait_empty() )
        this->notify_ready();
    }
    return;
  }

  ssize_t nbytes;
  size_t  nb = 0;
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
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
    strm.wr_free    += nbytes;
    this->bytes_sent += nbytes;
    this->send_count++;
    nb += nbytes;
    if ( strm.wr_pending == 0 ) {
      this->active_ns = this->poll.now_ns;
      this->clear_write_buffers();
      goto write_finished;
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
      if ( is_high ) {
        if ( strm.wr_pending < this->send_highwater / 2 ) {
          this->active_ns = this->poll.now_ns;
          this->pushpop( EV_WRITE, EV_WRITE_HI );
          goto write_notify;
        }
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
      if ( is_high )
        goto write_notify;
      return;
    }
  }
  if ( is_high )
    this->push( EV_WRITE_POLL );
  else
    this->push( EV_WRITE_HI );
}
/* use mmsg for udp sockets */
bool
EvDgram::alloc_mmsg( void ) noexcept
{
  static const size_t gsz = sizeof( struct sockaddr_storage ) +
                            sizeof( struct iovec );
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
  this->in_mhdr = (struct mmsghdr *) strm.alloc_temp( gsz * new_size +
                                    sizeof( struct mmsghdr ) * this->in_nsize );
  void * buf = strm.alloc_temp( 64 * 1024 * new_size );
  if ( this->in_mhdr == NULL || buf == NULL )
    return false;
  i = this->in_size;
  /* if any existing buffers exist, the pointers will be copied to the head */
  if ( i > 0 )
    ::memcpy( this->in_mhdr, sav, sizeof( sav[ 0 ] ) * i );
  /* initialize the rest of the buffers at the tail */
  void *p = (void *) &this->in_mhdr[ this->in_nsize ];
  for ( ; i < this->in_nsize; i++ ) {
    this->in_mhdr[ i ].msg_hdr.msg_name    = (struct sockaddr *) p;
    this->in_mhdr[ i ].msg_hdr.msg_namelen = sizeof( struct sockaddr_storage );
    p = &((uint8_t *) p)[ sizeof( struct sockaddr_storage ) ];

    this->in_mhdr[ i ].msg_hdr.msg_iov    = (struct iovec *) p;
    this->in_mhdr[ i ].msg_hdr.msg_iovlen = 1;
    p = &((uint8_t *) p)[ sizeof( struct iovec ) ];

    this->in_mhdr[ i ].msg_hdr.msg_iov[ 0 ].iov_base = buf;
    this->in_mhdr[ i ].msg_hdr.msg_iov[ 0 ].iov_len  = 64 * 1024;
    buf = &((uint8_t *) buf)[ 64 * 1024 ];

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
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
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
#if ! defined( NO_RECVMMSG ) && ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
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
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
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
    this->read_ns = this->poll.now_ns;
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
#if defined( _MSC_VER ) || defined( __MINGW32__ )
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
  bool is_high = this->test( EV_WRITE_HI );
  int  nmsgs = 0;
#if ! defined( NO_SENDMMSG ) && ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
  if ( this->out_nmsgs > 1 ) {
    nmsgs = ::sendmmsg( this->fd, this->out_mhdr, this->out_nmsgs, 0 );
    if ( nmsgs > 0 ) {
      for ( uint32_t i = 0; i < this->out_nmsgs; i++ )
        this->bytes_sent += this->out_mhdr[ i ].msg_len;
      goto write_notify;
    }
  }
  else
#else
#define NO_LABEL_WRITE_NOTIFY
#endif
  {
    for ( uint32_t i = 0; i < this->out_nmsgs; i++ ) {
      ssize_t nbytes;
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
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
#ifndef NO_LABEL_WRITE_NOTIFY
write_notify:;
#endif
  this->clear_buffers();
  this->pop3( EV_WRITE, EV_WRITE_HI, EV_WRITE_POLL );
  if ( is_high ) {
    if ( ! this->wait_empty() )
      this->notify_ready();
  }
}
/* if some alloc failed, kill the client */
void
EvSocket::close_error( uint16_t serr,  uint16_t err ) noexcept
{
  this->set_sock_err( serr, err );
  this->poll.idle_close( this, 0 );
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
    case EV_ERR_CONN_SELF:     return "EV_ERR_CONN_SELF, connected to self";
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
  if ( s != NULL && off < olen )
    off += ::snprintf( &o[ off ], olen - off, " error=%u/%s",
                       this->sock_err, s );
  if ( off < olen )
    off += ::snprintf( &o[ off ], olen - off, " fd=%d state=%x name=%s peer=%s",
                 this->fd, this->sock_state, this->name, this->peer_address.buf );
  if ( e != NULL ) {
    if ( off < olen )
      off += ::snprintf( &o[ off ], olen - off, " errno=%u/%s",
                         this->sock_errno, e );
  }
  else if ( this->sock_err == EV_ERR_WRITE_TIMEOUT ) {
    if ( off < olen )
      off += ::snprintf( &o[ off ], olen - off, " %u seconds",
                         this->sock_errno );
  }
  off = min_int( off, olen - 1 );
  if ( out == NULL )
    ::fprintf( stderr, "%.*s\n", (int) off, buf );
  return off;
}

void EvConnectionNotify::on_connect( EvSocket & ) noexcept {}
void EvConnectionNotify::on_shutdown( EvSocket &,  const char *,
                                      size_t ) noexcept {}
void EvConnectionNotify::on_data_loss( EvSocket &, EvPublish & ) noexcept {}

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
