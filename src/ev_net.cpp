#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/un.h>
#include <raikv/ev_net.h>
#include <raikv/ev_key.h>
#include <raikv/ev_publish.h>
#include <raikv/kv_pubsub.h>
#include <raikv/bit_iter.h>
#include <raikv/timer_queue.h>

using namespace rai;
using namespace kv;

uint8_t
EvPoll::register_type( const char *s ) noexcept
{
  uint8_t t = (uint8_t) kv_crc_c( s, ::strlen( s ), 0 );
  for ( int i = 0; i < 256; i++ ) {
    const char *x = this->sock_type_str[ t ];
    if ( x == NULL ) {
      this->sock_type_str[ t ] = s;
      return t;
    }
    if ( ::strcmp( x, s ) == 0 )
      return t;
    t++;
  }
  /* should be unique */
  fprintf( stderr, "no types left %s\n", s );
  exit( 1 );
  return 0;
}

int
EvPoll::init( int numfds,  bool prefetch/*,  bool single*/ ) noexcept
{
  uint32_t n  = align<uint32_t>( numfds, 2 ); /* 64 bit boundary */
  size_t   sz = ( sizeof( this->ev[ 0 ] ) +
                  sizeof( this->wr_poll[ 0 ] ) ) * n;
  if ( prefetch )
    this->prefetch_queue = EvPrefetchQueue::create();
  /*this->single_thread = single;*/

  if ( (this->efd = ::epoll_create( n )) < 0 ) {
    perror( "epoll" );
    return -1;
  }
  this->nfds = n;
  this->ev   = (struct epoll_event *) aligned_malloc( sz );
  if ( this->ev == NULL ) {
    perror( "malloc" );
    return -1;
  }
  this->wr_poll     = (EvSocket **) (void *) &this->ev[ n ];
  this->timer_queue = EvTimerQueue::create_timer_queue( *this );
  if ( this->timer_queue == NULL )
    return -1;
  return 0;
}

int
EvPoll::init_shm( EvShm &shm ) noexcept
{
  this->map    = shm.map;
  this->ctx_id = shm.ctx_id;
  this->dbx_id = shm.dbx_id;
  if ( (this->pubsub = KvPubSub::create( *this, 254 )) == NULL ) {
    fprintf( stderr, "unable to open kv pub sub\n" );
    return -1;
  }
  return 0;
}

int
EvPoll::wait( int ms ) noexcept
{
  struct epoll_event event;
  EvSocket *s;
  int n;
  while ( this->wr_count > 0 ) {
    s = this->wr_poll[ --this->wr_count ];
    ::memset( &event, 0, sizeof( struct epoll_event ) );
    event.data.fd = s->fd;
    event.events  = EPOLLOUT | EPOLLRDHUP | EPOLLET;
    if ( ::epoll_ctl( this->efd, EPOLL_CTL_MOD, s->fd, &event ) < 0 ) {
      perror( "epoll_ctl pollout" );
    }
    if ( this->wr_timeout_ns != 0 )
      this->push_write_queue( s );
  }
  n = ::epoll_wait( this->efd, this->ev, this->nfds, ms );
  if ( n < 0 ) {
    if ( errno == EINTR )
      return 0;
    perror( "epoll_wait" );
    return -1;
  }
  for ( int i = 0; i < n; i++ ) {
    s = this->sock[ this->ev[ i ].data.fd ];
    if ( ( this->ev[ i ].events & ( EPOLLIN | EPOLLRDHUP ) ) != 0 ) {
      if ( s->test( EV_WRITE_POLL ) ) { /* full output buffers, can't read */
        this->remove_write_queue( s );
        s->popall();
        s->idle_push( EV_CLOSE );
      }
      else {
        EvState ev = EV_READ;
        if ( s->test_opts( OPT_READ_HI ) )
          ev = EV_READ_HI;
        s->idle_push( ev );
      }
    }
    if ( ( this->ev[ i ].events & EPOLLOUT ) != 0 ) {
      if ( s->test( EV_WRITE_POLL ) ) { /* if not closed above */
        this->remove_write_queue( s );
        s->pop( EV_WRITE_POLL );
        s->idle_push( EV_WRITE_HI );
        ::memset( &event, 0, sizeof( struct epoll_event ) );
        event.data.fd = s->fd;
        event.events  = EPOLLIN | EPOLLRDHUP | EPOLLET;
        if ( ::epoll_ctl( this->efd, EPOLL_CTL_MOD, s->fd, &event ) < 0 ) {
          perror( "epoll_ctl pollin" );
        }
      }
    }
  }
  if ( ! this->ev_write.is_empty() ) {
    uint64_t ns = this->current_coarse_ns();
    do {
      s = this->ev_write.heap[ 0 ];
      if ( ns - s->PeerData::active_ns > this->wr_timeout_ns &&
           ns > s->PeerData::active_ns ) {
        this->idle_close( s, ns - s->PeerData::active_ns );
      }
    } while ( ! this->ev_write.is_empty() );
  }
  return n;
}
/* allocate an fd for a null sockets which are used for event based objects
 * that wait on timers and/or subscriptions */
int
EvPoll::get_null_fd( void ) noexcept
{
  if ( this->null_fd < 0 ) {
    this->null_fd = ::open( "/dev/null", O_RDWR | O_NONBLOCK );
    return this->null_fd;
  }
  return ::dup( this->null_fd );
}
/* enable epolling of sock fd */
int
EvPoll::add_sock( EvSocket *s ) noexcept
{
  if ( s->fd < 0 ) /* must be a valid fd */
    return -1;
  /* make enough space for fd */
  if ( (uint32_t) s->fd > this->maxfd ) {
    uint32_t xfd = align<uint32_t>( s->fd + 1, EvPoll::ALLOC_INCR );
    EvSocket **tmp;
    if ( xfd < this->nfds )
      xfd = this->nfds;
  try_again:;
    tmp = (EvSocket **)
          ::realloc( this->sock, xfd * sizeof( this->sock[ 0 ] ) );
    if ( tmp == NULL ) {
      perror( "realloc" );
      xfd /= 2;
      if ( xfd > (uint32_t) s->fd )
        goto try_again;
      return -1;
    }
    for ( uint32_t i = this->maxfd + 1; i < xfd; i++ )
      tmp[ i ] = NULL;
    this->sock  = tmp;
    this->maxfd = xfd - 1;
  }
  if ( ! s->test_opts( OPT_NO_POLL ) ) {
    /* add to poll set */
    struct epoll_event event;
    ::memset( &event, 0, sizeof( struct epoll_event ) );
    event.data.fd = s->fd;
    event.events  = EPOLLIN | EPOLLRDHUP | EPOLLET;
    if ( ::epoll_ctl( this->efd, EPOLL_CTL_ADD, s->fd, &event ) < 0 ) {
      perror( "epoll_ctl" );
      return -1;
    }
  }
  this->sock[ s->fd ] = s;
  this->fdcnt++;
  /* add to active list */
  s->set_list( IN_ACTIVE_LIST );
  this->active_list.push_tl( s );
  /* if sock starts in write mode, add it to the queue */
  s->prio_cnt = this->prio_tick;
  if ( s->state != 0 )
    this->push_event_queue( s );
  uint64_t ns = this->current_coarse_ns();
  s->start_ns   = ns;
  s->active_ns  = ns;
  s->id         = ++this->next_id;
  s->bytes_recv = 0;
  s->bytes_sent = 0;
  s->msgs_recv  = 0;
  s->msgs_sent  = 0;
  return 0;
}
/* remove a sock fd from epolling */
void
EvPoll::remove_sock( EvSocket *s ) noexcept
{
  struct epoll_event event;
  if ( s->fd < 0 )
    return;
  /* remove poll set */
  if ( (uint32_t) s->fd <= this->maxfd && this->sock[ s->fd ] == s ) {
    if ( ! s->test_opts( OPT_NO_POLL ) ) {
      ::memset( &event, 0, sizeof( struct epoll_event ) );
      event.data.fd = s->fd;
      event.events  = 0;
      if ( ::epoll_ctl( this->efd, EPOLL_CTL_DEL, s->fd, &event ) < 0 )
        perror( "epoll_ctl" );
    }
    this->sock[ s->fd ] = NULL;
    this->fdcnt--;
  }
  /* close if wants */
  if ( ! s->test_opts( OPT_NO_CLOSE ) && s->fd != this->null_fd ) {
    if ( ::close( s->fd ) != 0 ) {
      fprintf( stderr, "close: errno %d/%s, fd %d type %s\n",
               errno, strerror( errno ), s->fd, s->type_string() );
    }
  }
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
  struct sockaddr_storage addr;
  socklen_t addrlen = sizeof( addr );
  char buf[ 128 ], svc[ 32 ];

  if ( getpeername( s->fd, (struct sockaddr*) &addr, &addrlen ) == 0 &&
       getnameinfo( (struct sockaddr*) &addr, addrlen, buf, sizeof( buf ),
                  svc, sizeof( svc ), NI_NUMERICHOST | NI_NUMERICSERV ) == 0 ) {
    fprintf( stderr, "write timeout: %.3f secs, force close fd %d addr %s:%s\n",
             (double) ns / 1000000000.0, s->fd, buf, svc );
  }
  else {
    fprintf( stderr, "write timeout: %.3f secs, force close fd %d\n",
             (double) ns / 1000000000.0, s->fd );
  }
  /* log closed fd */
  this->remove_write_queue( s );
  s->popall();
  s->idle_push( EV_CLOSE );
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
      printf( "%s %s: %lu\n",
        sock.kind, EvSocket::state_string( (EvState) i ), diff );
    }
  }
}
#endif
/* vtable dispatch */
const char *
EvSocket::type_string( void ) noexcept
{
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
}

/* thsee are optional, but not necessary for a working protocol */
/* timer, return true to rearm same interval */
bool EvSocket::timer_expire( uint64_t, uint64_t ) noexcept { return false; }
/* pub sub, subject resolution, deliver publish message */
bool EvSocket::hash_to_sub( uint32_t, char *, size_t & ) noexcept { return false; }
bool EvSocket::on_msg( EvPublish & ) noexcept { return true; }
/* key prefetch, requires a batch of keys to hide latency */
void EvSocket::key_prefetch( EvKeyCtx & ) noexcept {}
int  EvSocket::key_continue( EvKeyCtx & ) noexcept { return 0; }

EvListen::EvListen( EvPoll &p,  const char *lname,  const char *name )
    : EvSocket( p, p.register_type( lname ) ), accept_cnt( 0 ),
      accept_sock_type( p.register_type( name ) )
{
  this->timer_id = (uint64_t) this->accept_sock_type << 56;
}

void
EvListen::read( void ) noexcept
{
  if ( this->accept() )
    this->accept_cnt++;
}

/* listeners don't do these */
void EvListen::write( void ) noexcept {}   /* nothing to write */
void EvListen::process( void ) noexcept {} /* nothing to process */
void EvListen::release( void ) noexcept {} /* no buffers */

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
          if ( s->state != 0 )
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
EvPoll::current_coarse_ns( void ) const noexcept
{
  if ( this->map != NULL )
    return this->map->hdr.current_stamp;
  return current_realtime_coarse_ns();
}

int
EvPoll::dispatch( void ) noexcept
{
  EvSocket * s;
  uint64_t start   = this->prio_tick,
           curr_ns = this->current_coarse_ns(),
           busy_ns = this->timer_queue->busy_delta( curr_ns ),
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
        busy_ns = this->timer_queue->busy_delta( curr_ns );
      }
      if ( busy_ns == 0 ) {
        /* if timer is already an event (in_queue=1), dispatch that first */
        if ( ! this->timer_queue->in_queue( IN_EVENT_QUEUE ) ) {
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
    /*printf( "dispatch %u %u (%x)\n", s->type, state, s->state );*/
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
        s->pop( EV_SHUTDOWN );
        break;
      case EV_CLOSE:
        s->popall();
        this->remove_sock( s );
        s->process_close();
        break;
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
    if ( s->state != 0 ) {
      if ( s->test( EV_WRITE_HI ) ) {
        if ( s->test( EV_WRITE_POLL ) ) {
          ret |= POLL_NEEDED;
          this->wr_poll[ this->wr_count++ ] = s;
          continue; /* don't put into event queue */
        }
        ret |= WRITE_PRESSURE;
      }
      s->prio_cnt = this->prio_tick;
      this->push_event_queue( s );
    }
  }
}

/* different publishers for different size route matches, one() is the most
 * common, but multi / queue need to be used with multiple routes */
bool
EvPoll::publish_one( EvPublish &pub,  uint32_t *rcount_total,
                     RoutePublishData &rpd ) noexcept
{
  uint32_t * routes    = rpd.routes;
  uint32_t   rcount    = rpd.rcount;
  uint32_t   hash[ 1 ];
  uint8_t    prefix[ 1 ];
  bool       flow_good = true;

  if ( rcount_total != NULL )
    *rcount_total += rcount;
  pub.hash       = hash;
  pub.prefix     = prefix;
  pub.prefix_cnt = 1;
  hash[ 0 ]      = rpd.hash;
  prefix[ 0 ]    = rpd.prefix;
  for ( uint32_t i = 0; i < rcount; i++ ) {
    EvSocket * s;
    if ( routes[ i ] <= this->maxfd &&
         (s = this->sock[ routes[ i ] ]) != NULL ) {
      flow_good &= s->on_msg( pub );
    }
  }
  return flow_good;
}

template<uint8_t N>
bool
EvPoll::publish_multi( EvPublish &pub,  uint32_t *rcount_total,
                       RoutePublishData *rpd ) noexcept
{
  EvSocket * s;
  uint32_t   min_route,
             rcount    = 0,
             hash[ 2 ];
  uint8_t    prefix[ 2 ],
             i, cnt;
  bool       flow_good = true;

  pub.hash   = hash;
  pub.prefix = prefix;
  for (;;) {
    for ( i = 0; i < N; ) {
      if ( rpd[ i++ ].rcount > 0 ) {
        min_route = rpd[ i - 1 ].routes[ 0 ];
        goto have_one_route;
      }
    }
    break; /* if no routes left */
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
    if ( (s = this->sock[ min_route ]) != NULL ) {
      rcount++;
      pub.prefix_cnt = cnt;
      flow_good &= s->on_msg( pub );
    }
  }
  if ( rcount_total != NULL )
    *rcount_total += rcount;
  return flow_good;
}
/* same as above with a prio queue heap instead of linear search */
bool
EvPoll::publish_queue( EvPublish &pub,  uint32_t *rcount_total ) noexcept
{
  RoutePublishQueue & queue     = this->pub_queue;
  RoutePublishData  * rpd       = queue.pop();
  EvSocket          * s;
  uint32_t            min_route;
  uint32_t            rcount    = 0;
  bool                flow_good = true;
  uint8_t             cnt;
  uint8_t             prefix[ 65 ];
  uint32_t            hash[ 65 ];

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
    if ( (s = this->sock[ min_route ]) != NULL ) {
      rcount++;
      pub.prefix_cnt = cnt;
      flow_good &= s->on_msg( pub );
    }
  }
  if ( rcount_total != NULL )
    *rcount_total += rcount;
  return flow_good;
}
/* match subject against route db and forward msg to fds subscribed, route
 * db contains both exact matches and wildcard prefix matches */
bool
RoutePublish::forward_msg( EvPublish &pub,  uint32_t *rcount_total,
                           uint8_t pref_cnt,  KvPrefHash *ph ) noexcept
{
  EvPoll   & poll      = static_cast<EvPoll &>( *this );
  uint32_t * routes    = NULL;
  uint32_t   rcount    = poll.sub_route.get_route( pub.subj_hash, routes ),
             hash;
  uint8_t    n         = 0;
  bool       flow_good = true;
  RoutePublishData rpd[ 65 ];

  if ( rcount_total != NULL )
    *rcount_total = 0;
  if ( rcount > 0 ) {
    rpd[ 0 ].prefix = 64;
    rpd[ 0 ].hash   = pub.subj_hash;
    rpd[ 0 ].rcount = rcount;
    rpd[ 0 ].routes = routes;
    n = 1;
  }

  BitIter64 bi( poll.sub_route.pat_mask );
  if ( bi.first() ) {
    uint8_t j = 0;
    do {
      while ( j < pref_cnt ) {
        if ( ph[ j++ ].pref == bi.i ) {
          hash = ph[ j - 1 ].get_hash();
          goto found_hash;
        }
      }
      hash = kv_crc_c( pub.subject, bi.i, poll.sub_route.prefix_seed( bi.i ) );
    found_hash:;
      rcount = poll.sub_route.push_get_route( n, hash, routes );
      if ( rcount > 0 ) {
        rpd[ n ].hash   = hash;
        rpd[ n ].prefix = bi.i;
        rpd[ n ].rcount = rcount;
        rpd[ n ].routes = routes;
        n++;
      }
    } while ( bi.next() );
  }
  /* likely cases <= 3 wilcard matches, most likely just 1 match */
  if ( n > 0 ) {
    if ( n == 1 ) {
      flow_good &= poll.publish_one( pub, rcount_total, rpd[ 0 ] );
    }
    else {
      switch ( n ) {
        case 2:
          flow_good &= poll.publish_multi<2>( pub, rcount_total, rpd );
          break;
        default: {
          for ( uint8_t i = 0; i < n; i++ )
            poll.pub_queue.push( &rpd[ i ] );
          flow_good &= poll.publish_queue( pub, rcount_total );
          break;
        }
      }
    }
  }
  return flow_good;
}
/* convert a hash into a subject string, this may have collisions */
bool
RoutePublish::hash_to_sub( uint32_t r,  uint32_t h,  char *key,
                           size_t &keylen ) noexcept
{
  EvPoll & poll = static_cast<EvPoll &>( *this );
  EvSocket *s;
  bool b = false;
  if ( r <= poll.maxfd && (s = poll.sock[ r ]) != NULL )
    b = s->hash_to_sub( h, key, keylen );
  return b;
}
/* track number of subscribes to keyspace subjects to enable them */
inline void
RoutePublish::update_keyspace_count( const char *sub,  size_t len,
                                     int add ) noexcept
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
    this->keyspace_cnt += add;
  if ( ::memcmp( kevt, sub, len ) == 0 )
    this->keyevent_cnt += add;
  if ( ::memcmp( lblk, sub, len ) == 0 )
    this->listblkd_cnt += add;
  if ( ::memcmp( zblk, sub, len ) == 0 )
    this->zsetblkd_cnt += add;
  if ( ::memcmp( sblk, sub, len ) == 0 )
    this->strmblkd_cnt += add;
  if ( ::memcmp( moni, sub, len ) == 0 )
    this->monitor__cnt += add;

  this->key_flags = ( ( this->keyspace_cnt == 0 ? 0 : EKF_KEYSPACE_FWD ) |
                      ( this->keyevent_cnt == 0 ? 0 : EKF_KEYEVENT_FWD ) |
                      ( this->listblkd_cnt == 0 ? 0 : EKF_LISTBLKD_NOT ) |
                      ( this->zsetblkd_cnt == 0 ? 0 : EKF_ZSETBLKD_NOT ) |
                      ( this->strmblkd_cnt == 0 ? 0 : EKF_STRMBLKD_NOT ) |
                      ( this->monitor__cnt == 0 ? 0 : EKF_MONITOR      ) );
  /*printf( "%.*s %d key_flags %x\n", (int) len, sub, add, this->key_flags );*/
}
/* external patterns from kv pubsub */
void
EvPoll::add_pattern_route( const char *sub,  size_t prefix_len,  uint32_t hash,
                           uint32_t fd ) noexcept
{
  size_t pre_len = ( prefix_len < 11 ? prefix_len : 11 );
  /* if first route added for hash */
  if ( this->sub_route.add_pattern_route( hash, fd, prefix_len ) == 1 ) {
    /*printf( "add_pattern %.*s\n", (int) prefix_len, sub );*/
    this->update_keyspace_count( sub, pre_len, 1 );
  }
}

void
EvPoll::del_pattern_route( const char *sub,  size_t prefix_len,  uint32_t hash,
                           uint32_t fd ) noexcept
{
  size_t pre_len = ( prefix_len < 11 ? prefix_len : 11 );
  /* if last route deleted */
  if ( this->sub_route.del_pattern_route( hash, fd, prefix_len ) == 0 ) {
    /*printf( "del_pattern %.*s\n", (int) prefix_len, sub );*/
    this->update_keyspace_count( sub, pre_len, -1 );
  }
}
/* external routes from kv pubsub */
void
EvPoll::add_route( const char *sub,  size_t sub_len,  uint32_t hash,
                   uint32_t fd ) noexcept
{
  /* if first route added for hash */
  if ( this->sub_route.add_route( hash, fd ) == 1 ) {
    if ( sub_len > 11 ) {
      /*printf( "add_route %.*s\n", (int) sub_len, sub );*/
      this->update_keyspace_count( sub, 11, 1 );
    }
  }
}

void
EvPoll::del_route( const char *sub,  size_t sub_len,  uint32_t hash,
                   uint32_t fd ) noexcept
{
  /* if last route deleted */
  if ( this->sub_route.del_route( hash, fd ) == 0 ) {
    if ( sub_len > 11 ) {
      /*printf( "del_route %.*s\n", (int) sub_len, sub );*/
      this->update_keyspace_count( sub, 11, -1 );
    }
  }
}
/* client subscribe, notify to kv pubsub */
void
RoutePublish::notify_sub( uint32_t h,  const char *sub,  size_t len,
                          uint32_t sub_id,  uint32_t rcnt,  char src_type,
                          const char *rep,  size_t rlen ) noexcept
{
  EvPoll & poll = static_cast<EvPoll &>( *this );
  if ( len > 11 )
    this->update_keyspace_count( sub, 11, 1 );
  poll.pubsub->do_sub( h, sub, len, sub_id, rcnt, src_type, rep, rlen );
}

void
RoutePublish::notify_unsub( uint32_t h,  const char *sub,  size_t len,
                            uint32_t sub_id,  uint32_t rcnt,
                            char src_type ) noexcept
{
  EvPoll & poll = static_cast<EvPoll &>( *this );
  if ( len > 11 )
    this->update_keyspace_count( sub, 11, -1 );
  poll.pubsub->do_unsub( h, sub, len, sub_id, rcnt, src_type );
}
/* client pattern subscribe, notify to kv pubsub */
void
RoutePublish::notify_psub( uint32_t h,  const char *pattern,  size_t len,
                           const char *prefix,  uint8_t prefix_len,
                           uint32_t sub_id,  uint32_t rcnt,
                           char src_type ) noexcept
{
  EvPoll & poll = static_cast<EvPoll &>( *this );
  size_t pre_len = ( prefix_len < 11 ? prefix_len : 11 );
  this->update_keyspace_count( prefix, pre_len, 1 );
  poll.pubsub->do_psub( h, pattern, len, prefix, prefix_len,
                        sub_id, rcnt, src_type );
}

void
RoutePublish::notify_punsub( uint32_t h,  const char *pattern,  size_t len,
                             const char *prefix,  uint8_t prefix_len,
                             uint32_t sub_id,  uint32_t rcnt,
                             char src_type ) noexcept
{
  EvPoll & poll = static_cast<EvPoll &>( *this );
  size_t pre_len = ( prefix_len < 11 ? prefix_len : 11 );
  this->update_keyspace_count( prefix, pre_len, -1 );
  poll.pubsub->do_punsub( h, pattern, len, prefix, prefix_len,
                          sub_id, rcnt, src_type );
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
        if ( s->in_queue( IN_EVENT_QUEUE ) ) {
          s->set_queue( IN_NO_QUEUE );
          this->ev_queue.remove( s ); /* close state */
        }
        if ( s->state != 0 )
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
/* convert sockaddr into a string and set peer_address[] */
void
PeerData::set_addr( const sockaddr *sa ) noexcept
{
  const size_t maxlen = sizeof( this->peer_address );
  char         buf[ maxlen ],
             * s = buf,
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

      case AF_LOCAL:
        len = ::strnlen( ((struct sockaddr_un *) sa)->sun_path,
                         sizeof( ((struct sockaddr_un *) sa)->sun_path ) );
        if ( len > maxlen - 1 )
          len = maxlen - 1;
        ::memcpy( s, ((struct sockaddr_un *) sa)->sun_path, len );
        t = &s[ len ];
        break;

      default:
        break;
    }
  }
  if ( t != NULL ) {
    /* set strlen */
    this->set_peer_address( buf, t - s );
  }
  else {
    this->set_peer_address( NULL, 0 );
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
    "id=%lu addr=%.*s fd=%d name=%.*s kind=%s age=%ld idle=%ld ",
    this->PeerData::id,
    (int) this->PeerData::get_peer_address_strlen(),
    this->PeerData::peer_address,
    this->PeerData::fd,
    (int) this->PeerData::get_name_strlen(), this->PeerData::name,
    this->PeerData::kind,
    ( cur_time_ns - this->PeerData::start_ns ) / ONE_NS,
    ( cur_time_ns - this->PeerData::active_ns ) / ONE_NS );
}

bool
EvSocket::client_match( PeerData &pd,  PeerMatchArgs *ka,  ... ) noexcept
{
  /* match filters, if any don't match return false */
  if ( ka->id != 0 )
    if ( (uint64_t) ka->id != pd.id ) /* match id */
      return false;
  if ( ka->ip_len != 0 ) /* match ip address string */
    if ( ka->ip_len != pd.get_peer_address_strlen() ||
         ::memcmp( ka->ip, pd.peer_address, ka->ip_len ) != 0 )
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
    if ( this->in_queue( IN_EVENT_QUEUE ) ) {
      this->set_queue( IN_NO_QUEUE );
      this->poll.ev_queue.remove( this ); /* close state */
    }
    if ( this->state != 0 )
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
      "rbuf=%u rsz=%u imsg=%lu br=%lu wbuf=%lu wsz=%lu omsg=%lu bs=%lu ",
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
                     "acpt=%lu ",
                     this->accept_cnt );
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
                     "imsg=%lu omsg=%lu br=%lu bs=%lu ",
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
/* start a timer event */
bool
RoutePublish::add_timer_seconds( int id,  uint32_t ival,  uint64_t timer_id,
                                 uint64_t event_id ) noexcept
{
  EvPoll & poll = static_cast<EvPoll &>( *this );
  return poll.timer_queue->add_timer_seconds( id, ival, timer_id, event_id );
}

bool
RoutePublish::add_timer_millis( int id,  uint32_t ival,  uint64_t timer_id,
                                uint64_t event_id ) noexcept
{
  EvPoll & poll = static_cast<EvPoll &>( *this );
  return poll.timer_queue->add_timer_units( id, ival, IVAL_MILLIS, timer_id,
                                            event_id );
}

bool
RoutePublish::remove_timer( int id,  uint64_t timer_id,
                            uint64_t event_id ) noexcept
{
  EvPoll & poll = static_cast<EvPoll &>( *this );
  return poll.timer_queue->remove_timer( id, timer_id, event_id );
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
      ssize_t nbytes = ::read( this->fd, &this->recv[ this->len ],
                               this->recv_size - this->len );
      if ( nbytes > 0 ) {
        this->len += nbytes;
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
      if ( nbytes < 0 ) {
        if ( errno != EINTR ) {
          if ( errno != EAGAIN ) {
            if ( errno != ECONNRESET )
              perror( "read" );
            this->popall();
            this->push( EV_CLOSE );
          }
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
  void * ex_recv_buf = aligned_malloc( newsz );
  if ( ex_recv_buf == NULL )
    return false;
  ::memcpy( ex_recv_buf, &this->recv[ this->off ], this->len );
  this->len -= this->off;
  this->off  = 0;
  if ( this->recv != this->recv_buf )
    ::free( this->recv );
  this->recv = (char *) ex_recv_buf;
  this->recv_size = newsz;
  return true;
}
/* write data to sock fd */
void
EvConnection::write( void ) noexcept
{
  struct msghdr h;
  ssize_t nbytes;
  size_t nb = 0;
  StreamBuf & strm = *this;
  if ( strm.sz > 0 )
    strm.flush();
  else if ( strm.wr_pending == 0 ) {
    this->pop3( EV_WRITE, EV_WRITE_HI, EV_WRITE_POLL );
    return;
  }
  ::memset( &h, 0, sizeof( h ) );
  h.msg_iov    = &strm.iov[ strm.woff ];
  h.msg_iovlen = strm.idx - strm.woff;
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
  if ( nbytes > 0 ) {
    strm.wr_pending -= nbytes;
    this->bytes_sent += nbytes;
    nb += nbytes;
    if ( strm.wr_pending == 0 ) {
      strm.reset();
      this->pop3( EV_WRITE, EV_WRITE_HI, EV_WRITE_POLL );
    }
    else {
      for (;;) {
        if ( (size_t) nbytes >= strm.iov[ strm.woff ].iov_len ) {
          nbytes -= strm.iov[ strm.woff ].iov_len;
          strm.woff++;
          if ( nbytes == 0 )
            break;
        }
        else {
          char *base = (char *) strm.iov[ strm.woff ].iov_base;
          strm.iov[ strm.woff ].iov_len -= nbytes;
          strm.iov[ strm.woff ].iov_base = &base[ nbytes ];
          break;
        }
      }
    }
    return;
  }
  if ( nbytes == 0 || ( nbytes < 0 && errno != EAGAIN && errno != EINTR ) ) {
    if ( nbytes < 0 && errno != ECONNRESET && errno != EPIPE ) {
      fprintf( stderr, "sendmsg: errno %d/%s, fd %d, state %d\n",
               errno, strerror( errno ), this->fd, this->state );
    }
    this->popall();
    this->push( EV_CLOSE );
  }
  if ( this->test( EV_WRITE_HI ) )
    this->push( EV_WRITE_POLL );
}
/* use mmsg for udp sockets */
bool
EvUdp::alloc_mmsg( void ) noexcept
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

ssize_t
EvUdp::discard_pkt( void ) noexcept
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
  nbytes = ::recvmsg( this->fd, &hdr, 0 );
  if ( nbytes > 0 )
    fprintf( stderr, "discard %ld bytes\n", (long) nbytes );
  return nbytes;
}

/* read udp packets */
void
EvUdp::read( void ) noexcept
{
  int     nmsgs  = 0;
  ssize_t nbytes = 0;

  if ( this->in_nmsgs == this->in_size && ! this->alloc_mmsg() ) {
    nbytes = this->discard_pkt();
  }
#ifndef NO_RECVMMSG
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
      nbytes = ::recvmsg( this->fd,
                          &this->in_mhdr[ this->in_nmsgs + nmsgs ].msg_hdr, 0 );
      if ( nbytes > 0 ) {
        this->in_mhdr[ this->in_nmsgs + nmsgs ].msg_len = nbytes;
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
    this->in_nsize = ( ( this->in_nmsgs < 8 ) ? this->in_nmsgs + 1 : 8 );
    this->push( EV_PROCESS );
    this->pushpop( EV_READ_LO, EV_READ );
    return;
  }
  this->in_nsize = 1;
  /* wait for epoll() to set EV_READ again */
  this->pop3( EV_READ, EV_READ_LO, EV_READ_HI );
  if ( ( nmsgs < 0 || nbytes < 0 ) && errno != EINTR ) {
    if ( errno != EAGAIN ) {
      if ( errno != ECONNRESET )
        perror( "recvmmsg" );
      this->popall();
      this->push( EV_CLOSE );
    }
  }
}
/* write udp packets */
void
EvUdp::write( void ) noexcept
{
  int nmsgs = 0;
#ifndef NO_SENDMMSG
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
  else {
    ssize_t nbytes = ::sendmsg( this->fd, &this->out_mhdr[ 0 ].msg_hdr, 0 );
    if ( nbytes > 0 ) {
      this->bytes_sent += nbytes;
      this->clear_buffers();
      this->pop3( EV_WRITE, EV_WRITE_HI, EV_WRITE_POLL );
      return;
    }
    if ( nbytes < 0 )
      nmsgs = -1;
  }
#else
  for ( uint32_t i = 0; i < this->out_nmsgs; i++ ) {
    ssize_t nbytes = ::sendmsg( this->fd, &this->out_mhdr[ i ].msg_hdr, 0 );
    if ( nbytes > 0 )
      this->bytes_sent += nbytes;
    if ( nbytes < 0 ) {
      nmsgs = -1;
      break;
    }
  }
#endif
  if ( nmsgs < 0 && errno != EAGAIN && errno != EINTR ) {
    if ( errno != ECONNRESET && errno != EPIPE ) {
      fprintf( stderr, "sendmsg: errno %d/%s, fd %d, state %d\n",
               errno, strerror( errno ), this->fd, this->state );
    }
    this->popall();
    this->push( EV_CLOSE );
  }
  this->clear_buffers();
  this->pop3( EV_WRITE, EV_WRITE_HI, EV_WRITE_POLL );
}
/* if some alloc failed, kill the client */
void
EvConnection::close_alloc_error( void ) noexcept
{
  fprintf( stderr, "Allocation failed! Closing connection\n" );
  this->popall();
  this->push( EV_CLOSE );
}
/* when a sock is not dispatch()ed, it may need to be rearranged in the queue
 * for correct priority */
void
EvSocket::idle_push( EvState s ) noexcept
{
  /* if no state, not currently in the queue */
  if ( this->state == 0 ) {
  do_push:;
    this->push( s );
    this->prio_cnt = this->poll.prio_tick;
    this->poll.push_event_queue( this );
  }
  /* check if added state requires queue to be rearranged */
  else {
    int x1 = __builtin_ffs( this->state ),
        x2 = __builtin_ffs( this->state | ( 1 << s ) );
    /* new state has higher priority than current state, reorder queue */
    if ( x1 > x2 ) {
      if ( this->in_queue( IN_EVENT_QUEUE ) ) {
        this->set_queue( IN_NO_QUEUE );
        this->poll.ev_queue.remove( this );
      }
      goto do_push; /* pop, then push */
    }
    /* otherwise, current state >= the new state, in the correct order */
    else {
      this->push( s );
    }
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
