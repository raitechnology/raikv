#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <arpa/nameser.h>
#include <poll.h>
#else
#include <raikv/win.h>
#endif
#include <raikv/ev_cares.h>
#include <raikv/ev_tcp.h>
#include <raikv/dlinklist.h>
#include <ares_dns.h>

using namespace rai;
using namespace kv;

CaresAddrInfo::~CaresAddrInfo() noexcept
{
  if ( this->addr_list != NULL )
    this->free_addr_list();
  this->stop();
  if ( this->host != NULL ) {
    ::free( this->host );
    this->host = NULL;
  }
}

void
EvCaresCallback::addr_resolve_cb( CaresAddrInfo & ) noexcept
{
}

void
CaresAddrInfo::stop( void ) noexcept
{
  if ( this->event_id != 0 ) {
    this->poll->timer.remove_timer_cb( *this, this->timer_id, this->event_id );
    this->event_id = 0;
  }
  for ( size_t i = 0; i < this->set.count; i++ ) {
    EvCaresAsync * c;
    if ( (c = this->set.ptr[ i ]) != NULL ) {
      c->process_close();
      this->set.ptr[ i ] = NULL;
    }
  }
  if ( this->channel != NULL ) {
    ares_destroy( this->channel );
    this->channel = NULL;
  }
  this->done     = true;
  this->status   = -1;
  this->timeouts = 0;
}

void
AddrInfoList::push( struct addrinfo *ai ) noexcept
{
  if ( this->tl != NULL )
    this->tl->ai_next = ai;
  else
    this->hd = ai;
  this->tl = ai;
  ai->ai_next = NULL;
}

void
AddrInfoList::append( AddrInfoList &l ) noexcept
{
  if ( l.hd == NULL )
    return;
  if ( this->hd == NULL ) {
    this->hd = l.hd;
    this->tl = l.tl;
    return;
  }
  this->tl->ai_next = l.hd;
  this->tl = l.tl;
}

void
CaresAddrInfo::split_ai( AddrInfoList &inet,  AddrInfoList &inet6 ) noexcept
{
  struct addrinfo * n, * ai_next;
  for ( n = this->addr_list; n != NULL; n = ai_next ) {
    ai_next = n->ai_next;
    if ( n->ai_family == AF_INET )
      inet.push( n );
    else
      inet6.push( n );
  }
  this->addr_list = NULL;
}

void
CaresAddrInfo::merge_ai( AddrInfoList &inet,  AddrInfoList &inet6 ) noexcept
{
  if ( this->ipv6_prefer ) {
    inet6.append( inet );
    this->addr_list = inet6.hd;
  }
  else {
    inet.append( inet6 );
    this->addr_list = inet.hd;
  }
}

void
CaresAddrInfo::free_addr_list( void ) noexcept
{
  while ( this->addr_list != NULL ) {
    struct addrinfo * next = this->addr_list->ai_next;
    ::free( this->addr_list );
    this->addr_list = next;
  }
}
/* version 1.16.0 has getaddrinfo */
#if ARES_VERSION < 0x011000
static void
cares_host_cb( void *arg,  int status,  int timeouts,
               struct hostent *hostent ) noexcept
{
  CaresAddrInfo &info = *(CaresAddrInfo *) arg;

  if ( status != 0 || timeouts != 0 ) {
    info.status   = status;
    info.timeouts = timeouts;
  }
  if ( --info.host_count == 0 )
    info.done = true;

  if ( hostent != NULL && hostent->h_addr_list != NULL &&
       hostent->h_length != 0 ) {
    AddrInfoList inet, inet6;
    info.split_ai( inet, inet6 );
    for ( size_t count = 0; hostent->h_addr_list[ count ] != NULL; count++ ) {
      size_t len = 0;
      if ( hostent->h_addrtype == AF_INET && hostent->h_length == 4 )
        len = sizeof( struct addrinfo ) + sizeof( struct sockaddr_in );
      else if ( hostent->h_addrtype == AF_INET6 && hostent->h_length == 16 )
        len = sizeof( struct addrinfo ) + sizeof( struct sockaddr_in6 );

      if ( len == 0 )
        continue;

      struct addrinfo * ai = (struct addrinfo *) ::malloc( len );
      ::memset( ai, 0, len );
      ai->ai_flags    = info.flags;
      ai->ai_family   = hostent->h_addrtype;
      ai->ai_socktype = info.socktype;
      ai->ai_protocol = info.protocol;
      ai->ai_addrlen  = len - sizeof( struct addrinfo );
      ai->ai_addr     = (struct sockaddr *) &ai[ 1 ];

      if ( hostent->h_addrtype == AF_INET && hostent->h_length == 4 ) {
        struct sockaddr_in * sa = (struct sockaddr_in *) ai->ai_addr;
        sa->sin_family = AF_INET;
        sa->sin_port   = htons( info.port );
        ::memcpy( &sa->sin_addr, hostent->h_addr_list[ count ], 4 );
        inet.push( ai );
      }
      else if ( hostent->h_addrtype == AF_INET6 && hostent->h_length == 16 ) {
        struct sockaddr_in6 * sa = (struct sockaddr_in6 *) ai->ai_addr;
        sa->sin6_family = AF_INET6;
        sa->sin6_port   = htons( info.port );
        ::memcpy( &sa->sin6_addr, hostent->h_addr_list[ count ], 16 );
        inet6.push( ai );
      }
    }
    info.merge_ai( inet, inet6 );
  }
  if ( info.done && info.notify_cb != NULL )
    info.notify_cb->addr_resolve_cb( info );
}
#else
static void
cares_addr_cb( void *arg, int status, int timeouts,
               struct ares_addrinfo *result ) noexcept
{
  CaresAddrInfo &info = *(CaresAddrInfo *) arg;

  if ( status != 0 || timeouts != 0 ) {
    info.status   = status;
    info.timeouts = timeouts;
  }
  if ( --info.host_count == 0 )
    info.done = true;

  if ( result != NULL ) {
    struct ares_addrinfo_node *n = result->nodes;
    AddrInfoList inet, inet6;
    info.split_ai( inet, inet6 );
    for ( ; n != NULL; n = n->ai_next ) {
      size_t len = 0;
      if ( n->ai_family == AF_INET || n->ai_family == AF_INET6 )
        len = sizeof( struct addrinfo ) + n->ai_addrlen;
      if ( len == 0 )
        continue;

      struct addrinfo * ai = (struct addrinfo *) ::malloc( len );
      ::memset( ai, 0, len );
      ai->ai_flags    = n->ai_flags;
      ai->ai_family   = n->ai_family;
      ai->ai_socktype = n->ai_socktype;
      ai->ai_protocol = n->ai_protocol;
      ai->ai_addrlen  = n->ai_addrlen;
      ai->ai_addr     = (struct sockaddr *) &ai[ 1 ];
      ::memcpy( ai->ai_addr, n->ai_addr, n->ai_addrlen );

      if ( ai->ai_family == AF_INET )
        inet.push( ai );
      else
        inet6.push( ai );
    }
    info.merge_ai( inet, inet6 );
    ares_freeaddrinfo( result );
  }
  if ( info.done && info.notify_cb != NULL )
    info.notify_cb->addr_resolve_cb( info );
}
#endif

int
CaresAddrInfo::get_address( const char *ip,  int port,  int opts ) noexcept
{
  if ( this->channel == NULL ) {
#ifdef ARES_LIB_INIT_ALL
    this->status = ares_library_init( ARES_LIB_INIT_ALL );
    if ( this->status != 0 )
      return this->status;
#endif
    ares_options opt;
    ::memset( &opt, 0, sizeof( opt ) );
    opt.timeout = this->timeout_ms;
    opt.tries   = this->tries;
    this->status =
      ares_init_options( &this->channel, &opt,
                         ARES_OPT_TIMEOUTMS | ARES_OPT_TRIES );
    if ( this->status != 0 ) {
      this->done = true;
      this->host_count = 0;
      if ( this->notify_cb != NULL )
        this->notify_cb->addr_resolve_cb( *this );
      return this->status;
    }
  }
  char ipbuf[ 32 ];
  ip = fix_ip4_device( ip, ipbuf );
  this->done     = false;
  this->status   = -1;
  this->timeouts = 0;
  this->port     = port;
  if ( this->host != ip ) {
    if ( ip == NULL ) {
      if ( this->host != NULL )
        ::free( this->host );
      this->host = NULL;
    }
    else {
      size_t iplen = ::strlen( ip );
      this->host = (char *) ::realloc( this->host, iplen + 1 );
      ::memcpy( this->host, ip, iplen + 1 );
    }
  }
  if ( ( opts & OPT_UDP ) == 0 ) {
    this->socktype = SOCK_STREAM;
    this->protocol = IPPROTO_TCP;
  }
  else {
    this->socktype = SOCK_DGRAM;
    this->protocol = IPPROTO_UDP;
  }
  if ( ( opts & OPT_LISTEN ) != 0 )
    this->flags = AI_PASSIVE;
  else
    this->flags = 0;

  if ( ( opts & OPT_NO_DNS ) != 0 )
    this->flags |= AI_NUMERICHOST;
  switch ( opts & ( OPT_AF_INET6 | OPT_AF_INET ) ) {
    case OPT_AF_INET:
      this->family = AF_INET;
      break;
    case OPT_AF_INET6:
      this->family = AF_INET6;
      break;
    default:
    case OPT_AF_INET | OPT_AF_INET6:
      this->family = AF_UNSPEC;
      break;
  }
  char svc[ 16 ], *svcp = NULL;
  if ( port != 0 ) {
    uint32_to_string( port, svc );
    svcp = svc;
  }
  this->host_count = 1;
  if ( ip == NULL ) {
    struct addrinfo hints, * nodes, * n;
    ::memset( &hints, 0, sizeof( hints ) );
    hints.ai_socktype = this->socktype;
    hints.ai_protocol = this->protocol;
    hints.ai_flags    = this->flags;
    hints.ai_family   = this->family;
    this->status = ::getaddrinfo( NULL, svcp, &hints, &nodes );
    if ( this->status != 0 )
      return this->status;
    if ( nodes != NULL ) {
      AddrInfoList inet, inet6;
      this->split_ai( inet, inet6 );
      for ( n = nodes; n != NULL; n = n->ai_next ) {
        size_t len = 0;
        if ( n->ai_family == AF_INET || n->ai_family == AF_INET6 )
          len = sizeof( struct addrinfo ) + n->ai_addrlen;
        if ( len == 0 )
          continue;

        struct addrinfo * ai = (struct addrinfo *) ::malloc( len );
        ::memcpy( ai, n, sizeof( *ai ) );
        ai->ai_addr = (struct sockaddr *) &ai[ 1 ];
        ::memcpy( ai->ai_addr, n->ai_addr, n->ai_addrlen );
        ai->ai_next = NULL;
        if ( ai->ai_family == AF_INET )
          inet.push( ai );
        else
          inet6.push( ai );
      }
      this->merge_ai( inet, inet6 );
      ::freeaddrinfo( nodes );
    }
  }
  else {
  #if ARES_VERSION < 0x011000
    if ( this->family == AF_INET )
      ares_gethostbyname( this->channel, ip, AF_INET, cares_host_cb, this );
    else if ( this->family == AF_INET6 )
      ares_gethostbyname( this->channel, ip, AF_INET6, cares_host_cb, this );
    else {
      ares_gethostbyname( this->channel, ip, AF_INET, cares_host_cb, this );
      /*this->host_count = 2;
      ares_gethostbyname( this->channel, ip, AF_INET, cares_host_cb, this );
      ares_gethostbyname( this->channel, ip, AF_INET6, cares_host_cb, this );*/
    }
  #else
    struct ares_addrinfo_hints hints;
    ::memset( &hints, 0, sizeof( hints ) );
    hints.ai_socktype = this->socktype;
    hints.ai_protocol = this->protocol;
    hints.ai_flags    = this->flags;
    hints.ai_family   = this->family;
    ares_getaddrinfo( this->channel, ip, svcp, &hints, cares_addr_cb, this );
  #endif
  }
  this->do_poll();
  return 0;
}

#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
void
CaresAddrInfo::do_pollfds( void ) noexcept
{
  pollfd pfd[ ARES_GETSOCK_MAXNUM ];
  ares_socket_t socks[ ARES_GETSOCK_MAXNUM ];
  uint32_t bitmap;
  int i, nfds;

  for (;;) {
    ::memset( socks, 0, sizeof( socks ) );
    bitmap = (uint32_t) ares_getsock( this->channel, socks, ARES_GETSOCK_MAXNUM );
    nfds = 0;
    for ( i = 0; i < ARES_GETSOCK_MAXNUM; i++ ) {
      if ( socks[ i ] > 0 ) {
        pfd[ nfds ].events = 0;
        if ( ARES_GETSOCK_READABLE( bitmap, i ) ) {
          pfd[ nfds ].fd = socks[ i ];
          pfd[ nfds ].events |= POLLRDNORM|POLLIN;
        }
        if ( ARES_GETSOCK_WRITABLE( bitmap, i ) ) {
          pfd[ nfds ].fd = socks[ i ];
          pfd[ nfds ].events |= POLLWRNORM|POLLOUT;
        }
        if ( pfd[ nfds ].events != 0 ) {
          pfd[ nfds ].revents = 0;
          nfds++;
        }
      }
    }
    if ( nfds != 0 ) {
      struct timeval * tvp, tv;
      tvp  = ares_timeout( this->channel, NULL, &tv );
      nfds = ::poll( pfd, nfds, tvp->tv_sec * 1000 + tvp->tv_usec / 1000 );
    }
    if ( nfds <= 0 )
      break;
    for ( i = 0; i < nfds; i++ ) {
      ares_process_fd(
        this->channel,
        pfd[ i ].revents & ( POLLRDNORM | POLLIN ) ? pfd[ i ].fd
                                                   : ARES_SOCKET_BAD,
        pfd[ i ].revents & ( POLLWRNORM | POLLOUT ) ? pfd[ i ].fd
                                                    : ARES_SOCKET_BAD );
    }
  }
}
#else
void
CaresAddrInfo::do_pollfds( void ) noexcept
{
  /* not used */
}
#endif

void
CaresAddrInfo::do_select( void ) noexcept
{
  int    nfds, count;
  fd_set read_fds, write_fds;
  struct timeval *tvp, tv;
  for (;;) {
    FD_ZERO( &read_fds );
    FD_ZERO( &write_fds );
    nfds = ares_fds( this->channel, &read_fds, &write_fds );
    if ( nfds == 0 )
      break;
    tvp   = ares_timeout( this->channel, NULL, &tv );
    count = select( nfds, &read_fds, &write_fds, NULL, tvp );
    if ( count < 0 && ( status = errno ) != EINVAL )
      break;
    ares_process( this->channel, &read_fds, &write_fds );
  }
}

void
CaresAddrInfo::do_poll( void ) noexcept
{
  ares_socket_t socks[ ARES_GETSOCK_MAXNUM ];
  EvCaresAsync * c;
  uint32_t i, k, bitmap = 0;
  bool has_write = false;

  this->poll_count++;
  if ( this->poll == NULL ) {
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
    this->do_pollfds();
#else
    this->do_select();
#endif
  }
  else {
    bitmap = (uint32_t)
      ares_getsock( this->channel, socks, ARES_GETSOCK_MAXNUM );
    /* add fds if polling them */
    for ( k = 0; k < ARES_GETSOCK_MAXNUM; k++ ) {
      if ( ARES_GETSOCK_READABLE( bitmap, k ) ) {
        for ( i = 0; i < this->set.count; i++ ) {
          if ( (c = this->set.ptr[ i ]) != NULL ) {
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
            if ( (ares_socket_t) c->fd == socks[ k ] )
#else
            SOCKET s;
            if ( wp_get_socket( c->fd, &s ) == 0 &&
                 (ares_socket_t) s == socks[ k ] )
#endif
            {
              c->poll_count = this->poll_count;
              break;
            }
          }
        }
        if ( i == this->set.count ) {
          c = this->poll->get_free_list<EvCaresAsync, CaresAddrInfo &>(
                this->sock_type, *this );
          this->set[ i ] = c;
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
          c->PeerData::init_peer( this->poll->get_next_id(), socks[ k ], -1,
                                  NULL, "c-ares" );
#else
          int fd = wp_register_fd( socks[ k ] );
          c->PeerData::init_peer( this->poll->get_next_id(), fd, -1,
                                  NULL, "c-ares" );
#endif
          c->poll_count = this->poll_count;
          this->poll->add_sock( c );
        }
        ares_process_fd( this->channel, socks[ k ], ARES_SOCKET_BAD );
      }
      if ( ARES_GETSOCK_WRITABLE( bitmap, k ) ) {
        ares_process_fd( this->channel, ARES_SOCKET_BAD, socks[ k ] );
        has_write = true;
      }
    }
    /* remove fds not used */
    for ( i = 0; i < this->set.count; i++ ) {
      if ( (c = this->set.ptr[ i ]) != NULL ) {
        if ( c->poll_count != this->poll_count ) {
          c->process_close();
          this->set.ptr[ i ] = NULL;
        }
      }
    }
  }
  /* no work left to do */
  if ( bitmap == 0 || this->done ) {
    if ( this->event_id != 0 ) {
      this->poll->timer.remove_timer_cb( *this, this->timer_id, this->event_id);
      this->event_id = 0;
    }
    bool b = this->done;
    this->done = true;
    if ( ! b && this->notify_cb != NULL )
      this->notify_cb->addr_resolve_cb( *this );
    return;
  }
  /* add timeout */
  struct timeval tval, *tv;
  tval.tv_sec = tval.tv_usec = 0;
  tv = ares_timeout( this->channel, NULL, &tval );
  uint64_t timeout_us = 0,
           cur_time   = 0;
  if ( tv != NULL ) {
    cur_time   = kv_current_monotonic_time_us();
    timeout_us = cur_time + tv->tv_sec * 1000000 + tv->tv_usec;
    if ( has_write ) { /* may need a ssl handshake or something */
      if ( cur_time + 100 < timeout_us )
        timeout_us = cur_time + 100;
    }
  }
  if ( this->event_id != 0 && timeout_us < this->event_id ) {
    this->poll->timer.remove_timer_cb( *this, this->timer_id, this->event_id );
    this->event_id = 0;
  }
  if ( this->event_id == 0 && timeout_us != 0 ) {
    this->timer_id++;
    this->event_id = timeout_us;
    this->poll->timer.add_timer_millis( *this, ( timeout_us - cur_time ) / 1000,
                                        this->timer_id, this->event_id );
  }
}

bool
CaresAddrInfo::timer_cb( uint64_t timer_id,  uint64_t event_id ) noexcept
{
  if ( timer_id == this->timer_id && this->event_id == event_id ) {
    this->event_id = 0;
    this->do_poll();
  }
  return false;
}

void
EvCaresAsync::write( void ) noexcept
{
}

void
EvCaresAsync::read( void ) noexcept
{
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
  ares_process_fd( this->info.channel, this->fd, ARES_SOCKET_BAD );
#else
  SOCKET s;
  if ( wp_get_socket( this->fd, &s ) == 0 )
    ares_process_fd( this->info.channel, s, ARES_SOCKET_BAD );
#endif
  this->pop3( EV_READ, EV_READ_LO, EV_READ_HI );
  this->info.do_poll();
}

void
EvCaresAsync::process( void ) noexcept
{
}

void
EvCaresAsync::release( void ) noexcept
{
#if defined( _MSC_VER ) || defined( __MINGW32__ )
  wp_unregister_fd( this->fd );
#endif
}
