#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <raikv/ev_tcp.h>

using namespace rai;
using namespace kv;

int
EvTcpListen::listen( const char *ip,  int port,  int opts,
                     const char *k ) noexcept
{
  static int on = 1, off = 0;
  int  status = 0,
       sock;
  char svc[ 16 ];
  struct addrinfo hints, * ai = NULL, * p = NULL;
  struct sockaddr_storage addr;
  socklen_t addrlen = sizeof( addr );

  this->sock_opts = opts;
  ::snprintf( svc, sizeof( svc ), "%d", port );
  ::memset( &hints, 0, sizeof( struct addrinfo ) );
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  hints.ai_flags    = AI_PASSIVE;
  switch ( opts & ( OPT_AF_INET6 | OPT_AF_INET ) ) {
    case OPT_AF_INET:
      hints.ai_family = AF_INET;
      break;
    case OPT_AF_INET6:
      hints.ai_family = AF_INET6;
      break;
    default:
    case OPT_AF_INET | OPT_AF_INET6:
      hints.ai_family = AF_UNSPEC;
      break;
  }
  status = ::getaddrinfo( ip, svc, &hints, &ai );
  if ( status != 0 )
    return this->set_sock_err( EV_ERR_GETADDRINFO, errno );

  sock = -1;
  /* try inet6 first, since it can listen to both ip stacks */
  for ( int fam = AF_INET6; ; fam = AF_INET ) {
    for ( p = ai; p != NULL; p = p->ai_next ) {
      if ( ( fam == AF_INET6 && ( opts & OPT_AF_INET6 ) != 0 ) ||
           ( fam == AF_INET  && ( opts & OPT_AF_INET ) != 0 ) ) {
        if ( fam == p->ai_family ) {
          sock = ::socket( p->ai_family, p->ai_socktype, p->ai_protocol );
          if ( sock < 0 )
            continue;
          if ( fam == AF_INET6 && ( opts & OPT_AF_INET ) != 0 ) {
            if ( ::setsockopt( sock, IPPROTO_IPV6, IPV6_V6ONLY, &off,
                               sizeof( off ) ) != 0 )
              if ( ( opts & OPT_VERBOSE ) != 0 )
                perror( "warning: IPV6_V6ONLY" );
          }
          if ( ( opts & OPT_REUSEADDR ) != 0 ) {
            if ( ::setsockopt( sock, SOL_SOCKET, SO_REUSEADDR, &on,
                               sizeof( on ) ) != 0 )
              if ( ( opts & OPT_VERBOSE ) != 0 )
                perror( "warning: SO_REUSEADDR" );
          }
          if ( ( opts & OPT_REUSEPORT ) != 0 ) {
            if ( ::setsockopt( sock, SOL_SOCKET, SO_REUSEPORT, &on,
                               sizeof( on ) ) != 0 )
              if ( ( opts & OPT_VERBOSE ) != 0 )
                perror( "warning: SO_REUSEPORT" );
          }
          this->PeerData::set_addr( p->ai_addr );
          status = ::bind( sock, p->ai_addr, p->ai_addrlen );
          if ( status == 0 )
            goto break_loop;
          ::close( sock );
          sock = -1;
        }
      }
    }
    if ( fam == AF_INET ) /* tried both */
      break;
  }
break_loop:;
  if ( status != 0 ) {
    status = this->set_sock_err( EV_ERR_BIND, errno );
    goto fail;
  }
  if ( sock == -1 ) {
    status = this->set_sock_err( EV_ERR_SOCKET, errno );
    goto fail;
  }
  status = ::listen( sock, 256 );
  if ( status != 0 ) {
    if ( ( opts & OPT_VERBOSE ) != 0 )
      perror( "listen" );
  }
  if ( ::getsockname( sock, (struct sockaddr *) &addr, &addrlen ) == 0 ) {
    this->PeerData::init_peer( sock, (struct sockaddr *) &addr, k );
  }
  else {
    this->PeerData::init_peer( sock, p->ai_addr, k );
  }
  ::fcntl( sock, F_SETFL, O_NONBLOCK | ::fcntl( sock, F_GETFL ) );
  if ( (status = this->poll.add_sock( this )) < 0 ) {
    this->fd = -1;
fail:;
    if ( sock != -1 )
      ::close( sock );
  }
  if ( ai != NULL )
    ::freeaddrinfo( ai );
  return status;
}

void
EvTcpListen::set_sock_opts( EvPoll &poll,  int sock,  int opts )
{
  static int on = 1;
  if ( (opts & OPT_KEEPALIVE) != 0 ) {
    if ( ::setsockopt( sock, SOL_SOCKET, SO_KEEPALIVE, &on,
                       sizeof( on ) ) != 0 )
      if ( ( opts & OPT_VERBOSE ) != 0 )
        perror( "warning: SO_KEEPALIVE" );
    if ( poll.so_keepalive_ns > 0 ) {
      int keepcnt   = 3,/* quarters, so better if keepalive is divisible by 4 */
          keepidle  = ( poll.so_keepalive_ns + (uint64_t) 3999999999UL )
                                             / (uint64_t) 4000000000UL,
          keepintvl = keepidle;

      /* adjust to keep within keepalive range */
      while ( keepcnt > 1 ) {
        uint64_t ns = keepidle + keepintvl * ( keepcnt - 1 );
        ns *= 1000000000;
        if ( ns < poll.so_keepalive_ns )
          break;
        keepcnt--;
      }
      if ( ::setsockopt( sock, SOL_TCP, TCP_KEEPCNT, &keepcnt,
                         sizeof( keepcnt ) ) != 0 )
        if ( ( opts & OPT_VERBOSE ) != 0 )
          perror( "warning: TCP_KEEPCNT" );
      if ( ::setsockopt( sock, SOL_TCP, TCP_KEEPIDLE, &keepidle,
                         sizeof( keepidle ) ) != 0 )
        if ( ( opts & OPT_VERBOSE ) != 0 )
          perror( "warning: TCP_KEEPIDLE" );
      if ( ::setsockopt( sock, SOL_TCP, TCP_KEEPINTVL, &keepintvl,
                         sizeof( keepintvl ) ) != 0 )
        if ( ( opts & OPT_VERBOSE ) != 0 )
          perror( "warning: TCP_KEEPINTVL" );
    }
  }
  if ( (opts & OPT_LINGER) != 0 ) {
    struct linger lin;
    lin.l_onoff  = 1;
    lin.l_linger = 10; /* 10 secs */
    if ( ::setsockopt( sock, SOL_SOCKET, SO_LINGER, &lin, sizeof( lin ) ) != 0 )
      if ( ( opts & OPT_VERBOSE ) != 0 )
        perror( "warning: SO_LINGER" );
  }
  if ( (opts & OPT_TCP_NODELAY) != 0 ) {
    if ( ::setsockopt( sock, SOL_TCP, TCP_NODELAY, &on, sizeof( on ) ) != 0 )
      if ( ( opts & OPT_VERBOSE ) != 0 )
        perror( "warning: TCP_NODELAY" );
  }
}

int
EvTcpConnection::connect( EvConnection &conn,  const char *ip,  int port,
                          int opts ) noexcept
{
  /* for setsockopt() */
  static int  off = 0;
  int  status = 0,
       sock;
  char svc[ 16 ];
  struct addrinfo hints, * ai = NULL, * p = NULL;
  struct sockaddr_storage addr;
  socklen_t addrlen = sizeof( addr );

  conn.sock_opts = opts;
  ::snprintf( svc, sizeof( svc ), "%d", port );
  ::memset( &hints, 0, sizeof( struct addrinfo ) );
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  switch ( opts & ( OPT_AF_INET6 | OPT_AF_INET ) ) {
    case OPT_AF_INET:
      hints.ai_family = AF_INET;
      break;
    case OPT_AF_INET6:
      hints.ai_family = AF_INET6;
      break;
    default:
    case OPT_AF_INET | OPT_AF_INET6:
      hints.ai_family = AF_UNSPEC;
      break;
  }
  status = ::getaddrinfo( ip, svc, &hints, &ai );
  if ( status != 0 )
    return conn.set_sock_err( EV_ERR_GETADDRINFO, errno );

  sock = -1;
  /* try inet6 first, since it can listen to both ip stacks */
  for ( int fam = AF_INET6; ; fam = AF_INET ) {
    for ( p = ai; p != NULL; p = p->ai_next ) {
      if ( ( fam == AF_INET6 && ( opts & OPT_AF_INET6 ) != 0 ) ||
           ( fam == AF_INET  && ( opts & OPT_AF_INET ) != 0 ) ) {
        if ( fam == p->ai_family ) {
          sock = ::socket( p->ai_family, p->ai_socktype, p->ai_protocol );
          if ( sock < 0 )
            continue;
          if ( fam == AF_INET6 && ( opts & OPT_AF_INET ) != 0 ) {
            if ( ::setsockopt( sock, IPPROTO_IPV6, IPV6_V6ONLY, &off,
                               sizeof( off ) ) != 0 )
              if ( ( opts & OPT_VERBOSE ) != 0 )
                perror( "warning: IPV6_V6ONLY" );
          }
          EvTcpListen::set_sock_opts( conn.poll, sock, opts );
          conn.PeerData::set_addr( p->ai_addr );
          if ( ( opts & OPT_CONNECT_NB ) != 0 )
            ::fcntl( sock, F_SETFL, O_NONBLOCK | ::fcntl( sock, F_GETFL ) );
          status = ::connect( sock, p->ai_addr, p->ai_addrlen );
          if ( status == 0 )
            goto break_loop;
          if ( ( opts & OPT_CONNECT_NB ) != 0 && errno == EINPROGRESS ) {
            status = 0;
            goto break_loop;
          }
          ::close( sock );
          sock = -1;
        }
      }
    }
    if ( fam == AF_INET ) /* tried both */
      break;
  }
break_loop:;
  if ( status != 0 ) {
    status = conn.set_sock_err( EV_ERR_CONNECT, errno );
    goto fail;
  }
  if ( sock == -1 ) {
    status = conn.set_sock_err( EV_ERR_SOCKET, errno );
    goto fail;
  }
  if ( ::getpeername( sock, (struct sockaddr *) &addr, &addrlen ) == 0 ) {
    conn.PeerData::init_peer( sock, (struct sockaddr *) &addr, "tcp_client" );
  }
  else {
    conn.PeerData::init_peer( sock, p->ai_addr, "tcp_client" );
  }
  if ( ( opts & OPT_CONNECT_NB ) == 0 )
    ::fcntl( sock, F_SETFL, O_NONBLOCK | ::fcntl( sock, F_GETFL ) );

  if ( (status = conn.poll.add_sock( &conn )) < 0 ) {
    conn.fd = -1;
fail:;
    if ( sock != -1 )
      ::close( sock );
  }
  if ( ai != NULL )
    ::freeaddrinfo( ai );
  return status;
}

