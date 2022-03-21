#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#ifndef _MSC_VER
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netdb.h>
#else
#include <raikv/win.h>
#endif
#include <errno.h>
#include <raikv/ev_tcp.h>

using namespace rai;
using namespace kv;

#ifndef _MSC_VER
typedef int SOCKET;
static const int INVALID_SOCKET = -1;

static inline bool invalid_socket( int fd ) { return fd < 0; }

static inline void set_nonblock( int fd ) {
  ::fcntl( fd, F_SETFL, O_NONBLOCK | ::fcntl( fd, F_GETFL ) );
}
static inline void closesocket( int fd ) {
  ::close( fd );
}
static inline void show_error( const char *msg ) {
  perror( msg );
}
static inline int get_errno( void ) {
  return errno;
}

static void
tcp_set_sock_opts( EvPoll &poll,  int sock,  int opts ) noexcept
{
  static int on = 1;
  if ( (opts & OPT_KEEPALIVE) != 0 ) {
    if ( ::setsockopt( sock, SOL_SOCKET, SO_KEEPALIVE, &on,
                       sizeof( on ) ) != 0 )
      if ( ( opts & OPT_VERBOSE ) != 0 )
        perror( "warning: SO_KEEPALIVE" );
    if ( poll.so_keepalive_ns > 0 ) {
      int keepcnt   = 3,/* quarters, so better if keepalive is divisible by 4 */
          keepidle  = (int) ( ( poll.so_keepalive_ns + (uint64_t) 3999999999UL )
                                                    / (uint64_t) 4000000000UL ),
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

static int
finish_init( int sock,  EvPoll &poll,  EvSocket &me,  struct sockaddr *addr,
             const char *k ) noexcept
{
  int status;
  set_nonblock( sock );
  me.PeerData::init_peer( sock, addr, k );
  if ( (status = poll.add_sock( &me )) != 0 )
    me.fd = -1;
  return status;
}

#else

static inline bool invalid_socket( SOCKET fd ) { return fd == INVALID_SOCKET; }
static inline void set_nonblock( SOCKET fd ) {
  u_long mode = 1;
  ioctlsocket( fd, FIONBIO, &mode );
}
static inline void show_error( const char *msg ) {
  err_map_win_error();
  perror( msg );
}
static inline int get_errno( void ) {
  err_map_win_error();
  return errno;
}

static void
tcp_set_sock_opts( EvPoll &poll,  SOCKET sock,  int opts ) noexcept
{
  static int on = 1;
  if ( (opts & OPT_KEEPALIVE) != 0 ) {
    if ( ::setsockopt( sock, SOL_SOCKET, SO_KEEPALIVE, (char *) &on,
                       sizeof( on ) ) != 0 )
      if ( ( opts & OPT_VERBOSE ) != 0 )
        show_error( "warning: SO_KEEPALIVE" );
    if ( poll.so_keepalive_ns > 0 ) {
      int keepcnt   = 3,/* quarters, so better if keepalive is divisible by 4 */
          keepidle  = (int) ( ( poll.so_keepalive_ns + (uint64_t) 3999999999UL )
                                                    / (uint64_t) 4000000000UL ),
          keepintvl = keepidle;

      /* adjust to keep within keepalive range */
      while ( keepcnt > 1 ) {
        uint64_t ns = keepidle + keepintvl * ( keepcnt - 1 );
        ns *= 1000000000;
        if ( ns < poll.so_keepalive_ns )
          break;
        keepcnt--;
      }
      struct tcp_keepalive alive;
      DWORD nbytes = 0;
      alive.onoff = TRUE;
      alive.keepalivetime = keepidle;
      alive.keepaliveinterval = keepintvl;
      /* keepcnt is 10 or 5, can't be changed */

      if ( WSAIoctl( sock, SIO_KEEPALIVE_VALS, &alive, sizeof( alive ),
                     NULL, 0, &nbytes, NULL, NULL ) != 0 )
        show_error( "warning: SIO_KEEPALIVE_VALS" );
    }
  }
  if ( (opts & OPT_LINGER) != 0 ) {
    struct linger lin;
    lin.l_onoff  = 1;
    lin.l_linger = 10; /* 10 secs */
    if ( ::setsockopt( sock, SOL_SOCKET, SO_LINGER, (char *) &lin,
                       sizeof( lin ) ) != 0 )
      if ( ( opts & OPT_VERBOSE ) != 0 )
        show_error( "warning: SO_LINGER" );
  }
  if ( (opts & OPT_TCP_NODELAY) != 0 ) {
    if ( ::setsockopt( sock, IPPROTO_TCP, TCP_NODELAY, (char *) &on,
                       sizeof( on ) ) != 0 )
      if ( ( opts & OPT_VERBOSE ) != 0 )
        show_error( "warning: TCP_NODELAY" );
  }
}

static int
finish_init( SOCKET sock,  EvPoll &poll,  EvSocket &me,  struct sockaddr *addr,
             const char *k ) noexcept
{
  int status, fd;
  set_nonblock( sock );
  fd = wp_register_fd( sock );
  me.PeerData::init_peer( fd, addr, k );
  if ( (status = poll.add_sock( &me )) != 0 ) {
    wp_unregister_fd( fd );
    me.fd = -1;
  }
  return status;
}
#endif

int
EvTcpListen::listen( const char *ip,  int port,  int opts ) noexcept
{
  return this->listen2( ip, port, opts, "tcp_listen" );
}


int
EvTcpListen::listen2( const char *ip,  int port,  int opts,
                      const char *k ) noexcept
{
  static int on = 1, off = 0;
  int    status = 0;
  SOCKET sock = INVALID_SOCKET;
  char   svc[ 16 ];
  struct addrinfo hints, * ai = NULL, * p = NULL;
  struct sockaddr_storage addr;
  socklen_t addrlen = sizeof( addr );
  struct sockaddr * peer_addr;

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
    return this->set_sock_err( EV_ERR_GETADDRINFO, get_errno() );

  /* try inet6 first, since it can listen to both ip stacks */
  for ( int fam = AF_INET6; ; fam = AF_INET ) {
    for ( p = ai; p != NULL; p = p->ai_next ) {
      if ( ( fam == AF_INET6 && ( opts & OPT_AF_INET6 ) != 0 ) ||
           ( fam == AF_INET  && ( opts & OPT_AF_INET ) != 0 ) ) {
        if ( fam == p->ai_family ) {
          sock = ::socket( p->ai_family, p->ai_socktype, p->ai_protocol );
          if ( invalid_socket( sock ) )
            continue;
          if ( fam == AF_INET6 && ( opts & OPT_AF_INET ) != 0 ) {
            if ( ::setsockopt( sock, IPPROTO_IPV6, IPV6_V6ONLY, (char *) &off,
                               sizeof( off ) ) != 0 )
              if ( ( opts & OPT_VERBOSE ) != 0 )
                show_error( "warning: IPV6_V6ONLY" );
          }
          if ( ( opts & OPT_REUSEADDR ) != 0 ) {
            if ( ::setsockopt( sock, SOL_SOCKET, SO_REUSEADDR, (char *) &on,
                               sizeof( on ) ) != 0 )
              if ( ( opts & OPT_VERBOSE ) != 0 )
                show_error( "warning: SO_REUSEADDR" );
          }
#ifdef SO_REUSEPORT
          if ( ( opts & OPT_REUSEPORT ) != 0 ) {
            if ( ::setsockopt( sock, SOL_SOCKET, SO_REUSEPORT, (char *) &on,
                               sizeof( on ) ) != 0 )
              if ( ( opts & OPT_VERBOSE ) != 0 )
                show_error( "warning: SO_REUSEPORT" );
          }
#endif
          this->PeerData::set_addr( p->ai_addr );
          status = ::bind( sock, p->ai_addr, (int) p->ai_addrlen );
          if ( status == 0 )
            goto break_loop;
          closesocket( sock );
          sock = INVALID_SOCKET;
        }
      }
    }
    if ( fam == AF_INET ) /* tried both */
      break;
  }
break_loop:;
  if ( status != 0 ) {
    status = this->set_sock_err( EV_ERR_BIND, get_errno() );
    goto fail;
  }
  if ( invalid_socket( sock ) ) {
    status = this->set_sock_err( EV_ERR_SOCKET, get_errno() );
    goto fail;
  }
  status = ::listen( sock, 256 );
  if ( status != 0 ) {
    if ( ( opts & OPT_VERBOSE ) != 0 )
      show_error( "listen" );
  }
  peer_addr = p->ai_addr;
  if ( ::getsockname( sock, (struct sockaddr *) &addr, &addrlen ) == 0 )
    peer_addr = (struct sockaddr *) &addr;

  status = finish_init( sock, this->poll, *this, peer_addr, k );
  if ( status != 0 ) {
fail:;
    if ( ! invalid_socket( sock ) )
      closesocket( sock );
  }
  if ( ai != NULL )
    ::freeaddrinfo( ai );
  return status;
}

bool
EvTcpListen::accept2( EvConnection &c,  const char *k ) noexcept
{
  struct sockaddr_storage addr;
  socklen_t addrlen = sizeof( addr );
  SOCKET    sock,
            listen_fd;
  int       status;

#ifdef _MSC_VER
  status = ::wp_get_socket( this->fd, &listen_fd );
  if ( status != 0 )
    goto fail;
#else
  listen_fd = this->fd;
#endif
  sock = ::accept( listen_fd, (struct sockaddr *) &addr, &addrlen );
  if ( invalid_socket( sock ) ) {
    if ( ! ev_would_block( get_errno() ) )
      show_error( "accept" );
    this->reset_read_poll();
    goto fail;
  }
  tcp_set_sock_opts( this->poll, sock, this->sock_opts );

  status = finish_init( sock, this->poll, c, (struct sockaddr *) &addr, k );
  if ( status != 0 ) {
    closesocket( sock );
    goto fail;
  }
  return true;
fail:;
  this->poll.push_free_list( &c );
  return false;
}

int
EvTcpConnection::connect( EvConnection &conn,  const char *ip,  int port,
                          int opts ) noexcept
{
  /* for setsockopt() */
  static const char k[] = "tcp_client";
  static int  off = 0;
  int    status = 0;
  SOCKET sock = INVALID_SOCKET;
  char   svc[ 16 ];
  struct addrinfo hints, * ai = NULL, * p = NULL;
  struct sockaddr_storage addr;
  socklen_t addrlen = sizeof( addr );
  struct sockaddr * peer_addr;

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
    return conn.set_sock_err( EV_ERR_GETADDRINFO, get_errno() );

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
            if ( ::setsockopt( sock, IPPROTO_IPV6, IPV6_V6ONLY, (char *) &off,
                               sizeof( off ) ) != 0 )
              if ( ( opts & OPT_VERBOSE ) != 0 )
                show_error( "warning: IPV6_V6ONLY" );
          }
          tcp_set_sock_opts( conn.poll, sock, opts );
          conn.PeerData::set_addr( p->ai_addr );
          if ( ( opts & OPT_CONNECT_NB ) != 0 )
            set_nonblock( sock );
          status = ::connect( sock, p->ai_addr, (int) p->ai_addrlen );
          if ( status == 0 )
            goto break_loop;
          if ( ( opts & OPT_CONNECT_NB ) != 0 &&
               ev_would_block( get_errno() ) ) {
            status = 0;
            goto break_loop;
          }
          closesocket( sock );
          sock = INVALID_SOCKET;
        }
      }
    }
    if ( fam == AF_INET ) /* tried both */
      break;
  }
break_loop:;
  if ( status != 0 ) {
    status = conn.set_sock_err( EV_ERR_CONNECT, get_errno() );
    goto fail;
  }
  if ( sock == -1 ) {
    status = conn.set_sock_err( EV_ERR_SOCKET, get_errno() );
    goto fail;
  }
  if ( ::getpeername( sock, (struct sockaddr *) &addr, &addrlen ) == 0 )
    peer_addr = (struct sockaddr *) &addr;
  else
    peer_addr = p->ai_addr;

  status = finish_init( sock, conn.poll, conn, peer_addr, k );
  if ( status != 0 ) {
fail:;
    if ( ! invalid_socket( sock ) )
      closesocket( sock );
  }
  if ( ai != NULL )
    ::freeaddrinfo( ai );
  return status;
}

