#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#ifndef _MSC_VER
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netdb.h>
#else
#include <raikv/win.h>
#endif
#include <errno.h>
#include <raikv/ev_net.h>

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

static int
finish_init( int sock,  EvPoll &poll,  EvSocket &me,  struct sockaddr *addr,
             const char *k,  uint32_t rte_id ) noexcept
{
  int status;
  set_nonblock( sock );
  me.PeerData::init_peer( sock, rte_id, addr, k );
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

static int
finish_init( SOCKET sock,  EvPoll &poll,  EvSocket &me,  struct sockaddr *addr,
             const char *k,  uint32_t rte_id ) noexcept
{
  int status, fd;
  set_nonblock( sock );
  fd = wp_register_fd( sock );
  me.PeerData::init_peer( fd, rte_id, addr, k );
  if ( (status = poll.add_sock( &me )) != 0 ) {
    wp_unregister_fd( fd );
    me.fd = -1;
  }
  return status;
}
#endif

int
EvUdp::listen2( const char *ip,  int port,  int opts,  const char *k,
                uint32_t rte_id ) noexcept
{
  static int on = 1, off = 0;
  int    status = 0;
  SOCKET sock = INVALID_SOCKET;
  char   svc[ 16 ];
  struct addrinfo hints, * ai = NULL, * p = NULL;

  this->sock_opts = opts;
  ::snprintf( svc, sizeof( svc ), "%d", port );
  ::memset( &hints, 0, sizeof( struct addrinfo ) );
  hints.ai_socktype = SOCK_DGRAM;
  hints.ai_protocol = IPPROTO_UDP;
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
          sock = ::socket( p->ai_family, SOCK_DGRAM, IPPROTO_UDP );
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
            if ( ::setsockopt( sock, SOL_SOCKET, SO_REUSEPORT, &on,
                               sizeof( on ) ) != 0 )
              if ( ( opts & OPT_VERBOSE ) != 0 )
                show_error( "warning: SO_REUSEPORT" );
          }
#endif
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
  status = finish_init( sock, this->poll, *this, p->ai_addr, k, rte_id );
  if ( status != 0 ) {
fail:;
    if ( ! invalid_socket( sock ) )
      closesocket( sock );
  }
  if ( ai != NULL )
    ::freeaddrinfo( ai );
  return status;
}

int
EvUdp::connect( const char *ip,  int port,  int opts,  const char *k,
                uint32_t rte_id ) noexcept
{
  static int off = 0;
  int    status = 0;
  SOCKET sock = INVALID_SOCKET;
  char   svc[ 16 ];
  struct addrinfo hints, * ai = NULL, * p = NULL;

  this->sock_opts = opts;
  ::snprintf( svc, sizeof( svc ), "%d", port );
  ::memset( &hints, 0, sizeof( struct addrinfo ) );
  hints.ai_socktype = SOCK_DGRAM;
  hints.ai_protocol = IPPROTO_UDP;
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
          sock = ::socket( p->ai_family, SOCK_DGRAM, IPPROTO_UDP );
          if ( sock < 0 )
            continue;
          if ( fam == AF_INET6 && ( opts & OPT_AF_INET ) != 0 ) {
            if ( ::setsockopt( sock, IPPROTO_IPV6, IPV6_V6ONLY, (char *) &off,
                               sizeof( off ) ) != 0 )
              if ( ( opts & OPT_VERBOSE ) != 0 )
                show_error( "warning: IPV6_V6ONLY" );
          }
          this->PeerData::set_addr( p->ai_addr );
          status = ::connect( sock, p->ai_addr, (int) p->ai_addrlen );
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
    status = this->set_sock_err( EV_ERR_CONNECT, get_errno() );
    goto fail;
  }
  if ( invalid_socket( sock ) ) {
    status = this->set_sock_err( EV_ERR_SOCKET, get_errno() );
    goto fail;
  }
  status = finish_init( sock, this->poll, *this, p->ai_addr, k, rte_id );
  if ( status != 0 ) {
fail:;
    if ( ! invalid_socket( sock ) )
      closesocket( sock );
  }
  if ( ai != NULL )
    ::freeaddrinfo( ai );
  return status;
}

