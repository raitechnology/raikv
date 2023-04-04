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
#include <sys/ioctl.h>
#include <linux/if.h>
#include <arpa/inet.h>
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
             const char *k,  uint32_t rte_id ) noexcept
{
  int status;
  set_nonblock( sock );
  me.PeerData::init_peer( poll.get_next_id(), sock, rte_id, addr, k );
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
             const char *k,  uint32_t rte_id ) noexcept
{
  int status, fd;
  set_nonblock( sock );
  fd = wp_register_fd( sock );
  me.PeerData::init_peer( poll.get_next_id(), fd, rte_id, addr, k );
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
  return this->listen2( ip, port, opts, "tcp_listen", -1 );
}

int
rai::kv::bind_socket( int sock,  int fam,  int opts,  struct sockaddr *ai_addr,
                      int ai_addrlen ) noexcept
{
  static int on = 1, off = 0;
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
  return ::bind( sock, ai_addr, ai_addrlen );
}

uint32_t
rai::kv::fix_ip4_address( uint32_t ipaddr ) noexcept
{
#ifndef _MSC_VER
  ifconf conf;
  ifreq  ifbuf[ 256 ],
       * ifp, ifa, ifm;
  int    s  = ::socket( PF_INET, SOCK_DGRAM, IPPROTO_UDP );
  if ( invalid_socket( s ) )
    return ipaddr;
  ::memset( ifbuf, 0, sizeof( ifbuf ) );
  ::memset( &conf, 0, sizeof( conf ) );

  conf.ifc_len = sizeof( ifbuf );
  conf.ifc_buf = (char *) ifbuf;

  if ( ::ioctl( s, SIOCGIFCONF, &conf ) != -1 ) {
    ifp = ifbuf;
    /* for each interface */
    for ( ; (uint8_t *) ifp < &((uint8_t *) ifbuf)[ conf.ifc_len ]; ifp++ ) {
      ::strcpy( ifa.ifr_name, ifp->ifr_name );
      ::strcpy( ifm.ifr_name, ifp->ifr_name );

      /* fetch flags check if multicast exists, get address and netmask */
      if ( ::ioctl( s, SIOCGIFADDR, &ifa )    >= 0 &&
           ifa.ifr_addr.sa_family             == AF_INET &&
           ::ioctl( s, SIOCGIFNETMASK, &ifm ) >= 0 ) {
        uint32_t mask, addr;
        mask = ((struct sockaddr_in &) ifm.ifr_netmask).sin_addr.s_addr;
        addr = ((struct sockaddr_in &) ifa.ifr_addr).sin_addr.s_addr;

        if ( ( addr & mask ) == ( ipaddr & mask ) ) {
          if ( ( ipaddr & ~mask ) == 0 )
            ipaddr = addr;
          break;
        }
      }
    }
  }
  ::close( s );
#else
  DWORD  nbytes;
  char   buf_out[ 64 * 1024 ];
  char   buf_in[ 64 * 1024 ];
  SOCKET s = socket( AF_INET, SOCK_DGRAM, IPPROTO_UDP );
  if ( invalid_socket( s ) )
    return ipaddr;

  if ( ::WSAIoctl( s, SIO_GET_INTERFACE_LIST,
                   buf_in, sizeof( buf_in ),
                   buf_out, sizeof( buf_out ),
                   &nbytes, NULL, NULL ) != SOCKET_ERROR ) {
    LPINTERFACE_INFO info;

    if ( nbytes != 0 ) {
      for ( info = (INTERFACE_INFO *) buf_out;
            info < (INTERFACE_INFO *) &buf_out[ nbytes ];
            info++ ) {
        if ( ( info->iiFlags & IFF_UP ) != 0 &&
             /*( info->iiFlags & IFF_MULTICAST ) != 0 &&*/
             ((struct sockaddr_in &) info->iiAddress).sin_family == AF_INET ) {
          uint32_t mask, addr;
          mask = ((struct sockaddr_in &) info->iiNetmask).sin_addr.s_addr;
          addr = ((struct sockaddr_in &) info->iiAddress).sin_addr.s_addr;

          if ( ( addr & mask ) == ( ipaddr & mask ) ) {
            if ( ( ipaddr & ~mask ) == 0 )
              ipaddr = addr;
            break;
          }
        }
      }
    }
  }
  ::closesocket( s );
#endif
  return ipaddr;
}

const char *
rai::kv::fix_ip4_device( const char *dev, char *ipbuf ) noexcept
{
  if ( dev == NULL )
    return NULL;
#ifndef _MSC_VER
  ifreq ifa;
  int   s  = ::socket( PF_INET, SOCK_DGRAM, IPPROTO_UDP );
  if ( ::strlen( dev ) < sizeof( ifa.ifr_name ) ) {
    ::strcpy( ifa.ifr_name, dev );
    if ( ::ioctl( s, SIOCGIFADDR, &ifa ) >= 0 &&
         ifa.ifr_addr.sa_family          == AF_INET ) {
      inet_ntop( AF_INET,
        &((struct sockaddr_in &) ifa.ifr_addr).sin_addr, ipbuf, 32 );
      ::close( s );
      return ipbuf;
    }
  }
  ::close( s );
#endif
  return dev;
}

int
AddrInfo::get_address( const char *ip,  int port,  int opts ) noexcept
{
  struct addrinfo hints;
  char   svc[ 16 ], ipbuf[ 32 ];
  int    status = 0;

  if ( this->ai != NULL ) {
    ::freeaddrinfo( this->ai );
    this->ai = NULL;
  }
  ::snprintf( svc, sizeof( svc ), "%d", port );
  ::memset( &hints, 0, sizeof( struct addrinfo ) );
  if ( ( opts & OPT_UDP ) == 0 ) {
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
  }
  else {
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_protocol = IPPROTO_UDP;
  }
  if ( ( opts & OPT_LISTEN ) != 0 )
    hints.ai_flags = AI_PASSIVE;
  if ( ( opts & OPT_NO_DNS ) != 0 )
    hints.ai_flags |= AI_NUMERICHOST;
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
  ip = fix_ip4_device( ip, ipbuf );
  status = ::getaddrinfo( ip, svc, &hints, &this->ai );
  if ( status != 0 )
    return status;
  if ( this->ai != NULL && this->ai->ai_next == NULL &&
       this->ai->ai_family == AF_INET ) {
    uint32_t & a = ((struct sockaddr_in *) this->ai->ai_addr)->sin_addr.s_addr;
    a = fix_ip4_address( a );
  }
  return 0;
}

AddrInfo::~AddrInfo() noexcept
{
  if ( this->ai != NULL ) {
    ::freeaddrinfo( this->ai );
    this->ai = NULL;
  }
}

int
EvTcpListen::listen2( const char *ip,  int port,  int opts,
                      const char *k,  uint32_t rte_id ) noexcept
{
  int    status = 0;
  SOCKET sock = INVALID_SOCKET;
  AddrInfo info;
  struct addrinfo * p = NULL;
  struct sockaddr_storage addr;
  socklen_t addrlen = sizeof( addr );
  struct sockaddr * peer_addr;

  this->sock_opts = opts;
  status = info.get_address( ip, port, opts | OPT_LISTEN );
  if ( status != 0 )
    return this->set_sock_err( EV_ERR_GETADDRINFO, get_errno() );
  /* try inet6 first, since it can listen to both ip stacks */
  for ( int fam = AF_INET6; ; fam = AF_INET ) {
    for ( p = info.ai; p != NULL; p = p->ai_next ) {
      if ( ( fam == AF_INET6 && ( opts & OPT_AF_INET6 ) != 0 ) ||
           ( fam == AF_INET  && ( opts & OPT_AF_INET ) != 0 ) ) {
        if ( fam == p->ai_family ) {
          sock = ::socket( p->ai_family, p->ai_socktype, p->ai_protocol );
          if ( invalid_socket( sock ) )
            continue;
          this->PeerData::set_addr( p->ai_addr );
          status = bind_socket( sock, fam, opts, p->ai_addr, p->ai_addrlen );
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

  status = finish_init( sock, this->poll, *this, peer_addr, k, rte_id );
  if ( status != 0 ) {
fail:;
    if ( ! invalid_socket( sock ) )
      closesocket( sock );
  }
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

  status = finish_init( sock, this->poll, c, (struct sockaddr *) &addr,
                        k, this->route_id );
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
  static const char k[] = "tcp_client";
  return EvTcpConnection::connect2( conn, ip, port, opts, k, -1 );
}

int
EvTcpConnection::connect2( EvConnection &conn,  const char *ip,  int port,
                           int opts,  const char *k,  uint32_t rte_id ) noexcept
{
  AddrInfo info;
  int status = info.get_address( ip, port, opts );
  if ( status != 0 )
    return conn.set_sock_err( EV_ERR_GETADDRINFO, get_errno() );
  return connect3( conn, info.ai, opts, k, rte_id );
}

int
EvTcpConnection::connect3( EvConnection &conn, const struct addrinfo *addr_list,
                           int opts,  const char *k,  uint32_t rte_id ) noexcept
{
  /* for setsockopt() */
  static int  off = 0;
  int      status = 0;
  SOCKET   sock = INVALID_SOCKET;
  const struct addrinfo * p = NULL;
  struct sockaddr_storage addr;
  socklen_t addrlen = sizeof( addr );
  struct sockaddr * peer_addr;

  conn.sock_opts = opts;
  /* try inet6 first, since it can listen to both ip stacks */
  for ( p = addr_list; p != NULL; p = p->ai_next ) {
    int fam = p->ai_family;
    if ( ( fam == AF_INET6 && ( opts & OPT_AF_INET6 ) != 0 ) ||
         ( fam == AF_INET  && ( opts & OPT_AF_INET ) != 0 ) ) {
      sock = ::socket( fam, p->ai_socktype, p->ai_protocol );
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

  status = finish_init( sock, conn.poll, conn, peer_addr, k, rte_id );
  if ( status != 0 ) {
fail:;
    if ( ! invalid_socket( sock ) )
      closesocket( sock );
  }
  return status;
}

