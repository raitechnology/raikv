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

/* split dev;mcast-ip */
static bool
split_mcast( const char *ip, const char *&dev_ip, const char *&mcast_ip,
             char *mcast_buf,  size_t buflen,  bool &is_hostname ) noexcept
{
  const char *p;
  if ( (p = ::strchr( ip, ';' )) == NULL )
    return false;
  size_t len = p - ip;
  if ( len >= buflen )
    return false;
  if ( len == 0 ) { /* use hostname */
    is_hostname = true;
    if ( ::gethostname( mcast_buf, buflen ) != 0 )
      return false;
    len = ::strlen( mcast_buf );
  }
  else {
    is_hostname = false;
    ::memcpy( mcast_buf, ip, len );
    mcast_buf[ len ] = '\0';
  }
  dev_ip = mcast_buf;
  mcast_buf = &mcast_buf[ len + 1 ];
  buflen -= len + 1;
  len = ::strlen( p + 1 );
  if ( len >= buflen )
    return false;
  ::memcpy( mcast_buf, p + 1, len );
  mcast_buf[ len ] = '\0';
  mcast_ip = mcast_buf;
  return true;
}

namespace {
struct UdpData {
  EvUdp      & udp;
  const char * ip;
  AddrInfo     info;
  struct sockaddr * ai_addr;
  SOCKET sock;
  int    port, opts, status;
  bool   is_connect;
  char   mcast_buf[ 256 ];

  UdpData( EvUdp &udp_sock,  const char *ipaddr,  int p,
           int sock_opts,  bool conn )
    : udp( udp_sock ), ip( ipaddr ), ai_addr( 0 ),
      sock( INVALID_SOCKET ), port( p ), opts( sock_opts ), status( 0 ),
      is_connect( conn ) {}

  bool multicast_setup( void ) noexcept;
  bool unicast_setup( void ) noexcept;
};
}

bool
UdpData::multicast_setup( void ) noexcept
{
  const char * dev_ip, *mcast_ip;
  struct addrinfo * p = NULL;
  bool is_hostname = false;

  if ( this->ip == NULL ||
       ! split_mcast( this->ip, dev_ip, mcast_ip, this->mcast_buf,
                      sizeof( this->mcast_buf ), is_hostname ) )
    return false;

  int op = this->opts | OPT_UDP;
  if ( is_hostname ) /* allow gethostname() resolution */
    op &= ~OPT_NO_DNS;
  if ( this->is_connect )
    op &= ~OPT_LISTEN;
  else
    op |= OPT_LISTEN;
  this->status = this->info.get_address( dev_ip, port, op );

  if ( this->status == 0 ) {
    for ( p = this->info.ai; p != NULL; p = p->ai_next )
      if ( p->ai_family == AF_INET )
        break;
  }
  if ( p == NULL ) {
    this->status = this->udp.set_sock_err( EV_ERR_GETADDRINFO, get_errno() );
    if ( this->status == 0 )
      this->status = -2;
    return true;
  }
  uint32_t if_addr = ((struct sockaddr_in *) p->ai_addr)->sin_addr.s_addr;
  struct ip_mreq mr;
  ::memset( &mr, 0, sizeof( mr ) );
  mr.imr_interface.s_addr = if_addr;

  this->sock = ::socket( AF_INET, SOCK_DGRAM, IPPROTO_UDP );
  if ( invalid_socket( this->sock ) )
    return true;
  this->status = ::setsockopt( this->sock, IPPROTO_IP, IP_MULTICAST_IF, (void *)
                               &mr.imr_interface, sizeof( mr.imr_interface ) );
  if ( this->status != 0 ) {
    this->status = this->udp.set_sock_err( EV_ERR_MULTI_IF, get_errno() );
    return true;
  }
  op |= OPT_NO_DNS;
  this->status = this->info.get_address( mcast_ip, port, op );
  if ( this->status != 0 ) {
    this->status = this->udp.set_sock_err( EV_ERR_GETADDRINFO, get_errno() );
    return true;
  }
  for ( p = this->info.ai; p != NULL; p = p->ai_next )
    if ( p->ai_family == AF_INET )
      break;
  if ( p == NULL ) {
    if ( ( op & OPT_VERBOSE ) != 0 )
      show_error( "no address matches mcast" );
    this->status = -3;
    return true;
  }
  this->udp.PeerData::set_addr( p->ai_addr );
  this->ai_addr = p->ai_addr;

  struct sockaddr_in sa;
  if ( this->is_connect ) {
    ::memset( &sa, 0, sizeof( sa ) );
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = if_addr;
    sa.sin_port = 0;
    this->status = bind_socket( this->sock, AF_INET, op,
                                (struct sockaddr *) &sa, sizeof( sa ) );
    if ( this->status != 0 ) {
      this->status = this->udp.set_sock_err( EV_ERR_BIND, get_errno() );
      return true;
    }
    status = ::connect( this->sock, p->ai_addr, (int) p->ai_addrlen );
    if ( this->status != 0  ) {
      this->status = this->udp.set_sock_err( EV_ERR_CONNECT, get_errno() );
      return true;
    }
    this->udp.mode = EvUdp::MCAST_CONNECT;
  }
  else {
    uint32_t mc_addr = ((struct sockaddr_in *) p->ai_addr)->sin_addr.s_addr;
    ::memset( &sa, 0, sizeof( sa ) );
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = mc_addr;
    sa.sin_port = htons( this->port );
    this->status = bind_socket( this->sock, AF_INET, op,
                                (struct sockaddr *) &sa, sizeof( sa ) );
    if ( this->status != 0 ) {
      this->status = this->udp.set_sock_err( EV_ERR_BIND, get_errno() );
      return true;
    }
    mr.imr_multiaddr.s_addr = mc_addr;
    this->status = ::setsockopt( this->sock, IPPROTO_IP, IP_ADD_MEMBERSHIP,
                                 (void *) &mr, sizeof( mr ) );
    if ( this->status != 0 ) {
      this->status = this->udp.set_sock_err( EV_ERR_ADD_MCAST, get_errno() );
      return true;
    }
    this->udp.mode = EvUdp::MCAST_LISTEN;
  }
  return true;
}

bool
UdpData::unicast_setup( void ) noexcept
{
  struct addrinfo * p = NULL;
  const char * dev_ip, * mcast_ip;
  bool is_hostname = false;

  if ( this->ip == NULL )
    dev_ip = NULL;
  else if ( ! split_mcast( this->ip, dev_ip, mcast_ip, this->mcast_buf,
                           sizeof( this->mcast_buf ), is_hostname ) )
    dev_ip = this->ip;

  int op = this->opts | OPT_UDP;
  if ( is_hostname ) /* allow gethostname() resolution */
    op &= ~OPT_NO_DNS;
  if ( this->is_connect )
    op &= ~OPT_LISTEN;
  else
    op |= OPT_LISTEN;
  this->status = this->info.get_address( dev_ip, this->port, op );
  if ( this->status != 0 ) {
    this->status = this->udp.set_sock_err( EV_ERR_GETADDRINFO, get_errno() );
    return false;
  }
  /* try inet6 first, since it can listen to both ip stacks */
  for ( int fam = AF_INET6; ; fam = AF_INET ) {
    for ( p = this->info.ai; p != NULL; p = p->ai_next ) {
      if ( ( fam == AF_INET6 && ( this->opts & OPT_AF_INET6 ) != 0 ) ||
           ( fam == AF_INET  && ( this->opts & OPT_AF_INET ) != 0 ) ) {
        if ( fam == p->ai_family ) {
          if ( ! invalid_socket( this->sock ) ) {
            closesocket( this->sock );
            this->sock = INVALID_SOCKET;
          }
          this->sock = ::socket( p->ai_family, SOCK_DGRAM, IPPROTO_UDP );
          if ( sock < 0 )
            continue;
          static int off = 0;
          if ( fam == AF_INET6 && ( this->opts & OPT_AF_INET ) != 0 ) {
            if ( ::setsockopt( this->sock, IPPROTO_IPV6, IPV6_V6ONLY,
                               (char *) &off, sizeof( off ) ) != 0 )
              if ( ( this->opts & OPT_VERBOSE ) != 0 )
                show_error( "warning: IPV6_V6ONLY" );
          }
          this->udp.PeerData::set_addr( p->ai_addr );
          this->ai_addr = p->ai_addr;

          if ( this->is_connect ) {
            this->status = ::connect( this->sock, p->ai_addr,
                                      (int) p->ai_addrlen );
            if ( this->status == 0 )
              return true;
          }
          else {
            this->status = bind_socket( this->sock, fam, op,
                                        p->ai_addr, p->ai_addrlen );
            if ( this->status == 0 )
              return true;
          }
        }
      }
    }
    if ( fam == AF_INET ) /* tried both */
      break;
  }
  if ( this->status != 0 ) {
    if ( this->is_connect )
      this->status = this->udp.set_sock_err( EV_ERR_CONNECT, get_errno() );
    else
      this->status = this->udp.set_sock_err( EV_ERR_BIND, get_errno() );
  }
  return true;
}

int
EvUdp::listen2( const char *ip,  int port,  int opts,  const char *k,
                uint32_t rte_id ) noexcept
{
  this->sock_opts = opts;
  this->mode      = UNICAST;

  UdpData data( *this, ip, port, opts, false );

  bool is_mcast = false;
  if ( ( opts & OPT_UNICAST ) == 0 )
    is_mcast = data.multicast_setup();
  if ( ! is_mcast ) {
    if ( ! data.unicast_setup() )
      return data.status;
  }

  int    status = data.status;
  SOCKET sock   = data.sock;

  if ( status == 0 ) {
    if ( invalid_socket( sock ) ) {
      status = this->set_sock_err( EV_ERR_SOCKET, get_errno() );
      goto fail;
    }
  }
  status = finish_init( sock, this->poll, *this, data.ai_addr, k, rte_id );
  if ( status != 0 ) {
fail:;
    if ( ! invalid_socket( sock ) )
      closesocket( sock );
  }
  return status;
}

int
EvUdp::connect( const char *ip,  int port,  int opts,  const char *k,
                uint32_t rte_id ) noexcept
{
  this->sock_opts = opts;
  this->mode      = UNICAST;

  UdpData data( *this, ip, port, opts, true );

  bool is_mcast = false;
  if ( ( opts & OPT_UNICAST ) == 0 )
    is_mcast = data.multicast_setup();
  if ( ! is_mcast ) {
    if ( ! data.unicast_setup() )
      return data.status;
  }

  int    status = data.status;
  SOCKET sock   = data.sock;

  if ( status == 0 ) {
    if ( invalid_socket( sock ) ) {
      status = this->set_sock_err( EV_ERR_SOCKET, get_errno() );
      goto fail;
    }
  }
  status = finish_init( sock, this->poll, *this, data.ai_addr, k, rte_id );
  if ( status != 0 ) {
fail:;
    if ( ! invalid_socket( sock ) )
      closesocket( sock );
  }
  return status;
}

