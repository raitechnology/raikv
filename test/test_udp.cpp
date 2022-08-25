#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#ifndef _MSC_VER
#include <sys/socket.h>
#include <arpa/inet.h>
#else
#include <raikv/win.h>
#endif
#include <raikv/ev_net.h>
#include <raikv/ev_tcp.h>

using namespace rai;
using namespace kv;

struct McastPingRec {
  double   time;
  uint32_t s_addr;
  uint16_t sin_port;
};

struct UdpSvc : public EvUdp {
  void * operator new( size_t, void *ptr ) { return ptr; }
  const char *name;
  UdpSvc( EvPoll &p, const char *nm ) : EvUdp( p, 0 ), name( nm ) {}
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual void process_shutdown( void ) noexcept;
  virtual void process_close( void ) noexcept;
};

struct UdpPing : public EvUdp {
  void * operator new( size_t, void *ptr ) { return ptr; }
  const char *name;
  McastPingRec * reply;
  UdpPing( EvPoll &p, const char *nm ) : EvUdp( p, 0 ), name( nm ), reply( 0 ) {}
  void send_ping( void ) noexcept;
  virtual bool timer_expire( uint64_t, uint64_t ) noexcept;
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual void process_shutdown( void ) noexcept;
  virtual void process_close( void ) noexcept;
};

void
UdpSvc::process( void ) noexcept
{
  if ( this->in_moff >= this->in_nmsgs ) {
    this->pop( EV_PROCESS );
    return;
  }
  uint32_t cnt   = this->in_nmsgs - this->in_moff;
  this->out_mhdr = (mmsghdr *) this->alloc_temp( cnt * sizeof( mmsghdr ) );
  iovec  * iov   = (iovec *) this->alloc_temp( cnt * sizeof( iovec ) );
  uint32_t k     = 0;

  struct sockaddr_in * sa;
  void               * dest;
  int                  dest_len;
  void               * msg;
  McastPingRec         rec;
  double               d;

  for ( uint32_t i = 0; i < cnt; i++ ) {
    uint32_t  j = this->in_moff++;
    mmsghdr & ih = this->in_mhdr[ j ];
    if ( ih.msg_hdr.msg_iovlen != 1 ||
         ( ih.msg_len != 5 + sizeof( d ) &&
           ih.msg_len != 5 + sizeof( McastPingRec ) ) )
      continue;

    msg = ih.msg_hdr.msg_iov[ 0 ].iov_base;
    if ( ih.msg_len == 5 + sizeof( d ) ) {
      ::memcpy( &d, (char *) msg + 5, sizeof( d ) );
      dest     = ih.msg_hdr.msg_name; /* back to sender */
      dest_len = ih.msg_hdr.msg_namelen;
    }
    else {
      ::memcpy( &rec, (char *) msg + 5, sizeof( rec ) );
      sa       = (struct sockaddr_in *) this->alloc_temp( sizeof( *sa ) );
      dest     = sa;
      dest_len = sizeof( *sa );
      d        = rec.time;
      sa->sin_family      = AF_INET;
      sa->sin_addr.s_addr = rec.s_addr;
      sa->sin_port        = rec.sin_port;
    }
    mmsghdr & oh  = this->out_mhdr[ k ];
    oh.msg_hdr.msg_name       = dest;
    oh.msg_hdr.msg_namelen    = dest_len;
    oh.msg_hdr.msg_iov        = &iov[ k ];
    oh.msg_hdr.msg_iovlen     = 1;
    oh.msg_hdr.msg_control    = NULL;
    oh.msg_hdr.msg_controllen = 0;
    oh.msg_hdr.msg_flags      = 0;
    oh.msg_len                = 0;

    struct sockaddr_in *p = (struct sockaddr_in *) dest;
    if ( p != NULL && p->sin_family == AF_INET ) {
      char buf[ 256 ];
      inet_ntop( AF_INET, &p->sin_addr, buf, sizeof( buf ) );
      printf( "recv %s:%u\n", buf, ntohs( p->sin_port ) );
    }
    void * out = this->alloc_temp( 5 + sizeof( d ) );
    iov[ k ].iov_base = out;
    iov[ k ].iov_len  = 5 + sizeof( d );
    ::memcpy( out, "ping ", 5 );
    ::memcpy( (char *) out + 5, &d, sizeof( d ) );
    k++;
  }
  if ( k == 0 ) {
    this->pop( EV_PROCESS );
    return;
  }
  this->out_nmsgs = k;
  this->pushpop( EV_WRITE, EV_PROCESS );
}

void
UdpSvc::release( void ) noexcept
{
  printf( "%s release %.*s\n", this->name,
          (int) this->get_peer_address_strlen(), this->peer_address.buf );
  this->EvUdp::release_buffers();
}

void
UdpSvc::process_shutdown( void ) noexcept
{
  printf( "%s shutdown %.*s\n", this->name,
          (int) this->get_peer_address_strlen(), this->peer_address.buf );
  this->pushpop( EV_CLOSE, EV_SHUTDOWN );
}

void
UdpSvc::process_close( void ) noexcept
{
  printf( "%s close %.*s\n", this->name,
          (int) this->get_peer_address_strlen(), this->peer_address.buf );
  this->EvSocket::process_close();
}

void
UdpPing::process( void ) noexcept
{
  if ( this->in_moff >= this->in_nmsgs ) {
    this->pop( EV_PROCESS );
    return;
  }
  uint32_t cnt = this->in_nmsgs - this->in_moff;
  for ( uint32_t i = 0; i < cnt; i++ ) {
    uint32_t  j   = this->in_moff++;
    mmsghdr & ih  = this->in_mhdr[ j ];
    iovec   & iov = ih.msg_hdr.msg_iov[ 0 ];
    double    d;

    if ( ih.msg_len == 5 + sizeof( d ) &&
         ::memcmp( iov.iov_base, "ping ", 5 ) == 0 ) {
      ::memcpy( &d, &((char *) iov.iov_base)[ 5 ], sizeof( d ) );
      double lat = current_monotonic_time_s() - d;
      const char *units[ 4 ] = { "s", "ms", "us", "ns" };
      int i = 0;
      while ( lat < 1.0 && i < 3 ) {
        lat *= 1000.0;
        i++;
      }
      printf( "latency: %.3f%s\n", lat, units[ i ] );
    }
  }
  this->pop( EV_PROCESS );
  this->clear_buffers();
}

void
UdpPing::process_shutdown( void ) noexcept
{
  printf( "%s shutdown %.*s\n", this->name,
          (int) this->get_peer_address_strlen(), this->peer_address.buf );
  this->pushpop( EV_CLOSE, EV_SHUTDOWN );
}

void
UdpPing::release( void ) noexcept
{
  printf( "%s release %.*s\n", this->name,
          (int) this->get_peer_address_strlen(), this->peer_address.buf );
  this->EvUdp::release_buffers();
}

void
UdpPing::process_close( void ) noexcept
{
  printf( "%s close %.*s\n", this->name,
          (int) this->get_peer_address_strlen(), this->peer_address.buf );
  if ( this->poll.quit == 0 )
    this->poll.quit = 1;
  this->EvSocket::process_close();
}

void
UdpPing::send_ping( void ) noexcept
{
  this->out_mhdr = (mmsghdr *) this->alloc_temp( sizeof( mmsghdr ) );
  iovec * iov    = (iovec *) this->alloc_temp( sizeof( iovec ) );
  double d = current_monotonic_time_s();
  if ( this->reply == NULL ) {
    iov->iov_base  = this->append2( "ping ", 5, &d, sizeof( d ) );
    iov->iov_len   = 5 + sizeof( d );
  }
  else {
    this->reply->time = d;
    iov->iov_base  = this->append2( "ping ", 5, this->reply,
                                    sizeof( *this->reply ) );
    iov->iov_len   = 5 + sizeof( *this->reply );
  }
  mmsghdr & oh   = this->out_mhdr[ 0 ];
  oh.msg_hdr.msg_name       = NULL; /* sendto is connected */
  oh.msg_hdr.msg_namelen    = 0;
  oh.msg_hdr.msg_iov        = iov;
  oh.msg_hdr.msg_iovlen     = 1;
  oh.msg_hdr.msg_control    = NULL;
  oh.msg_hdr.msg_controllen = 0;
  oh.msg_hdr.msg_flags      = 0;
  oh.msg_len                = 0;
  this->out_nmsgs = 1;
  this->idle_push( EV_WRITE );
}

bool
UdpPing::timer_expire( uint64_t, uint64_t ) noexcept
{
  this->send_ping();
  return true;
}

int
main( int argc, char *argv[] )
{ 
  SignalHandler sighndl;
  EvPoll      poll;
  UdpSvc      svc( poll, "svc" );
  UdpPing     ping( poll, "ping" );
  UdpPing     ping_recv( poll, "ping_recv" );
  PeerAddrStr paddr;

  int idle_count = 0; 
  poll.init( 5, false );
  
  const bool is_client = ( argc > 1 && ::strcmp( argv[ 1 ], "-c" ) == 0 );
  const char *h = ( is_client ?  ( argc > 2 ? argv[ 2 ] : NULL ) : NULL );
  if ( is_client ) {
    if ( ping.connect( h, 9000, DEFAULT_UDP_CONNECT_OPTS,
                       "udp_ping", -1 ) != 0 )
      return 1;
    paddr.set_sock_addr( ping.fd );
    printf( "connect %s -> %s\n", paddr.buf, ping.peer_address.buf );
    if ( ping.mode == EvUdp::MCAST_CONNECT ) {
      if ( ping_recv.listen2( h, 0, DEFAULT_UDP_LISTEN_OPTS | OPT_UNICAST,
                              "udp_ping_recv", -1 ) != 0 )
        return 1;
      struct sockaddr_in sa;
      socklen_t len = sizeof( sa );
      if ( ::getsockname( ping_recv.fd, (struct sockaddr *) &sa, &len ) != 0 ) {
        perror( "ping_recv" );
        return 1;
      }

      ping.reply = (McastPingRec *) ::malloc( sizeof( McastPingRec ) );
      ping.reply->s_addr   = sa.sin_addr.s_addr;
      ping.reply->sin_port = sa.sin_port;

      paddr.set_sock_addr( ping_recv.fd );
      printf( "recv %s\n", paddr.buf );
    }
    ping.send_ping();
    poll.timer.add_timer_seconds( ping.fd, 1, 1, 1 );
  }
  else {
    h = ( argc > 1 && ::strcmp( argv[ 1 ], "-r" ) == 0 ?
          ( argc > 2 ? argv[ 2 ] : NULL ) : NULL );
    if ( svc.listen2( h, 9000, DEFAULT_UDP_LISTEN_OPTS, "udp_svc", -1 ) != 0 )
      return 1;
    printf( "recv %s\n", svc.peer_address.buf );
  }
  sighndl.install();
  for (;;) {
    /* loop 5 times before quiting, time to flush writes */
    if ( poll.quit >= 5 && idle_count > 0 )
      break;
    /* dispatch network events */
    int idle = poll.dispatch();
    if ( idle == EvPoll::DISPATCH_IDLE )
      idle_count++;
    else
      idle_count = 0;
    /* wait for network events */ 
    poll.wait( idle_count > 2 ? 100 : 0 );
    if ( sighndl.signaled )
      poll.quit++;
  }
  return 0;
}

