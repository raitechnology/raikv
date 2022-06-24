#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#ifndef _MSC_VER
#include <sys/socket.h>
#else
#include <raikv/win.h>
#endif
#include <raikv/ev_net.h>
#include <raikv/ev_tcp.h>

using namespace rai;
using namespace kv;

struct UdpSvc : public EvUdp {
  void * operator new( size_t, void *ptr ) { return ptr; }
  UdpSvc( EvPoll &p ) : EvUdp( p, 0 ) {}
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual void process_shutdown( void ) noexcept;
  virtual void process_close( void ) noexcept;
};

struct UdpPing : public EvUdp {
  void * operator new( size_t, void *ptr ) { return ptr; }
  UdpPing( EvPoll &p ) : EvUdp( p, 0 ) {}
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
  uint32_t cnt = this->in_nmsgs - this->in_moff;
  this->out_mhdr = (mmsghdr *) this->alloc_temp( cnt * sizeof( mmsghdr ) );
  iovec  * iov = (iovec *) this->alloc_temp( cnt * sizeof( iovec ) );
  uint32_t k = 0;

  for ( uint32_t i = 0; i < cnt; i++ ) {
    uint32_t j = this->in_moff++;
    mmsghdr & ih = this->in_mhdr[ j ];
    if ( ih.msg_len > 0 && ih.msg_hdr.msg_iovlen == 1 ) {
      mmsghdr & oh = this->out_mhdr[ k ];
      oh.msg_hdr.msg_name       = ih.msg_hdr.msg_name; /* back to sender */
      oh.msg_hdr.msg_namelen    = ih.msg_hdr.msg_namelen;
      oh.msg_hdr.msg_iov        = &iov[ k ];
      oh.msg_hdr.msg_iovlen     = 1;
      oh.msg_hdr.msg_control    = NULL;
      oh.msg_hdr.msg_controllen = 0;
      oh.msg_hdr.msg_flags      = 0;
      oh.msg_len                = 0;
      iov[ k ].iov_base = ih.msg_hdr.msg_iov[ 0 ].iov_base;
      iov[ k ].iov_len  = ih.msg_len; /* send pkt that was recvd */
      k++;
    }
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
  printf( "release %.*s\n", (int) this->get_peer_address_strlen(),
          this->peer_address.buf );
  this->EvUdp::release_buffers();
}

void
UdpSvc::process_shutdown( void ) noexcept
{
  printf( "shutdown %.*s\n", (int) this->get_peer_address_strlen(),
          this->peer_address.buf );
  this->pushpop( EV_CLOSE, EV_SHUTDOWN );
}

void
UdpSvc::process_close( void ) noexcept
{
  printf( "close %.*s\n", (int) this->get_peer_address_strlen(),
          this->peer_address.buf );
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
    uint32_t j   = this->in_moff++;
    iovec  & iov = this->in_mhdr[ j ].msg_hdr.msg_iov[ 0 ];
    double   d;

    if ( iov.iov_len >= 5 + sizeof( d ) &&
         ::memcmp( iov.iov_base, "ping ", 5 ) == 0 ) {
      ::memcpy( &d, &((char *) iov.iov_base)[ 5 ], sizeof( d ) );
      printf( "latency: %.6f\n", current_monotonic_time_s() - d );
    }
  }
  this->pop( EV_PROCESS );
}

void
UdpPing::process_shutdown( void ) noexcept
{
  printf( "shutdown %.*s\n", (int) this->get_peer_address_strlen(),
          this->peer_address.buf );
  this->pushpop( EV_CLOSE, EV_SHUTDOWN );
}

void
UdpPing::release( void ) noexcept
{
  printf( "release %.*s\n", (int) this->get_peer_address_strlen(),
          this->peer_address.buf );
  this->EvUdp::release_buffers();
}

void
UdpPing::process_close( void ) noexcept
{
  printf( "close %.*s\n", (int) this->get_peer_address_strlen(),
          this->peer_address.buf );
  if ( this->poll.quit == 0 )
    this->poll.quit = 1;
}

void
UdpPing::send_ping( void ) noexcept
{
  double d = current_monotonic_time_s();
  this->out_mhdr = (mmsghdr *) this->alloc_temp( sizeof( mmsghdr ) );
  iovec * iov    = (iovec *) this->alloc_temp( sizeof( iovec ) );
  iov->iov_base  = this->append2( "ping ", 5, &d, sizeof( d ) );
  iov->iov_len   = 5 + sizeof( d );
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
  EvPoll  poll;
  UdpSvc  test( poll );
  UdpPing ping( poll );
  int idle_count = 0; 
  poll.init( 5, false );
  
  if ( argc > 1 && ::strcmp( argv[ 1 ], "-c" ) == 0 ) {
    if ( ping.connect( ( argc > 2 ? argv[ 2 ] : NULL ), 9000,
                       DEFAULT_UDP_CONNECT_OPTS, "udp_ping", -1 ) != 0 )
      return 1;
    ping.send_ping();
    poll.timer.add_timer_seconds( ping.fd, 1, 1, 1 );
  }
  else {
    if ( test.listen2( NULL, 9000, DEFAULT_UDP_LISTEN_OPTS, "udp_svc", -1 ) != 0 )
      return 1;
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

