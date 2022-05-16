#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <raikv/ev_net.h>
#include <raikv/ev_unix.h>

using namespace rai;
using namespace kv;

struct UnixSvc : public EvUnixDgram {
  void * operator new( size_t, void *ptr ) { return ptr; }
  UnixSvc( EvPoll &p ) : EvUnixDgram( p, 0 ) {}
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual void process_shutdown( void ) noexcept;
  virtual void process_close( void ) noexcept;
};

struct UnixPing : public EvUnixDgram {
  void * operator new( size_t, void *ptr ) { return ptr; }
  UnixPing( EvPoll &p ) : EvUnixDgram( p, 0 ) {}
  void send_ping( void ) noexcept;
  virtual bool timer_expire( uint64_t, uint64_t ) noexcept;
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual void process_shutdown( void ) noexcept;
  virtual void process_close( void ) noexcept;
};

void
UnixSvc::process( void ) noexcept
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
UnixSvc::release( void ) noexcept
{
  printf( "release %.*s\n", (int) this->get_peer_address_strlen(),
          this->peer_address.buf );
  this->EvDgram::release_buffers();
}

void
UnixSvc::process_shutdown( void ) noexcept
{
  printf( "shutdown %.*s\n", (int) this->get_peer_address_strlen(),
          this->peer_address.buf );
  this->pushpop( EV_CLOSE, EV_SHUTDOWN );
}

void
UnixSvc::process_close( void ) noexcept
{
  printf( "close %.*s\n", (int) this->get_peer_address_strlen(),
          this->peer_address.buf );
}

void
UnixPing::process( void ) noexcept
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
UnixPing::process_shutdown( void ) noexcept
{
  printf( "shutdown %.*s\n", (int) this->get_peer_address_strlen(),
          this->peer_address.buf );
  this->pushpop( EV_CLOSE, EV_SHUTDOWN );
}

void
UnixPing::release( void ) noexcept
{
  printf( "release %.*s\n", (int) this->get_peer_address_strlen(),
          this->peer_address.buf );
  this->EvDgram::release_buffers();
}

void
UnixPing::process_close( void ) noexcept
{
  printf( "close %.*s\n", (int) this->get_peer_address_strlen(),
          this->peer_address.buf );
  if ( this->poll.quit == 0 )
    this->poll.quit = 1;
}

void
UnixPing::send_ping( void ) noexcept
{
  double d = current_monotonic_time_s();
  struct sockaddr_un * sunaddr;
  this->out_mhdr = (mmsghdr *) this->alloc_temp( sizeof( mmsghdr ) );
  sunaddr = (struct sockaddr_un *)
            this->alloc_temp( sizeof( struct sockaddr_un ) );
  sunaddr->sun_family = AF_LOCAL;
  ::strcpy( sunaddr->sun_path, "/tmp/ping_svc" );
  iovec * iov    = (iovec *) this->alloc_temp( sizeof( iovec ) );
  iov->iov_base  = this->append2( "ping ", 5, &d, sizeof( d ) );
  iov->iov_len   = 5 + sizeof( d );
  mmsghdr & oh   = this->out_mhdr[ 0 ];
  oh.msg_hdr.msg_name       = sunaddr;
  oh.msg_hdr.msg_namelen    = sizeof( struct sockaddr_un );
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
UnixPing::timer_expire( uint64_t, uint64_t ) noexcept
{
  this->send_ping();
  return true;
}

int
main( int argc, char *argv[] )
{ 
  SignalHandler sighndl;
  EvPoll   poll;
  UnixSvc  test( poll );
  UnixPing ping( poll );
  int idle_count = 0; 
  poll.init( 5, false );
  
  if ( argc > 1 && ::strcmp( argv[ 1 ], "-c" ) == 0 ) {
    if ( ping.bind( "/tmp/ping_clnt", DEFAULT_UNIX_BIND_OPTS, "unix_ping" ) !=0)
      return 1;
    ping.send_ping();
    poll.timer.add_timer_seconds( ping.fd, 1, 1, 1 );
  }
  else {
    if ( test.bind( "/tmp/ping_svc", DEFAULT_UNIX_BIND_OPTS, "unix_svc" ) != 0 )
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

