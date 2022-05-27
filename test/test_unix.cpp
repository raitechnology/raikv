#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/ev_net.h>
#include <raikv/ev_unix.h>

using namespace rai;
using namespace kv;

struct UnixListen : public EvUnixListen {
  UnixListen( EvPoll &p ) noexcept;

  virtual EvSocket *accept( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual void process_close( void ) noexcept;
};

struct UnixConn : public EvConnection {
  void * operator new( size_t, void *ptr ) { return ptr; }
  UnixConn( EvPoll &p,  uint8_t st ) : EvConnection( p, st ) {}
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual void process_shutdown( void ) noexcept;
  virtual void process_close( void ) noexcept;
};

struct UnixPing : public EvConnection {
  void * operator new( size_t, void *ptr ) { return ptr; }
  UnixPing( EvPoll &p ) : EvConnection( p, 0 ) {}
  void send_ping( void ) noexcept;
  virtual bool timer_expire( uint64_t, uint64_t ) noexcept;
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual void process_shutdown( void ) noexcept;
  virtual void process_close( void ) noexcept;
};

UnixListen::UnixListen( EvPoll &p ) noexcept
         : EvUnixListen( p, "unix_listen", "unix_conn" ) {}

EvSocket *
UnixListen::accept( void ) noexcept
{
  UnixConn *c = this->poll.get_free_list<UnixConn>( this->accept_sock_type );
  if ( c == NULL )
    return NULL;
  if ( this->accept2( *c, "unix_accept" ) ) {
    printf( "accept %.*s\n", (int) c->get_peer_address_strlen(),
            c->peer_address.buf );
    return c;
  }
  return NULL;
}

void
UnixListen::release( void ) noexcept
{
  printf( "listen release\n" );
}

void
UnixListen::process_close( void ) noexcept
{
  printf( "listen close\n" );
}

void
UnixConn::process( void ) noexcept
{
  if ( this->off < this->len ) {
    this->append( &this->recv[ this->off ], ( this->len - this->off ) );
    this->off = this->len;
  }
  this->pop( EV_PROCESS );
  this->push_write();
}

void
UnixConn::release( void ) noexcept
{
  printf( "release %.*s\n", (int) this->get_peer_address_strlen(),
          this->peer_address.buf );
  this->EvConnection::release_buffers();
}

void
UnixConn::process_shutdown( void ) noexcept
{
  printf( "shutdown %.*s\n", (int) this->get_peer_address_strlen(),
          this->peer_address.buf );
  this->pushpop( EV_CLOSE, EV_SHUTDOWN );
}

void
UnixConn::process_close( void ) noexcept
{
  printf( "close %.*s\n", (int) this->get_peer_address_strlen(),
          this->peer_address.buf );
}

void
UnixPing::process( void ) noexcept
{
  for (;;) {
    double d;
    size_t buflen = this->len - this->off;
    if ( buflen >= 5 + sizeof( d ) &&
         ::memcmp( &this->recv[ this->off ], "ping ", 5 ) == 0 ) {
      ::memcpy( &d, &this->recv[ this->off + 5 ], sizeof( d ) );
      printf( "latency: %.6f\n", current_monotonic_time_s() - d );
      this->off += 5 + sizeof( d );
    }
    else {
      break;
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
  this->EvConnection::release_buffers();
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
  this->append2( "ping ", 5, &d, sizeof( d ) );
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
  EvPoll     poll;
  UnixListen test( poll );
  UnixPing   ping( poll );
  int        idle_count = 0; 
  poll.init( 5, false );
  
  if ( argc > 1 && ::strcmp( argv[ 1 ], "-c" ) == 0 ) {
    if ( EvUnixConnection::connect( ping,
         ( argc > 2 ? argv[ 2 ] : "/tmp/test_unix" ),
         DEFAULT_UNIX_CONNECT_OPTS | OPT_CONNECT_NB, "ping" ) != 0 )
      return 1;
    ping.send_ping();
    poll.timer.add_timer_seconds( ping.fd, 1, 1, 1 );
  }
  else {
    if ( test.listen( "/tmp/test_unix", DEFAULT_UNIX_LISTEN_OPTS ) != 0 )
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

