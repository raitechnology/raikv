#ifndef __rai_raikv__ev_cares_h__
#define __rai_raikv__ev_cares_h__

#include <raikv/ev_net.h>
#include <raikv/array_space.h>
#include <ares.h>

namespace rai {
namespace kv {

struct CaresAddrInfo;

struct EvCaresAsync : public EvSocket {
  CaresAddrInfo & info;
  uint32_t        poll_count;
  void * operator new( size_t, void *ptr ) { return ptr; }
  EvCaresAsync( EvPoll &p,  uint8_t tp,  CaresAddrInfo &i )
      : EvSocket( p, tp ), info( i ), poll_count( 0 ) {
    this->sock_opts = OPT_NO_CLOSE;
  }
  virtual void write( void ) noexcept;
  virtual void read( void ) noexcept;
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
};

struct CaresAddrInfo : public EvTimerCallback {
  EvPoll     & poll;
  ares_channel channel;
  ArrayCount<EvCaresAsync *, 2> set;
  struct   addrinfo * addr_list, * tail;
  uint64_t timer_id, event_id;
  uint32_t poll_count;
  int      status, timeouts, port, socktype, protocol, flags, host_count;
  uint8_t  sock_type;
  bool     done;

  CaresAddrInfo( EvPoll &p )
    : poll( p ), channel( 0 ), /*ai( 0 ),*/
      addr_list( 0 ), tail( 0 ), timer_id( 0 ), event_id( 0 ),
      status( -1 ), timeouts( 0 ), port( 0 ), socktype( 0 ), protocol( 0 ),
      flags( 0 ), host_count( 0 ), sock_type( p.register_type( "c-ares" ) ),
      done( false ) {}
  ~CaresAddrInfo() noexcept; /* ares_freeaddrinfo() */

  /* ares_getaddrinfo */
  int get_address( const char *ip,  int port,  int opts ) noexcept;
  void free_addr_list( void ) noexcept;
  void do_poll( void ) noexcept;
  virtual bool timer_cb( uint64_t timer_id,  uint64_t event_id ) noexcept;
};

}
}
#endif
