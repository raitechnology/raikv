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

struct EvCaresCallback {
  virtual void addr_resolve_cb( CaresAddrInfo &info ) noexcept;
};

struct AddrInfoList {
  struct addrinfo * hd, * tl;
  AddrInfoList() : hd( 0 ), tl( 0 ) {}
  void push( struct addrinfo *ai ) noexcept;
  void append( AddrInfoList &l ) noexcept;
};

struct CaresAddrInfo : public EvTimerCallback {
  EvPoll     * poll;
  ares_channel channel; /* c-ares context */

  ArrayCount<EvCaresAsync *, 2> set; /* set of fds monitoring */

  EvCaresCallback * notify_cb; /* notify when done */
  struct addrinfo * addr_list; /* head of address list */
  char   * host;       /* query ip */
  uint64_t timer_id,   /* timer id */
           event_id;   /* timer event id (usecs timer) */
  uint32_t poll_count; /* count of do_poll() */
  int      status,     /* last status */
           timeouts,   /* count of timeouts */
           port,       /* connect port */
           socktype,   /* SOCK_STREAM */
           protocol,   /* IPPROTO_TCP */
           flags,      /* AI_PASSIVE */
           family,     /* AF_INET */
           host_count, /* when queries multiple are in progress */
           timeout_ms, /* millisecs */
           tries;      /* count of queries */
  uint8_t  sock_type;  /* EvCaresAsync sock type */
  bool     ipv6_prefer,/* if order by ipv6 */
           done;       /* if query not in progress */

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  CaresAddrInfo( EvPoll *p,  EvCaresCallback *cb = NULL )
    : poll( p ), channel( 0 ), notify_cb( cb ),
      addr_list( 0 ), host( 0 ), timer_id( 0 ), event_id( 0 ),
      status( -1 ), timeouts( 0 ), port( 0 ), socktype( 0 ), protocol( 0 ),
      flags( 0 ), family( 0 ), host_count( 0 ), timeout_ms( 2500 ), tries( 3 ),
      sock_type( p ? p->register_type( "c-ares" ) : 0 ), ipv6_prefer( false ),
      done( true ) {}
  ~CaresAddrInfo() noexcept; /* ares_freeaddrinfo() */

  /* ares_getaddrinfo */
  int get_address( const char *ip,  int port,  int opts ) noexcept;
  void stop( void ) noexcept;
  void split_ai( AddrInfoList &inet,  AddrInfoList &inet6 ) noexcept;
  void merge_ai( AddrInfoList &inet,  AddrInfoList &inet6 ) noexcept;
  void free_addr_list( void ) noexcept;
  void do_pollfds( void ) noexcept;
  void do_select( void ) noexcept;
  void do_poll( void ) noexcept;
  virtual bool timer_cb( uint64_t timer_id,  uint64_t event_id ) noexcept;
};

}
}
#endif
