#ifndef __rai_raikv__ev_tcp_h__
#define __rai_raikv__ev_tcp_h__

#include <raikv/ev_net.h>

extern "C" {
  struct addrinfo;
}

namespace rai {
namespace kv {

struct AddrInfo {
  struct addrinfo *ai;
  AddrInfo() : ai( 0 ) {}
  ~AddrInfo() noexcept; /* freeaddrinfo() */
  /* getaddrinfo */
  int get_address( const char *ip,  int port,  int opts ) noexcept;
};
uint32_t fix_ip4_address( uint32_t ipaddr ) noexcept;
/* find address by device name */
const char *fix_ip4_device( const char *dev,  char *ipbuf ) noexcept;

/* bind and set sockopts */
int bind_socket( int sock,  int fam,  int opts,  struct sockaddr *ai_addr,
                 int ai_addrlen ) noexcept;
/* if network addr specified, replace the sockaddr sin_addr with actual */

struct EvTcpListen : public EvListen {
  EvTcpListen( EvPoll &p,  const char *lname,  const char *name )
    : EvListen( p, lname, name ) {}
  virtual int listen( const char *ip,  int port,  int opts ) noexcept;
  virtual int listen2( const char *ip,  int port,  int opts,
                       const char *k,  uint32_t rte_id ) noexcept;
  virtual bool accept2( EvConnection &c,  const char *k ) noexcept;
};

namespace EvTcpConnection {
  int connect( EvConnection &conn,  const char *ip,  int port,
               int opts ) noexcept;
  int connect2( EvConnection &conn,  const char *ip,  int port,
                int opts,  const char *k,  uint32_t rte_id ) noexcept;
}

}
}
#endif
