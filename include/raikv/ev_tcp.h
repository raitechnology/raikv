#ifndef __rai_raikv__ev_tcp_h__
#define __rai_raikv__ev_tcp_h__

#include <raikv/ev_net.h>

namespace rai {
namespace kv {

struct EvTcpListen : public EvListen {
  EvTcpListen( EvPoll &p,  uint8_t tp,  const char *name )
    : EvListen( p, tp, name ) {}
  int listen( const char *ip,  int port,  int opts,  const char *k ) noexcept;
  virtual bool accept( void ) noexcept { return false; }
  static void set_sock_opts( EvPoll &p,  int sock,  int opts );
};

namespace EvTcpConnection {
  int connect( EvConnection &conn,  const char *ip,  int port,
               int opts ) noexcept;
}

}
}
#endif
