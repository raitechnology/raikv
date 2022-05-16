#ifndef __rai_raikv__ev_tcp_h__
#define __rai_raikv__ev_tcp_h__

#include <raikv/ev_net.h>

namespace rai {
namespace kv {

struct EvTcpListen : public EvListen {
  EvTcpListen( EvPoll &p,  const char *lname,  const char *name )
    : EvListen( p, lname, name ) {}
  virtual int listen( const char *ip,  int port,  int opts ) noexcept;
  virtual int listen2( const char *ip,  int port,  int opts,
                       const char *k ) noexcept;
  virtual bool accept( void ) noexcept { return false; }
  virtual bool accept2( EvConnection &c,  const char *k ) noexcept;
};

namespace EvTcpConnection {
  int connect( EvConnection &conn,  const char *ip,  int port,
               int opts ) noexcept;
  int connect2( EvConnection &conn,  const char *ip,  int port,
                int opts,  const char *k ) noexcept;
}

}
}
#endif
