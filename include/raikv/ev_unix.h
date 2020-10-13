#ifndef __rai_raikv__ev_unix_h__
#define __rai_raikv__ev_unix_h__

#include <raikv/ev_net.h>

namespace rai {
namespace kv {

struct EvUnixListen : public EvListen {
  EvUnixListen( EvPoll &p,  uint8_t tp,  const char *name )
    : EvListen( p, tp, name ) {}
  int listen( const char *sock,  const char *k ) noexcept;
  virtual bool accept( void ) noexcept { return false; }
};

namespace EvUnixConnection {
  int connect( EvConnection &conn,  const char *sock ) noexcept;
}

}
}
#endif
