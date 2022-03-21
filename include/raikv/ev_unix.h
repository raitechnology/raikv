#ifndef __rai_raikv__ev_unix_h__
#define __rai_raikv__ev_unix_h__

#include <raikv/ev_net.h>

namespace rai {
namespace kv {

struct EvUnixListen : public EvListen {
  EvUnixListen( EvPoll &p,  const char *lname,  const char *name )
    : EvListen( p, lname, name ) {}
  virtual int listen( const char *sock,  int opts ) noexcept;
  virtual int listen2( const char *sock,  int opts,  const char *k ) noexcept;
  virtual bool accept( void ) noexcept { return false; }
  virtual bool accept2( EvConnection &c,  const char *k ) noexcept;
};

namespace EvUnixConnection {
  int connect( EvConnection &conn,  const char *sock,  int opts,
               const char *k ) noexcept;
}

}
}
#endif
