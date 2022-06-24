#ifndef __rai_raikv__ev_unix_h__
#define __rai_raikv__ev_unix_h__

#include <raikv/ev_net.h>

namespace rai {
namespace kv {

struct EvUnixListen : public EvListen {
  EvUnixListen( EvPoll &p,  const char *lname,  const char *name )
    : EvListen( p, lname, name ) {}
  virtual int listen( const char *path,  int opts ) noexcept;
  virtual int listen2( const char *path,  int opts,  const char *k,
                       uint32_t rte_id ) noexcept;
  virtual bool accept2( EvConnection &c,  const char *k ) noexcept;
};

namespace EvUnixConnection {
  int connect( EvConnection &conn,  const char *path,  int opts,
               const char *k,  uint32_t rte_id ) noexcept;
}

struct EvUnixDgram : public EvDgram {
  EvUnixDgram( EvPoll &p,  uint8_t t ) : EvDgram( p, t, EV_DGRAM_BASE ) {}
  int bind( const char *path,  int opts,  const char *k,
            uint32_t rte_id ) noexcept;
};

}
}
#endif
