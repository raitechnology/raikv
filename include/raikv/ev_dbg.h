#ifndef __rai_raikv__ev_dbg_h__
#define __rai_raikv__ev_dbg_h__

#ifdef EV_NET_DBG
namespace rai {
namespace kv {

struct EvStateCnt {
  uint64_t cnt[ 16 ];
};

struct EvStateDbg {
  EvStateCnt current,
             last;
  EvStateDbg() {
    ::memset( this, 0, sizeof( *this ) );
  }
  void print_dbg( void );
};

#define EV_DBG_INHERIT , public EvStateDbg
#define EV_DBG_DISPATCH( me, state ) me->current.cnt[ state ]++

}
}

/* no EV_NET_DBG */
#else

#define EV_DBG_INHERIT
#define EV_DBG_DISPATCH( me, state )

#endif

#endif
