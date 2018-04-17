#ifndef __rai__raikv__rela_ts_h__
#define __rai__raikv__rela_ts_h__

/* include include stdint.h */
#ifndef __rai__raikv__util_h__
#include <raikv/util.h>
#endif

#ifdef __cplusplus
namespace rai {
namespace kv {

/*
 * resolution at distance from base timestamp
 *
$ rela_test
shift  |   update   |    update         |   expires  |    expire
       | resolution |    period         | resolution |    period
-------+------------+-------------------+------------+---------------
     0 |       16ns | 1d 15h 5m 37s     |       16s  | 1d 12h 24m 31s 
     1 |       32ns | 3d 6h 11m 14s     |       32s  | 3d 49m 3s 
     2 |       64ns | 6d 12h 22m 29s    |       64s  | 6d 1h 38m 7s 
     3 |      128ns | 13d 44m 59s       |      128s  | 12d 3h 16m 15s 
     4 |      128ns | 26d 1h 29m 59s    |      128s  | 24d 6h 30m 23s 
     5 |      128ns | 39d 2h 14m 59s    |      128s  | 36d 9h 44m 31s 
     6 |      128ns | 52d 2h 59m 59s    |      128s  | 48d 12h 58m 39s 
     7 |      128ns | 65d 3h 44m 59s    |      128s  | 60d 16h 12m 47s 
MAX_UPDATE_NS  = 65d 3h 44m 59s 
MAX_EXPIRES_NS = 60d 16h 10m 40s 
0 EXP QUARTER: (0)  -> (129fca1d070000) 60d 16h 10m 40s 
1 EXP QUARTER: (49ab483a10000) 15d  -> (173a7ea0a80000) 75d 16h 10m 40s 
2 EXP QUARTER: (9356907420000) 30d  -> (1bd53324490000) 90d 16h 10m 40s 
3 EXP QUARTER: (dd01d8ae30000) 45d  -> (206fe7a7ea0000) 105d 16h 10m 40s 
 */

static const uint64_t NANOS           = 1000000000,
                      MAX_RELA_SECS   = ( (uint64_t) 1 << 13 ) - 1,
                      MAX_RELA_NS     = ( (uint64_t) 1 << 43 ) - 1,
                      RELA_CLOCK_PER  = ( 60 * 24 * 60 * 60 ) * NANOS,
                      RELA_CLOCK_QTR  = RELA_CLOCK_PER / 4;
static const uint32_t RELA_NS_HI_SHFT = 27;
static const uint64_t MAX_RELA_NS_LO  = ( (uint64_t) 1 << RELA_NS_HI_SHFT ) - 1;
static const uint32_t MIN_EXP_SHIFT   = 4,
                      MAX_EXP_SHIFT   = 3,
                      MIN_UPD_SHIFT   = 4,
                      MAX_UPD_SHIFT   = 3,
                      RELA_SHIFT_BITS = 3,
                      RELA_SHIFT_MASK = ( 1 << RELA_SHIFT_BITS ) - 1;
static const double   NANOSF          = 1000000000.0;

static const uint64_t MAX_EXPIRES_NS      =
  ( ( MAX_RELA_SECS + ( MAX_RELA_SECS * ( RELA_SHIFT_MASK - MAX_EXP_SHIFT ) ) )
  << ( MAX_EXP_SHIFT + MIN_EXP_SHIFT ) ) * NANOS;

static const uint64_t MAX_UPDATE_NS      =
  ( MAX_RELA_NS + ( MAX_RELA_NS * ( RELA_SHIFT_MASK - MAX_UPD_SHIFT ) ) )
  << ( MAX_UPD_SHIFT + MIN_UPD_SHIFT );

struct RelativeStamp {
  union {
    struct {
      uint32_t expires       : 13,
               expires_shift : 3,
               updatehi      : 16,
               updatelo      : 27, /* RELA_NS_HI_SHFT */
               update_shift  : 3,
               clock_base    : 2;
    } x;
    uint64_t stamp;
  } u;

  void zero( void ) {
    this->u.stamp = 0;
  }
  static uint32_t upd_resolution( uint32_t shft ) {
    if ( shft > MAX_UPD_SHIFT )
      shft = MAX_UPD_SHIFT;
    return 1U << ( shft + MIN_UPD_SHIFT );
  }
  static uint32_t exp_resolution( uint32_t shft ) {
    if ( shft > MAX_EXP_SHIFT )
      shft = MAX_EXP_SHIFT;
    return 1U << ( shft + MIN_EXP_SHIFT );
  }
  void set_expires( uint64_t secs,  uint32_t shft ) {
    this->u.x.expires       = (uint32_t) secs;
    this->u.x.expires_shift = shft;
  }
  void set_update( uint64_t ns,  uint32_t shft ) {
    this->u.x.updatehi      = (uint32_t) ( ns >> RELA_NS_HI_SHFT );
    this->u.x.updatelo      = (uint32_t) ( ns & MAX_RELA_NS_LO );
    this->u.x.update_shift  = shft;
  }
  void get_expires( uint64_t &secs,  uint32_t &shft ) {
    secs = this->u.x.expires;
    shft = this->u.x.expires_shift;
  }
  void get_update( uint64_t &ns,  uint32_t &shft ) {
    ns   = ( (uint64_t) this->u.x.updatehi << RELA_NS_HI_SHFT ) |
             (uint64_t) this->u.x.updatelo;
    shft = this->u.x.update_shift;
  }
  void set( uint64_t base,  uint64_t clock,  uint64_t exp,  uint64_t upd );

  void get( uint64_t base,  uint64_t clock,  uint64_t &exp,  uint64_t &upd );
};

} /* kv */
} /* rai */
#endif
#endif
