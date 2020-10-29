#ifndef __rai_raikv__monitor_h__
#define __rai_raikv__monitor_h__

namespace rai {
namespace kv {

struct HashTab;
struct HashTabStats;

/* monotor state:  update load, print ht stats, check broken locks */
struct Monitor {
  HashTab      & map;
  HashTabStats & hts;
  uint64_t       current_time,  /* monotonic time for interval calc */
                 last_time,     /* last monotonic time */
                 stats_ival,    /* print ops interval */
                 check_ival,    /* check broken locks interval */
                 stats_counter, /* print header every 16 stats ival */
                 last_stats,    /* time passed since last stats */
                 last_check;    /* time passed since last check */

  Monitor( HashTab &m,  uint64_t st_ival,  uint64_t ch_ival );

  void interval_update( void ); /* update load, check broken locks */

  void print_stats( void ); /* print ht.hdr */

  void print_ops( void );    /* op/s  1/ops chns  get  put spin ht va  entry
                                GC  drop  hits  miss */
  void check_broken_locks( void ); /* check for broken locks */
};

}
}

#endif
