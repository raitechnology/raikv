#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <math.h>
#include <errno.h>

#include <raikv/shm_ht.h>
#include <raikv/monitor.h>

using namespace rai;
using namespace kv;

Monitor::Monitor( HashTab &m,  uint64_t st_ival,  uint64_t ch_ival )
  : map( m ), hts( *HashTabStats::create( m ) )
{
  this->current_time  = 0;
  this->last_time     = 0;
  this->stats_ival    = st_ival;
  this->check_ival    = ch_ival;
  this->stats_counter = 0;
  this->last_stats    = 0;
  this->last_check    = 0;
}

void
Monitor::interval_update( void )
{
  this->map.hdr.current_stamp = current_realtime_ns();
  if ( this->current_time == 0 ) {
    this->check_broken_locks();
    this->map.update_load();
    this->current_time = current_monotonic_coarse_ns();
    return;
  }
  this->last_time = this->current_time;
  this->current_time = current_monotonic_coarse_ns();
  uint64_t ival = this->current_time - this->last_time;
  this->last_stats += ival;
  this->last_check += ival;
  if ( this->last_check >= this->check_ival ) {
    this->last_check %= this->check_ival;
    this->check_broken_locks();
    this->map.update_load();
  }
  if ( this->last_stats >= this->stats_ival || this->stats_counter == 0 ) {
    this->last_stats %= this->stats_ival;
    this->print_stats();
  }
  this->map.hdr.current_stamp = current_realtime_ns();
}

void
Monitor::print_stats( void )
{
  bool b = this->hts.fetch();
  if ( b || ( this->stats_counter == 0 && this->hts.ival > 0 ) ) {
    /* print hdr if stats counter == 0 */
    if ( this->stats_counter == 0 ) {
      fputs( print_map_geom( &this->map, MAX_CTX_ID ), stdout );
      for ( uint32_t db = 0; db < DB_COUNT; db++ ) {
        if ( this->map.hdr.test_db_opened( db ) ) {
          printf( "db[ %u ].entry_cnt:%s %lu\n", db,
                  ( ( db < 10 ) ? "   " : ( ( db < 100 ) ? "  " : " " ) ),
                  this->hts.db_stats[ db ].last.add -
                  this->hts.db_stats[ db ].last.drop );
        }
      }
    }
    /* print interval ops */
    this->print_ops();
    fflush( stdout );
  }
}

static char *
mstring( double f,  char *buf,  int64_t k )
{
  return mem_to_string( (int64_t) ceil( f ), buf, k );
}

void
Monitor::print_ops( void )
{
  HashCounters & ops  = this->hts.hops,
               & tot  = this->hts.htot;
  MemCounters  & chg  = this->hts.mops;
  double         ival = this->hts.ival;
  char buf[ 16 ], buf2[ 16 ], buf3[ 16 ], buf4[ 16 ], buf5[ 16 ],
       buf6[ 16 ], buf7[ 16 ], buf8[ 16 ], buf9[ 16 ];

  if ( ( this->stats_counter++ % 16 ) == 0 )
    printf( "   op/s   1/ops chns    get    put   spin ht va  "
            "entry    GC  drop   hits   miss\n" );

  double op, ns, ch;
  if ( ops.rd + ops.wr == 0 ) {
    op = 0;
    ns = 0;
    ch = 0;
  }
  else {
    op = (double) ( ops.rd + ops.wr ) / ival;
    ns = ival / (double) ( ops.rd + ops.wr ) * 1000000000.0;
    if ( ns > 99999.9 ) {
      if ( op < 1.0 )
        ns = 0;
      else
        ns = 99999.9;
    }
    ch = 1.0 + ( (double) ops.chains / (double) ( ops.rd + ops.wr ) );
  }
  printf( "%7s %7.1f %4.1f %6s %6s %6s %2u %2u %6s %5s %5s %6s %6s\n",
         mstring( op, buf, 1000 ), ns, ch,
         mstring( (double) ops.rd / ival, buf2, 1000 ),
         mstring( (double) ops.wr / ival, buf3, 1000 ),
         mstring( (double) ops.spins / ival, buf4, 1000 ),
         (uint32_t) ( this->map.hdr.ht_load * 100.0 + 0.5 ),
         (uint32_t) ( this->map.hdr.value_load * 100.0 + 0.5 ),
         mstring( tot.add - tot.drop, buf5, 1000 ),
         mstring( (double) chg.move_msgs / ival, buf6, 1000 ),
         mstring( (double) ops.drop / ival, buf7, 1000 ),
         mstring( (double) ops.hit / ival, buf8, 1000 ),
         mstring( (double) ops.miss / ival, buf9, 1000 ) );
}

void
Monitor::check_broken_locks( void )
{
  uint32_t hash_entry_size = this->map.hdr.hash_entry_size;
  for ( uint32_t ctx_id = 1; ctx_id < MAX_CTX_ID; ctx_id++ ) {
    uint32_t pid = this->map.ctx[ ctx_id ].ctx_pid;
    if ( pid == 0 || this->map.ctx[ ctx_id ].ctx_id == KV_NO_CTX_ID )
      continue;
    if ( ::kill( pid, 0 ) == 0 )
      continue;
    if ( errno == EPERM )
      continue;
    printf( "ctx %u: pid %u = kill errno %d/%s\n",
            ctx_id, pid, errno, strerror( errno ) );

    uint64_t used, recovered = 0;
    if ( (used = this->map.ctx[ ctx_id ].mcs_used) != 0 ) {
      for ( uint32_t id = 0; id < 64; id++ ) {
        if ( ( used & ( (uint64_t) 1 << id ) ) == 0 )
          continue;
        uint64_t mcs_id = ( ctx_id << ThrCtxEntry::MCS_SHIFT ) | id;
        ThrMCSLock &mcs = this->map.ctx[ ctx_id ].get_mcs_lock( mcs_id );
        MCSStatus status;
        printf(
        "ctx %u: pid %u, mcs %u, val 0x%lx, lock 0x%lx, next 0x%lx, link %lu\n",
                 ctx_id, pid, id, mcs.val.load(), mcs.lock.load(),
                 mcs.next.load(), mcs.lock_id );
        if ( mcs.lock_id != 0 ) {
          HashEntry *el = this->map.get_entry( mcs.lock_id - 1,
                                         this->map.hdr.hash_entry_size );
          ThrCtxOwner  closure( this->map.ctx );
          status = mcs.recover_lock( el->hash, ZOMBIE64, mcs_id, closure );
          if ( status == MCS_OK ) {
            ValueCtr &ctr = el->value_ctr( hash_entry_size );
            if ( ctr.seal == 0 )
              ctr.seal = 1; /* these are lost with the context thread */
            status = mcs.recover_unlock( el->hash, ZOMBIE64, mcs_id, closure );
            if ( status == MCS_OK ) {
              printf( "mcs_id %u:%u recovered\n", ctx_id, id );
              recovered |= ( (uint64_t) 1 << id );
            }
          }
          if ( status != MCS_OK ) {
            printf( "mcs_id %u:%u status %s\n", ctx_id, id,
                    status == MCS_WAIT ? "MCS_WAIT" : "MCS_INACTIVE" );
          }
        }
      }
      this->map.ctx[ ctx_id ].mcs_used &= ~recovered;
    }
    if ( used != recovered ) {
      printf( "ctx %u still has locks\n", ctx_id );
    }
    else {
      this->map.detach_ctx( ctx_id );
    }
  }
}
