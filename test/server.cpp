#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <math.h>
#include <ctype.h>

#include <raikv/shm_ht.h>

using namespace rai;
using namespace kv;

char *
mstring( double f,  char *buf,  int64_t k )
{
  return mem_to_string( (int64_t) ceil( f ), buf, 1000 );
}

void
print_ops( HashTab &map,  HashCounters &ops,  HashCounters &tot,
           MemCounters &chg,  MemCounters &mtot,  double ival,  uint32_t &ctr )
{
  char buf[ 16 ], buf2[ 16 ], buf3[ 16 ], buf4[ 16 ], buf5[ 16 ],
       buf6[ 16 ], buf7[ 16 ], buf8[ 16 ], buf9[ 16 ];
  if ( ( ctr++ % 16 ) == 0 )
    printf( "   op/s   1/ops chns    get    put   spin ht va  "
            "entry    GC  evic   hits   miss\n" );

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
         (uint32_t) ( map.hdr.ht_load * 100.0 + 0.5 ),
         (uint32_t) ( map.hdr.value_load * 100.0 + 0.5 ),
         mstring( tot.add - tot.drop, buf5, 1000 ),
         mstring( (double) chg.move_msgs / ival, buf6, 1000 ),
         mstring( (double) ops.htevict / ival, buf7, 1000 ),
         mstring( (double) ops.hit / ival, buf8, 1000 ),
         mstring( (double) ops.miss / ival, buf9, 1000 ) );
}
#if 0
void
print_mem( HashTab &map,  HashCounters &ops,  MemCounters &chg,
           MemCounters &tot,  double ival )
{
  char buf[ 16 ], buf2[ 16 ], buf3[ 16 ], buf4[ 16 ], buf5[ 16 ], buf6[ 16 ],
       buf7[ 16 ];
  printf( "mem %.1f[%s] msg %.1f[%s] avail %.1f[%s] "
          "mov %.1f%%[%s] msize %.1f[%s] evict %.1f%%[%s] esize %.1f[%s]\n",
          (double) chg.offset / ival,
         mstring( tot.offset, buf, 1024 ),
          (double) chg.msg_count / ival,
         mstring( tot.msg_count, buf2, 1000 ),
          (double) chg.avail_size / ival,
         mstring( tot.avail_size, buf3, 1024 ),
          ( (double) chg.move_msgs / ival * 100.0 ) /
            ( (double) ( ops.rd + ops.wr ) / ival ),
         mstring( tot.move_msgs, buf4, 1000 ),
          (double) chg.move_size / ival,
         mstring( tot.move_size, buf5, 1024 ),
          ( (double) chg.evict_msgs / ival * 100.0 ) /
            ( (double) ( ops.rd + ops.wr ) / ival ),
         mstring( tot.evict_msgs, buf6, 1000 ),
          (double) chg.evict_size / ival,
         mstring( tot.evict_size, buf7, 1024 ) );
}
#endif

void
check_thread_ctx( HashTab &map )
{
  uint32_t hash_entry_size = map.hdr.hash_entry_size;
  for ( uint32_t ctx_id = 1; ctx_id < MAX_CTX_ID; ctx_id++ ) {
    uint32_t pid = map.ctx[ ctx_id ].ctx_pid;
    if ( pid == 0 ||
         map.ctx[ ctx_id ].ctx_id == KV_NO_CTX_ID ||
         ::kill( pid, 0 ) == 0 )
      continue;

    uint64_t used, recovered = 0;
    if ( (used = map.ctx[ ctx_id ].mcs_used) != 0 ) {
      for ( uint32_t id = 0; id < 64; id++ ) {
        if ( ( used & ( (uint64_t) 1 << id ) ) == 0 )
          continue;
        uint64_t mcs_id = ( ctx_id << ThrCtxEntry::MCS_SHIFT ) | id;
        ThrMCSLock &mcs = map.ctx[ ctx_id ].get_mcs_lock( mcs_id );
        MCSStatus status;
        printf(
        "ctx %u: pid %u, mcs %u, val 0x%lx, lock 0x%lx, next 0x%lx, link %lu\n",
                 ctx_id, pid, id, mcs.val.val, mcs.lock.val, mcs.next.val,
                 mcs.lock_id );
        if ( mcs.lock_id != 0 ) {
          HashEntry *el = map.get_entry( mcs.lock_id - 1,
                                         map.hdr.hash_entry_size );
          ThrCtxOwner  closure( map.ctx );
          status = mcs.recover_lock( el->hash, ZOMBIE64, mcs_id, closure );
          if ( status == MCS_OK ) {
            ValueCtr &ctr = el->value_ctr( hash_entry_size );
            if ( ctr.seal == 0 || el->seal != ctr.seriallo ) {
              ctr.seal = 1; /* these are lost with the context thread */
              el->seal = ctr.seriallo;
            }
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
      map.ctx[ ctx_id ].mcs_used &= ~recovered;
    }
    if ( used != recovered ) {
      printf( "ctx %u still has locks\n", ctx_id );
    }
    else {
      map.detach_ctx( ctx_id );
    }
  }
}

int
main( int argc, char *argv[] )
{
  HashTabGeom   geom;
  HashTab     * map;
  SignalHandler sighndl;
  double        ratio   = 0.5;
  uint64_t      mbsize  = 256 * 1024 * 1024; /* 256MB */
  uint32_t      entsize = 64,                /* 64b */
                valsize = 64 * 1024;         /* 64K */
  uint8_t       arity   = 1;                 /* linear probe */
  uint16_t      buckets = 1;
  const char  * entsize_string = NULL, /* max entry size */
              * valsize_string = NULL, /* max value size */
              * cuckoo_string  = NULL, /* cuckoo arity+buckets */
              * ratio_string   = NULL, /* ratio entry/segment */
              * mb_string      = NULL, /* size of map file */
              * mn             = NULL; /* name of map file */

  if ( argc <= 1 ) {
  cmd_error:;
    fprintf( stderr, "raikv version: %s\n", kv_stringify( KV_VER ) );
    fprintf( stderr,
     "%s (map) [MB] [ratio] [max entry size]\n"
     "  map            -- name of map file (prefix w/ file:, sysv:, posix:)\n"
     "  MB             -- size of HT (MB * 1024 * 1024)\n"
     "  ratio          -- entry to segment memory ratio (float 0 -> 1)\n"
     "                    (1 = all ht, 0 = all msg -- must have some ht)\n"
     "  arity+buckets  -- cuckoo hash arity and buckets\n"
     "  max entry size -- max value size or min segment size (in KB * 1024)\n"
     "  ht entry size  -- hash entry size (multiple of 64, default 64)\n",
             argv[ 0 ]);
    return 1;
  }
  switch ( argc ) {
    default: goto cmd_error;
    case 7: entsize_string = argv[ 6 ];
    case 6: valsize_string = argv[ 5 ];
    case 5: cuckoo_string  = argv[ 4 ];
    case 4: ratio_string   = argv[ 3 ];
    case 3: mb_string      = argv[ 2 ];
    case 2: mn             = argv[ 1 ];
            break;
  }

  if ( mb_string != NULL ) {
    mbsize = (uint64_t) ( strtod( mb_string, 0 ) * (double) ( 1024 * 1024 ) );
    if ( mbsize == 0 )
      goto cmd_error;
    if ( ratio_string != NULL ) {
      ratio = strtod( ratio_string, 0 );
      if ( ratio < 0.0 || ratio > 1.0 )
        goto cmd_error;
      if ( cuckoo_string != NULL ) {
        if ( isdigit( cuckoo_string[ 0 ] ) &&
             cuckoo_string[ 1 ] == '+' &&
             isdigit( cuckoo_string[ 2 ] ) ) {
          arity   = cuckoo_string[ 0 ] - '0';
          buckets = atoi( &cuckoo_string[ 2 ] );
        }
        else {
          goto cmd_error;
        }
        if ( valsize_string != NULL ) {
          valsize = (uint32_t) atoi( valsize_string ) * (uint32_t) 1024;
          if ( valsize == 0 && ratio < 1.0 )
            goto cmd_error;

          if ( entsize_string != NULL ) {
            entsize = (uint32_t) atoi( entsize_string );
            if ( entsize == 0 )
              goto cmd_error;
          }
        }
      }
    }
    geom.map_size         = mbsize;
    geom.max_value_size   = ratio < 0.999 ? valsize : 0;
    geom.hash_entry_size  = align<uint32_t>( entsize, 64 );
    geom.hash_value_ratio = ratio;
    geom.cuckoo_buckets   = buckets;
    geom.cuckoo_arity     = arity;
    map = HashTab::create_map( mn, 0, geom );
  }
  else {
    map = HashTab::attach_map( mn, 0, geom );
  }
  /* sz is only used when server */
  if ( map == NULL )
    return 1;
  //print_map_geom( map, MAX_CTX_ID );

  HashDeltaCounters stats[ MAX_CTX_ID ];
  MemDeltaCounters *mstats;

  if ( map->hdr.nsegs > 0 )
    mstats = MemDeltaCounters::new_array( map->hdr.nsegs );
  else
    mstats = NULL;

  HashCounters ops,
               tot;
  MemCounters  chg;
  MemCounters  mtot;
  char         junk[ 8 ];
  ssize_t      j;
  uint32_t     ctr = 0;
  double       mono = 0,
               ival = 0,
               tmp  = 0;

  sighndl.install();
  fcntl( 0, F_SETFL, fcntl( 0, F_GETFL, 0 ) | O_NONBLOCK );
  map->update_load();
  mono = current_monotonic_coarse_s();
  ival = (double) ( map->hdr.current_stamp - map->hdr.create_stamp ) /
         NANOSF;
  for ( j = 1; ; ) {
    if ( j == 0 ) {
      for (;;) {
        usleep( 50 * 1000 );
        if ( sighndl.signaled )
          goto break_loop;
        ival = current_monotonic_coarse_s();
        tmp  = ival - mono;
        if ( (j = read( 0, junk, sizeof( junk ) )) > 0 || tmp >= 1.0 ) {
          mono = ival;
          ival = tmp;
          break;
        }
      }
      check_thread_ctx( *map );
      map->update_load();
    }
    bool b = ( mstats != NULL && map->get_mem_deltas( mstats, chg, mtot ) );
    b |= ( map->get_ht_deltas( stats, ops, tot ) );
    if ( j > 0 ) {
      fputs( print_map_geom( map, MAX_CTX_ID ), stdout );
      b   = true;
      ctr = 0;
    }
    j = 0;
    if ( b )
      print_ops( *map, ops, tot, chg, mtot, ival, ctr );
#if 0
      //print_mem( *map, ops, chg, tot, ival );
    if ( map->hdr.ht_load != last_ht_load ||
         map->hdr.value_load != last_value_load ) {
      last_ht_load    = map->hdr.ht_load;
      last_value_load = map->hdr.value_load;
      /*printf( "== load %.1f%%, ht load %.1f%%, value load %.1f%% load_pct=%u\n",
              map->hdr.current_load * 100.0, last_ht_load * 100.0,
              last_value_load * 100.0, map->hdr.load_percent );*/
    }
#endif
    fflush( stdout );
  }
break_loop:;
  printf( "bye\n" );
  fflush( stdout );
  delete map;
  return 0;
}

