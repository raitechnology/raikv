#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <raikv/shm_ht.h>
#include <raikv/key_buf.h>

using namespace rai;
using namespace kv;

void
incr_key( KeyBuf &kb )
{
  for ( uint8_t j = kb.keylen - 1; ; ) {
    if ( ++kb.u.buf[ --j ] <= '9' )
      break;
    kb.u.buf[ j ] = '0';
    if ( j == 0 ) {
      ::memmove( &kb.u.buf[ 1 ], kb.u.buf, kb.keylen );
      kb.u.buf[ 0 ] = '1';
      kb.keylen++;
      break;
    }
  }
}

void
print_ops( HashTab &map,  HashCounters &ops,  double ival )
{
  char buf[ 16 ], buf2[ 16 ], buf3[ 16 ], buf4[ 16 ];
  printf( "ops %.1f[%s] (%.1fns) (%.2f%%,coll=%.2f) "
         "(rd=%.1f[%s],wr=%.1f[%s],sp=%.1f[%s])\n",
         (double) ( ops.rd + ops.wr ) / ival,
         mem_to_string( ops.rd + ops.wr, buf ),
         ival / (double) ( ops.rd + ops.wr ) * 1000000000.0,
         ( (double) ops.add * 100.0 / (double) map.hdr.ht_size ),
         1.0 + ( (double) ops.chains / (double) ( ops.rd + ops.wr ) ),
         (double) ops.rd / ival,
         mem_to_string( ops.rd, buf2 ),
         (double) ops.wr / ival,
         mem_to_string( ops.wr, buf3 ),
         (double) ops.spins / ival,
         mem_to_string( ops.spins, buf4 ) );
}

SignalHandler sighndl;

void
test_one( HashTab &map,  uint32_t dbx_id,  uint32_t ctx_id,
          uint64_t test_count,  bool use_find, bool use_single,  bool one_iter )
{
  HashDeltaCounters stats;
  HashCounters ops, tot;
  WorkAlloc8k wrk;
  KeyBuf kb;
  double mono, ival, tmp;
  HashSeed hs;
  uint64_t i, h1, h2, k;
  void *p;

  stats.zero();
  map.hdr.get_hash_seed( map.hdr.stat_link[ dbx_id ].db_num, hs );
  kb.zero();
  kb.keylen = sizeof( k );

  sighndl.install();
  mono = current_monotonic_time_s();
  map.sum_ht_thr_delta( stats, ops, tot, ctx_id );
  while ( ! sighndl.signaled ) {
    KeyCtx kctx( map, dbx_id, &kb );
    kb.set( (uint64_t) 0 ); /* use value 0 */
    hs.get( h1, h2 );
    kb.hash( h1, h2 );
    kctx.set_hash( h1, h2 );
    if ( use_single )
      kctx.set( KEYCTX_IS_SINGLE_THREAD );
    if ( use_find ) {
      for ( i = 0; i < test_count; i++ )
        kctx.find( &wrk );
    }
    else {
      for ( i = 0; i < test_count; i++ ) {
        if ( kctx.acquire( &wrk ) <= KEY_IS_NEW ) {
          if ( kctx.resize( &p, 6 ) == KEY_OK )
            ::memcpy( p, "hello", 6 );
          kctx.release();
        }
      }
    }
    ival = current_monotonic_time_s();
    if ( ival - mono >= 1.0 ) {
      tmp = mono; mono  = ival; ival -= tmp;
      if ( map.sum_ht_thr_delta( stats, ops, tot, ctx_id ) > 0 )
        print_ops( map, ops, ival );
    }
    if ( one_iter )
      return;
  }
}

void
test_rand( HashTab &map,  uint32_t dbx_id,  uint32_t ctx_id,
           uint64_t test_count,  bool /*use_find*/,
           uint32_t prefetch,  bool use_single,  bool one_iter )
{
  HashDeltaCounters stats;
  HashCounters ops, tot;
  WorkAlloc8k wrk;
  KeyBuf kb;
  double mono, ival, tmp;
  void *p;
  HashSeed hs;
  uint64_t i, h1, h2, k;
  uint32_t j;

  stats.zero();
  map.hdr.get_hash_seed( map.hdr.stat_link[ dbx_id ].db_num, hs );
  kv::rand::xorshift1024star rand;
  if ( ! rand.init() )
    printf( "urandom failed\n" );

  KeyBufAligned * key = KeyBufAligned::new_array( NULL, test_count );
  for ( i = 0; i < test_count; i++ ) {
    key[ i ].zero();
    uint16_t keylen = rand.next() % 32 + 1;
    key[ i ].kb.keylen = keylen;
    for ( j = 0; j < keylen; j++ )
      key[ i ].kb.u.buf[ j ] =
        "abcdefghijklmnopqrstuvwxyz.:0123456789"[ rand.next() % 38 ];
  }
  sighndl.install();

  mono = current_monotonic_time_s();
  for ( i = 0, k = 0; i < 1000000; i++ ) {
    hs.get( h1, h2 );
    key[ k ].hash( h1, h2 );
    k  = ( k + 1 ) % test_count;
  }
  mono = current_monotonic_time_s() - mono;
  printf( "hash = %.1fns %.1f/s\n",
          mono / 1000000.0 * 1000000000.0,
          1000000.0 / mono );

  const uint32_t stride = prefetch;
  KeyCtx * kar = KeyCtx::new_array( map, dbx_id, NULL, stride );
  mono = current_monotonic_time_s();
  map.sum_ht_thr_delta( stats, ops, tot, ctx_id );
  while ( ! sighndl.signaled ) {
    if ( stride > 1 ) {
      kar = KeyCtx::new_array( map, dbx_id, kar, stride );
      for ( i = 0; i < test_count; i += stride ) {
        for ( j = 0; j < stride; j++ ) {
          kar[ j ].set_key_hash( key[ j ] );
          kar[ j ].prefetch( false );
          if ( use_single )
            kar[ j ].set( KEYCTX_IS_SINGLE_THREAD );
        }
        for ( j = 0; j < stride; j++ ) {
          if ( kar[ j ].acquire( &wrk ) <= KEY_IS_NEW ) {
            if ( kar[ j ].resize( &p, 6 ) == KEY_OK )
              ::memcpy( p, "hello", 6 );
            kar[ j ].release();
          }
        }
      }
    }
    else {
      KeyCtx &kctx = kar[ 0 ];
      if ( use_single )
        kctx.set( KEYCTX_IS_SINGLE_THREAD );
      for ( i = 0; i < test_count; i++ ) {
        kctx.set_key_hash( key[ i ] );
        if ( kctx.acquire( &wrk ) <= KEY_IS_NEW ) {
          if ( kctx.resize( &p, 6 ) == KEY_OK )
            ::memcpy( p, "hello", 6 );
          kctx.release();
        }
      }
    }
    ival = current_monotonic_time_s();
    if ( ival - mono >= 1.0 ) {
      tmp = mono; mono  = ival; ival -= tmp;
      if ( map.sum_ht_thr_delta( stats, ops, tot, ctx_id ) > 0 )
        print_ops( map, ops, ival );
    }
    if ( one_iter )
      return;
  }
}

void
test_incr( HashTab &map,  uint32_t dbx_id,  uint32_t ctx_id,
           uint64_t test_count,  bool use_find,
           uint32_t prefetch,  bool use_single,  bool one_iter )
{
  HashDeltaCounters stats;
  HashCounters ops, tot;
  WorkAlloc8k wrk;
  KeyBuf kb;
  double mono, ival, tmp;
  void *p;
  HashSeed hs;
  uint64_t i, h1, h2, k;
  uint32_t j;

  stats.zero();
  map.hdr.get_hash_seed( map.hdr.stat_link[ dbx_id ].db_num, hs );
  mono = current_monotonic_time_s();
  for ( i = 0, k = 0; i < 1000000; i++ ) {
    if ( k == 0 ) {
      kb.zero();
      kb.keylen = 2;
      kb.u.buf[ 0 ] = '0';
    }
    hs.get( h1, h2 );
    kb.hash( h1, h2 );
    k  = ( k + 1 ) % test_count;
    incr_key( kb );
  }
  mono = current_monotonic_time_s() - mono;
  printf( "hash = %.1fns %.1f/s\n",
          mono / 1000000.0 * 1000000000.0,
          1000000.0 / mono );

  sighndl.install();
  mono = current_monotonic_time_s();
  map.sum_ht_thr_delta( stats, ops, tot, ctx_id );

  const uint32_t  stride = ( prefetch > 1 ? prefetch : 1 );
  KeyCtx        * kar    = KeyCtx::new_array( map, dbx_id, NULL, stride );
  KeyBufAligned * kbar   = KeyBufAligned::new_array( NULL, stride );

  while ( ! sighndl.signaled ) {
    kb.zero();
    kb.keylen = 2;
    kb.u.buf[ 0 ] = '0';
    if ( stride > 1 ) {
      kar  = KeyCtx::new_array( map, dbx_id, kar, stride );
      kbar = KeyBufAligned::new_array( kbar, stride );

      for ( i = 0; i < test_count; i += stride ) {
        for ( j = 0; j < stride; j++ ) {
          incr_key( kb );
          kbar[ j ] = kb;
        }
        for ( j = 0; j < stride; j++ ) {
          kar[ j ].set_key_hash( kbar[ j ] );
          kar[ j ].prefetch( use_find );
          if ( use_single )
            kar[ j ].set( KEYCTX_IS_SINGLE_THREAD );
        }
        if ( use_find ) {
          for ( j = 0; j < stride; j++ )
            kar[ j ].find( &wrk );
        }
        else {
          for ( j = 0; j < stride; j++ ) {
            if ( kar[ j ].acquire( &wrk ) <= KEY_IS_NEW ) {
              if ( kar[ j ].resize( &p, 6 ) == KEY_OK )
                ::memcpy( p, "hello", 6 );
              kar[ j ].release();
            }
          }
        }
      }
    }
    else {
      KeyCtx &kctx = kar[ 0 ];
      for ( i = 0; i < test_count; i++ ) {
        kctx.set_key_hash( kb );
        if ( use_single )
          kctx.set( KEYCTX_IS_SINGLE_THREAD );
        if ( use_find )
          kctx.find( &wrk );
        else {
          if ( kctx.acquire( &wrk ) <= KEY_IS_NEW ) {
            if ( kctx.resize( &p, 6 ) == KEY_OK )
              ::memcpy( p, "hello", 6 );
            kctx.release();
          }
        }
        incr_key( kb );
      }
    }
    ival = current_monotonic_time_s();
    if ( ival - mono >= 1.0 ) {
      tmp = mono; mono  = ival; ival -= tmp;
      if ( map.sum_ht_thr_delta( stats, ops, tot, ctx_id ) > 0 )
        print_ops( map, ops, ival );
    }
    if ( one_iter )
      return;
  }
}

void
test_int( HashTab &map,  uint32_t dbx_id,  uint32_t ctx_id,
          uint64_t test_count,  bool use_find,
          uint32_t prefetch,  bool use_single,  bool one_iter )
{
  HashDeltaCounters stats;
  HashCounters ops, tot;
  WorkAlloc8k wrk;
  KeyBuf kb, ukb;
  double mono, ival, tmp;
  void *p;
  HashSeed hs;
  uint64_t i, h1, h2, k;
  uint32_t j;

  stats.zero();
  map.hdr.get_hash_seed( map.hdr.stat_link[ dbx_id ].db_num, hs );
  mono = current_monotonic_time_s();
  ukb.zero();
  for ( i = 0, k = 0; i < 1000000; i++ ) {
    ukb.set( k );
    hs.get( h1, h2 );
    ukb.hash( h1, h2 );
    k  = ( k + 1 ) % test_count;
  }
  mono = current_monotonic_time_s() - mono;
  printf( "unaligned hash = %.1fns %.1f/s\n",
          mono / 1000000.0 * 1000000000.0,
          1000000.0 / mono );

  KeyBufAligned akb;
  mono = current_monotonic_time_s();
  akb.zero();
  for ( i = 0, k = 0; i < 1000000; i++ ) {
    akb.set( k );
    hs.get( h1, h2 );
    akb.hash( h1, h2 );
    k  = ( k + 1 ) % test_count;
  }
  mono = current_monotonic_time_s() - mono;
  printf( "aligned hash = %.1fns %.1f/s\n",
          mono / 1000000.0 * 1000000000.0,
          1000000.0 / mono );

  sighndl.install();
  mono = current_monotonic_time_s();
  map.sum_ht_thr_delta( stats, ops, tot, ctx_id );

  const uint32_t  stride = ( prefetch > 1 ? prefetch : 1 );
  KeyCtx        * kar    = KeyCtx::new_array( map, dbx_id, NULL, stride );
  KeyBufAligned * kbar   = KeyBufAligned::new_array( NULL, stride );

  while ( ! sighndl.signaled ) {
    if ( stride > 1 ) {
      kar  = KeyCtx::new_array( map, dbx_id, kar, stride );
      kbar = KeyBufAligned::new_array( kbar, stride );

      for ( i = 0; i < test_count; i += stride ) {
        for ( j = 0; j < stride; j++ ) {
          kbar[ j ].set( i + j );
          kar[ j ].set_key_hash( kbar[ j ] );
          kar[ j ].prefetch( use_find );
          if ( use_single )
            kar[ j ].set( KEYCTX_IS_SINGLE_THREAD );
        }
        if ( use_find ) {
          for ( j = 0; j < stride; j++ )
            kar[ j ].find( &wrk );
        }
        else {
          for ( j = 0; j < stride; j++ ) {
            if ( kar[ j ].acquire( &wrk ) <= KEY_IS_NEW ) {
              if ( kar[ j ].resize( &p, 6 ) == KEY_OK )
                ::memcpy( p, "hello", 6 );
              kar[ j ].release();
            }
          }
        }
      }
    }
    else {
      KeyCtx &kctx = kar[ 0 ];
      for ( i = 0; i < test_count; i++ ) {
        kb.set( i );
        kctx.set_key_hash( kb );
        if ( use_single )
          kctx.set( KEYCTX_IS_SINGLE_THREAD );
        if ( use_find )
          kctx.find( &wrk );
        else if ( kctx.acquire( &wrk ) <= KEY_IS_NEW ) {
          if ( kctx.resize( &p, 6 ) == KEY_OK )
            ::memcpy( p, "hello", 6 );
          kctx.release();
        }
      }
    }
    ival = current_monotonic_time_s();
    if ( ival - mono >= 1.0 ) {
      tmp = mono; mono  = ival; ival -= tmp;
      if ( map.sum_ht_thr_delta( stats, ops, tot, ctx_id ) > 0 )
        print_ops( map, ops, ival );
    }
    if ( one_iter )
      return;
  }
}

static const char *
get_arg( int argc, char *argv[], int b, const char *f, const char *def )
{
  for ( int i = 1; i < argc - b; i++ )
    if ( ::strcmp( f, argv[ i ] ) == 0 )
      return argv[ i + b ];
  return def; /* default value */
}

int
main( int argc, char *argv[] )
{
  HashTabGeom   geom;
  HashTab     * map;
  double        load_pct;
  uint32_t      prefetch;
  uint8_t       db_num;

  /* [sysv2m:shm.test] [int] [50] [1] [ins] [0] [0] */
  const char * mn = get_arg( argc, argv, 1, "-m", KV_DEFAULT_SHM ),
             * te = get_arg( argc, argv, 1, "-t", "int" ),
             * pc = get_arg( argc, argv, 1, "-p", "50" ),
             * fe = get_arg( argc, argv, 1, "-f", "1" ),
             * op = get_arg( argc, argv, 1, "-o", "ins" ),
             * _1 = get_arg( argc, argv, 0, "-1", 0 ),
             * si = get_arg( argc, argv, 1, "-s", "0" ),
             * db = get_arg( argc, argv, 1, "-d", "0" ),
             * he = get_arg( argc, argv, 0, "-h", 0 );

  if ( he != NULL ) {
  cmd_error:;
    fprintf( stderr, "raikv version: %s\n", kv_stringify( KV_VER ) );
    fprintf( stderr,
  "%s [-m map] [-t test] [-p pct] [-f prefetch] [-o oper] "
     "[-1] [-s sin] [-d db-num]\n"
  "  map            = name of map file (prefix w/ file:, sysv:, posix:)\n"
  "  test           = test kind: one, int, rand, incr (def: int)\n"
  "  pct            = percent coverage of total hash entries (def: 50%%)\n"
  "  prefetch       = number of prefetches to perform (def: 1)\n"
  "  oper           = find or insert (def: ins)\n"
  "  -1             = one iteration (def: continue forever)\n"
  "  sin            = single thread, no locking (def: 0)\n"
  "  db-num         = database number to use (def: 0)\n", argv[ 0 ] );
    return 1;
  }

  load_pct = strtod( pc, 0 );
  if ( load_pct == 0 )
    goto cmd_error;
  prefetch = atoi( fe );
  if ( prefetch == 0 )
    goto cmd_error;
  db_num = (uint8_t) atoi( db );

  map = HashTab::attach_map( mn, 0, geom );
  if ( map == NULL )
    return 1;

  uint32_t ctx_id = map->attach_ctx( ::getpid() );
  if ( ctx_id == MAX_CTX_ID ) {
    printf( "no more ctx available\n" );
    return 3;
  }
  uint32_t dbx_id = map->attach_db( ctx_id, db_num );
  fputs( print_map_geom( map, ctx_id ), stdout );
  /*kv_hash128_func_t func = KV_DEFAULT_HASH;*/

  const uint64_t test_count = (uint64_t)
    ( ( (double) map->hdr.ht_size * load_pct ) / 100.0 ) & ~(uint64_t) 15;
  const bool use_find    = ::strncmp( op, "find", 4 ) == 0;
  const bool use_single  = si[ 0 ] != '0';
  const bool one_iter    = _1 != NULL;

  printf( "test: %s\n", te );
  printf( "elem count: %lu\n", test_count );
  printf( "prefetch: %u\n", prefetch );
  printf( "use find: %s\n", use_find ? "yes" : "no" );
  printf( "use single: %s\n", use_single ? "yes" : "no" );
  printf( "one iter: %s\n", one_iter ? "yes" : "no" );

  if ( ::strcmp( te, "one" ) == 0 ) {
    test_one( *map, dbx_id, ctx_id, test_count, use_find,
              use_single, one_iter );
  }
  else if ( ::strcmp( te, "rand" ) == 0 ) {
    test_rand( *map, dbx_id, ctx_id, test_count, use_find,
               prefetch, use_single, one_iter );
  }
  else if ( ::strcmp( te, "incr" ) == 0 ) {
    test_incr( *map, dbx_id, ctx_id, test_count, use_find,
               prefetch, use_single, one_iter );
  }
  else if ( ::strcmp( te, "int" ) == 0 ) {
    test_int( *map, dbx_id, ctx_id, test_count, use_find,
               prefetch, use_single, one_iter );
  }
  printf( "bye\n" );
  map->detach_ctx( ctx_id );
  delete map;

  return 0;
}
