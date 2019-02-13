#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>

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
test_one( HashTab &map,  uint8_t db,  uint32_t ctx_id,  kv_hash128_func_t func,
          uint64_t test_count,  bool use_find,  bool use_single )
{
  HashDeltaCounters stats;
  HashCounters ops, tot;
  WorkAlloc8k wrk;
  KeyBuf kb;
  double mono, ival, tmp;
  uint64_t i, h1, h2, k;
  void *p;

  stats.zero();
  kb.zero();
  kb.keylen = sizeof( k );

  sighndl.install();
  mono = current_monotonic_time_s();
  map.sum_ht_thr_delta( stats, ops, tot, ctx_id );
  while ( ! sighndl.signaled ) {
    KeyCtx kctx( map, ctx_id, &kb );
    kb.set( (uint64_t) 0 ); /* use value 0 */
    map.hdr.get_hash_seed( db, h1, h2 );
    kb.hash( h1, h2, func );
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
  }
}

void
test_rand( HashTab &map,  uint8_t db,  uint32_t ctx_id,  kv_hash128_func_t func,
           uint64_t test_count,  bool /*use_find*/,  uint32_t prefetch,
           bool use_single )
{
  HashDeltaCounters stats;
  HashCounters ops, tot;
  WorkAlloc8k wrk;
  KeyBuf kb;
  double mono, ival, tmp;
  void *p;
  uint64_t i, h1, h2, k;
  uint32_t j;

  stats.zero();
  kv::rand::xorshift1024star rand;
  if ( ! rand.init() )
    printf( "urandom failed\n" );

  KeyBufAligned * key = KeyBufAligned::new_array( test_count );
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
    map.hdr.get_hash_seed( db, h1, h2 );
    key[ k ].hash( h1, h2, func );
    k  = ( k + 1 ) % test_count;
  }
  mono = current_monotonic_time_s() - mono;
  printf( "hash = %.1fns %.1f/s\n",
          mono / 1000000.0 * 1000000000.0,
          1000000.0 / mono );

  mono = current_monotonic_time_s();
  map.sum_ht_thr_delta( stats, ops, tot, ctx_id );
  while ( ! sighndl.signaled ) {
    if ( prefetch > 1 ) {
      const uint32_t stride = prefetch;
      KeyCtx * kctx = KeyCtx::new_array( map, ctx_id, NULL, stride );

      for ( i = 0; i < test_count; i += stride ) {
        for ( j = 0; j < stride; j++ ) {
          kctx[ j ].set_key_hash( key[ j ] );
          kctx[ j ].prefetch( 2 );
          if ( use_single )
            kctx[ j ].set( KEYCTX_IS_SINGLE_THREAD );
        }
        for ( j = 0; j < stride; j++ ) {
          if ( kctx[ j ].acquire( &wrk ) <= KEY_IS_NEW ) {
            if ( kctx[ j ].resize( &p, 6 ) == KEY_OK )
              ::memcpy( p, "hello", 6 );
            kctx[ j ].release();
          }
        }
      }
    }
    else {
      KeyCtx kctx( map, ctx_id );
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
  }
}

void
test_incr( HashTab &map,  uint8_t db,  uint32_t ctx_id,  kv_hash128_func_t func,
           uint64_t test_count,  bool use_find,  uint32_t prefetch,
           bool use_single )
{
  HashDeltaCounters stats;
  HashCounters ops, tot;
  WorkAlloc8k wrk;
  KeyBuf kb;
  double mono, ival, tmp;
  void *p;
  uint64_t i, h1, h2, k;
  uint32_t j;

  stats.zero();
  mono = current_monotonic_time_s();
  for ( i = 0, k = 0; i < 1000000; i++ ) {
    if ( k == 0 ) {
      kb.zero();
      kb.keylen = 2;
      kb.u.buf[ 0 ] = '0';
    }
    map.hdr.get_hash_seed( db, h1, h2 );
    kb.hash( h1, h2, func );
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
  while ( ! sighndl.signaled ) {
    kb.zero();
    kb.keylen = 2;
    kb.u.buf[ 0 ] = '0';
    if ( prefetch > 1 ) {
      const uint32_t stride = prefetch;
      KeyCtx * kctx = KeyCtx::new_array( map, ctx_id, NULL, stride );
      KeyBufAligned * kbar = KeyBufAligned::new_array( stride );

      for ( i = 0; i < test_count; i += stride ) {
        for ( j = 0; j < stride; j++ ) {
          incr_key( kb );
          kbar[ j ] = kb;
        }
        for ( j = 0; j < stride; j++ ) {
          kctx[ j ].set_key_hash( kbar[ j ] );
          kctx[ j ].prefetch( 2 );
          if ( use_single )
            kctx[ j ].set( KEYCTX_IS_SINGLE_THREAD );
        }
        if ( use_find ) {
          for ( j = 0; j < stride; j++ )
            kctx[ j ].find( &wrk );
        }
        else {
          for ( j = 0; j < stride; j++ ) {
            if ( kctx[ j ].acquire( &wrk ) <= KEY_IS_NEW ) {
              if ( kctx[ j ].resize( &p, 6 ) == KEY_OK )
                ::memcpy( p, "hello", 6 );
              kctx[ j ].release();
            }
          }
        }
      }
    }
    else {
      KeyCtx kctx( map, ctx_id, &kb );
      for ( i = 0; i < test_count; i++ ) {
        map.hdr.get_hash_seed( db, h1, h2 );
        kb.hash( h1, h2, func );
        kctx.set_hash( h1, h2 );
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
  }
}

void
test_int( HashTab &map,  uint8_t db,  uint32_t ctx_id,  kv_hash128_func_t func,
          uint64_t test_count,  bool use_find,  uint32_t prefetch,
          bool use_single )
{
  HashDeltaCounters stats;
  HashCounters ops, tot;
  WorkAlloc8k wrk;
  KeyBuf kb, ukb;
  double mono, ival, tmp;
  void *p;
  uint64_t i, h1, h2, k;
  uint32_t j;

  stats.zero();
  mono = current_monotonic_time_s();
  ukb.zero();
  for ( i = 0, k = 0; i < 1000000; i++ ) {
    ukb.set( k );
    map.hdr.get_hash_seed( db, h1, h2 );
    ukb.hash( h1, h2, func );
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
    map.hdr.get_hash_seed( db, h1, h2 );
    akb.hash( h1, h2, func );
    k  = ( k + 1 ) % test_count;
  }
  mono = current_monotonic_time_s() - mono;
  printf( "aligned hash = %.1fns %.1f/s\n",
          mono / 1000000.0 * 1000000000.0,
          1000000.0 / mono );

  sighndl.install();
  mono = current_monotonic_time_s();
  map.sum_ht_thr_delta( stats, ops, tot, ctx_id );
  while ( ! sighndl.signaled ) {
    if ( prefetch > 1 ) {
      const uint32_t stride = prefetch;
      KeyCtx * kctx = KeyCtx::new_array( map, ctx_id, NULL, stride );
      KeyBufAligned * kbar = KeyBufAligned::new_array( stride );

      for ( i = 0; i < test_count; i += stride ) {
        for ( j = 0; j < stride; j++ ) {
          kbar[ j ].set( i + j );
          kctx[ j ].set_key_hash( kbar[ j ] );
          kctx[ j ].prefetch( 2 );
          if ( use_single )
            kctx[ j ].set( KEYCTX_IS_SINGLE_THREAD );
        }
        if ( use_find ) {
          for ( j = 0; j < stride; j++ )
            kctx[ j ].find( &wrk );
        }
        else {
          for ( j = 0; j < stride; j++ ) {
            if ( kctx[ j ].acquire( &wrk ) <= KEY_IS_NEW ) {
              if ( kctx[ j ].resize( &p, 6 ) == KEY_OK )
                ::memcpy( p, "hello", 6 );
              kctx[ j ].release();
            }
          }
        }
      }
    }
    else {
      KeyBufAligned kb;
      KeyCtx kctx( map, ctx_id, kb );
      for ( i = 0; i < test_count; i++ ) {
        kb.set( i );
        map.hdr.get_hash_seed( db, h1, h2 );
        kb.hash( h1, h2, func );
        kctx.set_hash( h1, h2 );
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
  }
}

static const char *
get_arg( int argc, char *argv[], int n, int b, const char *f, const char *def )
{
  if ( n > 0 && argc > n && argv[ 1 ][ 0 ] != '-' )
    return argv[ n ];
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
  const char * mn = get_arg( argc, argv, 1, 1, "-m", "sysv2m:shm.test" ),
             * te = get_arg( argc, argv, 2, 1, "-t", "int" ),
             * pc = get_arg( argc, argv, 3, 1, "-p", "50" ),
             * fe = get_arg( argc, argv, 4, 1, "-f", "1" ),
             * op = get_arg( argc, argv, 5, 1, "-o", "ins" ),
             * si = get_arg( argc, argv, 6, 1, "-s", "0" ),
             * db = get_arg( argc, argv, 7, 1, "-d", "0" ),
             * he = get_arg( argc, argv, 0, 0, "-h", 0 );

  if ( he != NULL ) {
  cmd_error:;
    fprintf( stderr, "raikv version: %s\n", kv_stringify( KV_VER ) );
    fprintf( stderr,
"%s [-m map] [-t test] [-p pct] [-f prefetch] [-o oper] [-s sin] [-d db-num]\n"
  "  map            = name of map file (prefix w/ file:, sysv:, posix:)\n"
  "  test           = test kind: one, int, rand, incr (def: int)\n"
  "  pct            = percent coverage of total hash entries (def: 50%%)\n"
  "  prefetch       = number of prefetches to perform (def: 1)\n"
  "  oper           = find or insert (def: ins)\n"
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

  uint32_t ctx_id = map->attach_ctx( ::getpid(), db_num, 0 );
  if ( ctx_id == MAX_CTX_ID ) {
    printf( "no more ctx available\n" );
    return 3;
  }
  fputs( print_map_geom( map, ctx_id ), stdout );
  kv_hash128_func_t func = KV_DEFAULT_HASH;

  const uint64_t test_count = (uint64_t)
    ( ( (double) map->hdr.ht_size * load_pct ) / 100.0 ) & ~(uint64_t) 15;
  const bool use_find    = ::strncmp( op, "find", 4 ) == 0;
  const bool use_single  = si[ 0 ] != '0';

  printf( "test: %s\n", te );
  printf( "elem count: %lu\n", test_count );
  printf( "prefetch: %u\n", prefetch );
  printf( "use find: %s\n", use_find ? "yes" : "no" );
  printf( "use single: %s\n", use_single ? "yes" : "no" );

  if ( ::strcmp( te, "one" ) == 0 ) {
    test_one( *map, db_num, ctx_id, func, test_count, use_find,
              use_single );
  }
  else if ( ::strcmp( te, "rand" ) == 0 ) {
    test_rand( *map, db_num, ctx_id, func, test_count, use_find,
               prefetch, use_single );
  }
  else if ( ::strcmp( te, "incr" ) == 0 ) {
    test_incr( *map, db_num, ctx_id, func, test_count, use_find,
               prefetch, use_single );
  }
  else if ( ::strcmp( te, "int" ) == 0 ) {
    test_int( *map, db_num, ctx_id, func, test_count, use_find,
               prefetch, use_single );
  }
  printf( "bye\n" );
  map->detach_ctx( ctx_id );
  delete map;

  return 0;
}

