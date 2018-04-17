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
extern void print_map_geom( HashTab * map,  uint32_t ctx_id );

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

int
main( int argc, char *argv[] )
{
  HashTabGeom   geom;
  HashTab     * map;
  SignalHandler sighndl;
  double        mono,
                ival,
                tmp;
  uint32_t      pcnt = 1;
  void        * p;
  const char  * ins_fnd  = NULL,
              * pref     = NULL,
              * load_pct = NULL,
              * oper     = NULL,
              * mn       = NULL;

  if ( argc <= 2 ) {
  cmd_error:;
    fprintf( stderr,
     "%s (map) (rand|incr|int|cli) [pct] [prefetch] [ins|find]\n"
     "  map           -- name of map file (prefix w/ file:, sysv:, posix:)\n"
     "  rand   [pct]  -- inserts random string keys, load pct\n"
     "  incr   [pct]  -- inserts incrementing strings, load pct\n"
     "  int    [pct]  -- inserts incrementing integers, load pct\n"
     "  cli           -- attach and prompt for input\n"
     "         [prefetch] == arg to rand/incr/int to use memory\n"
     "                           prefetching with a pipeline of N\n"
     "         [ins/find] == arg client to insert or find\n",
             argv[ 0 ]);
    return 1;
  }
  switch ( argc ) {
    default: goto cmd_error;
    case 6: ins_fnd  = argv[ 5 ];
    case 5: pref     = argv[ 4 ];
    case 4: load_pct = argv[ 3 ];
    case 3: oper     = argv[ 2 ];
            mn       = argv[ 1 ];
            break;
  }

  map = HashTab::attach_map( mn, 0, geom );
  if ( map == NULL )
    return 1;

  HashDeltaCounters stats[ MAX_CTX_ID ];
  HashCounters ops, tot;
  uint64_t   i, k, h = 0;
  uint32_t   j;

  uint32_t ctx_id = map->attach_ctx( ::getpid() );
  if ( ctx_id == MAX_CTX_ID ) {
    printf( "no more ctx available\n" );
    return 3;
  }
  print_map_geom( map, ctx_id );

  kv_hash64_func_t func = kv_hash_cityhash64;

  const uint64_t TEST_COUNT = (
    ( load_pct != NULL && atoi( load_pct ) > 0 && atoi( load_pct ) <= 100 ) ?
    ( map->hdr.ht_size * atoi( load_pct ) / 100 ) :
    ( map->hdr.ht_size / 2 + map->hdr.ht_size / 4 ) ) & ~(uint64_t) 15;
  const int use_prefetch = ( ( pref != NULL ) ? atoi( pref ) : 0 );
  const bool use_find    = ( ( ins_fnd != NULL ) ?
                             ::strcmp( ins_fnd, "find" ) == 0 : false );
  printf( "elem count = %lu\n", TEST_COUNT );
  printf( "use prefetch: %s\n", use_prefetch <= 1 ? "no" : "yes" );
  printf( "use find: %s\n", use_find ? "yes" : "no" );

  KeyCtxAlloc8k wrk;
  if ( ::strcmp( oper, "one" ) == 0 ) {
    KeyBuf kb;
    kb.zero();
    kb.keylen = sizeof( k );

    sighndl.install();
    mono = current_monotonic_time_s();
    map->get_ht_deltas( stats, ops, tot, ctx_id );
    while ( ! sighndl.signaled ) {
      KeyCtx kctx( map, ctx_id, &kb );
      kb.set( (uint64_t) 0 );
      kctx.set_hash( kb.hash( map->hdr.hash_key_seed, func ) );
      for ( i = 0; i < TEST_COUNT; i++ ) {
        if ( use_find )
          kctx.find( &wrk );
        else if ( kctx.acquire( &wrk ) <= KEY_IS_NEW ) {
          if ( kctx.resize( &p, 6 ) == KEY_OK )
            ::memcpy( p, "hello", 6 );
          kctx.release();
        }
      }
      ival = current_monotonic_time_s();
      if ( ival - mono >= 1.0 ) {
        tmp = mono; mono  = ival; ival -= tmp;
        if ( map->get_ht_deltas( stats, ops, tot, ctx_id ) > 0 )
          print_ops( *map, ops, ival );
      }
    }
  }
  else if ( ::strcmp( oper, "rand" ) == 0 ) {
    kv::rand::xorshift1024star rand;
    if ( ! rand.init() )
      printf( "urandom failed\n" );

    KeyBufAligned * key = KeyBufAligned::new_array( TEST_COUNT );
    for ( i = 0; i < TEST_COUNT; i++ ) {
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
      h += key[ k ].hash( map->hdr.hash_key_seed, func );
      k  = ( k + 1 ) % TEST_COUNT;
    }
    mono = current_monotonic_time_s() - mono;
    printf( "hash = %.1fns %.1f/s\n",
            mono / 1000000.0 * 1000000000.0,
            1000000.0 / mono );

    mono = current_monotonic_time_s();
    map->get_ht_deltas( stats, ops, tot, ctx_id );
    while ( ! sighndl.signaled ) {
      if ( use_prefetch > 1 ) {
        const uint32_t stride = use_prefetch;
        KeyCtx * kctx = KeyCtx::new_array( map, ctx_id, NULL, stride );

        for ( i = 0; i < TEST_COUNT; i += stride ) {
          for ( j = 0; j < stride; j++ ) {
            kctx[ j ].set_key_hash( key[ j ] );
            kctx[ j ].prefetch( pcnt );
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
        for ( i = 0; i < TEST_COUNT; i++ ) {
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
        if ( map->get_ht_deltas( stats, ops, tot, ctx_id ) > 0 )
          print_ops( *map, ops, ival );
      }
    }
  }
  else if ( ::strcmp( oper, "incr" ) == 0 ) {
    KeyBuf kb;
    mono = current_monotonic_time_s();
    for ( i = 0, k = 0; i < 1000000; i++ ) {
      if ( k == 0 ) {
        kb.zero();
        kb.keylen = 2;
        kb.u.buf[ 0 ] = '0';
      }
      h += kb.hash( map->hdr.hash_key_seed, func );
      k  = ( k + 1 ) % TEST_COUNT;
      incr_key( kb );
    }
    mono = current_monotonic_time_s() - mono;
    printf( "hash = %.1fns %.1f/s\n",
            mono / 1000000.0 * 1000000000.0,
            1000000.0 / mono );

    sighndl.install();
    mono = current_monotonic_time_s();
    map->get_ht_deltas( stats, ops, tot, ctx_id );
    while ( ! sighndl.signaled ) {
      kb.zero();
      kb.keylen = 2;
      kb.u.buf[ 0 ] = '0';
      if ( use_prefetch > 1 ) {
        const uint32_t stride = use_prefetch;
        KeyCtx * kctx = KeyCtx::new_array( map, ctx_id, NULL, stride );
        KeyBufAligned * kbar = KeyBufAligned::new_array( stride );

        for ( i = 0; i < TEST_COUNT; i += stride ) {
          for ( j = 0; j < stride; j++ ) {
            incr_key( kb );
            kbar[ j ] = kb;
          }
          for ( j = 0; j < stride; j++ ) {
            kctx[ j ].set_key_hash( kbar[ j ] );
            kctx[ j ].prefetch( pcnt );
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
        for ( i = 0; i < TEST_COUNT; i++ ) {
          kctx.set_hash( kb.hash( map->hdr.hash_key_seed, func ) );
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
        if ( map->get_ht_deltas( stats, ops, tot, ctx_id ) > 0 )
          print_ops( *map, ops, ival );
      }
    }
  }
  else if ( ::strcmp( oper, "int" ) == 0 ) {
    KeyBuf ukb;
    mono = current_monotonic_time_s();
    ukb.zero();
    for ( i = 0, k = 0; i < 1000000; i++ ) {
      ukb.set( k );
      h += ukb.hash( map->hdr.hash_key_seed, func );
      k  = ( k + 1 ) % TEST_COUNT;
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
      h += akb.hash( map->hdr.hash_key_seed, func );
      k  = ( k + 1 ) % TEST_COUNT;
    }
    mono = current_monotonic_time_s() - mono;
    printf( "aligned hash = %.1fns %.1f/s\n",
            mono / 1000000.0 * 1000000000.0,
            1000000.0 / mono );

    sighndl.install();
    mono = current_monotonic_time_s();
    map->get_ht_deltas( stats, ops, tot, ctx_id );
    while ( ! sighndl.signaled ) {
      if ( use_prefetch > 1 ) {
        const uint32_t stride = use_prefetch;
        KeyCtx * kctx = KeyCtx::new_array( map, ctx_id, NULL, stride );
        KeyBufAligned * kbar = KeyBufAligned::new_array( stride );

        for ( i = 0; i < TEST_COUNT; i += stride ) {
          for ( j = 0; j < stride; j++ ) {
            kbar[ j ].set( i + j );
            kctx[ j ].set_key_hash( kbar[ j ] );
            kctx[ j ].prefetch( pcnt );
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
        for ( i = 0; i < TEST_COUNT; i++ ) {
          kb.set( i );
          kctx.set_hash( kb.hash( map->hdr.hash_key_seed, func ) );
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
        if ( map->get_ht_deltas( stats, ops, tot, ctx_id ) > 0 )
          print_ops( *map, ops, ival );
      }
    }
  }
  printf( "bye\n" );
  map->detach_ctx( ctx_id );
  delete map;

  return 0;
}

