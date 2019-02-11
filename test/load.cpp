#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include <raikv/shm_ht.h>
#include <raikv/key_buf.h>

using namespace rai;
using namespace kv;

HashTabGeom geom;
HashTab   * map;
uint32_t    ctx_id = MAX_CTX_ID;

static void
shm_attach( const char *mn )
{
  map = HashTab::attach_map( mn, 0, geom );
  if ( map != NULL ) {
    ctx_id = map->attach_ctx( 1000 /*::getpid()*/, 0 );
    fputs( print_map_geom( map, ctx_id ), stdout );
  }
}

static void
shm_close( void )
{
  if ( ctx_id != MAX_CTX_ID ) {
    HashCounters & stat = map->ctx[ ctx_id ].stat;
    printf( "rd %" PRIu64 ", wr %" PRIu64 ", "
            "sp %" PRIu64 ", ch %" PRIu64 "\n",
            stat.rd, stat.wr, stat.spins, stat.chains );
    map->detach_ctx( ctx_id );
    ctx_id = MAX_CTX_ID;
  }
  delete map;
}

uint64_t
do_load( KeyCtx *kctx,  MsgCtx *mctx,  uint64_t count,  bool /*use_pref*/,
         uint64_t cur_time )
{
  WorkAlloc8k wrk;
  KeyStatus status;
  uint64_t loaded = 0, i = 0;
  do {
    /*if ( use_pref && i + 1 < count )
      kctx[ i + 1 ].prefetch( 1 );*/
    if ( (status = kctx[ i ].acquire( &wrk )) <= KEY_IS_NEW ) {
      kctx[ i ].update_stamps( 0, cur_time );
      status = kctx[ i ].load( mctx[ i ] );
      kctx[ i ].release();
    }
    if ( status != KEY_OK )
      mctx[ i ].nevermind();
    else
      loaded++;
  } while ( ++i < count );
  return loaded;
}
#if 0
uint64_t x_seg_values, x_immed_values, x_no_value, x_key_count, x_drops;
static void
compute_key_count( void )
{
  WorkAlloc8k wrk;
  KeyCtx        kctx( map, ctx_id );
  KeyStatus     status;
  x_seg_values = 0; x_immed_values = 0; x_no_value = 0;
  x_key_count = 0; x_drops = 0;
  for ( uint64_t pos = 0; pos < map->hdr.ht_size; pos++ ) {
    status = kctx.fetch( &wrk, pos );
    if ( status == KEY_OK ) {
      if ( kctx.entry->test( FL_DROPPED ) )
        x_drops++;
      if ( kctx.entry->test( FL_SEGMENT_VALUE ) )
        x_seg_values++;
      else if ( kctx.entry->test( FL_IMMEDIATE_VALUE ) )
        x_immed_values++;
      else
        x_no_value++;
      x_key_count++;
    }
  }
}
#endif
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
  SignalHandler sighndl;
  uint32_t precount = 4,
           use_pref = 1;

  /* [sysv2m:shm.test] [file] [cnt] [pre] */
  const char * mn = get_arg( argc, argv, 1, 1, "-m", "sysv2m:shm.test" ),
             * fn = get_arg( argc, argv, 2, 1, "-f", "file.dat" ),
             * cn = get_arg( argc, argv, 3, 1, "-c", "4" ),
             * pr = get_arg( argc, argv, 4, 1, "-p", "1" ),
             * he = get_arg( argc, argv, 0, 0, "-h", 0 );

  if ( he != NULL ) {
    fprintf( stderr, "raikv version: %s\n", kv_stringify( KV_VER ) );
    fprintf( stderr,
     "%s [-m map] [-f file] [-c cnt] [-p pre]\n"
     "  map             = name of map file (prefix w/ file:, sysv:, posix:)\n"
     "  file            = which file to load\n"
     "  count           = number of preloads (defaut 4)\n"
     "  prefetch        = if prefetch (defaut 1)\n",
             argv[ 0 ]);
    return 1;
  }

  if ( cn != NULL ) {
    precount = atoi( cn );
  }
  if ( pr != NULL ) {
    use_pref = atoi( pr );
  }
  shm_attach( mn );
  if ( map == NULL )
    return 1;
  sighndl.install();

  printf( "preload=%u, use_pref=%u\n", precount, use_pref );
  FILE *fp;
  if ( (fp = fopen( fn, "r" )) == NULL )
    perror( fn );
  else {
    KeyBufAligned * kba  = NULL;
    KeyCtx        * kctx = NULL;
    MsgCtx        * mctx = NULL;
    void          * ptr = NULL;
    char          * buf,
                    bufspc[ MAX_KEY_BUF_SIZE ],
                    sav[ MAX_KEY_BUF_SIZE ],
                    size[ 32 ];
    size_t          sz, maxsz = 0;
    uint64_t        i, j, last = 0, count = 0, loaded = 0,
                    totalb = 0, lastb = 0, allocfail = 0, lastallfail = 0;
    uint64_t        t1, t2, cur;
#if 0
    uint64_t        prev_add,
                    prev_drop,
                    prev_xseg,
                    prev_ximm,
                    prev_xnov,
                    prev_xkcnt,
                    prev_xdrop;
#endif
    if ( precount > 0 ) {
      kba  = KeyBufAligned::new_array( precount );
      kctx = KeyCtx::new_array( *map, ctx_id, NULL, precount );
      mctx = MsgCtx::new_array( *map, ctx_id, NULL, precount );
    }
    t1 = current_realtime_ns();
    cur = t1;
    sav[ 0 ] = '\0';
    for ( i = 0; ; ) {
      if ( precount > 0 )
        buf = kba[ i ].kb.u.buf;
      else
        buf = bufspc;
      if ( fgets( buf, MAX_KEY_BUF_SIZE, fp ) != NULL &&
           fgets( size, sizeof( size ), fp ) != NULL &&
           (sz = (size_t) atoi( size )) > 0 ) {
        ThrCtx & ctx = map->ctx[ ctx_id ];
        count++;
        totalb += sz;
#if 0
        prev_add   = ctx.stat.add;
        prev_drop  = ctx.stat.drop;
        prev_xseg  = x_seg_values;
        prev_ximm  = x_immed_values;
        prev_xnov  = x_no_value;
        prev_xkcnt = x_key_count;
        prev_xdrop = x_drops;
        compute_key_count();
        if ( (int64_t) ( ctx.stat.add - ctx.stat.drop ) !=
             (int64_t) ( x_key_count - x_drops ) ) {
          printf( "miscount count %lu key %s prev %s\n", count, buf, sav );
          printf( 
              "add %lu drop %lu total %lu\n"
              "seg %lu immed %lu no_val %lu key_count %lu drops %lu tot %lu\n",
              prev_add, prev_drop, prev_add - prev_drop,
              prev_xseg, prev_ximm, prev_xnov, prev_xkcnt, prev_xdrop,
              prev_xkcnt - prev_xdrop );
          printf( 
              "add %lu drop %lu total %lu\n"
              "seg %lu immed %lu no_val %lu key_count %lu drops %lu tot %lu\n",
              ctx.stat.add, ctx.stat.drop, ctx.stat.add - ctx.stat.drop,
              x_seg_values, x_immed_values, x_no_value, x_key_count, x_drops,
              x_key_count - x_drops );
          sighndl.signaled = true;
          break;
        }
#endif
        if ( ( count & 127 ) == 0 ) {
          t2 = current_realtime_ns();
          cur = t2;
          if ( ( t2 - t1 ) >= NANOS / 2 ) {
//            compute_key_count();
            printf( "%.1f msg/s %.1f bytes/s %.1f fail/s "
                "add %lu drop %lu total %lu\n",
         /* "seg %lu immed %lu no_val %lu key_count %lu drops %lu tot %lu\n",*/
                (double) ( count - last ) / ( (double) ( t2 - t1 ) / NANOSF ),
                (double) ( totalb - lastb ) / ( (double) ( t2 - t1 ) / NANOSF ),
                (double) ( allocfail - lastallfail ) /
                  ( (double) ( t2 - t1 ) / NANOSF ),
                ctx.stat.add, ctx.stat.drop, ctx.stat.add - ctx.stat.drop /*,
                x_seg_values, x_immed_values, x_no_value, x_key_count, x_drops,
                x_key_count - x_drops*/ );
            t1 = t2;
            last = count;
            lastb = totalb;
            lastallfail = allocfail;
          }
        }
        if ( precount > 0 ) {
          KeyFragment &kb = kba[ i ];
          kb.keylen = ::strlen( kb.u.buf );
          while ( kb.keylen > 0 && kb.u.buf[ kb.keylen - 1 ] <= ' ' )
            kb.u.buf[ --kb.keylen ] = '\0';
          kb.u.buf[ kb.keylen++ ] = '\0';
          if ( use_pref )
            mctx[ i ].prefetch_segment( sz );
          uint64_t h1, h2;
          map->hdr.get_hash_seed( 0, h1, h2 );
          kb.hash( h1, h2 );
          mctx[ i ].set_key( kb );
          mctx[ i ].set_hash( h1, h2 );
          kctx[ i ].set_key( kb );
          kctx[ i ].set_hash( h1, h2 );
          if ( use_pref )
            kctx[ i ].prefetch( 1 );
          if ( mctx[ i ].alloc_segment( &ptr, sz, 0 ) == KEY_OK ) {
            if ( fread( ptr, 1, sz, fp ) == sz ) {
              if ( ++i == precount ) {
                j = do_load( kctx, mctx, precount, use_pref, cur );
                loaded += j;
                mctx[ 0 ].prefetch_ptr = mctx[ i - 1 ].prefetch_ptr;
                i = 0;
                allocfail += ( precount - j );
              }
              else {
                mctx[ i ].prefetch_ptr = mctx[ i - 1 ].prefetch_ptr;
              }
            }
            else {
              mctx[ i ].nevermind();
            }
          }
          else {
            allocfail++;
          }
        }
        else {
          if ( sz > maxsz ) {
            ptr = realloc( ptr, sz * 2 );
            maxsz = sz * 2;
          }
          if ( fread( ptr, 1, sz, fp ) != sz ) {
            printf( "truncated msg\n" );
            break;
          }
        }
        ::strcpy( sav, buf );
      }
      else {
        fseek( fp, 0, SEEK_SET );
      }
      if ( sighndl.signaled )
        break;
    }
    if ( precount > 0 && i > 0 )
      loaded += do_load( kctx, mctx, i, use_pref, cur );
    printf( "%lu msgs read, %lu msgs loaded, %lu alloc fail\n",
            count, loaded, allocfail );
    fclose( fp );
  }
  shm_close();
  return 0;
}

