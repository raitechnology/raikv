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
    ctx_id = map->attach_ctx( 1000 /*::getpid()*/, 0, 0 );
    fputs( print_map_geom( map, ctx_id ), stdout );
  }
}

static void
shm_close( void )
{
  if ( ctx_id != MAX_CTX_ID ) {
    HashCounters & stat = map->ctx[ ctx_id ].stat1;
    printf( "rd %" PRIu64 ", wr %" PRIu64 ", "
            "sp %" PRIu64 ", ch %" PRIu64 "\n",
            stat.rd, stat.wr, stat.spins, stat.chains );
    map->detach_ctx( ctx_id );
    ctx_id = MAX_CTX_ID;
  }
  delete map;
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
  SignalHandler sighndl;
  bool do_publish = false;
  int count = 0, begin = 0;

  /* [sysv2m:shm.test] [file] [cnt] [pre] */
  const char * mn = get_arg( argc, argv, 1, "-m", "sysv2m:shm.test" ),
             * su = get_arg( argc, argv, 1, "-s", "subject" ),
             * be = get_arg( argc, argv, 1, "-b", "0" ),
             * cn = get_arg( argc, argv, 1, "-c", "400" ),
             * pu = get_arg( argc, argv, 0, "-p", NULL ),
             * tr = get_arg( argc, argv, 0, "-t", NULL ),
             * ve = get_arg( argc, argv, 0, "-v", NULL ),
             * he = get_arg( argc, argv, 0, "-h", 0 );

  if ( he != NULL ) {
    fprintf( stderr, "raikv version: %s\n", kv_stringify( KV_VER ) );
    fprintf( stderr,
     "%s [-m map] [-s subject] [-b first] [-c cnt] [-p] [-t] [-v]\n"
     "  map             = name of map file (prefix w/ file:, sysv:, posix:)\n"
     "  subject         = subject to subscribe publish\n"
     "  first           = first sequence (default: 0)\n" 
     "  count           = number of publish (default: 400)\n" 
     "  publish         = do publish or subscribe\n"
     "  publish vector  = do publish vector of messages\n"
     "  trim            = trim message queue\n",
             argv[ 0 ]);
    return 1;
  }

  if ( cn != NULL ) {
    count = atoi( cn );
  }
  if ( be != NULL ) {
    begin = atoi( be );
  }
  if ( pu != NULL || ve != NULL ) {
    do_publish = true;
  }
  shm_attach( mn );
  if ( map == NULL )
    return 1;
  sighndl.install();

  KeyBuf      kb;
  WorkAlloc8k wrk;
  KeyCtx      kctx( *map, ctx_id, &kb );
  void      * ptr,
            * data[ 1024 ];
  uint64_t    data_sz[ 1024 ],
              data_buf[ 2048 ];
  uint64_t    h1, h2, sz, i, j, k, t, t2,
              sum = 0, seqno = 0, sum_count = 0, m = 0,
              first = 0, last = 0;
  bool        is_first = true;
  KeyStatus   status;

  kb.zero();
  kb.keylen = ::strlen( su ) + 1;
  if ( kb.keylen > MAX_KEY_BUF_SIZE )
    kb.keylen = MAX_KEY_BUF_SIZE;
  ::strcpy( kb.u.buf, su );
  map->hdr.get_hash_seed( 0, h1, h2 );
  kb.hash( h1, h2 );
  kctx.set_hash( h1, h2 );

  if ( tr != NULL ) {
    if ( (status = kctx.acquire( &wrk )) <= KEY_IS_NEW ) {
      kctx.trim_msg( (uint64_t) begin );
      kctx.release();
    }
  }
  else {
    for ( i = (uint64_t) begin; count > 0; ) {
      if ( do_publish ) {
        if ( (status = kctx.acquire( &wrk )) <= KEY_IS_NEW ) {
          if ( ve != NULL ) {
            t = get_rdtsc();
            j = count;
            if ( j > sizeof( data ) / sizeof( data[ 0 ] ) )
              j = sizeof( data ) / sizeof( data[ 0 ] );
            for ( k = 0; k < j; k++ ) {
              data[ k ] = &data_buf[ k * 2 ];
              data_buf[ k * 2 ] = t;
              data_buf[ k * 2 + 1 ] = i + k;
              data_sz[ k ] = 8 + 8;
            }
            if ( kctx.append_vector( j, data, data_sz ) == KEY_OK ) {
              count -= j;
              i += j;
            }
          }
          else {
            sz = 8 + 8;
            if ( (status = kctx.append_msg( &ptr, sz )) == KEY_OK ) {
              t = get_rdtsc();
              uint64_t *n = (uint64_t *) ptr;
              n[ 0 ] = t;
              n[ 1 ] = i;
            }
            i++;
            count--;
          }
          kctx.release();
        }
      }
      else {
        if ( (status = kctx.find( &wrk )) == KEY_OK ) {
          if ( (size_t) count < sizeof( data ) / sizeof( data[ 0 ] ) )
            j = i + count;
          else
            j = i + sizeof( data ) / sizeof( data[ 0 ] );
          seqno = i;
          if ( kctx.msg_value( seqno, j, data, data_sz ) == KEY_OK ) {
            if ( is_first ) {
              first = seqno;
              last  = j;
              is_first = false;
            }
            else {
              last = j;
            }
            t = get_rdtsc();
            for ( k = seqno; k < j; k++ ) {
              uint64_t *n = (uint64_t *) data[ k - seqno ];
              t2 = n[ 0 ];
              m += n[ 1 ];
              sum += t - t2;
              sum_count++;
              count--;
            }
            i = j;
          }
          /*else {
            i += sizeof( data ) / sizeof( data[ 0 ] );
          }*/
        }
      }
      if ( sighndl.signaled )
        break;
    }
    if ( sum > 0 ) {
      printf( "%lu (%lu) m (%lu) %lu -> %lu\n",
              sum/sum_count, sum_count, m/sum_count, first, last );
    }
  }
  shm_close();
  return 0;
}

