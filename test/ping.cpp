#include <stdio.h>
#include <stdint.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <string.h>
#include <stdlib.h>
#ifndef _MSC_VER
#include <unistd.h>
#else
#include <raikv/win.h>
#endif
#include <raikv/shm_ht.h>
#include <raikv/key_buf.h>

using namespace rai;
using namespace kv;

HashTabGeom geom;
HashTab   * map;
uint32_t    ctx_id = MAX_CTX_ID,
            dbx_id = MAX_STAT_ID;

static void
shm_attach( const char *mn )
{
  map = HashTab::attach_map( mn, 0, geom );
  if ( map != NULL ) {
    ctx_id = map->attach_ctx( ::getpid() );
    dbx_id = map->attach_db( ctx_id, 0 );
    fputs( print_map_geom( map, ctx_id ), stdout );
  }
}

static void
shm_close( void )
{
  if ( ctx_id != MAX_CTX_ID ) {
    HashCounters & stat = map->stats[ dbx_id ];
    printf( "rd %" PRIu64 ", wr %" PRIu64 ", "
            "sp %" PRIu64 ", ch %" PRIu64 "\n",
            stat.rd, stat.wr, stat.spins, stat.chains );
    map->detach_ctx( ctx_id );
    ctx_id = MAX_CTX_ID;
  }
  delete map;
}

#if 0
static void
my_yield( void )
{
  pthread_yield();
}
#endif

void
do_spin( uint64_t cnt )
{
#ifndef _MSC_VER
  __asm__ __volatile__(
       "xorq %%rcx, %%rcx\n\t"
    "1: addq $1, %%rcx\n\t"
       "cmpq %0, %%rcx\n\t"
       "jne 1b\n\t"
    :
    : "r" (cnt)
    : "%rcx" );
#else
  while ( cnt-- )
    Sleep( 0 );
#endif
}

struct PingRec {
  uint64_t ping_time,
           pong_time,
           serial;
};

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
  KeyBuf        pingkb, pongkb;
  WorkAlloc8k   wrk;
  uint64_t      size;
  uint64_t      last_serial = 0, mbar = 0, spin_times;
  uint64_t      last_count = 0, count = 0, nsdiff = 0, diff, t, last = 0;
  bool          use_pause, use_none, use_spin;

  const char * mn = get_arg( argc, argv, 1, "-m", KV_DEFAULT_SHM ),
             * pi = get_arg( argc, argv, 1, "-p", "ping" ),
             * sp = get_arg( argc, argv, 1, "-s", "pause" ),
             * he = get_arg( argc, argv, 0, "-h", NULL );

  /* do pong first */
  if ( he != NULL ) {
    fprintf( stderr, "raikv version: %s\n", kv_stringify( KV_VER ) );
    fprintf( stderr,
     "%s [-m map] [-p ping|pong] [-s pause|none|spin]\n"
     "  map             -- name of map file (prefix w/ file:, sysv:, posix:)\n"
     "  ping|pong       -- which queue to read\n"
     "  pause|spin|none -- method of read spin loop\n",
             argv[ 0 ]);
    return 1;
  }

  pingkb.set_string( "ping" );
  pongkb.set_string( "pong" );
  spin_times = 200;
#if 0
  uint64_t t2;
  do {
    t = current_monotonic_time_ns();
    spin_times++;
    do_spin( spin_times * 100000 );
    t2 = current_monotonic_time_ns();
  } while ( t2 - t < 15 * 100000 );
  printf( "do_spin ( %" PRIu64 " ) = 15ns\n", spin_times );
#endif
  shm_attach( mn );
  if ( map == NULL )
    return 1;
  sighndl.install();

  KeyCtx pingctx( *map, dbx_id, &pingkb ),
         pongctx( *map, dbx_id, &pongkb );
  HashSeed hs;
  uint64_t h1, h2;
  map->hdr.get_hash_seed( 0, hs );
  hs.get( h1, h2 );
  pingkb.hash( h1, h2 );
  pingctx.set_hash( h1, h2 );
  hs.get( h1, h2 );
  pongkb.hash( h1, h2 );
  pongctx.set_hash( h1, h2 );

  if ( sp != NULL && ::strcmp( sp, "pause" ) == 0 )
    use_pause = true;
  else
    use_pause = false;
  if ( sp != NULL && ::strcmp( sp, "none" ) == 0 )
    use_none = true;
  else
    use_none = false;
  use_spin = ( ! use_pause && ! use_none );
  printf( "use_pause %s\n", use_pause ? "true" : "false" );
  printf( "use_spin  %s\n", use_spin ? "true" : "false" );
  printf( "use_none  %s\n", use_none ? "true" : "false" );
  fflush( stdout );

  if ( ::strcmp( pi, "ping" ) == 0 ) {
    PingRec init, *ptr;
    ::memset( &init, 0, sizeof( init ) );
    //init.ping_time = current_monotonic_time_ns();
    init.ping_time = get_rdtsc();
    init.serial    = 1;
    if ( pingctx.acquire( &wrk ) <= KEY_IS_NEW ) {
      if ( pingctx.resize( &ptr, sizeof( PingRec ) ) == KEY_OK )
        *ptr = init;
      pingctx.release();
    }
    if ( pongctx.acquire( &wrk ) <= KEY_IS_NEW ) {
      if ( pongctx.value( &ptr, size ) == KEY_OK ) {
        init = *ptr;
        init.serial++;
        if ( pongctx.resize( &ptr, sizeof( PingRec ) ) == KEY_OK )
          *ptr = init;
      }
      pongctx.release();
    }
    last_serial = 1;
    while ( ! sighndl.signaled ) {
      if ( pingctx.find( &wrk ) == KEY_OK &&
           pingctx.value( &ptr, size ) == KEY_OK ) {
        if ( size >= sizeof( PingRec ) &&
             ptr->serial > last_serial ) {
          uint64_t pong_time = ptr->pong_time;
          last_serial = ptr->serial;
          t = get_rdtsc();
          if ( t > ptr->ping_time ) {
            diff = t - ptr->ping_time;
            nsdiff += diff;
            count++;
          }
          KeyStatus status;
          if ( (status = pongctx.acquire( &wrk )) <= KEY_IS_NEW ) {
            if ( (status = pongctx.value( &ptr, size )) == KEY_OK ) {
              init = *ptr;
              init.serial++;
              init.ping_time = t;
              init.pong_time = pong_time;
              if ( pongctx.resize( &ptr, sizeof( PingRec ) ) == KEY_OK ) {
                *ptr = init;
              }
            }
            else {
              printf( "status %d\n", status );
            }
            pongctx.release();
          }
          else {
            printf( "acquire status %d\n", status );
          }
          if ( t - last >= (uint64_t) 2000000000U ) {
            double mb = (double) mbar / (double) ( count - last_count );
            printf( "ping %.1fcy mb=%.1f %" PRIu64 "\n",
                    (double) ( t - last ) / (double) ( count - last_count ),
                    mb, spin_times );
            last_count = count;
            last = t;
            if ( use_spin ) {
              if ( mb > 0.6 )
                spin_times++;
              else if ( mb <= 0.6 && spin_times > 1 )
                spin_times--;
            }
            mbar = 0;
          }
          goto got_ping;
        }
      }
      mbar++;
      //memory_barrier();
    got_ping:;
      //usleep( 100 );
      if ( use_pause )
        kv_sync_pause();
      else if ( use_spin )
        do_spin( spin_times );
    }
  }
  else {
    PingRec init, *ptr;
    ::memset( &init, 0, sizeof( init ) );
    //init.ping_time = current_monotonic_time_ns();
    init.ping_time = get_rdtsc();
    init.serial    = 1;
    if ( pongctx.acquire( &wrk ) <= KEY_IS_NEW ) {
      if ( pongctx.resize( &ptr, sizeof( PingRec ) ) == KEY_OK )
        *ptr = init;
      pongctx.release();
    }
    last_serial = 1;
    while ( ! sighndl.signaled ) {
      if ( pongctx.find( &wrk ) == KEY_OK &&
           pongctx.value( &ptr, size ) == KEY_OK ) {
        if ( size >= sizeof( PingRec ) &&
             ptr->serial > last_serial ) {
          uint64_t ping_time = ptr->ping_time;
          last_serial = ptr->serial;
          init = *ptr;
          t = get_rdtsc();
          if ( t > ptr->pong_time ) {
            diff = t - ptr->pong_time;
            nsdiff += diff;
            count++;
          }
          KeyStatus status;
          if ( (status = pingctx.acquire( &wrk )) <= KEY_IS_NEW ) {
            if ( (status = pingctx.value( &ptr, size )) == KEY_OK ) {
              init = *ptr;
              init.serial++;
              init.pong_time = t;
              init.ping_time = ping_time;
              if ( pingctx.resize( &ptr, sizeof( PingRec ) ) == KEY_OK ) {
                *ptr = init;
              }
            }
            else {
              printf( "status %d\n", status );
            }
            pingctx.release();
          }
          else {
            printf( "acquire status %d\n", status );
          }
          if ( t - last >= (uint64_t) 2000000000U ) {
            double mb = (double) mbar / (double) ( count - last_count );
            printf( "pong %.1fcy mb=%.1f %" PRIu64 "\n",
                    (double) ( t - last ) / (double) ( count - last_count ),
                    mb, spin_times );
            last_count = count;
            last = t;
            if ( use_spin ) {
              if ( mb > 0.6 )
                spin_times++;
              else if ( mb <= 0.6 && spin_times > 1 )
                spin_times--;
            }
            mbar = 0;
          }
          goto got_pong;
        }
      }
      mbar++;
      //memory_barrier();
    got_pong:;
      //usleep( 100 );
      if ( use_pause )
        kv_sync_pause();
      else if ( use_spin )
        do_spin( spin_times );
    }
  }
  shm_close();
  printf( "count %" PRIu64 ", diff %.1f\n", count,
          (double) nsdiff / (double) count );
  return 0;
}

