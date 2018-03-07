#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <benchmark/benchmark.h>
#include <raikv/shm_ht.h>

using namespace rai;
using namespace kv;

HashTabGeom geom;
HashTab   * map;
uint32_t    ctx_id = MAX_CTX_ID;

static const uint64_t MAP_SIZE = 8 * 1024 * 1024;

static uint32_t max( uint32_t x,  uint32_t y ) { return ( x > y ? x : y ); }

static void
shm_open( void )
{
  geom.map_size         = MAP_SIZE;
  geom.max_value_size   = 0;
  geom.hash_entry_size  = 64;
  geom.hash_value_ratio = 1;
  //map = HashTab::attach_map( "myshm", geom );
  map = HashTab::alloc_map( geom );
  printf( "geom map size %" PRIu64 ", entry size %" PRIu32 ", "
                "value size %" PRIu32 ", ratio %.1f "
                "ht size %" PRIu64 "\n",
           geom.map_size, geom.hash_entry_size,
        max( map->hdr.max_immed_value_size, map->hdr.max_segment_value_size ),
           geom.hash_value_ratio, map->hdr.ht_size );
  ctx_id = map->attach_ctx( ::getpid() );
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
  /*if ( map != NULL )
    map->close();*/
}

void
BM_Hash( benchmark::State& state )
{
  KeyBuf   kb;
  uint64_t i = 0;
  for ( auto _ : state ) {
    kb.set( i );
    kb.hash();
    i = ( i + 1 ) % ( MAP_SIZE / 256 );
  }
}
BENCHMARK( BM_Hash );

void
BM_Insert4( benchmark::State& state )
{
  KeyBuf kb[ 4 ];
  KeyCtx kctx[ 4 ];
  uint64_t i = 0;
  void   * p;
  uint32_t j;
  KeyCtx::init_array( 4, kctx, map, ctx_id, kb );
  for ( auto _ : state ) {
    for ( j = 0; j < 4; j++ ) {
      uint64_t k = i + j;
      kb[ j ].set( k );
    }
    KeyCtx::prefetch_array( 4, kctx );
    for ( j = 0; j < 4; j++ ) {
      if ( kctx[ j ].acquire() <= KEY_IS_NEW ) {
        if ( kctx[ j ].resize( &p, 6 ) == KEY_OK )
          ::memcpy( p, "world", 6 );
        kctx[ j ].release();
      }
    }
    i = ( i + 4 ) % ( MAP_SIZE / 256 );
  }
}
BENCHMARK( BM_Insert4 );

void
BM_Insert( benchmark::State& state )
{
  KeyBuf kb;
  KeyCtx kctx( map, ctx_id );
  uint64_t i = 0;
  void   * p;
  kctx.set_key( &kb );
  for ( auto _ : state ) {
    kb.set( i );
    kctx.set_hash( kb.hash() );
    if ( kctx.acquire() <= KEY_IS_NEW ) {
      if ( kctx.resize( &p, 6 ) == KEY_OK )
        ::memcpy( p, "world", 6 );
      kctx.release();
    }
    i = ( i + 1 ) % ( MAP_SIZE / 256 );
  }
}
BENCHMARK( BM_Insert );

void
BM_Iterate4( benchmark::State& state )
{
  CacheLine wrk[ 8 ];
  KeyBuf kb[ 4 ];
  KeyCtx kctx[ 4 ];
  uint64_t i = 0;
  uint32_t j;
  KeyCtx::init_array( 4, kctx, map, ctx_id, kb );
  for ( auto _ : state ) {
    for ( j = 0; j < 4; j++ ) {
      uint64_t k = i + j;
      kb[ j ].set( k );
    }
    KeyCtx::prefetch_array( 4, kctx );
    for ( j = 0; j < 4; j++ )
      kctx[ j ].find( wrk, 8 );
    i = ( i + 4 ) % ( MAP_SIZE / 256 );
  }
}
BENCHMARK( BM_Iterate4 );

void
BM_Iterate( benchmark::State& state )
{
  CacheLine wrk[ 8 ];
  KeyBuf kb;
  KeyCtx kctx( map, ctx_id );
  uint64_t i = 0;
  kctx.set_key( &kb );
  for ( auto _ : state ) {
    kb.set( i );
    kctx.set_hash( kb.hash() );
    kctx.find( wrk, 8 );
    i = ( i + 1 ) % ( MAP_SIZE / 256 );
  }
}
BENCHMARK( BM_Iterate );

int
main( int argc, char *argv[] )
{
  shm_open();
  benchmark::Initialize( &argc, argv );
  benchmark::RunSpecifiedBenchmarks();
  shm_close();
  return 0;
}

