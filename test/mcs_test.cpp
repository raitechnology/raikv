#include <stdio.h>
#include <time.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <raikv/atom.h>

using namespace rai;
using namespace kv;

struct CtxOwner;

struct MCSLock64 : public MCSLock<uint64_t, CtxOwner> {
};

struct Entry {
  AtomUInt64 h;
  uint32_t count;
};

struct Ctx {
  MCSLock64  mcs;
  uint64_t spin, wait;
  uint32_t lck, unl, count, unused;
  uint64_t pad[ 2 ];
};

static const uint32_t CTX_CNT = 8;
static const uint32_t HT_CNT = 8;
Ctx   ctx[ CTX_CNT ];
Entry ht[ HT_CNT ];

struct CtxOwner {
  Ctx * ar;
  CtxOwner( Ctx *p ) : ar( p ) {}
  MCSLock64& owner( const uint64_t id ) {
    return this->ar[ id ].mcs;
  }
};

AtomUInt32 go;
static const uint64_t zombie = (uint64_t) 0x80000000U << 32;

void
loop( uint64_t my_id )
{
  CtxOwner closure( ctx );
  Ctx &me = ctx[ my_id ];
  while ( ! go.val )
    kv_sync_pause();

  for ( uint32_t i = 0; i < 1000000; i++ ) {
    uint32_t j = i % HT_CNT;
    uint64_t lock;
    me.lck = j;
    lock = ctx[ my_id ].mcs.acquire( ht[ j ].h, j, zombie, my_id,
                                     ctx[ my_id ].spin, closure );
    me.lck = 0;
    ht[ j ].count++;
    me.count++;
    me.unl = j;
    ctx[ my_id ].mcs.release( ht[ j ].h, lock, zombie, my_id,
                              ctx[ my_id ].wait, closure );
    me.unl = 0;
  }
}

void *
run( void *p )
{
  loop( (uint64_t) p );
  return 0;
}

int
main( int argc, char *argv[] )
{
  uint64_t i;
  pthread_t thr[ CTX_CNT ];
  for ( i = 0; i < HT_CNT; i++ ) {
    ht[ i ].h = i;
  }
  for ( i = 0; i < CTX_CNT; i++ ) {
    pthread_create( &thr[ i ], NULL, run, (void *) i );
  }
  go = 1;
  for ( i = 0; i < CTX_CNT; i++ ) {
    pthread_join( thr[ i ], NULL );
  }
  for ( i = 0; i < HT_CNT; i++ ) {
    printf( "ht [%lu]: count=%u\n", (uint64_t) ht[ i ].h, ht[ i ].count );
  }
  for ( i = 0; i < CTX_CNT; i++ ) {
    printf( "ctx[%lu]: spin=%lu wait=%lu\n", i, ctx[ i ].spin, ctx[ i ].wait );
  }
  return 0;
}

