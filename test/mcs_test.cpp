#include <stdio.h>
#include <time.h>
#include <stdint.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <string.h>
#ifndef _MSC_VER
#include <unistd.h>
#include <pthread.h>
#else
#include <raikv/win.h>
#endif
#include <raikv/atom.h>

using namespace rai;
using namespace kv;

struct Ctx64Owner;
struct MCSLock64 : public MCSLock<uint64_t, Ctx64Owner> {
};
struct Entry64 {
  AtomUInt64 h;
  uint32_t count;
};
struct Ctx64 {
  MCSLock64 mcs;
  uint64_t  spin, wait;
  uint32_t  lck, unl, count, unused;
};

struct Ctx32Owner;
struct MCSLock32 : public MCSLock<uint32_t, Ctx32Owner> {
};
struct Entry32 {
  AtomUInt32 h;
  uint32_t   count;
};
struct Ctx32 {
  MCSLock32 mcs;
  uint32_t  unused2;
  uint64_t  spin, wait;
  uint32_t  lck, unl, count, unused;
};

static const uint32_t CTX_CNT = 8;
static const uint32_t HT_CNT = 8;
Ctx64   ctx64[ CTX_CNT ];
Entry64 ht64[ HT_CNT ];

Ctx32   ctx32[ CTX_CNT ];
Entry32 ht32[ HT_CNT ];

struct Ctx64Owner {
  Ctx64 * ar;
  Ctx64Owner( Ctx64 *p ) : ar( p ) {}
  MCSLock64& owner( const uint64_t id ) {
    return this->ar[ id ].mcs;
  }
};

struct Ctx32Owner {
  Ctx32 * ar;
  Ctx32Owner( Ctx32 *p ) : ar( p ) {}
  MCSLock32& owner( const uint64_t id ) {
    return this->ar[ id ].mcs;
  }
};

AtomUInt32 go;
static const uint64_t zombie64 = (uint64_t) 0x80000000U << 32;
static const uint32_t zombie32 = 0x80000000U;

void
loop64( uint64_t my_id )
{
  Ctx64Owner closure( ctx64 );
  Ctx64 &me = ctx64[ my_id ];

  printf( "start64 %" PRIu64 "\n", my_id );
  while ( ! go )
    kv_sync_pause();

  for ( uint32_t i = 0; i < 1000000; i++ ) {
    uint32_t j = i % HT_CNT;
    uint64_t lock;
    me.lck = j;
    lock = ctx64[ my_id ].mcs.acquire( ht64[ j ].h, j, zombie64, my_id,
                                       ctx64[ my_id ].spin, closure );
    me.lck = 0;
    ht64[ j ].count++;
    me.count++;
    me.unl = j;
    ctx64[ my_id ].mcs.release( ht64[ j ].h, lock, zombie64, my_id,
                                ctx64[ my_id ].wait, closure );
    me.unl = 0;
  }
  printf( "fini64 %" PRIu64 "\n", my_id );
}

void
loop32( uint64_t my_id )
{
  Ctx32Owner closure( ctx32 );
  Ctx32 &me = ctx32[ my_id ];

  printf( "start32 %" PRIu64 "\n", my_id );
  while ( ! go )
    kv_sync_pause();

  for ( uint32_t i = 0; i < 1000000; i++ ) {
    uint32_t j = i % HT_CNT;
    uint32_t lock;
    me.lck = j;
    lock = ctx32[ my_id ].mcs.acquire( ht32[ j ].h, j, zombie32, my_id,
                                       ctx32[ my_id ].spin, closure );
    me.lck = 0;
    ht32[ j ].count++;
    me.count++;
    me.unl = j;
    ctx32[ my_id ].mcs.release( ht32[ j ].h, lock, zombie32, my_id,
                                ctx32[ my_id ].wait, closure );
    me.unl = 0;
  }
  printf( "fini32 %" PRIu64 "\n", my_id );
}

#ifndef _MSC_VER
void *
run( void *p )
{
  loop64( (uint64_t) p );
  loop32( (uint64_t) p );
  return 0;
}
#else
DWORD WINAPI
run( void *p )
{
  loop64( (uint64_t) p );
  loop32( (uint64_t) p );
  return 0;
}
#endif

int
main( int, char ** )
{
  uint64_t i;
  printf( "sizeof MCSLock64 %u\n", (uint32_t) sizeof( MCSLock64 ) );
  printf( "sizeof Ctx64 %u\n", (uint32_t) sizeof( Ctx64 ) );
  printf( "sizeof MCSLock32 %u\n", (uint32_t) sizeof( MCSLock32 ) );
  printf( "sizeof Ctx32 %u\n", (uint32_t) sizeof( Ctx32 ) );

  for ( i = 0; i < HT_CNT; i++ ) {
    ht64[ i ].h = i;
    ht32[ i ].h = i;
  }
#ifndef _MSC_VER
  pthread_t thr[ CTX_CNT ];
  for ( i = 0; i < CTX_CNT; i++ ) {
    pthread_create( &thr[ i ], NULL, run, (void *) i );
  }
  go = 1;
  for ( i = 0; i < CTX_CNT; i++ ) {
    pthread_join( thr[ i ], NULL );
  }
#else
  HANDLE thr[ CTX_CNT ];
  for ( i = 0; i < CTX_CNT; i++ ) {
    thr[ i ] = CreateThread( NULL, 0, run, (void *) i, 0, NULL );
  }
  go = 1;
  WaitForMultipleObjects( CTX_CNT, thr, TRUE, INFINITE );
  for ( i = 0; i < CTX_CNT; i++ ) {
    CloseHandle( thr[ i ] );
  }
#endif
  for ( i = 0; i < HT_CNT; i++ ) {
    printf( "ht64 [%" PRIu64 "]: count=%u\n", (uint64_t) ht64[ i ].h,
            ht64[ i ].count );
  }
  for ( i = 0; i < CTX_CNT; i++ ) {
    printf( "ctx64[%" PRIu64 "]: spin=%" PRIu64 " wait=%" PRIu64 "\n", i,
            ctx64[ i ].spin, ctx64[ i ].wait );
  }
  for ( i = 0; i < HT_CNT; i++ ) {
    printf( "ht32 [%u]: count=%u\n", (uint32_t) ht32[ i ].h, ht32[ i ].count );
  }
  for ( i = 0; i < CTX_CNT; i++ ) {
    printf( "ctx32[%u]: spin=%" PRIu64 " wait=%" PRIu64 "\n", (uint32_t) i,
            ctx32[ i ].spin, ctx32[ i ].wait );
  }
  return 0;
}

