#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/uint_ht.h>
#include <raikv/key_hash.h>
#include <raikv/util.h>

using namespace rai;
using namespace kv;

template <class Int, uint64_t iters>
void
do_test( void )
{
  IntHashTabT<Int> *ht = IntHashTabT<Int>::resize( NULL );
  const char *s = ( sizeof( Int ) == 4 ? "uint32" : "uint64" );
  Int i, rszcnt = 0, pos, v;
  uint64_t t;

  printf( "\ninsert %lu elements (%s):\n", iters, s );
  t = kv_current_monotonic_time_ns();
  for ( i = 0; i < iters; i++ ) {
    ht->upsert( kv_crc_c( &i, sizeof( i ), 0 ), i );
    if ( ht->need_resize() ) {
      ht = IntHashTabT<Int>::resize( ht );
      rszcnt++;
    }
  }
  t = kv_current_monotonic_time_ns() - t;
  printf( "ns per ins %.2f\n",  (double) t / (double) iters );
  printf( "count %lu\n", (uint64_t) ht->elem_count );
  printf( "resized %lu\n", (uint64_t) rszcnt );

  printf( "\nfind all %lu elements (%s):\n", iters, s );
  t = kv_current_monotonic_time_ns();
  for ( i = 0; i < iters; i++ ) {
    if ( ! ht->find( kv_crc_c( &i, sizeof( i ), 0 ), pos, v ) ) {
      fprintf( stderr, "not found %lu\n", (uint64_t) i );
    }
    else if ( v != i ) {
      fprintf( stderr, "collision %lu != %lu\n", (uint64_t) v, (uint64_t) i );
    }
  }
  t = kv_current_monotonic_time_ns() - t;
  printf( "ns per find %.2f\n",  (double) t / (double) iters );
  printf( "count %lu\n", (uint64_t) ht->elem_count );

  printf( "\ndelete even elements (%s):\n", s );
  t = kv_current_monotonic_time_ns();
  for ( i = 0; i < iters; i += 2 ) {
    if ( ! ht->find( kv_crc_c( &i, sizeof( i ), 0 ), pos, v ) ) {
      fprintf( stderr, "not found %lu\n", (uint64_t) i );
    }
    else {
      ht->remove( pos );
    }
  }
  t = kv_current_monotonic_time_ns() - t;
  printf( "ns per del %.2f\n",  (double) t / (double) ( iters / 2 ));
  printf( "resized %lu\n", (uint64_t) rszcnt );
  printf( "count %lu\n", (uint64_t) ht->elem_count );

  t = kv_current_monotonic_time_ns();
  if ( ht->need_resize() )
    ht = IntHashTabT<Int>::resize( ht );
  t = kv_current_monotonic_time_ns() - t;
  printf( "resize %lu ns\n", t );

  printf( "\ndelete odd elements (%s):\n", s );
  t = kv_current_monotonic_time_ns();
  for ( i = 1; i < iters; i += 2 ) {
    if ( ! ht->find( kv_crc_c( &i, sizeof( i ), 0 ), pos, v ) ) {
      fprintf( stderr, "not found %lu\n", (uint64_t) i );
    }
    else {
      ht->remove( pos );
    }
  }
  t = kv_current_monotonic_time_ns() - t;
  printf( "ns per del %.2f\n",  (double) t / (double) ( iters / 2 ));
  printf( "count %lu\n", (uint64_t) ht->elem_count );

  t = kv_current_monotonic_time_ns();
  if ( ht->need_resize() )
    ht = IntHashTabT<Int>::resize( ht );
  t = kv_current_monotonic_time_ns() - t;
  printf( "resize %lu ns\n", t );
}

int
main( void )
{
  do_test<uint32_t, 100000>();
  do_test<uint64_t, 100000>();

  return 0;
}

