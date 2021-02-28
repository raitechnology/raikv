#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/uint_ht.h>
#include <raikv/key_hash.h>
#include <raikv/util.h>

using namespace rai;
using namespace kv;

template <class Int, class Value, size_t iters>
void
do_test( const char *kind )
{
  IntHashTabT<Int,Value> *ht = IntHashTabT<Int,Value>::resize( NULL );
  size_t   pos, i, rszcnt = 0;
  Value    v;
  uint64_t t;

  printf( "\nsizeof( 64 ): %lu\n", IntHashTabT<Int,Value>::alloc_size( 64 ) );
  printf( "insert %lu elements (%s):\n", iters, kind );
  t = kv_current_monotonic_time_ns();
  for ( i = 0; i < iters; i++ ) {
    ht->upsert( Int( kv_crc_c( &i, sizeof( i ), 0 ) ), Value( i ) );
    if ( ht->need_resize() ) {
      ht = IntHashTabT<Int,Value>::resize( ht );
      rszcnt++;
    }
  }
  t = kv_current_monotonic_time_ns() - t;
  printf( "ns per ins %.2f\n",  (double) t / (double) iters );
  printf( "count %lu\n", (uint64_t) ht->elem_count );
  printf( "resized %lu\n", (uint64_t) rszcnt );

  printf( "\nfind all %lu elements (%s):\n", iters, kind );
  t = kv_current_monotonic_time_ns();
  for ( i = 0; i < iters; i++ ) {
    if ( ! ht->find( Int( kv_crc_c( &i, sizeof( i ), 0 ) ), pos, v ) ) {
      fprintf( stderr, "not found %lu\n", (uint64_t) i );
    }
    else if ( v != i ) {
      fprintf( stderr, "collision %lu != %lu\n", (uint64_t) v, (uint64_t) i );
    }
  }
  t = kv_current_monotonic_time_ns() - t;
  printf( "ns per find %.2f\n",  (double) t / (double) iters );
  printf( "count %lu\n", (uint64_t) ht->elem_count );

  printf( "\ndelete even elements (%s):\n", kind );
  t = kv_current_monotonic_time_ns();
  for ( i = 0; i < iters; i += 2 ) {
    if ( ! ht->find( Int( kv_crc_c( &i, sizeof( i ), 0 ) ), pos, v ) ) {
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
    ht = IntHashTabT<Int,Value>::resize( ht );
  t = kv_current_monotonic_time_ns() - t;
  printf( "resize %lu ns\n", t );

  printf( "\ndelete odd elements (%s):\n", kind );
  t = kv_current_monotonic_time_ns();
  for ( i = 1; i < iters; i += 2 ) {
    if ( ! ht->find( Int( kv_crc_c( &i, sizeof( i ), 0 ) ), pos, v ) ) {
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
    ht = IntHashTabT<Int,Value>::resize( ht );
  t = kv_current_monotonic_time_ns() - t;
  printf( "resize %lu ns\n", t );
}

struct Hash {
  uint32_t hash[ 4 ];
  Hash() {}
  Hash( uint32_t h ) {
    for ( size_t i = 0; i < 4; i++ )
      this->hash[ i ] = h;
  }
  bool operator==( const Hash &h1 ) const {
    for ( size_t i = 0; i < 4; i++ )
      if ( this->hash[ i ] != h1.hash[ i ] )
        return false;
    return true;
  }
  Hash &operator=( const Hash &h1 ) {
    for ( size_t i = 0; i < 4; i++ )
      this->hash[ i ] = h1.hash[ i ];
    return *this;
  }
  size_t operator&( size_t mod ) const {
    return ( ( (size_t) this->hash[ 0 ] << 32 ) |
               (size_t) this->hash[ 1 ] ) & mod;
  }
};
int
main( void )
{
  const char *t1 = "uint32_t*2", *t2 = "uint64_t*2", *t3 = "Hash*uint32_t";
  do_test<uint32_t, uint32_t, 100000>( t1 );
  do_test<uint64_t, uint64_t, 100000>( t2 );
  do_test<Hash, uint32_t, 100000>( t3 );

  return 0;
}

