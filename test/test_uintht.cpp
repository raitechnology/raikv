#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
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
  size_t   pos, i;
  Value    v;
  uint64_t t, ins_time, find_time, even_time, odd_time;

  t = kv_current_monotonic_time_ns();
  for ( i = 0; i < iters; i++ ) {
    ht->upsert( Int( kv_hash_uint( (uint32_t) i ) ), Value( i ) );
    IntHashTabT<Int,Value>::check_resize( ht );
  }
  ins_time = kv_current_monotonic_time_ns() - t;

  t = kv_current_monotonic_time_ns();
  for ( i = 0; i < iters; i++ ) {
    if ( ! ht->find( Int( kv_hash_uint( (uint32_t) i ) ), pos, v ) ) {
      fprintf( stderr, "not found %" PRIu64 "\n", (uint64_t) i );
    }
    else if ( v != i ) {
      fprintf( stderr, "collision %" PRIu64 " != %" PRIu64 "\n", (uint64_t) v, (uint64_t) i );
    }
  }
  find_time = kv_current_monotonic_time_ns() - t;

  t = kv_current_monotonic_time_ns();
  for ( i = 0; i < iters; i += 2 ) {
    if ( ! ht->find( Int( kv_hash_uint( (uint32_t) i ) ), pos, v ) ) {
      fprintf( stderr, "not found %" PRIu64 "\n", (uint64_t) i );
    }
    else {
      ht->remove( pos );
      IntHashTabT<Int,Value>::check_resize( ht );
    }
  }
  even_time = kv_current_monotonic_time_ns() - t;

  t = kv_current_monotonic_time_ns();
  for ( i = 1; i < iters; i += 2 ) {
    if ( ! ht->find( Int( kv_hash_uint( (uint32_t) i ) ), pos, v ) ) {
      fprintf( stderr, "not found %" PRIu64 "\n", (uint64_t) i );
    }
    else {
      ht->remove( pos );
      IntHashTabT<Int,Value>::check_resize( ht );
    }
  }
  odd_time = kv_current_monotonic_time_ns() - t;

  printf( "%s ", kind );
  printf( "cnt %" PRIu64 "/%" PRIu64 ", ns per ins: %.2f, find %.2f, even %.2f, odd %.2f\n",
           iters, ht->elem_count,
           (double) ins_time / (double) iters,
           (double) find_time / (double) iters,
           (double) even_time / (double) iters / 2,
           (double) odd_time / (double) iters / 2 );
  delete ht;
}

template <class Int, class Value, size_t iters>
void
do_test2( const char *kind )
{
  IntHashTabX<Int,Value> ht;
  size_t   pos, i;
  Value    v;
  uint64_t t, ins_time, find_time, even_time, odd_time;

  t = kv_current_monotonic_time_ns();
  for ( i = 0; i < iters; i++ ) {
    ht.upsert( Int( kv_hash_uint( (uint32_t) i ) ), Value( i ) );
  }
  ins_time = kv_current_monotonic_time_ns() - t;

  t = kv_current_monotonic_time_ns();
  for ( i = 0; i < iters; i++ ) {
    if ( ! ht.find( Int( kv_hash_uint( (uint32_t) i ) ), pos, v ) ) {
      fprintf( stderr, "not found %" PRIu64 "\n", (uint64_t) i );
    }
    else if ( v != i ) {
      fprintf( stderr, "collision %" PRIu64 " != %" PRIu64 "\n", (uint64_t) v, (uint64_t) i );
    }
  }
  find_time = kv_current_monotonic_time_ns() - t;

  t = kv_current_monotonic_time_ns();
  for ( i = 0; i < iters; i += 2 ) {
    if ( ! ht.find( Int( kv_hash_uint( (uint32_t) i ) ), pos ) ) {
      fprintf( stderr, "not found %" PRIu64 "\n", (uint64_t) i );
    }
    else {
      ht.remove( pos );
    }
  }
  even_time = kv_current_monotonic_time_ns() - t;

  t = kv_current_monotonic_time_ns();
  for ( i = 1; i < iters; i += 2 ) {
    if ( ! ht.find( Int( kv_hash_uint( (uint32_t) i ) ), pos ) ) {
      fprintf( stderr, "not found %" PRIu64 "\n", (uint64_t) i );
    }
    else {
      ht.remove( pos );
    }
  }
  odd_time = kv_current_monotonic_time_ns() - t;

  printf( "%s ", kind );
  printf( "cnt %" PRIu64 "/%" PRIu64 ", ns per ins: %.2f, find %.2f, even %.2f, odd %.2f\n",
           iters, ht.elem_count,
           (double) ins_time / (double) iters,
           (double) find_time / (double) iters,
           (double) even_time / (double) iters / 2,
           (double) odd_time / (double) iters / 2 );
}

template <class Int, size_t iters>
void
do_test3( const char *kind )
{
  IntHashTabU<Int> *ht = IntHashTabU<Int>::resize( NULL );
  size_t   pos, i;
  uint64_t t, ins_time, find_time, even_time, odd_time;

  t = kv_current_monotonic_time_ns();
  for ( i = 0; i < iters; i++ ) {
    ht->upsert( Int( kv_hash_uint( (uint32_t) i ) ) );
    IntHashTabU<Int>::check_resize( ht );
  }
  ins_time = kv_current_monotonic_time_ns() - t;

  t = kv_current_monotonic_time_ns();
  for ( i = 0; i < iters; i++ ) {
    if ( ! ht->find( Int( kv_hash_uint( (uint32_t) i ) ), pos ) ) {
      fprintf( stderr, "not found %" PRIu64 "\n", (uint64_t) i );
    }
  }
  find_time = kv_current_monotonic_time_ns() - t;

  t = kv_current_monotonic_time_ns();
  for ( i = 0; i < iters; i += 2 ) {
    if ( ! ht->find( Int( kv_hash_uint( (uint32_t) i ) ), pos ) ) {
      fprintf( stderr, "not found %" PRIu64 "\n", (uint64_t) i );
    }
    else {
      ht->remove( pos );
      IntHashTabU<Int>::check_resize( ht );
    }
  }
  even_time = kv_current_monotonic_time_ns() - t;

  t = kv_current_monotonic_time_ns();
  for ( i = 1; i < iters; i += 2 ) {
    if ( ! ht->find( Int( kv_hash_uint( (uint32_t) i ) ), pos ) ) {
      fprintf( stderr, "not found %" PRIu64 "\n", (uint64_t) i );
    }
    else {
      ht->remove( pos );
      IntHashTabU<Int>::check_resize( ht );
    }
  }
  odd_time = kv_current_monotonic_time_ns() - t;

  printf( "%s ", kind );
  printf( "cnt %" PRIu64 "/%" PRIu64 ", ns per ins: %.2f, find %.2f, even %.2f, odd %.2f\n",
           iters, ht->elem_count,
           (double) ins_time / (double) iters,
           (double) find_time / (double) iters,
           (double) even_time / (double) iters / 2,
           (double) odd_time / (double) iters / 2 );
  delete ht;
}

struct Hash {
  uint32_t hash[ 4 ];
  Hash() {}
  Hash( const Hash &h1 ) {
    for ( size_t i = 0; i < 4; i++ )
      this->hash[ i ] = h1.hash[ i ];
  }
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

  for ( int i = 0; i < 5; i++ ) {
    printf( "\ndo_test\n" );
    do_test<uint32_t, uint32_t, 50000>( t1 );
    do_test<uint64_t, uint64_t, 50000>( t2 );
    do_test<Hash, uint32_t, 50000>( t3 );

    printf( "\ndo_test2\n" );
    do_test2<uint32_t, uint32_t, 50000>( t1 );
    do_test2<uint64_t, uint64_t, 50000>( t2 );
    do_test2<Hash, uint32_t, 50000>( t3 );

    printf( "\ndo_test3\n" );
    do_test3<uint32_t, 50000>( t1 );
    do_test3<uint64_t, 50000>( t2 );
    do_test3<Hash, 50000>( t3 );
  }

  return 0;
}

