#include <stdio.h>
#include <string.h>
#include <stdint.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <stdlib.h>
#include <assert.h>
#include <raikv/route_ht.h>

#ifdef _MSC_VER
/* disable delete() constructor (4291), fopen deprecated (4996) */
#pragma warning( disable : 4291 4996 )
#endif

using namespace rai;
using namespace kv;

uint32_t
djb( const char *s,  size_t len )
{
  uint32_t key = 5381;
  for ( ; len > 0; len -= 1 ) {
    uint8_t c = (uint8_t) *s++;
    key = (uint32_t) c ^ ( ( key << 5 ) + key );
  }
  return key;
}

int
main( int argc, char *argv[] )
{
#ifndef _MSC_VER
  static char words[] = "/usr/share/dict/words";
#else
  static char words[] = "words";
#endif
  RouteVec<RouteSub> vec;
  char        buf[ 8192 ];
  uint32_t    h;
  size_t      len, cnt = 0, i;
  uint64_t    isum, sum;
  const char * input = argc > 1 ? argv[ 1 ] : words;
  FILE * fp = fopen( input, "r" );

  if ( fp == NULL ) {
    fprintf( stderr, "%s not found\n", input );
    return 1;
  }

  printf( "vec  size %" PRId64 "\n", sizeof( vec ) );
  printf( "rtht size %" PRId64 "\n", sizeof( RouteHT<RouteSub> ) );
  printf( "ht   size %" PRId64 "\n", RouteHT<RouteSub>::HT_SIZE * 4 );
  printf( "blck size %" PRId64 "\n", RouteHT<RouteSub>::BLOCK_SIZE * 8 );
  for ( i = 0; ; i++ ) {
    fseek( fp, 0, SEEK_SET );
    cnt = 0;
    while ( fgets( buf, sizeof( buf ), fp ) != NULL ) {
      len = ::strlen( buf );
      while ( len > 0 && buf[ len - 1 ] <= ' ' )
        buf[ --len ] = '\0';
      if ( len > 0 ) {
        h = djb( buf, len );
        if ( vec.upsert( h, buf, len ) != NULL )
          cnt++;
      }
    }
    printf( "insert cnt %" PRIu64 " == pop %" PRIu64 ", alloc vec_count %u\n", cnt,
            vec.pop_count(), vec.vec_size );
    if ( i == 3 )
      break;
    assert( cnt == vec.pop_count() );
    fseek( fp, 0, SEEK_SET );
    cnt = 0;
    while ( fgets( buf, sizeof( buf ), fp ) != NULL ) {
      len = ::strlen( buf );
      while ( len > 0 && buf[ len - 1 ] <= ' ' )
        buf[ --len ] = '\0';
      if ( len > 0 ) {
        h = djb( buf, len );
        if ( vec.find( h, buf, len ) != NULL )
          cnt++;
      }
    }
    printf( "find cnt %" PRIu64 " == pop %" PRIu64 ", alloc vec_count %u\n", cnt,
            vec.pop_count(), vec.vec_size );
    assert( cnt == vec.pop_count() );
    fseek( fp, 0, SEEK_SET );
    cnt = 0;
    while ( fgets( buf, sizeof( buf ), fp ) != NULL ) {
      len = ::strlen( buf );
      while ( len > 0 && buf[ len - 1 ] <= ' ' )
        buf[ --len ] = '\0';
      if ( len > 0 ) {
        h = djb( buf, len );
        if ( vec.remove( h, buf, len ) )
          cnt++;
      }
    }
    printf( "remove cnt %" PRIu64 " > pop %" PRIu64 ", alloc vec_count %u\n", cnt,
            vec.pop_count(), vec.vec_size );
    assert( 0 == vec.pop_count() );
  }
  memcpy( buf, "one.", 5 );
  h = djb( buf, 4 );
  i = 0;
  isum = 0;
  for (;;) {
    buf[ 4 + i ] = 'x';
    if ( vec.upsert( h, buf, 4 + i + 1 ) == NULL ) {
      printf( "failed after i = %" PRId64 " collisions\n", i );
      break;
    }
    isum += 4 + i + 1;
    i++;
    RouteLoc loc;
    RouteSub *d = vec.find_by_hash( h, loc );
    sum = 0;
    while ( d != NULL ) {
      sum += d->len;
      d = vec.find_next_by_hash( h, loc );
    }
    if ( sum != isum ) {
      printf( "missing %" PRId64 "\n", i );
      break;
    }
  }
  ::memcpy( buf, "anotherone", 11 );
  len = ::strlen( buf );
  h = djb( buf, len );
  if ( vec.upsert( h, buf, len ) == NULL ) {
    printf( "failed upsert\n" );
  }
  if ( vec.find( h, buf, len ) == NULL ) {
    printf( "failed to find\n" );
  }
  return 0;
}

