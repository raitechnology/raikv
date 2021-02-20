#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <raikv/route_ht.h>

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
main( int, char ** )
{
  RouteVec<RouteSub> vec;
  char        buf[ 8192 ];
  uint32_t    h;
  size_t      len, cnt =0;
  int         i;
  uint64_t    isum, sum;
  FILE * fp = fopen( "/usr/share/dict/words", "r" );

  printf( "vec  size %ld\n", sizeof( vec ) );
  printf( "rtht size %ld\n", sizeof( RouteHT<RouteSub> ) );
  printf( "ht   size %ld\n", RouteHT<RouteSub>::HT_SIZE * 4 );
  printf( "blck size %ld\n", RouteHT<RouteSub>::BLOCK_SIZE * 8 );
  for ( i = 0; ; i++ ) {
    fseek( fp, 0, SEEK_SET );
    cnt = 0;
    while ( fgets( buf, sizeof( buf ), fp ) != NULL ) {
      len = ::strlen( buf );
      while ( len > 0 && buf[ len - 1 ] <= ' ' )
        buf[ --len ] = '\0';
      if ( len > 0 ) {
        h = djb( buf, len );
        if ( vec.insert_unique( h, buf, len ) != NULL )
          cnt++;
      }
    }
    printf( "insert cnt %lu == pop %lu, alloc vec_count %u\n", cnt,
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
    printf( "find cnt %lu == pop %lu, alloc vec_count %u\n", cnt,
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
    printf( "remove cnt %lu > pop %lu, alloc vec_count %u\n", cnt,
            vec.pop_count(), vec.vec_size );
    assert( 0 == vec.pop_count() );
  }
  strcpy( buf, "one." );
  h = djb( buf, 4 );
  i = 0;
  isum = 0;
  for (;;) {
    buf[ 4 + i ] = 'x';
    if ( vec.upsert( h, buf, 4 + i + 1 ) == NULL ) {
      printf( "failed after i = %d collisions\n", i );
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
      printf( "missing %d\n", i );
      break;
    }
  }
  ::strcpy( buf, "anotherone" );
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

