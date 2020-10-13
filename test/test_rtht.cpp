#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <raikv/route_ht.h>

using namespace rai;
using namespace kv;

struct RouteData {
  uint32_t hash;
  /*uint32_t route[ 128 / 32 ];*/
  uint16_t len;
  char     value[ 2 ];
  bool equals( const void *s,  uint16_t l ) const {
    return l == this->len && ::memcmp( s, this->value, l ) == 0;
  }
  void copy( const void *s,  uint16_t l ) {
    ::memcpy( this->value, s, l );
  }
};

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

#if 0
uint64_t
get_rdtsc( void )
{
   uint32_t lo, hi;
  __asm__ __volatile__("rdtsc" : "=a" (lo), "=d" (hi));
  return ( (uint64_t) hi << 32 ) | (uint64_t) lo;
}

void escape( void *p ) {
  __asm__ __volatile__ ( "" : : "g"(p) : "memory" );
}
#endif

int
main( int, char ** )
{
  RouteVec<RouteData> vec;
  char        buf[ 256 ];
  uint32_t    h;
  size_t      len, cnt =0;
  FILE * fp = fopen( "/usr/share/dict/words", "r" );

  for ( int i = 0; i < 8; i++ ) {
    fseek( fp, 0, SEEK_SET );
    cnt = 0;
    while ( fgets( buf, sizeof( buf ), fp ) != NULL ) {
      len = ::strlen( buf );
      while ( len > 0 && buf[ len - 1 ] <= ' ' )
        buf[ --len ] = '\0';
      if ( len > 0 ) {
        h = djb( buf, len );
        /* escape( &h ); */
        if ( vec.upsert( h, buf, len ) != NULL )
          cnt++;
        /*::memcpy( data->route, cr, sizeof( cr ) );*/
      }
    }
    printf( "cnt %lu == pop %lu, alloc vec_count %u\n", cnt,
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
        /* escape( &h ); */
        if ( vec.find( h, buf, len ) != NULL )
          cnt++;
        /*::memcpy( data->route, cr, sizeof( cr ) );*/
      }
    }
    printf( "cnt %lu == pop %lu, alloc vec_count %u\n", cnt,
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
        /* escape( &h ); */
        if ( vec.remove( h, buf, len ) )
          cnt++;
        /*::memcpy( data->route, cr, sizeof( cr ) );*/
      }
    }
    printf( "cnt %lu > pop %lu, alloc vec_count %u\n", cnt,
            vec.pop_count(), vec.vec_size );
    assert( 0 == vec.pop_count() );
  }
#if 0
  for ( size_t i = 0; i < vec.vec_size; i++ ) {
    total += vec.vec[ i ]->count;
    avail += vec.vec[ i ]->ht_size();
    total_size += vec.vec[ i ]->free_off * 4;
    avail_size += sizeof( vec.vec[ i ]->block );
    printf( "%lu. count %u size %u\n", i, vec.vec[ i ]->count,
            vec.vec[ i ]->free_off * 4 );
  }
  printf( "total %lu / %lu = %.3f\n", total, avail,
          (double) total / (double) avail );
  printf( "total sz %lu / %lu = %.3f\n", total_size, avail_size,
          (double) total_size / (double) avail_size );
  RouteHT<RouteData> cache, cache2;
  RouteData * data;
  uint64_t    cr[ 2 ];
  char        buf[ 256 ];
  uint32_t    h;
  size_t      len;
  uint16_t    pos;
  printf( "size %lu/%lu\n", sizeof( cache.entry ), sizeof( cache.block ) );
  cr[ 0 ] = cr[ 1 ] = 0;
  while ( fgets( buf, sizeof( buf ), stdin ) != NULL ) {
    len = ::strlen( buf );
    while ( len > 0 && buf[ len - 1 ] <= ' ' )
      buf[ --len ] = '\0';
    if ( len > 0 ) {
      if ( ! cache.fits( len ) )
        break;
      cr[ 0 ]++;
      h = djb( buf, len );
      cache.insert( h, buf, len );
      /*::memcpy( data->route, cr, sizeof( cr ) );*/
    }
  }
  cache.remove( djb( "aeroplane", 9 ), "aeroplane", 9 );
  uint64_t st = get_rdtsc();
  uint64_t en = get_rdtsc();
  printf( "count %u size %u cpy %luns (%luus)\n", cache.count,
          cache.free_off * 4,
          ( en - st ) / cache.count / 3, ( en - st ) / 3000 );
  for ( pos = 0; (data = cache.iter( pos )) != NULL; ) {
    /*printf( "%u (%u). %.*s\n", pos,
            data->hash % cache.ht_size(),
            (int) data->len, data->value );*/
    if ( djb( data->value, data->len ) != data->hash ) {
      printf( "not equal %u\n", pos );
      printf( "%.*s\n", (int) data->len, data->value );
      break;
    }
  }
  st = get_rdtsc();
  cache.split( cache2 );
  en = get_rdtsc();
  printf( "count %u size %u count %u size %u, cpy %luns (%luus)\n", cache.count,
        cache.free_off * 4, cache2.count, cache2.free_off * 4,
        ( en - st ) / ( cache.count + cache2.count ) / 3, ( en - st ) / 3000 );
#endif
  return 0;
}

