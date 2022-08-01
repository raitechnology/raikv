#include <stdio.h>
#include <string.h>
#include <stdint.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <stdlib.h>
#include <assert.h>
#include <raikv/route_ht.h>
#include <raikv/key_hash.h>
#include <raikv/util.h>
#include <raikv/os_file.h>
#include <raikv/array_space.h>

#ifdef _MSC_VER
/* disable delete() constructor (4291), fopen deprecated (4996) */
#pragma warning( disable : 4291 4996 )
#endif

using namespace rai;
using namespace kv;

#define hash_f( x, y ) kv_crc_c( x, y, 0 )
/*#define hash_f( x, y ) kv_djb( x, y, 0 )*/

struct SubTab : public RouteVec<RouteSub> {
  virtual void * new_vec_data( uint32_t id,  size_t sz ) noexcept {
    char path[ 32 ];
    ::snprintf( path, sizeof( path ), "rtht.%u", id );
    MapFile map( path, sz );
    int fl = MAP_FILE_SHM | MAP_FILE_CREATE | MAP_FILE_RDWR | MAP_FILE_NOUNMAP;
    if ( ! map.open( fl ) )
      return NULL;
    return map.map;
  }

  virtual void free_vec_data( uint32_t id,  void *p,  size_t sz ) noexcept {
    char path[ 32 ];
    ::snprintf( path, sizeof( path ), "rtht.%u", id );
    MapFile::unmap( p, sz );
    MapFile::unlink( path, true );
  }

  VecData *get_data( uint32_t i ) {
    char path[ 32 ];
    ::snprintf( path, sizeof( path ), "rtht.%u", i );
    MapFile map( path, 0 );
    int fl = MAP_FILE_SHM | MAP_FILE_RDWR | MAP_FILE_NOUNMAP;
    if ( ! map.open( fl ) )
      return NULL;
    if ( map.map_size != sizeof( VecData ) ) {
      map.no_unmap = false;
      return NULL;
    }
    return (VecData *) map.map;
  }

  bool load( const char *s ) {
    ArrayCount<VecData *, 64> tmp;
    uint32_t  id = atoi( s );
    size_t    i;
    VecData * start = this->get_data( id ),
            * data;
    if ( start == NULL )
      return false;
    tmp[ start->index ] = start;
    for ( data = start; data->index != 0; ) {
      if ( data->prev_id == data->id )
        goto fail;
      if ( (data = this->get_data( data->prev_id )) == NULL )
        goto fail;
      tmp[ data->index ] = data;
    }
    for ( data = start; data->next_id != data->id; ) {
      if ( (data = this->get_data( data->next_id )) == NULL )
        goto fail;
      tmp[ data->index ] = data;
    }
    for ( i = 0; i < tmp.count; i++ ) {
      if ( tmp.ptr[ i ] == NULL )
        goto fail;
    }
    this->preload( &tmp.ptr[ 0 ], tmp.count );
    tmp.clear();
    return true;
  fail:;
    for ( i = 0; i < tmp.count; i++ ) {
      if ( tmp.ptr[ i ] != NULL )
        MapFile::unmap( tmp.ptr[ i ], sizeof( VecData ) );
    }
    tmp.clear();
    return false;
  }
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
#ifndef _MSC_VER
  static char words[] = "/usr/share/dict/words";
#else
  static char words[] = "words";
#endif
  SubTab       vec;
  uint32_t     h;
  size_t       len, cnt = 0, i, k;
  uint64_t     isum, sum, t, t2;
  const char * input = get_arg( argc, argv, 1, "-i", words ),
             * load  = get_arg( argc, argv, 1, "-l", NULL ),
             * save  = get_arg( argc, argv, 0, "-x", NULL ),
             * fail  = get_arg( argc, argv, 0, "-f", NULL );
  MapFile      map( input );

  if ( load != NULL ) {
    if ( ! vec.load( load ) ) {
      fprintf( stderr, "load %s failed\n", load );
      return 1;
    }
  }
  if ( ! map.open() ) {
    fprintf( stderr, "%s not found\n", input );
    return 1;
  }
  uint64_t     words_size;
  const char * dict_words, * w;
  uint64_t     off;
  const void * p;

  printf( "vec  size %" PRId64 "\n", sizeof( vec ) );
  printf( "rtht size %" PRId64 "\n", sizeof( RouteHT<RouteSub> ) );
  printf( "ht   size %" PRId64 "\n", RouteHT<RouteSub>::HT_SIZE * 4 );
  printf( "blck size %" PRId64 "\n", RouteHT<RouteSub>::BLOCK_SIZE * 8 );

  words_size = map.map_size;
  dict_words = (const char *) map.map;

  for ( i = 0; ; i++ ) {
    if ( (cnt = vec.pop_count()) == 0 ) {
      t = current_monotonic_time_ns();
      for ( off = 0; off < words_size; ) {
        w = &dict_words[ off ];
        if ( (p = ::memchr( w, '\n', words_size - off )) == NULL )
          break;
        len = (const char *) p - w;
        if ( len > 0 ) {
          h = hash_f( w, len );
          if ( vec.upsert( h, w, len ) != NULL )
            cnt++;
        }
        off += len + 1;
      }
      t2 = current_monotonic_time_ns();
      printf( "insert cnt %" PRIu64 " == pop %" PRIu64
              ", alloc vec_count %u, time %.3fus, %.1fns per\n",
              cnt, vec.pop_count(), vec.vec_size,
              (double) ( t2 - t ) / 1000.0,
              (double) ( t2 - t ) / (double) cnt );
    }
    if ( i == 3 )
      break;
    if ( load == NULL )
      assert( cnt == vec.pop_count() );
    cnt = 0;
    t = current_monotonic_time_ns();
    for ( off = 0; off < words_size; ) {
      w = &dict_words[ off ];
      if ( (p = ::memchr( w, '\n', words_size - off )) == NULL )
        break;
      len = (const char *) p - w;
      if ( len > 0 ) {
        h = hash_f( w, len );
        if ( vec.find( h, w, len ) != NULL )
          cnt++;
      }
      off += len + 1;
    }
    t2 = current_monotonic_time_ns();
    printf( "find cnt %" PRIu64 " == pop %" PRIu64
            ", alloc vec_count %u, time %.3fus, %.1fns per\n", cnt,
            vec.pop_count(), vec.vec_size,
            (double) ( t2 - t ) / 1000.0,
            (double) ( t2 - t ) / (double) cnt );
    if ( load == NULL )
      assert( cnt == vec.pop_count() );
    cnt = 0;
    t = current_monotonic_time_ns();
    for ( off = 0; off < words_size; ) {
      w = &dict_words[ off ];
      if ( (p = ::memchr( w, '\n', words_size - off )) == NULL )
        break;
      len = (const char *) p - w;
      if ( len > 0 ) {
        h = hash_f( w, len );
        if ( vec.remove( h, w, len ) )
          cnt++;
      }
      off += len + 1;
    }
    t2 = current_monotonic_time_ns();
    printf( "remove cnt %" PRIu64 " > pop %" PRIu64
            ", alloc vec_count %u, time %.3fus, %.1fns per\n", cnt,
            vec.pop_count(), vec.vec_size,
            (double) ( t2 - t ) / 1000.0,
            (double) ( t2 - t ) / (double) cnt );
    if ( load == NULL )
      assert( 0 == vec.pop_count() );
  }
  if ( fail != NULL ) {
    char buf[ 8192 ];
    memcpy( buf, "one.", 5 );
    h = hash_f( buf, 4 );
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
    h = hash_f( buf, len );
    if ( vec.upsert( h, buf, len ) == NULL ) {
      printf( "failed upsert\n" );
    }
    if ( vec.find( h, buf, len ) == NULL ) {
      printf( "failed to find\n" );
    }
  }
  printf( "vec_size %u\n", vec.vec_size );
  for ( k = 0; k < vec.vec_size; k++ ) {
    uint32_t id_prev = ( k == 0 ? vec.vec[ 0 ]->id : vec.vec[ k - 1 ]->id ),
             id_next = ( k == vec.vec_size - 1 ? vec.vec[ k ]->id :
                                                 vec.vec[ k + 1 ]->id );
    printf( "%s%u%s ", ( id_prev == vec.vec[ k ]->prev_id ? "-" : "x" ),
     vec.vec[ k ]->id, ( id_next == vec.vec[ k ]->next_id ? "-" : "x" ) );
  }
  printf( "\n" );

  if ( save == NULL )
    vec.release();
  return 0;
}

