#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <raikv/util.h>
#include <raikv/os_file.h>
#include <raikv/key_hash.h>
#include <raikv/delta_coder.h>
#include <raikv/uint_ht.h>
#include <raikv/radix_sort.h>
#include <raikv/bloom.h>

using namespace rai;
using namespace kv;

static const char * dict_words,
                 ** word;
static uint32_t   * word_len,
                    words_cnt;
static uint64_t     words_size;
static const uint32_t max_words = 512 * 1024;

bool
load_words( MapFile &map ) noexcept
{
  words_size = map.map_size;
  dict_words = (const char *) map.map;
  word       = (const char **) ::malloc( max_words * sizeof( word[ 0 ] ) );
  word_len   = (uint32_t *)    ::malloc( max_words * sizeof( word_len[ 0 ] ) );

  uint64_t off = 0, left = words_size;
  while ( off < words_size ) {
    const void *p = ::memchr( &dict_words[ off ], '\n', left );
    if ( p == NULL )
      break;
    word[ words_cnt ]     = &dict_words[ off ];
    word_len[ words_cnt ] = (uint32_t) ( (const char *) p - word[ words_cnt ] );
    off  = ( (const char *) p - dict_words ) + 1;
    left = words_size - off;
    if ( ++words_cnt == max_words )
      break;
  }
  return words_cnt > 0;
}

static uint32_t
hash_word( const void *w,  size_t len ) noexcept
{
  return kv_crc_c( w, len, 0 );
/*           c = kv_hash_uint( h );
  return ( (uint64_t) c << 32 ) | (uint64_t) h;*/
}

void
serialize( const BloomBits &bits ) noexcept
{
  static uint32_t shard[ 4 ] = { 1, 2, 3, 4 };
  BloomCodec spc, spc2;
  BloomBits * bits2;
  uint32_t pref[ 65 ], pref2[ 65 ];
  size_t npref = 65;
  void * shards;
  size_t shards_size;
  void * queue;
  size_t queue_size;
  ::memset( pref, 0, sizeof( pref ) );

  uint64_t t2, t1 = current_monotonic_time_ns();
  spc.encode( pref, 65, shard, sizeof( shard ), NULL, 0, bits );
  t2 = current_monotonic_time_ns();

  printf( "encode time %.3f usecs\n", ((double) t2 - (double) t1) / 1000.0 );
  printf( "bytes sz %u, %.1f bytes / entry, %.1f "
          "bytes / bloom size (%" PRIu64 ")\n",
           spc.code_sz * 4, (double) spc.code_sz * 4 / (double) bits.count,
           (double) spc.code_sz * 4 / (double) bits.width, bits.width );
  t1 = current_monotonic_time_ns();
  if ( (bits2 = spc2.decode( pref2, npref, shards, shards_size,
                          queue, queue_size, spc.ptr, spc.code_sz )) == NULL ) {
    fprintf( stderr, "failed to decode\n" );
  }
  else {
    t2 = current_monotonic_time_ns();
    printf( "decode time %.3f usecs\n", ((double) t2 - (double) t1) / 1000.0 );
    if ( bits.width != bits2->width ||
         ::memcmp( bits.bits, bits2->bits, bits.width ) != 0 )
      printf( "decode not equal\n" );

    if ( shards_size != sizeof( shard ) ||
         ::memcmp( shard, shards, sizeof( shard ) ) != 0 ) {
      printf( "shrds not equal\n" );
    }
    if ( shards != NULL )
      ::free( shards );
    if ( npref != 65 ) {
      printf( "npref not 65\n" );
    }
    else {
      for ( int i = 0; i < 65; i++ ) {
        if ( pref2[ i ] != pref[ i ] ) {
          printf( "pref not equal\n" );
          break;
        }
      }
    }
    for ( int i = 0; i < 4; i++ ) {
      size_t pos, pos2;
      if ( bits.ht[ i ]->first( pos ) ) {
        do {
          uint32_t h, val, val2;
          bits.ht[ i ]->get( pos, h, val );
          if ( ! bits2->ht[ i ]->find( h, pos2, val2 ) ) {
            printf( "hash %x not found\n", h );
            break;
          }
          else if ( val != val2 ) {
            printf( "val not correct %u %u\n", val, val2 );
            break;
          }
        } while ( bits.ht[ i ]->next( pos ) );
      }
    }
    delete bits2;
  }
}

double
false_rate( BloomBits *filter,  uint32_t n ) noexcept
{
  uint32_t cnt = 0, total = words_cnt - n;
  for ( ; n < words_cnt; n++ ) {
    uint32_t h = hash_word( word[ n ], word_len[ n ] );
    if ( filter->is_member( h ) )
      cnt++;
  }
  return (double) cnt * 100.0 / (double) total;
}

uint32_t
filter_count( BloomBits *filter,  uint32_t n ) noexcept
{
  uint32_t cnt = 0;
  for ( uint32_t x = 0; x < n; x++ ) {
    uint32_t h = hash_word( word[ x ], word_len[ x ] );
    if ( filter->is_member( h ) )
      cnt++;
  }
  return cnt;
}

void
test_filter( uint8_t width,  size_t *elem_count,  double *false_ratio,
             size_t &cnt ) noexcept
{
  uint32_t n, h;
  uint64_t t1, t2;
  cnt = 0;
  BloomBits * filter = BloomBits::resize( NULL, 0, width );
  for ( n = 0; n < words_cnt; ) {
    h = hash_word( word[ n ], word_len[ n ] );
    filter->add( h );
    if ( filter->test_resize() ) {
      serialize( *filter );
      size_t collision_cnt = 0;
      for ( int i = 0; i < 4; i++ )
        collision_cnt += filter->ht[ i ]->elem_count;
      t1 = current_monotonic_time_ns();
      if ( filter_count( filter, n ) != n ) {
        printf( "!!! failed filter_count( %u )\n", n );
      }
      elem_count[ cnt ] = n;
      false_ratio[ cnt ] = false_rate( filter, n + 1 ),
      t2 = current_monotonic_time_ns();
      printf( "%.1f ns/lookup %.2f%% false rate\n",
              (double) ( t2 - t1 ) / (double) words_cnt, false_ratio[ cnt ] );
      printf( "resize cnt %u (%.1f bits) "
              "(%" PRIu64 " ht coll) %" PRIu64 "(%u,%u,%u,%u) -> ",
              n, (double) filter->width * 8.0 / (double) n, collision_cnt,
              filter->width, filter->SHFT1, filter->SHFT2, filter->SHFT3,
              filter->SHFT4 );
      fflush( stdout );
      filter = BloomBits::resize( filter, 0, width );
      printf( "%" PRIu64 " (%u,%u,%u,%u)\n", 
              filter->width, filter->SHFT1, filter->SHFT2, filter->SHFT3,
              filter->SHFT4 );
      fflush( stdout );
      cnt++;
      n = 0;
    }
    else {
      n++;
    }
  }
  t1 = current_monotonic_time_ns();
  if ( filter_count( filter, words_cnt ) != words_cnt ) {
    printf( "!!! failed filter_count( %u )\n", n );
  }
  t2 = current_monotonic_time_ns();
  printf( "%.1f ns/lookup for %u words (%.2f us total time)\n",
          (double) ( t2 - t1 ) / (double) words_cnt, words_cnt,
          (double) ( t2 - t1 ) / 1000.0 );
  delete filter;
}

int
main( int argc,  const char *argv[] )
{
  #ifndef _MSC_VER
  static char words[] = "/usr/share/dict/words";
  #else
  static char words[] = "words";
  #endif
  const char * input = ( argc > 1 ? argv[ 1 ] : words );
  MapFile map( input );
  if ( ! map.open() || ! load_words( map ) )
    return 1;

  size_t elem_count[ 1024 ];
  double false_ratio[ 1024 ];
  size_t cnt[ ( 64 - 12 ) / 4 ];
  size_t i = 0, off = 0;
  uint8_t width;
  for ( width = 12; width < 64; width += 4 ) {
    test_filter( width, &elem_count[ off ], &false_ratio[ off ], cnt[ i ] );
    off += cnt[ i ];
    i++;
  }
  i = 0;
  off = 0;
  for ( width = 12; width < 64; width += 4 ) {
    printf( "width:%u -- ", width );
    for ( size_t j = 0; j < cnt[ i ]; j++ ) {
      printf( "%" PRIu64 "=%.2f%% ", elem_count[ off + j ], false_ratio[ off + j ] );
    }
    printf( "\n" );
    off += cnt[ i ];
    i++;
  }
  return 0;
}
