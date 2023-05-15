#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <raikv/route_db.h>
#include <raikv/util.h>
#include <raikv/bit_set.h>

using namespace rai;
using namespace kv;

void print_routedb( RouteDB &rte )
{
  size_t i;
  for ( size_t k = 0; k <= MAX_PRE; k++ ) {
    UIntHashTab * xht = rte.rt_hash[ k ];
    if ( xht->elem_count > 0 ) {
      printf( "rt[%" PRIu64 "] %" PRIu64 "/%" PRIu64 " :\n", k, xht->elem_count, xht->tab_size() );
      if ( xht->first( i ) ) {
        do {
          RouteRef ref( rte.zip, 0 );
          uint32_t h = xht->tab[ i ].hash,
                   v = xht->tab[ i ].val,
                   cnt;
          cnt = ref.decompress( v, 0 );
          printf( "[%" PRIu64 "][%x] %x(%s) ", i, h, v,
                  DeltaCoder::is_not_encoded( v )?"str":"enc");
          printf( "[ " );
          for ( uint32_t j = 0; j < cnt; j++ )
            printf( "%u ", ref.routes[ j ] );
          printf( "]\n" );
        } while ( xht->next( i ) );
      }
    }
  }
  printf( "zht:\n" );
  if ( rte.zip.zht->first( i ) ) {
    do {
      uint32_t  h = rte.zip.zht->tab[ i ].hash,
                v = rte.zip.zht->tab[ i ].val;
      CodeRef * p = (CodeRef *) (void *) &rte.zip.code_buf.ptr[ v ];
      printf( "[%" PRIu64 "][%x] hash:%x off:%u ref:%u ecnt:%u rcnt:%u\n", i, h,
              p->hash, v, p->ref, p->ecnt, p->rcnt );
    } while ( rte.zip.zht->next( i ) );
  }
  printf( "bloom:\n" );
  for ( BloomRoute *b = rte.bloom_list.hd( 0 ); b != NULL; b = b->next ) {
    BloomCodec c, c2;
    b->bloom[ 0 ]->encode( c );
    uint32_t pref[ MAX_RTE ];
    void * shards;
    size_t shards_size;
    BloomBits *bits = c2.decode( pref, MAX_RTE, shards, shards_size, c.ptr, c.code_sz );
    if ( bits != NULL ) {
      if ( bits->width != b->bloom[ 0 ]->bits->width ||
           ::memcmp( bits->bits, b->bloom[ 0 ]->bits->bits, bits->width ) != 0 )
        printf( "!! codec not equal!!\n" );
      delete bits;
    }
    printf( "b[%u] %" PRIu64 "/%" PRIu64 ",cnt=%" PRIu64 ",resz=%" PRIu64 ",code=%u\n",
            b->r, b->bloom[ 0 ]->bits->pop_count(),
            b->bloom[ 0 ]->bits->width * 8,
            b->bloom[ 0 ]->bits->count,
            b->bloom[ 0 ]->bits->resize_count, c.code_sz * 4 );
  }
  printf( "code_end %" PRIu64 ", code_size %" PRIu64 ", code_free %" PRIu64 "\n",
          rte.zip.code_end, rte.zip.code_buf.size, rte.zip.code_free );
  printf( "code_spc_size %" PRIu64 ", route_ref_size %" PRIu64 "\n",
          rte.zip.zroute_spc.size, rte.zip.route_spc( SUB_RTE ).size );
  printf( "zht %" PRIu64 "/%" PRIu64 "\n", rte.zip.zht->elem_count, rte.zip.zht->tab_size() );
  printf( "cache_elems %" PRIu64 " cache_free %" PRIu64 "\n", rte.cache.end, rte.cache.free );
  printf( "entry_count %u cache_count %" PRIu64 "\n", rte.entry_count, rte.cache.count );
}

int
split_args( char *start,  char *end,  char **args,  size_t *len,
            size_t maxargs ) noexcept
{
  char *p;
  size_t n;
  for ( p = start; ; p++ ) {
    if ( p >= end )
      return 0;
    if ( *p > ' ' )
      break;
  }
  n = 0;
  args[ 0 ] = p;
  for (;;) {
    if ( ++p == end || *p <= ' ' ) {
      len[ n ] = p - args[ n ];
      if ( ++n == maxargs )
        return (int) n;
      while ( p < end && *p <= ' ' )
        p++;
      if ( p == end )
        break;
      args[ n ] = p;
    }
  }
  return (int) n;
}

bool
arg_equal( const char *arg,  size_t len,  const char *match ) noexcept
{
  return len == ::strlen( match ) && ::memcmp( arg, match, len ) == 0;
}

void
bloom_add( RouteDB &rte,  uint32_t hash,  uint32_t r ) noexcept
{
  for ( BloomRoute *b = rte.bloom_list.hd( 0 ); b != NULL; b = b->next ) {
    if ( b->r == r ) {
      b->bloom[ 0 ]->add( hash );
      return;
    }
  }
  printf( "bloom route %u not found\n", r );
}

void
bloom_del( RouteDB &rte,  uint32_t hash,  uint32_t r ) noexcept
{
  for ( BloomRoute *b = rte.bloom_list.hd( 0 ); b != NULL; b = b->next ) {
    if ( b->r == r ) {
      b->bloom[ 0 ]->del( hash );
      return;
    }
  }
  printf( "bloom route %u not found\n", r );
}

void
interactive_update( RouteDB &rte ) noexcept
{
  char   * args[ 30 ];
  size_t   arglen[ 30 ];
  char     buf[ 1024 ];

  for (;;) {
    printf( "[add, del, badd, bdel, get, print, gc] [subject] [route]\n> " );
    if ( fgets( buf, sizeof( buf ), stdin ) == NULL )
      break;
    int n = split_args( buf, &buf[ ::strlen( buf ) ], args, arglen, 30 );
    if ( n >= 2 ) {
      uint32_t hash = kv_crc_c( args[ 1 ], arglen[ 1 ], 0 );
      if ( n >= 3 ) {
        uint32_t r = atoi( args[ 2 ] ),
                 j = rte.get_sub_route_count( hash );
        int      t = arg_equal( args[ 0 ], arglen[ 0 ], "add" ) ? 0 :
                     arg_equal( args[ 0 ], arglen[ 0 ], "del" ) ? 1 :
                     arg_equal( args[ 0 ], arglen[ 0 ], "badd" ) ? 2 :
                     arg_equal( args[ 0 ], arglen[ 0 ], "bdel" ) ? 3 : -1;

        if ( t >= 0 ) {
          for ( int i = 2; ; ) {
            if ( t == 0 )
              rte.add_sub_route( hash, r );
            else if ( t == 1 )
              rte.del_sub_route( hash, r );
            else if ( t == 2 )
              bloom_add( rte, hash, r );
            else
              bloom_del( rte, hash, r );
            if ( ++i == n )
              break;
            r = atoi( args[ i ] );
          }
          printf( "%d rts %.*sed\n",
                  (int) ( rte.get_sub_route_count( hash ) - j ),
                  (int) arglen[ 0 ], args[ 0 ] );
        }
        else {
          printf( "what?\n" );
        }
      }
      else if ( n == 2 ) {
        if ( arg_equal( args[ 0 ], arglen[ 0 ], "get" ) ) {
          RouteLookup look( args[ 1 ], arglen[ 1 ], hash, 0 );
          rte.get_sub_route( look );
          if ( look.rcount == 0 )
            printf( "not found\n" );
          else {
            printf( "%u [", look.rcount );
            for ( uint32_t i = 0; i < look.rcount; i++ )
              printf( "%u ", look.routes[ i ] );
            printf( "]\n" );
          }
          look.deref( rte );
        }
        else {
          printf( "what?\n" );
        }
      }
    }
    else if ( n == 1 && arg_equal( args[ 0 ], arglen[ 0 ], "print" ) ) {
      print_routedb( rte );
    }
    else if ( n == 1 && arg_equal( args[ 0 ], arglen[ 0 ], "gc" ) ) {
      rte.zip.gc_code_ref_space();
    }
    else {
      printf( "what?\n" );
    }
  }
}

int
cmp( const void *p1,  const void *p2 ) noexcept
{
  if ( *(uint32_t *) p1 < *(uint32_t *) p2 )
    return -1;
  if ( *(uint32_t *) p1 > *(uint32_t *) p2 )
    return 1;
  return 0;
}

size_t
remove_dups( uint32_t *row,  size_t n ) noexcept
{
  if ( n <= 1 )
    return n;
  ::qsort( row, n, sizeof( row[ 0 ] ), cmp );

  size_t i, j = 0;
  for ( i = 1;; i++ ) {
    for ( ; i < n && row[ i - 1 ] == row[ i ]; i++ )
      ;
    row[ j++ ] = row[ i - 1 ];
    if ( i == n )
      break;
  }
  return j;
}

uint32_t
remove_one( uint32_t *row, uint32_t val, uint32_t cnt ) noexcept
{
  uint32_t i, j;
  for ( i = 0; i < cnt; i++ ) {
    if ( row[ i ] == val ) {
      for ( j = i + 1; j < cnt; j++ )
        row[ i++ ] = row[ j ];
      return i;
    }
  }
  return cnt;
}

uint32_t
hash_int( uint32_t i ) noexcept
{
  char buf[ 16 ];
  size_t n = uint32_to_string( i, buf );
  return kv_crc_c( buf, n, 0 );
}

struct TestDB {
  static const uint32_t SUB = 1024, RTE = 64;
  BitSpace rdb[ SUB ];
  uint32_t cnt[ SUB ];          /* index into rdb[ S ][ cnt[ S ] ] */
  RouteDB  rte;                 /* hash sub to routes zip */
  size_t   sub_count;
  rand::xoroshiro128plus r;     /* random source */
  BloomRoute * brt[ RTE ];

  TestDB( BloomDB &db ) : rte( db ), sub_count( 0 ) {
    ::memset( this->cnt, 0, sizeof( this->cnt ) );
    ::memset( this->brt, 0, sizeof( this->brt ) );
    this->r.static_init();      /* init the same rand sequence */
  }

  void generate_routes( size_t count ) noexcept;
  void add_routes( void ) noexcept;
  void add_bloom_routes( void ) noexcept;
  void verify_routes( void ) noexcept;
  void remove_random( size_t count ) noexcept;
};

void
TestDB::generate_routes( size_t count ) noexcept
{
  /* create a db of routes in rdb */
  uint32_t i;
  for ( i = 0; i < count; i++ ) {
    uint64_t val = this->r.next();
    uint32_t j   = val % SUB;
    val >>= 16;
    val  %= RTE;
    if ( ! this->rdb[ j ].test_set( (uint32_t) val ) )
      this->cnt[ j ]++;
  }
}

void
TestDB::add_routes( void ) noexcept
{
  /* add the routes to rte */
  for ( uint32_t j = 0; j < SUB; j++ ) {
    uint32_t h = hash_int( j + 1 ), val;
    if ( this->rdb[ j ].first( val ) ) {
      do {
        this->rte.add_sub_route( h, val );  /* hash(str(j)) -> [ val, ... ] */
        this->sub_count++;
      } while ( this->rdb[ j ].next( val ) );
    }
  }
}

void
TestDB::add_bloom_routes( void ) noexcept
{
  /* add the routes to rte */
  for ( uint32_t j = 0; j < SUB; j++ ) {
    uint32_t h = hash_int( j + 1 );
    for ( uint32_t val = 0; val < RTE; val++ ) {
      if ( this->rdb[ j ].is_member( val ) ) {
        if ( this->brt[ val ] == NULL ) {
          BloomRef * ref = this->rte.create_bloom_ref(
            NULL, BloomBits::resize( NULL, 0, 20 ), "test",
            this->rte.g_bloom_db );
          this->brt[ val ] = this->rte.create_bloom_route( val, ref, 0 );
        }
        if ( this->brt[ val ]->bloom[ 0 ]->add( h ) ) {
          BloomBits * bits = this->brt[ val ]->bloom[ 0 ]->bits;
          bits = BloomBits::resize( bits, 0, 20 );
          for ( uint32_t k = 0; k <= j; k++ ) {
            if ( this->rdb[ k ].is_member( val ) ) {
              bits->add( hash_int( k + 1 ) );
            }
          }
          this->brt[ val ]->bloom[ 0 ]->bits = bits;
        }
        this->sub_count++;
      }
    }
  }
}

void
TestDB::verify_routes( void ) noexcept
{
  uint32_t false_pos = 0, total = 0;
  /* verify the rte are the same as rdb */
  for ( uint32_t i = 0; i < SUB; i++ ) {
    RouteLookup look( NULL, 0, hash_int( i + 1 ), 0 );
    this->rte.get_sub_route( look );
    if ( look.rcount != this->cnt[ i ] ) {
      printf( "failed %u k %u cnt %u\n", i, look.rcount, this->cnt[ i ] );
      false_pos += look.rcount - this->cnt[ i ];
    }
    total += this->cnt[ i ];
    look.deref( this->rte );
  }
  printf( "false positive rate %.2f%%\n",
           (double) false_pos * 100.0 / (double) total );
}

void
TestDB::remove_random( size_t count ) noexcept
{
  /* remove some routes */
  for ( uint32_t i = 0; i < count; i++ ) {
    uint64_t val  = this->r.next();
    uint32_t h, j = val % SUB, k;
    val >>= 16;
    h     = hash_int( j + 1 );
    val  %= RTE;
    k     = rte.del_sub_route( h, (uint32_t) val );
    if ( k != this->cnt[ j ] ) {
      this->sub_count--;
      if ( ! this->rdb[ j ].test_clear( (uint32_t) val ) )
        printf( "mismatch %u, k %u != cnt %u\n", i, k, this->cnt[ j ] );
      else
        this->cnt[ j ]--;
    }
  }
}

int
main( int argc, char *argv[] )
{
  BloomDB db;
  TestDB test( db );
#if 0
  uint32_t ar[ 10 ] = { 10, 11, 31, 32, 33 };
  uint32_t ar2[ 10 ] = { 10, 21, 29 };
  uint32_t i, j;
  printf( "merge %u\n", j = merge_route( ar, 5, ar2, 3 ) );
  for ( i = 0; i < j; i++ )
    printf( "%u ", ar[ i ] );
  printf( "\n" );
#endif
  size_t cnt = 8000;
  if ( argc > 1 && atoi( argv[ 1 ] ) > 0 )
    cnt = atoi( argv[ 1 ] );
  test.generate_routes( cnt );
  test.add_bloom_routes();
  test.verify_routes();
  /*while ( test.sub_count > 1000 )
    test.remove_random( 1000 );
  test.verify_routes();*/

  interactive_update( test.rte );
  return 0;
}

