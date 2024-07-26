#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <raikv/util.h>
#include <raikv/dlinklist.h>

using namespace rai;

struct Elem {
  Elem * next,
       * back;
  uint32_t i;

  void * operator new( size_t, void *ptr ) { return ptr; }
  Elem( uint32_t j ) : next( 0 ), back( 0 ), i( j ) {}
};

typedef kv::DLinkList<Elem> List;

void
print_elem_list( List &list ) noexcept
{
  for ( Elem *e = list.hd; e != NULL; e = e->next ) {
    printf( "%u ", e->i );
  }
  printf( "\n" );
}

int cmp_count;
static int
cmp_elem( const Elem &x,  const Elem &y ) noexcept
{
  cmp_count++;
  if ( x.i < y.i ) return -1;
  if ( x.i == y.i ) return 0;
  return 1;
}

static const size_t esz = ( sizeof( Elem ) + 7 ) / 8;
static uint64_t mem[ esz * 64 * 1024 + esz ];

int
run_test( int num, kv::rand::xoroshiro128plus &r ) noexcept
{
  List list;
  int i, j;
  size_t off = 0;

  list.sort<cmp_elem>();
  list.push_hd( new ( &mem[ off ] ) Elem( r.next() % 30000 ) );
  off += esz;
  list.sort<cmp_elem>();

  for ( i = 0; i < num ; i++ ) {
    list.push_hd( new ( &mem[ off ] ) Elem( r.next() % 30000 ) );
    off += esz;
  }
  //print_elem_list( list );
  list.sort<cmp_elem>();
  i = 1; j = 1;
  for ( Elem * e = list.hd; e->next != NULL; e = e->next ) {
    if ( e->i > e->next->i ) {
      fprintf( stderr, "failed next sort %u > %u\n", e->i, e->next->i );
    }
    i++;
  }
  for ( Elem * e = list.tl; e->back != NULL; e = e->back ) {
    if ( e->i < e->back->i ) {
      fprintf( stderr, "failed back sort %u < %u\n", e->i, e->back->i );
    }
    j++;
  }
  if ( i != num + 1 || j != num + 1 ) {
    printf( "count wrong: %d %d != %d\n", i, j, num + 1 );
  }
  return i;
}

int
main( void )
{
  kv::rand::xoroshiro128plus r;
  int cnt;

  r.static_init();
  for ( int i = 16; i <= 64 * 1024; i *= 2 ) {
    cnt = run_test( i, r );
    printf( "sort-%d cmp_count %d elems %d N * %.1f\n",
            i, cmp_count, cnt, (double) cmp_count / (double) cnt );
    cmp_count = 0;

    cnt = run_test( i, r );
    printf( "sort-%d cmp_count %d elems %d N * %.1f\n",
            i, cmp_count, cnt, (double) cmp_count / (double) cnt );
    cmp_count = 0;
  }
  return 0;
}

