#ifndef __rai_raikv__cube_route_h__
#define __rai_raikv__cube_route_h__

namespace rai {
namespace kv {

/*
 * A multicast forwarding method which uses a maximum of 4 forwards per node
 * with a maximum depth of 4 for a 128 node map.  There is no randomness, a
 * given node mapping will always produce the same forwarding rules. It uses
 * bitmaps for manipulation of the links for a compact database.  The branch4
 * function chooses a maximum 4 node ranges to forward.  These do not intersect
 * with the sending node so that a cycle is not possible and a traversal of all
 * current nodes in the set at the orign will occur regardless of the graph
 * changing en route.
 */

/*#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>*/

template <size_t BITS, class RangeType>
struct CubeRoute {
  static const size_t BITS_W = BITS / 64;

  uint64_t w[ BITS_W ];

  void copy_from( const void *p ) {
    ::memcpy( this->w, p, sizeof( this->w ) );
  }
  void or_from( const void *p ) {
    for ( size_t i = 0; i < BITS_W; i++ ) {
      uint64_t x;
      ::memcpy( &x, &((const char *) p)[ i * sizeof( x ) ], sizeof( x ) );
      this->w[ i ] |= x;
    }
  }
  void copy_to( void *p ) {
    ::memcpy( p, this->w, sizeof( this->w ) );
  }
  /* no error checking here, i must be >= 0 && < BITS */
  bool is_set( size_t i ) const {
    return ( this->w[ i / 64 ] & ( (uint64_t) 1 << ( i % 64 ) ) ) != 0;
  }

  bool is_empty( void ) const {
    uint64_t x = 0;
    for ( size_t i = 0; i < BITS_W; i++ )
      x |= this->w[ i ];
    return x == 0;
  }

  void clear( size_t i ) {
    this->w[ i / 64 ] &= ~( (uint64_t) 1 << ( i % 64 ) );
  }

  void set( size_t i ) {
    this->w[ i / 64 ] |= ( (uint64_t) 1 << ( i % 64 ) );
  }

  size_t test_set( size_t i ) {
    uint64_t & wd = this->w[ i / 64 ];
    uint64_t   m  = (uint64_t) 1 << ( i % 64 );
    if ( ( wd & m ) == 0 ) {
      wd |= m;
      return 1;
    }
    return 0;
  }

  void zero( void ) {
    for ( size_t i = 0; i < BITS_W; i++ )
      this->w[ i ] = 0;
  }

  void flip( void ) {
    for ( size_t i = 0; i < BITS_W; i++ )
      this->w[ i ] = ~this->w[ i ];
  }

  void mask( size_t i ) { /* ( 1 << i ) - 1 */
    size_t j = i / 64;
    if ( i >= 64 ) {
      size_t k = j;
      do {
        this->w[ --k ] = ~(uint64_t) 0;
      } while ( k != 0 );
      i %= 64;
    }
    if ( i > 0 )
      this->w[ j++ ] = ( (uint64_t) 1 << i ) - 1;
    while ( j < BITS_W )
      this->w[ j++ ] = 0;
  }

  uint8_t fold8( void ) const {
    uint64_t x = 0;
    for ( size_t j = 0; j < BITS_W; j++ )
      x ^= this->w[ j ];
    uint32_t y = (uint32_t) ( x >> 32 ) ^ (uint32_t) x;
    uint16_t z = (uint16_t) ( y >> 16 ) ^ (uint16_t) y;
    return (uint8_t) ( z >> 8 ) ^ (uint8_t) z;
  }

  bool equals( const CubeRoute &b ) const {
    for ( size_t j = 0; j < BITS_W; j++ )
      if ( this->w[ j ] != b.w[ j ] )
        return false;
    return true;
  }

  bool test_bits( const CubeRoute &b ) const {
    for ( size_t j = 0; j < BITS_W; j++ )
      if ( ( this->w[ j ] & b.w[ j ] ) != 0 )
        return true;
    return false;
  }

  void or_bits( const CubeRoute &b ) {
    for ( size_t j = 0; j < BITS_W; j++ )
      this->w[ j ] |= b.w[ j ];
  }

  void and_bits( const CubeRoute &b ) {
    for ( size_t j = 0; j < BITS_W; j++ )
      this->w[ j ] &= b.w[ j ];
  }

  void and_mask( size_t i ) {
    CubeRoute b;
    b.mask( i );
    this->and_bits( b );
  }

  void not_bits( const CubeRoute &b ) {
    for ( size_t j = 0; j < BITS_W; j++ )
      this->w[ j ] &= ~b.w[ j ];
  }

  void not_mask( size_t i ) {
    CubeRoute b;
    b.mask( i );
    this->not_bits( b );
  }

  void shift_right( size_t i ) {
    if ( i > BITS )
      this->zero();
    else {
      this->rotate_right( i );
      this->and_mask( BITS - i );
    }
  }

  void shift_left( size_t i ) {
    if ( i > BITS )
      this->zero();
    else {
      this->rotate_left( i );
      this->not_mask( i );
    }
  }

  void rotate_right( size_t i ) {
    uint64_t w2[ BITS_W ];
    size_t j, k;
    if ( i >= 64 ) {
      j = ( i / 64 ) % BITS_W;
      for ( k = 0; k < BITS_W; k++ ) {
        w2[ k ] = this->w[ j ];
        j = ( j + 1 ) % BITS_W;
      }
      i %= 64;
      ::memcpy( this->w, w2, sizeof( this->w ) );
    }
    if ( i > 0 ) {
      for ( k = 0; k < BITS_W; k++ )
        w2[ k ] = ( this->w[ k ] >> i ) |
                  ( this->w[ ( k + 1 ) % BITS_W ] << ( 64 - i ) );
      ::memcpy( this->w, w2, sizeof( this->w ) );
    }
  }

  void rotate_left( size_t i ) {
    uint64_t w2[ BITS_W ];
    size_t j, k;
    if ( i >= 64 ) {
      j = ( i / 64 ) % BITS_W;
      for ( k = 0; k < BITS_W; k++ ) {
        w2[ j ] = this->w[ k ];
        j = ( j + 1 ) % BITS_W;
      }
      i %= 64;
      ::memcpy( this->w, w2, sizeof( this->w ) );
    }
    if ( i > 0 ) {
      for ( k = 0; k < BITS_W; k++ )
        w2[ k ] = ( this->w[ k ] << i ) |
                  ( this->w[ ( k - 1 ) % BITS_W ] >> ( 64 - i ) );
      ::memcpy( this->w, w2, sizeof( this->w ) );
    }
  }

  size_t popcount( void ) const {
    size_t p = __builtin_popcountl( this->w[ 0 ] ), i = 1;
    while ( i < BITS_W )
      p += __builtin_popcountl( this->w[ i++ ] );
    return p;
  }

  bool first_set( size_t &i ) const {
    for ( size_t j = 0; j < BITS_W; j++ ) {
      if ( this->w[ j ] != 0 ) {
        i = j * 64;
        if ( ( this->w[ j ] & 1 ) != 0 )
          return true;
        return this->next_set( i );
      }
    }
    return false;
  }

  static inline bool next_ffs( uint64_t x,  size_t &i ) {
    if ( ++i == 64 )
      return false;
    if ( (x >>= i) == 0 )
      return false;
    i += __builtin_ffsll( x ) - 1;
    return true;
  }

  bool next_set( size_t &i ) const {
    size_t off = i / 64,
           shft = i % 64;
    if ( off >= BITS_W )
      return false;
    for (;;) {
      if ( next_ffs( this->w[ off ], shft ) ) {
        i = off * 64 + shft;
        return true;
      }
      if ( ++off == BITS_W )
        return false;
      if ( ( this->w[ off ] & 1 ) != 0 ) {
        i = off * 64;
        return true;
      }
      shft = 0;
    }
  }

  void clip( size_t start,  size_t end ) {
    if ( start > 0 || end < BITS ) {
      if ( start < end ) { /* clip bits to the center */
        if ( start > 0 )
          this->not_mask( start );
        if ( end < BITS )
          this->and_mask( end + 1 );
      }
      else {               /* clip bits to the edges */
        CubeRoute x;
        x.mask( start - ( end + 1 ) );
        x.shift_left( end + 1 );
        this->not_bits( x );
      }
    }
  }
  /* Return count * 2 of ranges[] of nodes to visit, [ {start, end}, ... ],
   * start and end are inclusive.  This chooses ranges which do not intersect
   * with node so that a cycle is not possible and a traversal of all nodes
   * will occur if the graph doesn't change.  If the graph changes, the
   * traversal will include all original nodes still present and may or may not
   * include new nodes */
  size_t branch4( size_t node,  size_t start,  size_t end,
                  RangeType *range ) const {
    if ( node == start && node == end )
      return 0;
    CubeRoute b = *this;
    b.clip( start, end );
    return branch4x( node, b, range );
  }

  static size_t branch4x( size_t node,  CubeRoute &b,  RangeType *range ) {
    size_t i, j, pop, cnt;
    b.rotate_right( node ); /* shift the node to the origin, bit 0 */
    b.clear( 0 );           /* already visited node */
    pop = b.popcount();     /* nodes left to visit */
    if ( pop <= 1 ) {
      if ( ! b.first_set( i ) )
        return 0;
      i = ( i + node ) % BITS;
      range[ 0 ] = (RangeType) i;
      range[ 1 ] = (RangeType) i;
      return 2;
    }
    cnt = 0;
    j = 0;
    if ( ! b.first_set( i ) )
      return 0;
    if ( pop < 4 ) {        /* if less then 4, visit each node */
      do {
        range[ j ] = (RangeType) ( ( i + node ) % BITS );
        range[ j + 1 ] = range[ j ];
        j += 2;
        cnt++;
      } while ( b.next_set( i ) );
      return j;
    }
    range[ 0 ] = 0;        /* counts geq 4, will visit at most 4 nodes */
    range[ 1 ] = (RangeType) ( pop / 4 - 1 ); /* 0 -> 1/4 */
    range[ 2 ] = (RangeType) ( pop / 4 );
    range[ 3 ] = (RangeType) ( pop / 2 - 1 ); /* 1/4 -> 1/2 */
    range[ 4 ] = (RangeType) ( pop / 2 );
    range[ 5 ] = (RangeType) ( pop / 2 + pop / 4 - 1 ); /* 1/2 -> 3/4 */
    range[ 6 ] = (RangeType) ( pop / 2 + pop / 4 );
    range[ 7 ] = (RangeType) ( pop - 1 );     /* 3/4 -> N-1 */
    do {
      while ( (RangeType) cnt == range[ j ] )
        range[ j++ ] = (RangeType) ( ( i + node ) % BITS );
      cnt++;
    } while ( b.next_set( i ) );
    return j;
  }

  void print_bits( void ) const {
    size_t i;
    for ( i = BITS; i > 0; i-- ) {
      printf( "%c", this->is_set( i - 1 ) ? '1' : '0' );
    }
    printf( "\n" );
  }

  void print_pop( void ) const {
    size_t i;
    printf( "pop %lu: ", this->popcount() );
    if ( this->first_set( i ) ) {
      do {
        printf( "%lu ", i );
      } while ( this->next_set( i ) );
    }
    printf( "\n" );
  }

  void print_traverse( int x,  size_t node,  size_t start,  size_t end ) {
    size_t i, j;
    RangeType range[ 8 ];

    if ( (j = this->branch4( node, start, end, range )) == 0 ) {
      printf( "%*s [%lu]\n", x, "", node );
    }
    else {
      printf( "%*s [%lu] %lu -> %lu\n", x, "", node, start, end );
      for ( i = 0; i < j; i += 2 ) {
        this->print_traverse( x + 2, range[ i ], range[ i ], range[ i + 1 ] );
      }
    }
  }
};

typedef CubeRoute<128, uint8_t> CubeRoute128;

}
}

#if 0
/* the below prints:
 *
0000000000000000000000000111010101001111111111110000011100000111000000000000000
0000000001111111111111111000100100011010001010111
pop 50: 0 1 2 4 6 10 12 13 17 20 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38
 39 64 65 66 72 73 74 80 81 82 83 84 85 86 87 88 89 90 91 94 96 98 100 101 102
 [20] 0 -> 128
   [24] 24 -> 35
     [25] 25 -> 26
       [26]
     [27] 27 -> 29
       [28]
       [29]
     [30] 30 -> 31
       [31]
     [32] 32 -> 35
       [33]
       [34]
       [35]
   [36] 36 -> 81
     [37] 37 -> 38
       [38]
     [39] 39 -> 65
       [64]
       [65]
     [66] 66 -> 72
       [72]
     [73] 73 -> 81
       [74]
       [80]
       [81]
   [82] 82 -> 96
     [83] 83 -> 84
       [84]
     [85] 85 -> 87
       [86]
       [87]
     [88] 88 -> 89
       [89]
     [90] 90 -> 96
       [91]
       [94]
       [96]
   [98] 98 -> 17
     [100] 100 -> 102
       [101]
       [102]
     [0] 0 -> 2
       [1]
       [2]
     [4] 4 -> 10
       [6]
       [10]
     [12] 12 -> 17
       [13]
       [17]
*/
int
main( int argc, char *argv[] )
{
  CubeRoute128 bis/*, bis2*/;
  size_t i, j, pop, cnt, node;
  RangeType range[ 4 ];

  bis.zero();
  bis.w[ 0 ] = 0xffff123457ULL;
  bis.w[ 1 ] = 0x754fff0707ULL;
  bis.print_bits();
  bis.print_pop();
  bis.print_traverse( 0, 20, 0, 128 );

  return 0;
}
#endif

#endif
