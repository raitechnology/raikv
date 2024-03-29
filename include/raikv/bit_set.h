#ifndef __rai_raikv__bit_set_h__
#define __rai_raikv__bit_set_h__

#include <raikv/util.h>
#include <raikv/array_space.h>

namespace rai {
namespace kv {

template <class T>
struct BitSpaceT : public ArraySpace<T, 2> {
  static const size_t WORD_BITS = sizeof( T ) * 8;

  size_t bit_size( void ) const {
    return this->size * WORD_BITS;
  }
  void extend( uint32_t b ) {
    if ( (size_t) b >= this->size * WORD_BITS )
      this->make( align<size_t>( b + 1, WORD_BITS ) / WORD_BITS, true );
  }
  uint64_t & ref( uint32_t b,  uint64_t &mask ) const {
    mask = ( (uint64_t) 1 ) << ( b % WORD_BITS );
    return this->ptr[ b / WORD_BITS ];
  }
  void add( uint32_t b ) {
    this->extend( b );
    uint64_t mask, & w = this->ref( b, mask );
    w |= mask;
  }
  void add( const uint32_t *b,  size_t bcnt ) {
    for ( size_t i = 0; i < bcnt; i++ )
      this->add( b[ i ] );
  }
  void add( const BitSpaceT &b1 ) {
    size_t max_sz = max_int<size_t>( b1.size, this->size ), i;
    this->make( max_sz, true );
    for ( i = 0; i < b1.size; i++ )
      this->ptr[ i ] |= b1.ptr[ i ];
  }
  void remove( uint32_t b ) {
    if ( b < this->size * WORD_BITS ) {
      uint64_t mask, & w = this->ref( b, mask );
      w &= ~mask;
    }
  }
  void remove( const BitSpaceT &b1 ) {
    size_t min_sz = max_int<size_t>( b1.size, this->size ), i;
    for ( i = 0; i < b1.size; i++ )
      this->ptr[ i ] &= ~b1.ptr[ i ];
  }
  bool is_member( uint32_t b ) const {
    if ( b < this->size * WORD_BITS ) {
      uint64_t mask, & w = this->ref( b, mask );
      return ( w & mask ) != 0;
    }
    return false;
  }
  bool test_set( uint32_t b ) {
    this->extend( b );
    uint64_t mask, & w = this->ref( b, mask );
    bool     is_set = ( w & mask ) != 0;
    w |= mask;
    return is_set;
  }
  bool test_clear( uint32_t b ) {
    if ( (size_t) b >= this->size * WORD_BITS )
      return false;
    uint64_t mask, & w = this->ref( b, mask );
    bool     is_set = ( w & mask ) != 0;
    w &= ~mask;
    return is_set;
  }
  bool is_empty( void ) const {
    for ( size_t i = 0; i < this->size; i++ )
      if ( this->ptr[ i ] != 0 )
        return false;
    return true;
  }
  size_t count( void ) const {
    size_t cnt = 0;
    for ( size_t i = 0; i < this->size; i++ )
      if ( this->ptr[ i ] != 0 )
        cnt += kv_popcountl( this->ptr[ i ] );
    return cnt;
  }
  void and_bits( const BitSpaceT &b1,  const BitSpaceT &b2 ) {
    size_t min_sz = min_int<size_t>( b1.size, b2.size );
    this->make( min_sz, true );
    for ( size_t i = 0; i < min_sz; i++ )
      this->ptr[ i ] = b1.ptr[ i ] & b2.ptr[ i ];
    if ( this->size > min_sz )
      ::memset( &this->ptr[ min_sz ], 0,
                ( this->size - min_sz ) * sizeof( this->ptr[ 0 ] ) );
  }
  void or_bits( const BitSpaceT &b1,  const BitSpaceT &b2 ) {
    size_t min_sz = min_int<size_t>( b1.size, b2.size ),
           max_sz = max_int<size_t>( b1.size, b2.size ), i;
    this->make( max_sz, true );
    for ( i = 0; i < min_sz; i++ )
      this->ptr[ i ] = b1.ptr[ i ] | b2.ptr[ i ];
    for ( ; i < max_sz && i < b1.size; i++ )
      this->ptr[ i ] = b1.ptr[ i ];
    for ( ; i < max_sz && i < b2.size; i++ )
      this->ptr[ i ] = b2.ptr[ i ];
  }
  void xor_bits( const BitSpaceT &b1,  const BitSpaceT &b2 ) {
    size_t min_sz = min_int<size_t>( b1.size, b2.size ),
           max_sz = max_int<size_t>( b1.size, b2.size ), i;
    this->make( max_sz, true );
    for ( i = 0; i < min_sz; i++ )
      this->ptr[ i ] = b1.ptr[ i ] ^ b2.ptr[ i ];
    for ( ; i < max_sz && i < b1.size; i++ )
      this->ptr[ i ] = b1.ptr[ i ];
    for ( ; i < max_sz && i < b2.size; i++ )
      this->ptr[ i ] = b2.ptr[ i ];
  }
  bool first( uint32_t &b ) const {
    b = 0;
    if ( this->size == 0 )
      return false;
    return this->scan( b );
  }
  bool next( uint32_t &b ) const {
    if ( ++b >= this->size * WORD_BITS )
      return false;
    return this->scan( b );
  }
  bool scan( uint32_t &b ) const {
    uint32_t off = b / WORD_BITS;
    T x = this->ptr[ off ] >> ( b % WORD_BITS );
    while ( x == 0 ) {
      b = ++off * WORD_BITS;
      if ( (size_t) off >= this->size )
        return false;
      x = this->ptr[ off ];
    }
    b += kv_ffsl( x ) - 1;
    return true;
  }
  bool intersects( const BitSpaceT &x ) const {
    size_t min_sz = min_int<size_t>( x.size, this->size ), i;
    for ( i = 0; i < min_sz; i++ ) {
      if ( ( x.ptr[ i ] & this->ptr[ i ] ) != 0 )
        return true;
    }
    return false;
  }
  bool superset( const BitSpaceT &x ) const {
    size_t min_sz = min_int<size_t>( x.size, this->size ),
           max_sz = max_int<size_t>( x.size, this->size ), i;
    for ( i = 0; i < min_sz; i++ ) {
      if ( ( x.ptr[ i ] | this->ptr[ i ] ) != this->ptr[ i ] )
        return false;
    }
    for ( ; i < max_sz && i < x.size; i++ )
      if ( x.ptr[ i ] != 0 )
        return false;
    return true;
  }
  bool equals( uint32_t b ) const {
    if ( b >= this->size * WORD_BITS )
      return false;
    for ( size_t i = 0; i < this->size; i++ ) {
      if ( this->ptr[ i ] != 0 ) {
        uint64_t mask, & w = this->ref( b, mask );
        if ( this->ptr[ i ] != mask )
          return false;
        if ( &w != &this->ptr[ i ] )
          return false;
      }
    }
    return true;
  }
  bool equals( const BitSpaceT &x ) const {
    size_t min_sz = min_int<size_t>( x.size, this->size ),
           max_sz = max_int<size_t>( x.size, this->size ), i;
    for ( i = 0; i < min_sz; i++ ) {
      if ( x.ptr[ i ] != this->ptr[ i ] )
        return false;
    }
    for ( ; i < max_sz && i < x.size; i++ )
      if ( x.ptr[ i ] != 0 )
        return false;
    for ( ; i < max_sz && i < this->size; i++ )
      if ( this->ptr[ i ] != 0 )
        return false;
    return true;
  }
  bool operator==( const BitSpaceT &x ) const {
    return this->equals( x );
  }
  bool operator!=( const BitSpaceT &x ) const {
    return ! this->equals( x );
  }
};

struct BitSpace : public BitSpaceT<uint64_t> {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  BitSpace() {}
};

template <class T>
struct BitSetT {
  static const uint32_t WORD_BITS = sizeof( T ) * 8;
  T * ptr;
  BitSetT( void *p = NULL ) : ptr( (T *) p ) {}

  T & ref( uint32_t b,  T &mask ) const {
    mask = ( (T) 1 ) << ( b % WORD_BITS );
    return this->ptr[ b / WORD_BITS ];
  }
  void add( uint32_t b ) {
    T mask, & w = this->ref( b, mask );
    w |= mask;
  }
  void remove( uint32_t b ) {
    T mask, & w = this->ref( b, mask );
    w &= ~mask;
  }
  bool is_member( uint32_t b ) const {
    T mask, & w = this->ref( b, mask );
    return ( w & mask ) != 0;
  }
  bool test_set( uint32_t b ) {
    T mask, & w = this->ref( b, mask );
    bool is_set = ( w & mask ) != 0;
    w |= mask;
    return is_set;
  }
  bool test_clear( uint32_t b ) {
    T mask, & w = this->ref( b, mask );
    bool is_set = ( w & mask ) != 0;
    w &= ~mask;
    return is_set;
  }
  bool first( uint32_t &b,  uint32_t max_bit ) const {
    b = 0;
    if ( max_bit == 0 )
      return false;
    return this->scan( b, max_bit );
  }
  bool next( uint32_t &b,  uint32_t max_bit ) const {
    if ( ++b >= max_bit )
      return false;
    return this->scan( b, max_bit );
  }
  bool scan( uint32_t &b,  uint32_t max_bit ) const {
    uint32_t off = b / WORD_BITS;
    T x = this->ptr[ off ] >> ( b % WORD_BITS );
    while ( x == 0 ) {
      b = ++off * WORD_BITS;
      if ( b >= max_bit )
        return false;
      x = this->ptr[ off ];
    }
    b += kv_ffsl( x ) - 1;
    return true;
  }
  bool set_first( uint32_t &b,  uint32_t max_bit ) {
    uint32_t off = 0;
    b = 0;
    T x = ~this->ptr[ off ];
    while ( x == 0 ) {
      b = ++off * WORD_BITS;
      if ( b >= max_bit )
        return false;
      x = ~this->ptr[ off ];
    }
    b += kv_ffsl( x ) - 1;
    this->ptr[ off ] |= (T) 1 << ( b % WORD_BITS );
    return true;
  }
  bool index( uint32_t &b,  uint32_t pos,  uint32_t max_bit ) {
    uint32_t cnt = 0, off = 0;
    b = 0;
    for ( ; off * WORD_BITS < max_bit; off++ ) {
      T x = this->ptr[ off ];
      if ( x != 0 ) {
        uint32_t n = kv_popcountl( x );
        if ( n + cnt <= pos ) {
          cnt += n;
          continue;
        }
        for ( ; ; b++ ) {
          b += kv_ffsl( x >> b ) - 1;
          if ( cnt++ == pos ) {
            b += off * WORD_BITS;
            return true;
          }
        }
      }
    }
    return false;
  }
  uint32_t count( uint32_t max_bit ) const {
    uint32_t cnt = 0;
    for ( uint32_t off = 0; off * WORD_BITS < max_bit; off++ )
      if ( this->ptr[ off ] != 0 )
        cnt += kv_popcountl( this->ptr[ off ] );
    return cnt;
  }
  static uint32_t size( uint32_t max_bit ) {
    return ( WORD_BITS - 1 + max_bit ) / WORD_BITS;
  }
  void zero( uint32_t max_bit ) {
    for ( uint32_t off = 0; off * WORD_BITS < max_bit; off++ )
      this->ptr[ off ] = 0;
  }
  bool is_empty( uint32_t max_bit ) const {
    for ( uint32_t off = 0; off * WORD_BITS < max_bit; off++ )
      if ( this->ptr[ off ] != 0 )
        return false;
    return true;
  }
  void and_bits( const BitSetT &b1,  size_t bsz1,
                 const BitSetT &b2,  size_t bsz2 ) {
    size_t min_sz = min_int<size_t>( bsz1, bsz2 );
    for ( size_t i = 0; i * WORD_BITS < min_sz; i++ )
      this->ptr[ i ] = b1.ptr[ i ] & b2.ptr[ i ];
  }
  void mask_bits( const BitSetT &mask,  size_t mask_bsz,
                  const BitSetT &b2,  size_t bsz2 ) {
    size_t min_sz = min_int<size_t>( mask_bsz, bsz2 ),
           max_sz = max_int<size_t>( mask_bsz, bsz2 ), i;
    for ( i = 0; i * WORD_BITS < min_sz; i++ )
      this->ptr[ i ] = ~mask.ptr[ i ] & b2.ptr[ i ];
    for ( ; i * WORD_BITS < max_sz && i * WORD_BITS < bsz2; i++ )
      this->ptr[ i ] = b2.ptr[ i ];
  }
  void or_bits( const BitSetT &b1,  size_t bsz1,
                const BitSetT &b2,  size_t bsz2 ) {
    size_t min_sz = min_int<size_t>( bsz1, bsz2 ),
           max_sz = max_int<size_t>( bsz1, bsz2 ), i;
    for ( i = 0; i * WORD_BITS < min_sz; i++ )
      this->ptr[ i ] = b1.ptr[ i ] | b2.ptr[ i ];
    for ( ; i * WORD_BITS < max_sz && i * WORD_BITS < bsz1; i++ )
      this->ptr[ i ] = b1.ptr[ i ];
    for ( ; i * WORD_BITS < max_sz && i * WORD_BITS < bsz2; i++ )
      this->ptr[ i ] = b2.ptr[ i ];
  }
  void xor_bits( const BitSetT &b1,  size_t bsz1,
                 const BitSetT &b2,  size_t bsz2 ) {
    size_t min_sz = min_int<size_t>( bsz1, bsz2 ),
           max_sz = max_int<size_t>( bsz1, bsz2 ), i;
    for ( i = 0; i < min_sz; i++ )
      this->ptr[ i ] = b1.ptr[ i ] ^ b2.ptr[ i ];
    for ( ; i < max_sz && i < bsz1; i++ )
      this->ptr[ i ] = b1.ptr[ i ];
    for ( ; i < max_sz && i < bsz2; i++ )
      this->ptr[ i ] = b2.ptr[ i ];
  }
  void and_bits( const BitSetT &b1,  size_t bsz1,
                 const BitSpaceT<T> &bs ) {
    BitSetT<T> b2( bs.ptr );
    size_t min_sz = min_int<size_t>( bsz1, bs.bit_size() );
    return this->and_bits( b1, min_sz, b2, min_sz );
  }
  void or_bits( const BitSetT &b1,  size_t bsz1,
                const BitSpaceT<T> &bs ) {
    BitSetT<T> b2( bs.ptr );
    size_t min_sz = min_int<size_t>( bsz1, bs.bit_size() );
    return this->or_bits( b1, bsz1, b2, min_sz );
  }
  void mask_bits( const BitSetT &mask,  size_t mask_bsz,
                  const BitSpaceT<T> &bs ) {
    BitSetT<T> b2( bs.ptr );
    size_t min_sz = min_int<size_t>( mask_bsz, bs.bit_size() );
    return this->mask_bits( mask, mask_bsz, b2, min_sz );
  }
};

typedef BitSetT<uint64_t> UIntBitSet;

struct BitSet64 {
  uint64_t w;
  BitSet64( uint64_t word = 0 ) : w( word ) {}

  bool is_member( uint32_t i ) const {
    return ( this->w & ( (uint64_t) 1 << i ) ) != 0;
  }
  bool next( uint32_t &i ) const {
    bool b = ( ++i < 64 );
    if ( b ) {
      uint64_t x = this->w >> i;
      b = ( x != 0 );
      if ( b ) i += kv_ffsl( x ) - 1;
    }
    return b;
  }
  bool first( uint32_t &i ) const {
    bool b = ( this->w != 0 );
    if ( b ) i = kv_ffsl( this->w ) - 1;
    return b;
  }
};

struct UIntArray {
  static const size_t WORD_BITS = sizeof( uint64_t ) * 8;
  static const uint64_t WORD32 = 0xffffffffU;
  uint64_t * ptr;
  uint64_t   mask;
  uint8_t    bits;
  UIntArray( uint64_t *p = NULL,  uint8_t bit_width = 0 )
    : ptr( p ), mask( 0 ), bits( 0 ) {
    if ( bit_width > 0 )
      this->set_bits( bit_width );
  }
  void set_bits( uint8_t bit_width ) {
    this->mask = ( (uint64_t) 1 << bit_width ) - 1;
    this->bits = bit_width;
  }
  void set_max_value( uint64_t max_val ) {
    uint8_t b = 1;
    for ( uint64_t w = 1; w <= max_val; w = ( w << 1 ) | 1 )
      b++;
    this->set_bits( b );
  }
  size_t index_word_size( size_t i ) {
    return UIntArray::index_word_size( this->bits, i );
  }
  void zero( size_t i ) {
    size_t w = UIntArray::index_word_size( this->bits, i );
    for ( i = 0; i < w; i++ )
      this->ptr[ i ] = 0;
  }
  static size_t index_word_size( uint8_t bits,  size_t i ) {
    return ( i * (size_t) bits + 63 ) / 64;
  }
  static uint64_t rotl( uint64_t x,  size_t k ) {
    return ( x << k ) | ( x >> ( WORD_BITS - k ) );
  }
  static uint64_t rotr( uint64_t x,  size_t k ) {
    return ( x >> k ) | ( x << ( WORD_BITS - k ) );
  }
  static uint64_t low32( uint64_t x ) {
    return x & WORD32;
  }
  static uint64_t high32( uint64_t x ) {
    return x & ~WORD32;
  }

  void set( size_t i,  uint64_t value ) {
    UIntArray::set( i, value, this->ptr, this->bits, this->mask );
  }
  static void set( size_t i,  uint64_t value,  uint64_t *w,
                   uint8_t bits,  uint64_t mask ) {
    size_t   idx  = i * (size_t) bits,
             off  = idx / 64,
             shft = idx % 64;
    uint64_t v;

    if ( shft + (size_t) bits > WORD_BITS ) {
      v  = ( high32( w[ off ] ) >> 32 ) | ( low32( w[ off + 1 ] ) << 32 );
      shft -= 32;
      v  = ( rotr( v, shft ) & ~mask ) | ( value & mask );
      v  = rotl( v, shft );
      w[ off ]     = ( low32( v )  << 32 ) | low32( w[ off ] );
      w[ off + 1 ] = ( high32( v ) >> 32 ) | high32( w[ off + 1 ] );
    }
    else {
      v = ( rotr( w[ off ], shft ) & ~mask ) | ( value & mask );
      w[ off ] = rotl( v, shft );
    }
  }

  uint64_t get( size_t i ) {
    return UIntArray::get( i, this->ptr, this->bits, this->mask );
  }
  static uint64_t get( size_t i, uint64_t *w, uint8_t bits,  uint64_t mask ) {
    size_t   idx  = i * (size_t) bits,
             off  = idx / 64,
             shft = idx % 64;
    uint64_t v    = w[ off ];

    if ( shft + (size_t) bits > WORD_BITS ) {
      v  = ( high32( v ) >> 32 ) | ( low32( w[ off + 1 ] ) << 32 );
      shft -= 32;
    }
    return ( v >> shft ) & mask;
  }
};

struct UIntArraySpace : public ArraySpace<uint64_t, 8> {
  static const size_t WORD_BITS = sizeof( uint64_t ) * 8;
  static const uint64_t WORD32 = 0xffffffffU;
  uint64_t mask;
  uint8_t  bits;
  UIntArraySpace( uint8_t bit_width = 0 ) {
    this->set_bits( bit_width );
  }
  void set_bits( uint8_t bit_width ) {
    this->mask = ( (uint64_t) 1 << bit_width ) - 1;
    this->bits = bit_width;
  }
  void set_max_value( uint64_t max_val ) {
    uint8_t b = 1;
    for ( uint64_t w = 1; w < max_val; w = ( w << 1 ) | 1 )
      b++;
    this->set_bits( b );
  }
  size_t index_word_size( size_t i ) {
    return UIntArray::index_word_size( this->bits, i );
  }
  void set( size_t i,  uint64_t value ) {
    uint64_t * w = this->make( this->index_word_size( i + 1 ), true );
    UIntArray::set( i, value, w, this->bits, this->mask );
  }

  uint64_t get( size_t i ) {
    uint64_t * w = this->make( this->index_word_size( i + 1 ), true );
    return UIntArray::get( i, w, this->bits, this->mask );
  }
};

}
}

#endif
