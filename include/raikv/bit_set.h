#ifndef __rai_raikv__bit_set_h__
#define __rai_raikv__bit_set_h__

#include <raikv/util.h>
#include <raikv/array_space.h>

namespace rai {
namespace kv {

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
    for (;;) {
      T x = this->ptr[ off ] >> ( b % WORD_BITS );
      if ( x == 0 ) {
        b = ++off * WORD_BITS;
        if ( b >= max_bit )
          return false;
      }
      else {
        if ( ( x & 1 ) != 0 )
          return true;
        b += __builtin_ffsl( x ) - 1;
        return true;
      }
    }
  }
  bool set_first( uint32_t &b,  uint32_t max_bit ) {
    uint32_t off = 0;
    b = 0;
    for (;;) {
      T x = ~this->ptr[ off ];
      if ( x == 0 ) {
        b = ++off * WORD_BITS;
        if ( b >= max_bit )
          return false;
      }
      else {
        if ( ( x & 1 ) == 0 )
          b += __builtin_ffsl( x ) - 1;
        break;
      }
    }
    this->ptr[ off ] |= (T) 1 << ( b % WORD_BITS );
    return true;
  }
  bool index( uint32_t &b,  uint32_t pos,  uint32_t max_bit ) {
    uint32_t cnt = 0;
    for ( uint32_t off = 0; off * WORD_BITS < max_bit; off++ ) {
      T x = this->ptr[ off ];
      if ( x != 0 ) {
        uint32_t n = cnt;
        cnt += __builtin_popcountl( x );
        if ( cnt > pos ) {
          b    = off * WORD_BITS;
          pos -= n;
          for (;;) {
            if ( ( x & 1 ) != 0 ) {
              if ( pos == 0 )
                return true;
              pos -= 1;
            }
            x >>= 1;
            b  += 1;
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
        cnt += __builtin_popcountl( this->ptr[ off ] );
    return cnt;
  }
  void zero( uint32_t max_bit ) {
    for ( uint32_t off = 0; off * WORD_BITS < max_bit; off++ )
      this->ptr[ off ] = 0;
  }
};

typedef BitSetT<uint64_t> UIntBitSet;

struct BitSpace : public ArraySpace<uint64_t, 2> {
  static const size_t WORD_BITS = sizeof( uint64_t ) * 8;
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  BitSpace() {}

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
  void remove( uint32_t b ) {
    if ( b < this->size * WORD_BITS ) {
      uint64_t mask, & w = this->ref( b, mask );
      w &= ~mask;
    }
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
        cnt += __builtin_popcountl( this->ptr[ i ] );
    return cnt;
  }
  void and_bits( const BitSpace &b1,  const BitSpace &b2 ) {
    size_t min_sz = min<size_t>( b1.size, b2.size );
    this->make( min_sz, false );
    for ( size_t i = 0; i < min_sz; i++ )
      this->ptr[ i ] = b1.ptr[ i ] & b2.ptr[ i ];
    if ( this->size > min_sz )
      ::memset( &this->ptr[ min_sz ], 0,
                ( this->size - min_sz ) * sizeof( this->ptr[ 0 ] ) );
  }
  void or_bits( const BitSpace &b1,  const BitSpace &b2 ) {
    size_t min_sz = min<size_t>( b1.size, b2.size ),
           max_sz = max<size_t>( b1.size, b2.size ), i;
    this->make( max_sz, false );
    for ( i = 0; i < min_sz; i++ )
      this->ptr[ i ] = b1.ptr[ i ] | b2.ptr[ i ];
    for ( ; i < max_sz && i < b1.size; i++ )
      this->ptr[ i ] = b1.ptr[ i ];
    for ( ; i < max_sz && i < b2.size; i++ )
      this->ptr[ i ] = b2.ptr[ i ];
  }
  void xor_bits( const BitSpace &b1,  const BitSpace &b2 ) {
    size_t min_sz = min<size_t>( b1.size, b2.size ),
           max_sz = max<size_t>( b1.size, b2.size ), i;
    this->make( max_sz, false );
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
    for (;;) {
      uint64_t x = this->ptr[ off ] >> ( b % WORD_BITS );
      if ( x == 0 ) {
        b = ++off * WORD_BITS;
        if ( (size_t) off >= this->size )
          return false;
      }
      else {
        if ( ( x & 1 ) != 0 )
          return true;
        b += __builtin_ffsl( x ) - 1;
        return true;
      }
    }
  }
#if 0
  bool rotate( uint32_t &b ) const {
    if ( this->size == 0 )
      return false;
    size_t off = ++b / WORD_BITS,
           cnt = 0;
    if ( off >= this->size ) {
      off = 0;
      b   = 0;
    }
    for (;;) {
      uint64_t x = this->ptr[ off ] >> ( b % WORD_BITS );
      if ( x != 0 ) {
        if ( ( x & 1 ) != 0 )
          return true;
        b += __builtin_ffsl( x ) - 1;
        return true;
      }
      if ( ++off == this->size ) {
        off = 0;
        if ( ++cnt == 2 )
          return false;
      }
      b = off * WORD_BITS;
    }
  }
#endif
  bool operator==( const BitSpace &x ) const {
    size_t min_sz = min<size_t>( x.size, this->size ),
           max_sz = max<size_t>( x.size, this->size ), i;
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
  bool operator!=( const BitSpace &x ) const {
    return ! ( *this == x );
  }
};

struct BitIter64 {
  const uint64_t w;
  uint8_t i;

  BitIter64( uint64_t word ) : w( word ), i( 0 ) {}

  bool next( void ) {
    uint64_t x = this->w >> ++this->i;
    if ( ( x & 1 ) == 0 ) {
      if ( x == 0 )
        return false;
      this->i += __builtin_ffsl( x ) - 1;
    }
    return true;
  }

  bool first( void ) {
    if ( this->w == 0 )
      return false;
    this->i = 0;
    if ( ( this->w & 1 ) != 0 )
      return true;
    return this->next();
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
