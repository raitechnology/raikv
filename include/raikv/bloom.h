#ifndef __rai_raikv__bloom_h__
#define __rai_raikv__bloom_h__

#include <raikv/uint_ht.h>
#include <raikv/array_space.h>
#include <raikv/key_hash.h>

namespace rai {
namespace kv {

/* each hash slice is in it's own bit arenas, unlike a normal bloom filter
 * where all hashes use the same arena -- this stretches the utilization of
 * each bit, the false positive rate is less than the bit utilization rate
 *
 * 64 bits avail for bloom hashing, which gives a max of 858 million entries
 * before degrading with false positives, which increases dramatically at
 * when bits / entry < 10
 *   shift1 = 32    total size ( 1 << 32 ) / 8 * 2 = 1,073,741,824 bytes
 *   shift2 = 32    429 million entries at 20 bits / entry,
 *                  858 million entries at 10 bits / entry
 */
struct BloomBits {
  typedef uint64_t WORD;
  static const uint8_t WORD_BIT_SIZE  = sizeof( WORD ) * 8;
  static const uint8_t WORD_BYTE_SIZE = sizeof( WORD );

  const uint8_t  SHFT1, SHFT2, SHFT3, SHFT4; /* where a slice starts */
  const uint32_t seed;                       /* hash seed */
  const size_t   width;                      /* total bytes in table */
  size_t         count;                      /* count of entries */
  const size_t   resize_count;               /* when to resize bloom */
  UIntHashTab  * ht[ 4 ];
  WORD         * bits;
  const uint8_t  bwidth;

  static size_t get_width( uint32_t shft1, uint32_t shft2, uint32_t shft3,
                           uint32_t shft4 ) {
   return ( ( (size_t) 1U << shft1 ) / 8 ) + ( ( (size_t) 1U << shft2 ) / 8 ) +
          ( ( (size_t) 1U << shft3 ) / 8 ) + ( ( (size_t) 1U << shft4 ) / 8 );
  }
  static size_t get_resize( uint32_t shft1,  uint32_t shft2,  uint32_t shft3,
                            uint32_t shft4,  uint32_t N ) {
    /* static const uint8_t N = 20; balance false rate vs size */
    /* using /usr/share/dict/words as the test data
     * speed          false rate  bits/entry  bloom bits  entries  mem usage
     * 13.9 ns/lookup   <= 0.02%   33.0 bits  (8,8,8,8)        31       128
     * 13.7 ns/lookup      0.03%   33.0 bits  (9,9,9,9)        62       256
     * 13.8 ns/lookup      0.05%   33.0 bits  (10,10,10,10)   124       512
     * 13.7 ns/lookup      0.08%   33.0 bits  (11,11,11,11)   248      1024
     * 13.7 ns/lookup      0.03%   33.0 bits  (12,12,12,12)   496      2048
     * 13.8 ns/lookup      0.02%   33.0 bits  (13,13,13,13)   992      4096
     * 14.0 ns/lookup      0.04%   33.0 bits  (14,14,14,14)  1985      8192
     * 14.0 ns/lookup      0.14%   33.0 bits  (15,15,15,15)  3971     16384
     * 13.9 ns/lookup      0.19%   36.0 bits  (17,17,17,0)  10922     49152
     * 13.7 ns/lookup      0.08%   36.0 bits  (18,18,18,0)  21845     98304
     * 13.8 ns/lookup      0.07%   36.0 bits  (19,19,19,0)  43960    196608
     * 14.0 ns/lookup      0.05%   36.0 bits  (20,20,20,0)  87381    393216
     * 14.3 ns/lookup      0.05%   36.0 bits  (21,21,21,0) 174762    786432
     * 13.6 ns/lookup      0.19%   45.0 bits  (22,22,0,0)  186413   1048576
     * 13.5 ns/lookup      0.20%   45.0 bits  (23,23,0,0)  372827   2097152 */
    size_t resz = get_width( shft1, shft2, shft3, shft4 ) * 8;
    if ( shft4 != 0 )
      resz /= 13 + N; /* 13+N bits/entry ( 4 slices not 16 bits ) */
    else if ( shft3 == 0 )
      resz /= 25 + N; /* 25+N bits/entry ( 2 slices or 16 bits ) */
    else
      resz /= 16 + N; /* 16+N bits/entry ( 3 slices ) */
    return resz;
  }
  uint32_t MASK1( void ) const { return ( 1U << this->SHFT1 ) - 1; }
  uint32_t MASK2( void ) const { return ( 1U << this->SHFT2 ) - 1; }
  uint32_t MASK3( void ) const { return ( 1U << this->SHFT3 ) - 1; }
  uint32_t MASK4( void ) const { return ( 1U << this->SHFT4 ) - 1; }
  uint32_t SIZE1( void ) const { return ( 1U << this->SHFT1 ) / WORD_BIT_SIZE; }
  uint32_t SIZE2( void ) const { return ( 1U << this->SHFT2 ) / WORD_BIT_SIZE; }
  uint32_t SIZE3( void ) const { return ( 1U << this->SHFT3 ) / WORD_BIT_SIZE; }
  uint32_t SIZE4( void ) const { return ( 1U << this->SHFT4 ) / WORD_BIT_SIZE; }

  BloomBits( void *b, uint32_t shft1, uint32_t shft2, uint32_t shft3,
             uint32_t shft4,  uint32_t sd,  uint8_t width,  UIntHashTab **tab )
     : SHFT1( shft1 ), SHFT2( shft2 ), SHFT3( shft3 ), SHFT4( shft4 ),
       seed( sd ), width( get_width( shft1, shft2, shft3, shft4 ) ), count( 0 ),
       resize_count( get_resize( shft1, shft2, shft3, shft4, width ) ),
       bits( (WORD *) b ), bwidth( width ) {
    if ( tab == NULL ) {
      for ( int i = 0; i < 4; i++ )
        this->ht[ i ] = UIntHashTab::resize( NULL );
    }
    else {
      for ( int i = 0; i < 4; i++ ) {
        tab[ i ]->clear_all();
        this->ht[ i ] = tab[ i ];
      }
    }
    this->count = 0;
    ::memset( b, 0, this->width );
  }
  ~BloomBits() {
    for ( int i = 0; i < 4; i++ )
      delete this->ht[ i ];
  }
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  /* resize() doubles the size of the table, but does not populate it */
  static uint32_t calc_shift( uint32_t shft1,  uint32_t &shft2,
                              uint32_t &shft3,  uint32_t &shft4 ) noexcept;
  static BloomBits *resize( BloomBits *b,  uint32_t seed,  uint8_t width,
                            uint32_t shft1 = 8,  uint32_t shft2 = 8,
                            uint32_t shft3 = 8,  uint32_t shft4 = 8 ) noexcept;
  static BloomBits *reduce_size( BloomBits *b,  uint32_t seed ) noexcept;
  static BloomBits *increase_size( BloomBits *b,  uint32_t seed ) noexcept;
  static BloomBits *reseed( BloomBits *b,  uint32_t seed ) noexcept;

  static BloomBits *alloc( BloomBits *b,  uint32_t seed,  uint8_t width,
                           uint32_t shft1,  uint32_t shft2,
                           uint32_t shft3,  uint32_t shft4 ) noexcept;
  /* if resize needed */
  bool test_resize( void ) const {
    return ( this->count > this->resize_count && this->SHFT1 < 32 );
  }
  bool test_resize_smaller( void ) const {
    return ( this->count < this->resize_count / 8 && this->SHFT1 > 6 );
  }

  void zero( void ) {
    ::memset( this->bits, 0, this->width );
    for ( int i = 0; i < 4; i++ )
      this->ht[ i ]->clear_all();
  }
  /* first bit set in bits[], used to marshall bits */
  bool first( size_t &i ) const {
    i = 0;
    if ( ( this->bits[ 0 ] & 1 ) != 0 )
      return true;
    return this->next( i );
  }
  /* next bit set in bits[] */
  bool next( size_t &i ) const {
    size_t  off;
    uint8_t shft;
    i   += 1;
    off  = i / WORD_BIT_SIZE;
    shft = i % WORD_BIT_SIZE;
    for (;;) {
      if ( off * WORD_BYTE_SIZE == this->width )
        return false;
      WORD x = ( this->bits[ off ] >> shft );
      if ( x != 0 ) {
        if ( ( x & 1 ) != 0 )
          return true;
        i += kv_ffsl( x ) - 1;
        return true;
      }
      off++;
      i = off * WORD_BIT_SIZE;
      shft = 0;
    }
  }
  /* set a bit, used to unmarshall bits[] */
  void set_bit( size_t i ) {
    size_t  off  = i / WORD_BIT_SIZE;
    uint8_t shft = i % WORD_BIT_SIZE;
    this->bits[ off ] |= ( (WORD) 1 << shft );
  }
  /* how many bits are set, not useful except for stats/debugging */
  size_t pop_count( void ) const {
    uint64_t w[ 4 ];
    size_t   off,
             cnt = 0,
             end = this->width / WORD_BYTE_SIZE - sizeof( w ) / WORD_BYTE_SIZE;
    for ( off = 0; off <= end; off += sizeof( w ) / WORD_BYTE_SIZE ) {
      ::memcpy( w, &this->bits[ off ], sizeof( w ) );
      cnt += kv_popcountl( w[ 0 ] );
      cnt += kv_popcountl( w[ 1 ] );
      cnt += kv_popcountl( w[ 2 ] );
      cnt += kv_popcountl( w[ 3 ] );
    }
    for ( ; off < this->width / WORD_BYTE_SIZE; off++ )
      cnt += kv_popcountl( this->bits[ off ] );
    return cnt;
  }
  /* split hash into one of 4 slices */
  static uint32_t bit_slice( uint32_t mask, uint32_t shft, uint64_t h ) {
    return ( h >> shft ) & mask;
  }
  /* set a bit at slice offset */
  void set_slice( size_t off,  uint32_t slice ) {
    off += slice / WORD_BIT_SIZE;
    this->bits[ off ] |= (WORD) 1 << ( slice % WORD_BIT_SIZE );
  }
  /* clear a bit at slice offset */
  void clear_slice( size_t off,  uint32_t slice ) {
    off += slice / WORD_BIT_SIZE;
    this->bits[ off ] &= ~( (WORD) 1 << ( slice % WORD_BIT_SIZE ) );
  }
  /* test a bit at slice offset */
  bool test_slice( size_t off,  uint32_t slice ) const {
    off += slice / WORD_BIT_SIZE;
    return
      ( this->bits[ off ] & ( (WORD) 1 << ( slice % WORD_BIT_SIZE ) ) ) != 0;
  }
  /* test a bit at slice offset, return old state
   * this is the main way to set a hash since collisions are tracked in ht[] */
  bool test_set_slice( size_t off,  uint32_t slice ) {
    off += slice / WORD_BIT_SIZE;
    WORD mask = (WORD) 1 << ( slice % WORD_BIT_SIZE ),
         old  = this->bits[ off ];
    this->bits[ off ] |= mask;
    return ( old & mask );
  }
  /* add the collisions to ht[] */
  void add_ht( uint32_t c[ 4 ],  uint8_t slice ) {
    for ( uint32_t i = 0; i < 4; i++ ) {
      if ( ( slice & ( 1 << i ) ) != 0 ) {
        size_t   pos;
        uint32_t val;
        if ( this->ht[ i ]->find( c[ i ], pos, val ) ) {
          this->ht[ i ]->set( c[ i ], pos, val + 1 );
        }
        else {
          this->ht[ i ]->set_rsz( this->ht[ i ], c[ i ], pos, 1 );
        }
      } 
    }
  }
  /* subtract collisions from ht[] */
  void remove_ht( uint32_t c[ 4 ],  uint8_t &slice ) {
    for ( uint32_t i = 0; i < 4; i++ ) {
      if ( ( slice & ( 1 << i ) ) != 0 ) {
        size_t   pos;
        uint32_t val;
        if ( this->ht[ i ]->find( c[ i ], pos, val ) ) {
          if ( val == 1 ) {
            this->ht[ i ]->remove_rsz( this->ht[ i ], pos );
          }
          else {
            this->ht[ i ]->set( c[ i ], pos, val - 1 );
          }
          slice &= ~( 1 << i );
        }
      }
    } 
  }
  /* insert hash and track collisions for each slice */
  uint8_t test_set( uint64_t h,  uint32_t c[ 4 ] ) {
    size_t   i = 0;
    uint32_t s = 0;
    uint8_t  slice = 0;
    c[ 0 ] = bit_slice( this->MASK1(), s, h );
    if ( this->test_set_slice( i, c[ 0 ] ) )
      slice |= 1;
    i += this->SIZE1(); s += this->SHFT1;
    c[ 1 ] = bit_slice( this->MASK2(), s, h );
    if ( this->test_set_slice( i, c[ 1 ] ) )
      slice |= 2;
    if ( this->SHFT3 > 0 ) {
      i += this->SIZE2(); s += this->SHFT2;
      c[ 2 ] = bit_slice( this->MASK3(), s, h );
      if ( this->test_set_slice( i, c[ 2 ] ) )
        slice |= 4;
      if ( this->SHFT4 > 0 ) {
        i += this->SIZE3(); s += this->SHFT3;
        c[ 3 ] = bit_slice( this->MASK4(), s, h );
        if ( this->test_set_slice( i, c[ 3 ] ) )
          slice |= 8;
      }
    }
    return slice;
  }
  /* split hash into slices */
  uint8_t split( uint64_t h,  uint32_t c[ 4 ] ) const {
    size_t   i = 0;
    uint32_t s = 0;
    uint8_t  slice = 1 | 2;
    c[ 0 ]  = bit_slice( this->MASK1(), s, h );
    i += this->SIZE1(); s += this->SHFT1;
    c[ 1 ] = bit_slice( this->MASK2(), s, h );
    if ( this->SHFT3 > 0 ) {
      i += this->SIZE2(); s += this->SHFT2;
      c[ 2 ] = bit_slice( this->MASK3(), s, h );
      slice |= 4;
      if ( this->SHFT4 > 0 ) {
        i += this->SIZE3(); s += this->SHFT3;
        c[ 3 ] = bit_slice( this->MASK4(), s, h );
        slice |= 8;
      }
    }
    return slice;
  }
  /* clear the slices, used when removing a hash */
  void clear( uint32_t c[ 4 ],  uint8_t slice ) {
    size_t i = 0;
    if ( ( slice & 1 ) != 0 ) {
      this->clear_slice( i, c[ 0 ] );
    }
    i += this->SIZE1();
    if ( ( slice & 2 ) != 0 ) {
      this->clear_slice( i, c[ 1 ] );
    }
    if ( this->SHFT3 > 0 ) {
      i += this->SIZE2();
      if ( ( slice & 4 ) != 0 ) {
        this->clear_slice( i, c[ 2 ] );
      }
      if ( this->SHFT4 > 0 ) {
        i += this->SIZE3();
        if ( ( slice & 8 ) != 0 ) {
          this->clear_slice( i, c[ 3 ] );
        }
      }
    }
  }
  uint64_t to_hash64( uint32_t h ) const {
    /* the seed changes this hash function so that collisions occur in
     * different locations; a seed can be chosen to alter which keys collide,
     * in case there is a hot key with a collision */
    uint32_t z = kv_hash_uint2( this->seed, h - this->seed );
    return ( (uint64_t) z << 32 ) |
             (uint64_t) kv_hash_uint2( z + this->seed, this->seed - h );
#if 0
    uint32_t c1 = kv_hash_uint2( this->seed, h ),
             c2 = kv_hash_uint2( this->seed+1, h );
    return ( (uint64_t) c1 << 32 ) | (uint64_t) c2;
#endif
  }
  /* test if hash is present */
  bool is_member( uint32_t hash ) const {
    uint64_t h = this->to_hash64( hash );
    size_t   i = 0;
    uint32_t s = 0;
    bool     b = true;
    b &= this->test_slice( i, bit_slice( this->MASK1(), s, h ) );
    if ( b ) {
      i += this->SIZE1(); s += this->SHFT1;
      b &= this->test_slice( i, bit_slice( this->MASK2(), s, h ) );
      if ( b && this->SHFT3 > 0 ) {
        i += this->SIZE2(); s += this->SHFT2;
        b &= this->test_slice( i, bit_slice( this->MASK3(), s, h ) );
        if ( b && this->SHFT4 > 0 ) {
          i += this->SIZE3(); s += this->SHFT3;
          b &= this->test_slice( i, bit_slice( this->MASK4(), s, h ) );
        }
      }
    }
    return b;
  }
  /* add hash and update the collision counters */
  void add( uint32_t hash ) {
    uint32_t c[ 4 ];
    uint64_t h = this->to_hash64( hash );
    uint8_t slice = this->test_set( h, c );
    this->add_ht( c, slice );
    this->count++;
  }
  /* remove hash and subtract collision counters */
  void remove( uint32_t hash ) {
    uint32_t c[ 4 ];
    uint64_t h = this->to_hash64( hash );
    uint8_t slice = this->split( h, c );
    this->remove_ht( c, slice );
    this->clear( c, slice );
    this->count--;
  }
  uint32_t get_ht_refs( uint32_t c[ 4 ],  uint8_t slice ) {
    uint32_t min_ref = 0;
    for ( uint32_t i = 0; i < 4; i++ ) {
      if ( ( slice & ( 1 << i ) ) != 0 ) {
        size_t   pos;
        uint32_t val;
        if ( ! this->ht[ i ]->find( c[ i ], pos, val ) )
          return 0;
        if ( min_ref == 0 || val < min_ref )
          min_ref = val;
      }
    }
    return min_ref;
  }
  uint32_t ht_refs( uint32_t hash ) {
    uint32_t c[ 4 ];
    uint64_t h = this->to_hash64( hash );
    uint8_t slice = this->split( h, c );
    return this->get_ht_refs( c, slice );
  }
};

/* serialize BloomBits,
 *   distinct sections:
 *     1. bloom bits set,
 *     2. 4 * hash collisions
 *
 *   offset to ht codes              <- 4 bytes (int offset)
 *   | geom header
 *   |   shft1, shft2, shft3, shft4  <- 4 bytes
 *   |   bloom count                 <- 8 bytes
 *   |   ht entry_count              <- 8 bytes
 *   | bits size1                    <- 4 bytes (int offset)
 *   |   bits 0 -> N                 <- size1 codes
 *   | bits sizeN                    <- 4 bytes
 *   V   bits Y -> Z                 <- sizeN codes
 *   offset to ht2                   <- 4 bytes
 *   | ht1 size1                     <- 4 bytes (int offset)
 *   V   ht1 bits 0 -> N             <- ht size1 codes
 *   | count size1                   <- 4 bytes
 *   V   count bits 0 -> N           <- count size codes
 *   offset to ht3     (repeat for each ht[ 0 .. 3 ] )
 *     code ht2
 *   offset to ht4    
 *     code ht3
 *   offset to end                   <- 4 bytes (int offset)
 *     code ht4
 */
struct BloomCodec : public ArraySpace< uint32_t, 1024 > {
  uint32_t code_sz, last, idx;

  BloomCodec() : code_sz( 0 ), last( 0 ), idx( 0 ) {}

  void encode( const uint32_t *pref,  size_t npref,
               const void *details,  size_t detail_size,
               const BloomBits &bits ) noexcept;
  void size_hdr( size_t add_size ) noexcept;
  void finalize( void ) noexcept;
  void encode_pref( const uint32_t *pref,  size_t npref ) noexcept;
  void encode_details( const void *details,  size_t details_size ) noexcept;
  void encode_delta( const uint32_t *values, uint32_t &nvals ) noexcept;
  void encode_int( const uint32_t *values, uint32_t &nvals ) noexcept;
  void encode_geom( const BloomBits &bits ) noexcept;
  void encode_bloom( const BloomBits &bits ) noexcept;
  void encode_ht( const BloomBits &bits ) noexcept;

  BloomBits *decode( uint32_t *pref,  size_t npref,  void *&details,
                     size_t &details_size,  const void *code,
                     size_t len ) noexcept;
  uint32_t decode_pref( const uint32_t *code,  size_t len,  uint32_t *pref,
                        size_t npref ) noexcept;
  uint32_t decode_details( const uint32_t *code,  uint32_t off,  size_t len,
                           void *&details,  size_t &details_size ) noexcept;
  BloomBits *decode_geom( const uint32_t *buf,  uint32_t &len,
                          uint32_t *elem_count ) noexcept;
  BloomBits *decode_bloom( const uint32_t *buf,  uint32_t len,
                           uint32_t *elem_count ) noexcept;
  bool decode_ht( const uint32_t *buf,  uint32_t len ) noexcept;
  bool decode_count( BloomBits &bits, uint8_t n,  const uint32_t *buf,
                     uint32_t len ) noexcept;
};

}
}
#endif
