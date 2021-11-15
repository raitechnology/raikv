#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/delta_coder.h>
#include <raikv/util.h>
#include <raikv/radix_sort.h>
#include <raikv/bloom.h>

using namespace rai;
using namespace kv;

BloomBits *
BloomBits::resize( BloomBits *b,  uint32_t seed,  uint32_t shft1,
                   uint32_t shft2,  uint32_t shft3,  uint32_t shft4 ) noexcept
{
  if ( b != NULL ) {
    if ( b->count < b->resize_count / 8 ) /* make smaller */
      shft1 = b->SHFT1 - 1;
    else
      shft1 = b->SHFT1 + 1;
  }
  else if ( shft1 < 6 ) {
    shft1 = 6;
  }

  if ( shft1 * 4 < 64 ) {
    shft2 = shft3 = shft4 = shft1; /* 8 -> 15, 4 hashes */
  }
  else if ( shft1 * 3 <= 64 ) {
    if ( shft1 == 16 ) /* 16,16,16,16 and 17,17,17,0 are too close */
      shft1 = 17;
    shft2 = shft3 = shft1; /* 17,18,19,20,21 */
    shft4 = 0;
  }
  else {
    shft2 = shft1; /* 22 -> 30, 2 hashes, must be < 64 bits */
    shft3 = shft4 = 0;
  }
  size_t sz = BloomBits::get_width( shft1, shft2, shft3, shft4 ) +
              sizeof( BloomBits );
  /*size_t sz = ( ( 1U << shft1 ) / 8 ) +
              ( ( 1U << shft2 ) / 8 ) +
              ( ( 1U << shft3 ) / 8 ) +
              ( ( 1U << shft4 ) / 8 ) + sizeof( BloomBits );*/
  UIntHashTab ** ht = NULL, * htcpy[ 4 ];
  if ( b != NULL ) {
    for ( int i = 0; i < 4; i++ )
      htcpy[ i ] = b->ht[ i ];
    ht = htcpy;
  }
  void    * p      = ::realloc( (void *) b, sz );
  uint8_t * bits   = &((uint8_t *) p)[ sizeof( BloomBits ) ];
  return new ( p ) BloomBits( bits, shft1, shft2, shft3, shft4, seed, ht );
}

struct UIntElem {
  uint32_t h, val;
};

struct UIntElemCmp {
  static uint32_t key( UIntElem &v ) {
    return v.h;
  }
  static bool less( UIntElem &v1,  UIntElem v2 ) {
    return v1.h < v2.h;
  }
};

void
BloomCodec::size_hdr( size_t add_size ) noexcept
{
  if ( this->idx != 0 )
    this->ptr[ this->idx - 1 ] = this->code_sz;
  this->make( this->code_sz + add_size );
  this->idx = ++this->code_sz;
}

void
BloomCodec::finalize( void ) noexcept
{
  if ( this->idx != 0 )
    this->ptr[ this->idx - 1 ] = this->code_sz;
}

void
BloomCodec::encode_delta( const uint32_t *values, uint32_t &nvals ) noexcept
{
  uint32_t   sz   = this->code_sz;
  uint32_t * code = this->make( sz + nvals + 1 );
  if ( this->last >= values[ 0 ] )
    this->last = 0;
  code[ sz ] = DeltaCoder::encode_stream( nvals, values, this->last,
                                          &code[ sz + 1 ] );
  this->code_sz += code[ sz ] + 1;
  this->last     = values[ nvals - 1 ];
  nvals = 0;
}

void
BloomCodec::encode_int( const uint32_t *values, uint32_t &nvals ) noexcept
{
  uint32_t   sz   = this->code_sz;
  uint32_t * code = this->make( sz + nvals + 1 );
  code[ sz ] = IntCoder::encode_stream( nvals, values, &code[ sz + 1 ] );
  this->code_sz += code[ sz ] + 1;
  nvals = 0;
}

static const uint32_t HT_COUNT   = 0x80000000U, /* 8 | 4 | 2 | 1 = ht[0..3] */
                      LOW26_FLAG = 0x08000000U,
                      BIG25_FLAG = 0x04000000U;
void
BloomCodec::encode_geom( const BloomBits &bits ) noexcept
{
  uint32_t   i    = this->code_sz,
             j    = 0,
             k,
           * code = this->make( i + 8 );
  code[ i ]  = bits.SHFT1 << 24;
  code[ i ] |= bits.SHFT2 << 16;
  code[ i ] |= bits.SHFT3 << 8;
  code[ i ] |= bits.SHFT4;
  i++;
  code[ i ]  = bits.seed;
  i++;
  for ( k = 0; k < 4; k++ )
    if ( bits.ht[ k ]->elem_count != 0 )
      j |= HT_COUNT >> k;
  if ( bits.count < LOW26_FLAG ) {
    j |= ( LOW26_FLAG | bits.count );
    code[ i++ ] = j;
  }
  else {
    j |= ( BIG25_FLAG | (uint32_t) ( bits.count >> 32 ) );
    code[ i++ ] = j;
    code[ i++ ] = (uint32_t) bits.count;
  }
  for ( k = 0; k < 4; k++ )
    if ( ( j & ( HT_COUNT >> k ) ) != 0 )
      code[ i++ ] = (uint32_t) bits.ht[ k ]->elem_count;

  this->code_sz = i;
}

static const size_t MAX_VALUES = 1024;

void
BloomCodec::encode_bloom( const BloomBits &bits ) noexcept
{
  this->size_hdr( bits.count / 2 + bits.count / 4 );/* about 2.5 bytes / count*/
  this->encode_geom( bits );
  if ( bits.count * 2 < bits.resize_count ) {
    size_t   k = 0;
    uint32_t values[ MAX_VALUES ], nvals = 0;
    if ( bits.first( k ) ) {
      do {
        values[ nvals++ ] = k;
        if ( nvals == MAX_VALUES )
          this->encode_delta( values, nvals );
      } while ( bits.next( k ) );

      if ( nvals > 0 )
        this->encode_delta( values, nvals );
    }
  }
  else {
    uint32_t *out = this->make( this->code_sz + bits.width / 4 );
    out = &out[ this->code_sz ];
    ::memcpy( out, bits.bits, bits.width );
    this->code_sz += bits.width / 4;
  }
}

void
BloomCodec::encode_ht( const BloomBits &bits ) noexcept
{
  size_t      k = 0;
  uint32_t    values[ MAX_VALUES ], nvals = 0,
              elem_count = 0;
  UIntElem  * elem;
  UIntElemCmp cmp;
  size_t      pos;
  ArraySpace< UIntElem, 16 > spc2;

  for ( int i = 0; i < 4; i++ ) {
    UIntHashTab & ht = *bits.ht[ i ];
    elem_count = ht.elem_count;
    if ( elem_count == 0 )
      continue;

    this->size_hdr( elem_count );
    this->last = 0;
    k = 0;
    elem = spc2.make( elem_count );
    if ( ht.first( pos ) ) {
      do {
        ht.get( pos, elem[ k ].h, elem[ k ].val );
        k++;
      } while ( ht.next( pos ) );
    }
    RadixSort<UIntElem, uint32_t, UIntElemCmp> sort( cmp );
    sort.init( elem, elem_count, bits.MASK1(), false );
    sort.sort();

    for ( k = 0; k < elem_count; k++ ) {
      values[ nvals++ ] = elem[ k ].h;
      if ( nvals == MAX_VALUES )
        this->encode_delta( values, nvals );
    }
    if ( nvals > 0 )
      this->encode_delta( values, nvals );

    this->size_hdr( elem_count );
    for ( k = 0; k < elem_count; k++ ) {
      values[ nvals++ ] = elem[ k ].val - 1;
      if ( nvals == MAX_VALUES )
        this->encode_int( values, nvals );
    }
    if ( nvals > 0 )
      this->encode_int( values, nvals );
  }
}

void
BloomCodec::encode_pref( const uint32_t *pref,  size_t npref ) noexcept
{
  uint32_t   sz    = this->code_sz,
           * code  = this->make( sz + npref + npref / 4 + 1 ),
             i, j;
  uint8_t  * off   = (uint8_t *) (void *) code;
  j = 0;
  for ( i = 0; i < npref; i++ ) {
    if ( pref[ i ] != 0 )
      off[ j++ ] = i;
  }
  off[ j++ ] = 0xff;
  sz += ( j + 3 ) / 4;
  for ( i = 0; i < npref; i++ ) {
    if ( pref[ i ] != 0 )
      code[ sz++ ] = pref[ i ];
  }
  this->code_sz = sz;
}

void
BloomCodec::encode_details( const void *details,  size_t details_size ) noexcept
{
  uint32_t   i    = this->code_sz,
           * code = this->make( i + ( details_size + 3 ) / 4 + 1 );
  code[ i ] = (uint32_t) details_size;
  if ( details_size > 0 )
    ::memcpy( &code[ i + 1 ], details, details_size );
  this->code_sz += ( details_size + 3 ) / 4 + 1;
}

void
BloomCodec::encode( const uint32_t *pref,  size_t npref,
                    const void *details,  size_t details_size,
                    const BloomBits &bits ) noexcept
{
  this->encode_pref( pref, npref );
  this->encode_details( details, details_size );
  this->encode_bloom( bits );
  this->encode_ht( bits );
  this->finalize();
}

uint32_t
BloomCodec::decode_pref( const uint32_t *code,  size_t len,  uint32_t *pref,
                         size_t npref ) noexcept
{
  const uint8_t * off = (uint8_t *) (void *) code,
                * end = (uint8_t *) (void *) &code[ len ];
  uint32_t i, j, sz;

  for ( i = 0; i < npref; i++ )
    pref[ i ] = 0;
  for ( j = 0; ; ) {
    if ( off[ j++ ] == 0xff )
      break;
    if ( &off[ j ] >= end )
      return 0;
  }
  sz = ( j + 3 ) / 4;
  j  = 0;
  for ( i = 0; i < npref; i++ ) {
    if ( off[ j ] == 0xff )
      break;
    if ( off[ j ] >= npref )
      return 0;
    if ( sz >= len )
      return 0;
    pref[ off[ j++ ] ] = code[ sz++ ];
  }
  return sz;
}

uint32_t
BloomCodec::decode_details( const uint32_t *code,  uint32_t off,  size_t len,
                            void *&details,  size_t &details_size ) noexcept
{
  details_size = code[ off ];
  details = NULL;
  if ( details_size == 0 )
    return off + 1;
  if ( off + ( details_size + 3 ) / 4 > len )
    return 0;
  details = ::malloc( details_size );
  ::memcpy( details, &code[ off + 1 ], details_size );
  return off + ( details_size + 3 ) / 4 + 1;
}

BloomBits *
BloomCodec::decode( uint32_t *pref,  size_t npref,  void *&details,
                    size_t &details_size,  const void *code_ptr,
                    size_t len ) noexcept
{
  ArraySpace<uint32_t, 1> tmp;
  uint32_t  * code;
  if ( ( (uintptr_t) code_ptr & 3 ) == 0 )
    code = (uint32_t *) code_ptr;
  else {
    code = tmp.make( len );
    ::memcpy( code, code_ptr, len * sizeof( uint32_t ) );
  }
  uint32_t    off = 0,
              last,
              sz,
            * end = &code[ len ],
            * buf,
              elem_count[ 4 ];
  BloomBits * bits;

  if ( len == 0 )
    return NULL;
  off = this->decode_pref( code, len, pref, npref );
  if ( off == 0 || off >= len )
    return NULL;
  off = this->decode_details( code, off, len, details, details_size );
  if ( off == 0 || off >= len )
    return NULL;
  last = off;
  off  = code[ off ];
  buf  = &code[ last + 1 ];
  sz   = off - ( last + 1 );

  if ( &buf[ sz ] > end ) {
    fprintf( stderr, "bloom overrun\n" );
    return NULL;
  }
  if ( (bits = this->decode_bloom( buf, sz, elem_count )) == NULL )
    return NULL;

  for ( int i = 0; i < 4; i++ ) {
    if ( elem_count[ i ] == 0 )
      continue;
    if ( off >= len ) {
      fprintf( stderr, "bloom ht overrun\n" );
      goto fail;
    }
    last = off;
    off  = code[ off ];
    buf  = &code[ last + 1 ];
    sz   = off - ( last + 1 );

    if ( &buf[ sz ] > end ) {
      fprintf( stderr, "bloom slice overrun\n" );
      goto fail;
    }
    this->code_sz = 0;
    if ( ! this->decode_ht( buf, sz ) ) {
      fprintf( stderr, "decode slice %d failed\n", i );
      goto fail;
    }
    last = off;
    off  = code[ off ];
    buf  = &code[ last + 1 ];
    sz   = off - ( last + 1 );

    if ( &buf[ sz ] > end ) {
      fprintf( stderr, "bloom count overrun\n" );
      goto fail;
    }

    if ( ! this->decode_count( *bits, i, buf, sz ) ) {
      fprintf( stderr, "decode count %d failed\n", i );
      goto fail;
    }
  }
  return bits;
fail:;
  delete bits;
  return NULL;
}

BloomBits *
BloomCodec::decode_geom( const uint32_t *buf,  uint32_t &len,
                         uint32_t *elem_count ) noexcept
{
  if ( len < 2 )
    return NULL;
  uint8_t  shft1 = buf[ 0 ] >> 24,
           shft2 = ( buf[ 0 ] >> 16 ) & 0xff,
           shft3 = ( buf[ 0 ] >> 8 ) & 0xff,
           shft4 = buf[ 0 ] & 0xff;
  uint32_t seed  = buf[ 1 ];

  BloomBits * bits = BloomBits::resize( NULL, seed, shft1, shft2, shft3, shft4);
  uint32_t    i = 2,
              j,
              k = buf[ i++ ];

  if ( ( k & LOW26_FLAG ) != 0 )
    bits->count = k & ( LOW26_FLAG - 1 );
  else {
    bits->count = k & ( BIG25_FLAG - 1 );
    bits->count = ( bits->count << 32 ) | buf[ i++ ];
  }
  for ( j = 0; j < 4; j++ ) {
    if ( ( k & ( HT_COUNT >> j ) ) != 0 ) {
      if ( len < i )
        return NULL;
      elem_count[ j ] = buf[ i++ ];
      bits->ht[ j ]->size_ht( bits->ht[ j ], elem_count[ j ] );
    }
    else {
      elem_count[ j ] = 0;
    }
  }
  len = i;
  return bits;
}

BloomBits *
BloomCodec::decode_bloom( const uint32_t *buf,  uint32_t len,
                          uint32_t *elem_count ) noexcept
{
  uint32_t   off  = len;
  BloomBits *bits = this->decode_geom( buf, off, elem_count );
  if ( bits == NULL )
    return NULL;

  if ( bits->count * 2 < bits->resize_count ) {
    uint32_t last = 0,
             values[ MAX_VALUES ], nvals;
    for ( uint32_t i = off; i < len; ) {
      uint32_t sz = buf[ i ];
      if ( sz > MAX_VALUES || i + sz + 1 > len ) {
        fprintf( stderr, "invalid size %u\n", sz );
        delete bits;
        return NULL;
      }
      nvals = DeltaCoder::decode_stream( sz, &buf[ i + 1 ], last, values );
      last  = values[ nvals - 1 ];
      i    += sz + 1;

      for ( uint32_t j = 0; j < nvals; j++ )
        bits->set_bit( values[ j ] );
    }
  }
  else {
    if ( len != bits->width / 4 + off ) {
      fprintf( stderr, "incorrect len %u\n", len );
      delete bits;
      return NULL;
    }
    ::memcpy( bits->bits, &buf[ off ], bits->width );
  }
  return bits;
}

bool
BloomCodec::decode_ht( const uint32_t *buf,  uint32_t len ) noexcept
{
  uint32_t last = 0,
         * values, nvals;
  for ( uint32_t i = 0; i < len; ) {
    uint32_t sz = buf[ i ];
    if ( sz > MAX_VALUES || i + sz + 1 > len ) {
      fprintf( stderr, "invalid size %u\n", sz );
      return false;
    }
    values = this->make( this->code_sz + MAX_VALUES );
    values = &values[ this->code_sz ];
    nvals  = DeltaCoder::decode_stream( sz, &buf[ i + 1 ], last, values );
    last   = values[ nvals - 1 ];
    i     += sz + 1;

    this->code_sz += nvals;
  }
  return true;
}

bool
BloomCodec::decode_count( BloomBits &bits,  uint8_t n,  const uint32_t *buf,
                          uint32_t len ) noexcept
{
  uint32_t values[ MAX_VALUES ], nvals,
           k = 0;

  for ( uint32_t i = 0; i < len; ) {
    uint32_t sz = buf[ i ];
    if ( sz > MAX_VALUES || i + sz + 1 > len ) {
      fprintf( stderr, "invalid size %u\n", sz );
      return false;
    }
    nvals = IntCoder::decode_stream( sz, &buf[ i + 1 ], values );
    i    += sz + 1;

    for ( uint32_t j = 0; j < nvals; j++ ){
      uint32_t count = values[ j ] + 1;
      if ( k == this->code_sz ) {
        fprintf( stderr, "trucated count %u\n", k );
        return false;
      }
      bits.ht[ n ]->upsert( this->ptr[ k++ ], count );
    }
  }
  return true;
}

