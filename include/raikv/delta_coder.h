#ifndef __rai_raikv__delta_coder_h__
#define __rai_raikv__delta_coder_h__

namespace rai {
namespace kv {

/*
 * A delta coder with a range coder-like behavior at the word level.  The
 * maximum uint value that can be represented is 1<<30, but the zero prefix
 * provides an escape.  The prefix of a word indicates the bit size of the
 * delta coded members.  A zero prefix could be interpreted as a pointer, E.g.
 * 32 bits are a union:  0+ offset 31 bits | 1+ small inline code <= 30 bits
 */

static struct DeltaTable {
  uint32_t prefix_mask,
           first_mask,
           next_mask;
  uint8_t  first_shift,
           next_shift;
} delta_tab[ 16 ] = { /* 
 cnt bit-size / cnt = val bit size (after first val)
   V   V    prefix_mask  first_mask next_mask  first_shift  next_shift */
/* 1)  30*/{ 0xc0000000, 0x3fffffff,        0,           0,          0 },
/* 2)  29*/{ 0xe0000000,     0x7fff,   0x3fff,          14,         14 },
/* 3)  28*/{ 0xf0000000,      0x3ff,    0x1ff,          18,          9 },
/* 4)  27*/{ 0xf8000000,      0x1ff,     0x3f,          18,          6 },
/* 5)  26*/{ 0xfc000000,       0x3f,     0x1f,          20,          5 },
/* 6)  25*/{ 0xfe000000,       0x1f,      0xf,          20,          4 },
/* 7)  24*/{ 0xff000000,       0x3f,        7,          18,          3 },
/* 8)  23*/{ 0xff800000,      0x1ff,        3,          14,          2 },
/* 9)  22*/{ 0xffc00000,       0x3f,        3,          16,          2 },
/* 10) 21*/{ 0xffe00000,        0x7,        3,          18,          2 },
/* 11) 20*/{ 0xfff00000,      0x3ff,        1,          10,          1 },
/* 12) 19*/{ 0xfff80000,       0xff,        1,          11,          1 },
/* 13) 18*/{ 0xfffc0000,       0x3f,        1,          12,          1 },
/* 14) 17*/{ 0xfffe0000,        0xf,        1,          13,          1 },
/* 15) 16*/{ 0xffff0000,        0x3,        1,          14,          1 },
           { 0, 0, 0, 0, 0 }
};

static const uint32_t MAX_DELTA_CODE_LENGTH = 15, /* no more than 15 per code */
                      DELTA_START_PREFIX_MASK = 0xc0000000; /* first prefix */

#if 0
void gen_table( void ) {
  int cnt = 1;
  for ( int bits = 30; ; bits-- ) {
    DeltaTable & p = delta_tab[ cnt - 1 ];
    int     n = bits / cnt,
            m = n + bits % cnt;
    p.first_shift = n * ( cnt - 1 );
    p.next_shift  = n;
    p.prefix_mask = 0;

    for ( int i = 31; i >= bits; i-- )
      p.prefix_mask |= 1U << i;

    p.first_mask = ( 1U << m ) - 1;
    p.next_mask  = ( 1U << n ) - 1;
    //p.range      = p.first_mask + 1 + ( p.next_mask + 1 ) * ( cnt - 1 );

    if ( ++cnt == 15 )
      break;
  }
  /* last entry is blank */
  ::memset( &delta_tab[ 15 ], 0, sizeof( delta_tab[ 15 ] ) );
}
#endif

struct DeltaCoder {
  /* bit must be set, otherwise not encoded */
  static bool is_not_encoded( uint32_t code ) {
    static const uint32_t IS_ENCODED_MASK = 0x80000000U;
    return ( code & IS_ENCODED_MASK ) == 0;
  }
  /* code a seqn of incrementing, non-repeating values into one uint32 word */
  static uint32_t encode( uint32_t nvals,  const uint32_t *values,
                          uint32_t base ) {
    if ( nvals > 15 ) return 0;
    DeltaTable & p = delta_tab[ nvals - 1 ];
    uint32_t code  = p.prefix_mask << 1,
             last  = values[ 0 ],
             delta = last - base,
             val   = delta & p.first_mask;

    if ( val != delta )
      return 0; /* doesn't fit */

    if ( nvals > 1 ) {
      uint32_t i;
      uint8_t  shift = p.first_shift;

      code |= val << shift;
      for ( i = 1; i < nvals - 1; i++ ) {
        shift -= p.next_shift;
        delta  = values[ i ] - ( last + 1 );
        val    = delta & p.next_mask;
        last   = values[ i ];
        code  |= val << shift;
        if ( val != delta )
          return 0;
      }
      delta = values[ i ] - ( last + 1 );
      val   = delta & p.next_mask;
      if ( val != delta )
        return 0;
    }
    code |= val;
    return code; /* the result */
  }
  /* code vals into a stream of codes, bin search to find optimal coding */
  static uint32_t encode_stream( uint32_t nvals,  const uint32_t *values,
                                 uint32_t last,  uint32_t *code ) {
    uint32_t j = 0;
    for ( uint32_t i = 0; i < nvals; ) {
      uint32_t size = nvals - ( i + 1 ),
               k    = 1,
               sav, cnt, piv, c;
      if ( size > 14 ) /* typical pattern, k+piv = 8 -> 4 -> 2 -> 1 */
        size = 14;
      for ( cnt = 0; ; ) {
        piv = size / 2;
        //printf( "%u: encode( %u cnt=%u)\n", i, k + piv, cnt );
        c = encode( k + piv, &values[ i ], last );
        if ( c != 0 ) { /* as size gets smaller, code gets better */
          sav = c;
          cnt = k + piv;
        }
        if ( size == 0 )
          break;
        if ( c == 0 )
          size = piv;
        else {
          size -= piv + 1;
          k    += piv + 1;
        }
      }
      if ( cnt == 0 ) /* the value could not be coded */
        return 0;
      i   += cnt;
      last = values[ i - 1 ];
      if ( code != NULL )
        code[ j ] = sav;
      j += 1;
    }
    return j;
  }
  /* calculate length of encoding */
  static uint32_t encode_stream_length( uint32_t nvals,  const uint32_t *values,
                                        uint32_t last ) {
    return encode_stream( nvals, values, last, NULL );
  }
  /* calculate length of decoded code by examining the prefix */
  static uint32_t decode_length( uint32_t code ) {
    uint32_t mask = DELTA_START_PREFIX_MASK;
    uint32_t nvals = 1;
    for (;;) {
      if ( ( mask & code ) != mask ) {
        if ( ( mask & code ) != ( mask << 1 ) )
          return 0; /* prefix doesn't match */
        return nvals;
      }
      if ( ++nvals > MAX_DELTA_CODE_LENGTH )
        return 0;
      mask |= ( mask >> 1 );
    }
  }
  /* decode the set encoded above */
  static uint32_t decode( uint32_t code,  uint32_t *values,  uint32_t base ) {
    uint32_t nvals = decode_length( code );
    if ( nvals == 0 )
      return 0;

    DeltaTable & p = delta_tab[ nvals - 1 ];
    uint8_t  shift = p.first_shift;
    uint32_t last  = ( code >> shift ) & p.first_mask;

    last       += base;
    values[ 0 ] = last;

    if ( nvals > 1 ) {
      uint32_t i, delta;

      for ( i = 1; i < nvals - 1; i++ ) {
        shift      -= p.next_shift;
        delta       = ( code >> shift ) & p.next_mask;
        last       += delta + 1;
        values[ i ] = last;
      }
      delta       = code & p.next_mask;
      last       += delta + 1;
      values[ i ] = last;
    }
    return nvals;
  }
  /* decode the set encoded above */
  static uint32_t decode_one( uint32_t code,  uint32_t base ) {
    uint32_t nvals = decode_length( code );
    if ( nvals == 0 )
      return 0;
    DeltaTable & p = delta_tab[ nvals - 1 ];
    uint8_t  shift = p.first_shift;
    return ( ( code >> shift ) & p.first_mask ) + base;
  }
  /* decode a stream of codes into values */
  static uint32_t decode_stream( uint32_t ncodes,  const uint32_t *code,  
                                 uint32_t last,  uint32_t *values ) {
    uint32_t i = 0;
    for ( uint32_t j = 0; j < ncodes; j++ ) {
      i   += decode( code[ j ], &values[ i ], last );
      last = values[ i - 1 ];
    }
    return i;
  }
  /* calculate length of decoding */
  static uint32_t decode_stream_length( uint32_t ncodes,
                                        const uint32_t *code ) {
    uint32_t i = 0;
    for ( uint32_t j = 0; j < ncodes; j++ )
      i += decode_length( code[ j ] );
    return i;
  }
  /* test if x is a member */
  static bool test( uint32_t code,  uint32_t x,  uint32_t &base ) {
    uint32_t nvals = decode_length( code );
    if ( nvals == 0 )
      return false;

    DeltaTable & p = delta_tab[ nvals - 1 ];
    uint8_t  shift = p.first_shift;
    uint32_t last  = ( code >> shift ) & p.first_mask;

    last += base;
    base  = last;
    if ( last >= x )
      return ( last == x );

    if ( nvals > 1 ) {
      uint32_t i, delta;

      for ( i = 1; i < nvals - 1; i++ ) {
        shift -= p.next_shift;
        delta  = ( code >> shift ) & p.next_mask;
        last  += delta + 1;
        base   = last;
        if ( last >= x )
          return ( last == x );
      }
      delta = code & p.next_mask;
      last += delta + 1;
      base  = last;
      if ( last >= x )
        return ( last == x );
    }
    return false;
  }
  /* check if x is a member of stream */
  static bool test_stream( uint32_t ncodes,  const uint32_t *code,  
                           uint32_t last,  uint32_t x ) {
    for ( uint32_t j = 0; j < ncodes; j++ ) {
      if ( test( code[ j ], x, last ) ) {
        if ( last > x )
          return false;
        return true;
      }
    }
    return false;
  }
};

}
}
#endif
