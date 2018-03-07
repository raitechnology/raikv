#ifndef __rai__raikv__radix_sort_h__
#define __rai__raikv__radix_sort_h__

#ifdef __cplusplus
extern "C" {
#endif
/* a generic ht element for sorting */
typedef struct kv_ht_sort_s {
  uint64_t key;
  void   * item;
} kv_ht_sort_t;

/* sort ar[] elems in hashtable order */
void kv_ht_radix_sort( kv_ht_sort_t *ar,  uint32_t ar_size,  uint64_t ht_size );
#ifdef __cplusplus
} /* extern "C" */
#endif

#ifdef __cplusplus
namespace rai {
namespace kv {

/* This is an in-place radix sort with tail sorting using shell and bubble.
 *
 * Compare and swap using the most significant bit down to 1, this
 * is a variation of an in-place quick sort using bits as the pivots.
 *
 * Example:

struct ExampleObjType {
  uint64_t key;
  void   * ptr;
};

struct ExampleObjCompare {
  static KeyType key( ExampleObjType &v ) { // implement key() & less()
    return v.key;
  }
  static bool less( ExampleObjType &v1,  ExampleObjType &v2 ) {
    return ( v1.key < v2.key );
  }
};

struct ExampleRadixSort : public<ExampleObjType, uint64_t, ExampleObjCompare> {
  ExampleObjCompare example_cmp;
  ExampleRadixSort : RadixSort( example_cmp ) {}
};
*/
template <class ObjType, class KeyType, class ObjCompare>
struct RadixSort {
  ObjCompare & cmp;
  ObjType    * vals;      /* the array of elements to sort */
  uint32_t     val_count, /* the count in vals[] */
               bit_count;  /* how many bits are being sorted */
  bool         sort_sub_key;/* if the equal keys should be compared */

  RadixSort( ObjCompare &compare ) : cmp( compare ), vals( 0 ), val_count( 0 ),
    bit_count( 0 ), sort_sub_key( false ) {}

  void init( ObjType *valp,  uint32_t count,  KeyType max_val,
             bool has_sub_key ) {
    this->vals         = valp;
    this->val_count    = count;
    this->bit_count    = 0;  /* computed below */
    this->sort_sub_key = has_sub_key;

    /* compute the bit mask and bit_count from max value */
    KeyType m = max_val;
    /* no mask or max value, create one by or-ing all the keys */
    if ( m == 0 ) {
      for ( uint32_t i = 0; i < count; i++ )
        m |= this->cmp.key( valp[ i ] );
    }
    /* find the most significant bit (ex: m = 0x83, bit_count = 8) */
    for ( this->bit_count = 1; m != 1; m >>= 1 )
      this->bit_count++;
  }

  static void swap( ObjType &v1,  ObjType &v2 ) {
    ObjType tmp = v1; v1 = v2; v2 = tmp;
  }

  void sort( void ) {
    struct {
      uint32_t off,
               count,
               shift;
    } stack[ 256 * ( sizeof( KeyType ) - 1 ) + 128 ]; /* enough stack for key */
    KeyType  mask;
    uint32_t top,
             off,
             count,
             shift;
    /* check if there are some elements to sort */
    if ( this->val_count <= 1 )
      return;
    /* shift is the bit position of mask = ( 1 << ( shift - 1 ) ) */
    if ( (shift = this->bit_count) == 0 )
      mask = 0;
    else /* mask is the most significant bit currently being sorted */
      mask = 1 << ( shift - 1 );
    /* off is the current offset into this->vals[] being sorted */
    off   = 0;
    /* count is the length of elements currently sorted starting from off */
    count = this->val_count;
    /* top is the current top of the stack + 1 */
    top   = 0;
    for (;;) {
      /* use shell or bubble when element count sorted gets small, if mask is
       * zero, then all elements (> 1) have the same prefix and are sorted
       * with the less() function, which could have additional sub keys to
       * compare
       *
       * if many elements have the same key (no bits left in mask to sort), the 
       * count of elements to sort here could be large */
      if ( count < 32 || mask == 0 ) {
        if ( mask != 0 || this->sort_sub_key ) {
          switch ( count ) {
            case 4:
              /* bubble */
              if ( this->cmp.less( this->vals[ off + 3 ], this->vals[ off ] ) )
                swap( this->vals[ off ], this->vals[ off + 3 ] );
              if ( this->cmp.less( this->vals[ off + 3 ],
                                   this->vals[ off + 1 ] ) )
                swap( this->vals[ off + 1 ], this->vals[ off + 3 ] );
              if ( this->cmp.less( this->vals[ off + 3 ],
                                   this->vals[ off + 2 ] ) )
                swap( this->vals[ off + 2 ], this->vals[ off + 3 ] );
              if ( this->cmp.less( this->vals[ off + 2 ],
                                   this->vals[ off + 1 ] ) )
                swap( this->vals[ off + 1 ], this->vals[ off + 2 ] );
            case 3:
              if ( this->cmp.less( this->vals[ off + 2 ], this->vals[ off ] ) )
                swap( this->vals[ off ], this->vals[ off + 2 ] );
              if ( this->cmp.less( this->vals[ off + 2 ],
                                   this->vals[ off + 1 ] ) )
                swap( this->vals[ off + 1 ], this->vals[ off + 2 ] );
            case 2:
              if ( this->cmp.less( this->vals[ off + 1 ], this->vals[ off ] ) )
                swap( this->vals[ off ], this->vals[ off + 1 ] );
            case 1: /* 0,1 shouldn't happen */
            case 0:
              break;

            default: {
              /* shell */
              static const uint32_t incs[] = { 48, 21, 7, 3, 1 };
              ObjType * ptr = &this->vals[ off ],
                        v;
              uint32_t  i, j, k, ink;

              for ( k = 0; k < sizeof( incs ) / sizeof( incs[ 0 ] ); k++ ) {
                ink = incs[ k ];
                for ( i = ink; i < count; i++ ) {
                  if ( this->cmp.less( ptr[ i ], ptr[ i - ink ] ) ) {
                    v = ptr[ i ];
                    j = i;
                    do {
                      ptr[ j ] = ptr[ j - ink ];
                      j -= ink;
                    } while ( j >= ink && this->cmp.less( v, ptr[ j - ink ] ) );
                    ptr[ j ] = v;
                  }
                }
              }
            }
          }
        }
      }
      else if ( shift > 1 ) {
        /* 2 to 8 bit radix sort */
        KeyType  m;
        uint32_t i, j, k, n, x, y,
                 o[ 256 ], /* the offset of 8 bit bucket */
                 c[ 256 ]; /* the count of elements in bucket */
        /* number of bits to sort */
        k = ( shift > 8 ) ? 8 : shift;
        shift -= k;
        mask >>= k;
        /* n is the number of buckets up to 256 */
        n = (uint32_t) 1 << k;
        /* set m to be the mask of bits, up to 8 of them */
        m = (KeyType) ( n - 1 ) << shift;
        /* reset bucket counts */
        if ( k > 4 )
          ::memset( c, 0, sizeof( c[ 0 ] ) * n );
        else {
          i = 0;
          do {
            c[ i ] = 0;
          } while ( ++i != n );
        }
        x = off + count;
        i = off;
        /* count keys in each bucket */
        do {
          y = (uint32_t) ( ( this->cmp.key( this->vals[ i ] ) & m ) >> shift );
          c[ y ]++;
        } while ( ++i != x );
        /* if all keys in a single bucket, no need to sort elements */
        if ( c[ y ] == count ) {
          stack[ top ].off   = off;
          stack[ top ].count = count;
          stack[ top ].shift = shift;
          top++;
        }
        else {
          /* need to keep sorting when buckets have more than 1 element */
          if ( c[ 0 ] > 1 ) {
            stack[ top ].off   = off;
            stack[ top ].count = c[ 0 ];
            stack[ top ].shift = shift;
            top++;
          }
          /* compute the offsets of each bucket from the counts in c[] */
          o[ 0 ] = off;
          for ( i = 1; i < n; i++ ) {
            o[ i ] = o[ i - 1 ] + c[ i - 1 ];
            /* push another bucket onto the stack if more than 1 element */
            if ( c[ i ] > 1 ) {
              stack[ top ].off   = o[ i ];
              stack[ top ].count = c[ i ];
              stack[ top ].shift = shift;
              top++;
            }
          }
          /* swap elements from lower buckets to upper buckets */
          for ( i = 0; i < n; i++ ) {
            while ( c[ i ] > 0 ) {
              j = (uint32_t)
                 ( ( this->cmp.key( this->vals[ o[ i ] ] ) & m ) >> shift );
              if ( j != i ) {
                swap( this->vals[ o[ i ] ], this->vals[ o[ j ] ] );
                o[ j ]++; c[ j ]--; /* one more element in order in bucket j */
              }
              else {
                o[ i ]++; c[ i ]--; /* one more element in order in bucket i */
              }
            }
          }
        }
      }
      else {
        /* 1 bit radix, simplified version of above */
        uint32_t i = 0, j = count;

        for (;;) {
          /* march left to right, find a bit set */
          while ( i < j &&
                  ( this->cmp.key( this->vals[ off + i ] ) & mask ) == 0 )
            i++;
          /* march right to left, find a bit unset */
          while ( i < j && 
                  ( this->cmp.key( this->vals[ off + j - 1 ] ) & mask ) != 0 )
            j--;
          if ( i == j )
            break;
          /* swap high for low */
          swap( this->vals[ off + i ], this->vals[ off + j - 1 ] );
          i++;
          j--;
        }
        /* keep sorting when more than 1 element */
        if ( i > 1 ) {
          stack[ top ].off   = off;
          stack[ top ].count = i;
          stack[ top ].shift = shift - 1;
          top++;
        }
        if ( count - j > 1 ) {
          stack[ top ].off   = off + j;
          stack[ top ].count = count - j;
          stack[ top ].shift = shift - 1;
          top++;
        }
      }
      if ( top == 0 )
        break;

      --top;
      off   = stack[ top ].off;
      count = stack[ top ].count;
      shift = stack[ top ].shift;
      if ( shift == 0 )
        mask = 0;
      else /* mask is the most significant bit currently being sorted */
        mask = 1 << ( shift - 1 );
    }
  }
};

} // namespace kv
} // namespace rai
#endif // __cplusplus

#endif
