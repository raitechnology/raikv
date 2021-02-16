#ifndef __rai_raikv__uint_ht_h__
#define __rai_raikv__uint_ht_h__

namespace rai {
namespace kv {

template <class Int>
struct IntHashTabT {
  struct Elem {
    Int hash;
    Int val;
  };

  Int  elem_count,  /* num elems used */
       tab_mask;    /* tab_size - 1 */
  Elem tab[ 1 ];

  IntHashTabT( Int sz ) : tab_mask( sz - 1 ) {
    this->clear_all();
  }
  void clear_all( void ) {
    Int sz = this->tab_mask + 1;
    uint8_t * u = (uint8_t *) (void *) &this->tab[ sz ];
    ::memset( u, 0, ( sz + 7 ) / 8 );
    this->elem_count = 0;
  }
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  Int tab_size( void ) const { return this->tab_mask + 1; }
  bool is_empty( void ) const { return this->elem_count == 0; }
  bool is_used( Int pos ) const {
    const uint8_t * u   = (uint8_t *) (void *) &this->tab[ this->tab_mask + 1 ];
    const Int       off = pos / 8;
    const uint8_t   bit = ( 1U << ( pos % 8 ) );
    return ( u[ off ] & bit ) != 0;
  }
  bool test_set( Int pos ) {
    uint8_t     * u   = (uint8_t *) (void *) &this->tab[ this->tab_mask + 1 ];
    const Int     off = pos / 8;
    const uint8_t bit = ( 1U << ( pos % 8 ) );
    bool b = ( u[ off ] & bit ) != 0;
    u[ off ] |= bit;
    return b;
  }
  /* set the hash at pos, which should be located using find() */
  void set( Int h,  Int pos,  Int v ) {
    if ( ! this->test_set( pos ) )
      this->elem_count++;
    this->tab[ pos ].hash = h;
    this->tab[ pos ].val  = v;
  }
  void get( Int pos,  Int &h,  Int &v ) const {
    h = this->tab[ pos ].hash;
    v = this->tab[ pos ].val;
  }
  void clear( Int pos ) {
    uint8_t     * u = (uint8_t *) (void *) &this->tab[ this->tab_mask + 1 ];
    const Int     off = pos / 8;
    const uint8_t bit = ( 1U << ( pos % 8 ) );
    u[ off ] &= ~bit;
    this->elem_count--;
  }
  /* room for {h,v} array + used bits */
  static size_t alloc_size( Int sz ) {
    return sizeof( IntHashTabT ) +
           ( ( sz - 1 ) * sizeof( Elem ) ) +
           ( ( sz + 7 ) / 8 * sizeof( uint8_t ) );
  }
  /* preferred is between load of 33% and 66% */
  size_t preferred_size( void ) const {
    Int cnt = this->elem_count + this->elem_count / 2;
    return cnt ? ( (size_t) 1 << ( 64 - __builtin_clzl( cnt ) ) ) : 1;
  }
  /* alloc, copy, delete, null argument is ok */
  static IntHashTabT *resize( IntHashTabT *xht ) {
    size_t sz  = ( xht == NULL ? 1 : xht->preferred_size() ),
           asz = IntHashTabT::alloc_size( sz );
    void * p   = ::malloc( asz );
    IntHashTabT * yht = NULL;
    if ( p != NULL ) {
      yht = new ( p ) IntHashTabT( sz );
      if ( xht != NULL ) {
        yht->copy( *xht );
        delete xht;
      }
    }
    return yht;
  }
  /* copy from cpy into this, does not check for duplicate entries or resize */
  void copy( const IntHashTabT &cpy ) {
    Int sz = cpy.tab_size(), cnt = cpy.elem_count;
    if ( cnt == 0 )
      return;
    for ( Int i = 0; i < sz; i++ ) {
      if ( cpy.is_used( i ) ) {
        Int h   = cpy.tab[ i ].hash,
            v   = cpy.tab[ i ].val,
            pos = h & this->tab_mask;
        for ( ; ; pos = ( pos + 1 ) & this->tab_mask ) {
          if ( ! this->is_used( pos ) ) {
            this->set( h, pos, v );
            if ( --cnt == 0 )
              return;
            break;
          }
        }
      }
    }
  }
  bool first( Int &pos ) const {
    pos = 0;
    return this->scan( pos );
  }
  bool next( Int &pos ) const {
    pos++;
    return this->scan( pos );
  }
  bool scan( Int &pos ) const {
    for ( ; pos < this->tab_size(); pos++ )
      if ( this->is_used( pos ) )
        return true;
    return false;
  }
  /* find hash, return it's position and the value if found */
  bool find( Int h,  Int &pos,  Int &val ) const {
    for ( pos = h & this->tab_mask; ; pos = ( pos + 1 ) & this->tab_mask ) {
      if ( ! this->is_used( pos ) )
        return false;
      if ( this->tab[ pos ].hash == h ) {
        val = this->tab[ pos ].val;
        return true;
      }
    }
  }
  /* udpate or insert hash */
  void upsert( Int h,  Int val ) {
    Int pos, val2;
    this->find( h, pos, val2 );
    this->set( h, pos, val );
  }
  /* check if current size is preferred */
  bool need_resize( void ) const {
    return this->tab_size() != this->preferred_size();
  }
  /* remove by reorganizing table, check the natural position of elems directly
   * following the removed elem, leaving no find() gaps in the table */
  void remove( Int pos ) {
    this->clear( pos );
    for (;;) {
      pos = ( pos + 1 ) & this->tab_mask;
      if ( ! this->is_used( pos ) )
        break;
      Int h = this->tab[ pos ].hash,
          j = h & this->tab_mask;
      if ( pos != j ) {
        this->clear( pos );
        while ( this->is_used( j ) )
          j = ( j + 1 ) & this->tab_mask;
        this->set( h, j, this->tab[ pos ].val );
      }
    }
  }
};

typedef IntHashTabT<uint32_t> UIntHashTab;
typedef IntHashTabT<uint64_t> ULongHashTab;

}
}
#endif
