#ifndef __rai_raikv__uint_ht_h__
#define __rai_raikv__uint_ht_h__

namespace rai {
namespace kv {
/* common vars used in the different hash tables below
 * has routines that track the slot usage with bit string that is sized 1 bit
 * per ht entry
 * the bits are stored after the ht elememts:
 * HashTable {
 *   size_t     elem_count
 *              tab_mask
 *              min_count
 *              max_count
 *   Hash+Value ht[ 0 ]
 *              ...
 *              ht[ tab_mask ]
 *   Bit        bits[ 0 ]
 *              ...
 *              bits[ tab_mask ]
 * }
 */
#if __GNUC__ >= 11
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
#endif
struct IntHashUsage {
  size_t elem_count,  /* num elems used */
         tab_mask,    /* tab_size - 1 */
         min_count,   /* when to resize to smaller size */
         max_count;   /* when to resize to larger size */

  IntHashUsage( size_t sz ) {
    this->reset_size( sz );
  }
  void reset_size( size_t sz ) {
    this->elem_count = 0;
    this->tab_mask   = sz - 1;
    this->min_count  = sz / 2 - sz / 4; /* 25% */
    this->max_count  = sz / 2 + sz / 4; /* 75% */
  }
  void copy_geom( const IntHashUsage &bt ) {
    this->elem_count = bt.elem_count;
    this->tab_mask   = bt.tab_mask;
    this->min_count  = bt.min_count;
    this->max_count  = bt.max_count;
  }
  static size_t bits_size( size_t bits ) {
    return ( ( bits + 63 ) / 64 ) * 8;
  }
  size_t tab_size( void ) const { return this->tab_mask + 1; }
  /* clear the bits that indicate a slot is used */
  void clear_all_elems( void *u ) {
    ::memset( u, 0, bits_size( this->tab_size() ) );
    this->elem_count = 0;
  }
  /* preferred is between load of 25% and 75% */
  size_t preferred_size( void ) const {
    size_t sz = this->tab_size();
    if ( this->elem_count <  this->min_count ) return sz / 2;
    if ( this->elem_count >= this->max_count ) return sz * 2;
    return sz;
  }
  /* check if current size is preferred */
  bool need_resize( void ) const {
    return this->tab_size() != this->preferred_size();
  }
  bool is_empty( void ) const { return this->elem_count == 0; }
  /* test bit is used at pos */
  bool is_used( size_t pos,  const void *usage_bits ) const {
    const size_t   off = pos / 64;
    const uint64_t bit = ( (uint64_t) 1 << ( pos % 64 ) );
    return ( ((const uint64_t *) usage_bits)[ off ] & bit ) != 0;
  }
  /* test and set, return the old state, used to insert new element */
  bool test_set( size_t pos,  void *usage_bits ) {
    const size_t   off = pos / 64;
    const uint64_t bit = ( (uint64_t) 1 << ( pos % 64 ) );
    const bool     b   = ( ((uint64_t *) usage_bits)[ off ] & bit ) != 0;
    if ( ! b ) {
      ((uint64_t *) usage_bits)[ off ] |= bit;
      this->elem_count++;
    }
    return b;
  }
  /* remove elem by clearing bit */
  void clear( size_t pos,  void *usage_bits ) {
    const size_t   off = pos / 64;
    const uint64_t bit = ( (uint64_t) 1 << ( pos % 64 ) );
    ((uint64_t *) usage_bits)[ off ] &= ~bit;
    this->elem_count--;
  }
  /* scan for next bit set */
  bool scan( size_t &pos,  const void *usage_bits ) const {
    for ( ; pos < this->tab_size(); pos++ )
      if ( this->is_used( pos, usage_bits ) )
        return true;
    return false;
  }
};
/* copy from one table to another
 * this is most of the cost for inserting / deleting
 */
#if 0
/* this version has a locality test to separate the
 * two areas that an entry will be relocated
 * but could not see a difference with the simpler algo */
template <class IntHashTab>
void copy_tab( IntHashTab &dest,  const IntHashTab &src )
{
  const size_t     cpy_size  = src.tab_size();
  const uint64_t * cpy_u     = (const uint64_t *) src.used_c();
  void           * u         = dest.used();
  size_t           pos_i[ 64 ],
                   pos_j[ 64 ];
  const uint32_t   j_pattern =
    ( dest.tab_mask > src.tab_mask ) ?
    ( dest.tab_mask & ~src.tab_mask ) : ( src.tab_mask & ~dest.tab_mask );

  for ( size_t off = 0; off < cpy_size; off += 64 ) {
    size_t i = 0, j = 0, k = off;
    for ( uint64_t bits = *cpy_u++; bits != 0; ) {
      if ( ( bits & 1 ) != 0 ) {
        if ( ( src.tab[ off ].hash & j_pattern ) == 0 )
          pos_i[ i++ ] = k;
        else
          pos_j[ j++ ] = k;
      }
      bits >>= 1; k++;
    }
    for ( k = 0; k < i; k++ ) {
      size_t old_pos = pos_i[ k ];
      size_t new_pos = src.tab[ old_pos ].hash & dest.tab_mask;
      while ( dest.test_set( new_pos, u ) )
        new_pos = ( new_pos + 1 ) & dest.tab_mask;
      dest.tab[ new_pos ] = src.tab[ old_pos ];
    }
    for ( k = 0; k < j; k++ ) {
      size_t old_pos = pos_j[ k ];
      size_t new_pos = src.tab[ old_pos ].hash & dest.tab_mask;
      while ( dest.test_set( new_pos, u ) )
        new_pos = ( new_pos + 1 ) & dest.tab_mask;
      dest.tab[ new_pos ] = src.tab[ old_pos ];
    }
  }
}
#endif
template <class IntHashTab>
void copy_tab( IntHashTab &dest,  const IntHashTab &src )
{
  const size_t cpy_size = src.tab_size(); /* size of source copy */
  const void * cpy_u    = src.used_c();   /* bits for source usage */
  void       * u        = dest.used();    /* bits for dest usage */
  for ( size_t i = 0; i < cpy_size; i++ ) {
    if ( src.is_used( i, cpy_u ) ) {
      size_t new_pos = src.tab[ i ].hash & dest.tab_mask; /* new position */
      for (;;) {
        if ( ! dest.test_set( new_pos, u ) ) { /* add to chain */
          dest.tab[ new_pos ] = src.tab[ i ];  /* copy hash and value */
          break;
        }
        new_pos = ( new_pos + 1 ) & dest.tab_mask; /* find empty slot */
      }
    }
  }
}
/* remove position by reorganizing table, check the natural position of elems
 * directly following the removed elem, leaving no find() gaps in the table */
template <class IntHashTab>
void remove_tab( IntHashTab &x,  size_t pos )
{
  void * u = x.used();
  x.clear( pos, u );  /* clear usage bit */
  for (;;) {
    pos = ( pos + 1 ) & x.tab_mask; /* check the chain following pos */
    if ( ! x.is_used( pos, u ) )
      return;
    size_t i = x.tab[ pos ].hash & x.tab_mask; /* natural position of entry */
    if ( pos != i ) {               /* if next is not in natural pos */
      x.clear( pos, u );            /* clear bit and find a better spot */
      while ( x.test_set( i, u ) )  /* this may be the original slot */
        i = ( i + 1 ) & x.tab_mask;
      if ( pos != i )
        x.tab[ i ] = x.tab[ pos ];  /* copy the hash and value */
    }
  }
}
/* make a copy at new size and delete the old */
template <class IntHashTab>
void resize_tab( IntHashTab *&xht,  size_t sz )
{
  size_t asz = IntHashTab::alloc_size( sz ); /* allocate ht new size */
  void * p   = ::malloc( asz );
  IntHashTab * yht = NULL;
  if ( p != NULL ) {
    yht = new ( p ) IntHashTab( sz );
    if ( xht != NULL ) {
      yht->copy( *xht );  /* copy to new table */
      delete xht;         /* delete the old table */
    }
    xht = yht;            /* use the copy with the new size */
  }
}
/* if tab is resizable, make a copy at new size and delete the old */
template <class IntHashTab>
bool check_resize_tab( IntHashTab *&xht )
{
  size_t sz = 1;
  if ( xht != NULL ) {
    sz = xht->preferred_size();
    if ( sz == xht->tab_size() )
      return false;
  }
  resize_tab<IntHashTab>( xht, sz );
  return true;
}
template <class IntHashTab>
void size_tab( IntHashTab *&xht,  size_t cnt )
{
  size_t sz = 0;
  if ( cnt > 0 ) {
    for ( sz = 2; ; sz *= 2 ) {
      size_t min_cnt = sz / 2 - sz / 4, /* 25% */
             max_cnt = sz / 2 + sz / 4; /* 75% */
      if ( cnt >= min_cnt && cnt < max_cnt )
        break;
    }
  }
  if ( xht == NULL || xht->tab_size() != sz )
    resize_tab<IntHashTab>( xht, sz );
}
/* usage:
 *
 * typedef IntHashTabT<uint,uint> MyHT;
 * MyHT * ht = MyHT::resize( NULL );
 * ht->upsert( x, y );
 * MyHt::check_resize( ht );
 * if ( ht->find( x, pos ) ) {
 *   ht->remove( pos );
 *   MyHt::check_resize( ht );
 * }
 */
template <class Int, class Value>
struct IntHashTabT : public IntHashUsage
{
  struct Elem {
    Int   hash;
    Value val;
  };
  Elem tab[ 2 ];    /* [ 2 ] to make it aligned on uint64_t */

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  IntHashTabT( size_t sz ) : IntHashUsage( sz ) {
    this->clear_all_elems( this->used() );
  }
  void * used( void ) { return &this->tab[ this->tab_size() ]; }
  const void * used_c( void ) const { return &this->tab[ this->tab_size() ]; }
  void clear_all( void ) {
    this->clear_all_elems( this->used() );
  }
  /* set the hash at pos, which should be located using find() */
  void set( Int h,  size_t pos,  Value v ) {
    this->test_set( pos, this->used() );
    this->tab[ pos ].hash = h;
    this->tab[ pos ].val  = v;
  }
  static void set_rsz( IntHashTabT *&xht,  Int h,  size_t pos,  Value val ) {
    xht->set( h, pos, val );
    check_resize( xht );
  }
  void get( size_t pos,  Int &h,  Value &v ) const {
    h = this->tab[ pos ].hash;
    v = this->tab[ pos ].val;
  }
  /* room for {h,v} array + used bits */
  static size_t alloc_size( size_t sz ) {
    size_t n = sizeof( IntHashTabT );
    if ( sz > 2 )
      n += ( sz - 2 ) * sizeof( Elem );
    return n + IntHashUsage::bits_size( sz );
  }
  /* alloc, copy, delete, null argument is ok */
  static IntHashTabT *resize( IntHashTabT *xht ) {
    check_resize( xht );
    return xht;
  }
  static bool check_resize( IntHashTabT *&xht ) {
    return check_resize_tab<IntHashTabT>( xht );
  }
  static void size_ht( IntHashTabT *&xht, size_t count ) {
    size_tab<IntHashTabT>( xht, count );
  }
  /* copy from cpy into this, does not check for duplicate entries or resize */
  void copy( const IntHashTabT &cpy ) {
    copy_tab<IntHashTabT>( *this, cpy );
  }
  /* iterate through elements */
  bool first( size_t &pos ) const {
    pos = 0;
    return this->scan( pos, this->used_c() );
  }
  bool next( size_t &pos ) const {
    pos++;
    return this->scan( pos, this->used_c() );
  }
  /* find hash by scanning chain, return it's position and the value if found */
  bool find( Int h,  size_t &pos ) const {
    const void * u = this->used_c();
    for ( pos = h & this->tab_mask; ; pos = ( pos + 1 ) & this->tab_mask ) {
      if ( ! this->is_used( pos, u ) )
        return false;
      if ( this->tab[ pos ].hash == h )
        return true;
    }
  }
  bool find( Int h,  size_t &pos,  Value &val ) const {
    if ( ! this->find( h, pos ) )
      return false;
    val = this->tab[ pos ].val;
    return true;
  }
  /* udpate or insert hash */
  void upsert( Int h,  Value val ) {
    size_t pos;
    this->find( h, pos );
    this->set( h, pos, val );
  }
  static void upsert_rsz( IntHashTabT *&xht,  Int h,  Value val ) {
    xht->upsert( h, val );
    check_resize( xht );
  }
  /* remove at pos */
  void remove( size_t pos ) {
    remove_tab<IntHashTabT>( *this, pos );
  }
  bool find_remove( Int h ) {
    size_t pos;
    if ( ! this->find( h, pos ) )
      return false;
    this->remove( pos );
    return true;
  }
  void remove_rsz( IntHashTabT *&xht,  size_t pos ) {
    this->remove( pos );
    check_resize( xht );
  }
  bool find_remove_rsz( IntHashTabT *&xht,  Int h ) {
    size_t pos;
    if ( ! this->find( h, pos ) )
      return false;
    this->remove_rsz( xht, pos );
    return true;
  }
  size_t mem_size( void ) const {
    return IntHashTabT::alloc_size( this->tab_size() );
  }
};
typedef IntHashTabT<uint32_t, uint32_t> UIntHashTab;
typedef IntHashTabT<uint64_t, uint64_t> UInt64HashTab;

/* allocates the tab elems separately, slightly slower, but less clutter,
 * no need to call check_resize()
 * usage:
 *
 * typedef IntHashTabX<uint,uint> MyHT;
 * MyHT ht;
 * ht.upsert( x, y );
 * if ( ht.find( x, pos ) )
 *   ht.remove( pos );
 */
template <class Int, class Value>
struct IntHashTabX : public IntHashUsage
{
  struct Elem {
    Int   hash;
    Value val;
  };
  Elem   buf[ 2 ];
  Elem * tab;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  IntHashTabX( void *p,  size_t sz ) : IntHashUsage( sz ) {
    this->tab = (Elem *) p;
    this->clear_all_elems( this->used() );
  }
  IntHashTabX() : IntHashUsage( 1 ) {
    this->tab = this->buf;
    this->clear_all_elems( this->used() );
  }
  ~IntHashTabX() {
    if ( this->tab != this->buf )
      ::free( this->tab );
  }
  void clear_all( void ) {
    this->clear_all_elems( this->used() );
  }
  /* move after resizing, this->tab = mv.tab; mv.tab = mv.buf */
  void move( IntHashTabX &mv ) {
    if ( this->tab != this->buf )
      ::free( this->tab );
    this->copy_geom( mv );
    if ( mv.tab == mv.buf ) {
      ::memcpy( (void *) this->buf, (void *) mv.buf, sizeof( this->buf ) );
      this->tab = this->buf;
    }
    else {
      this->tab = mv.tab;
      mv.tab = mv.buf;
    }
  }
  void * used( void ) { return &this->tab[ this->tab_size() ]; }
  const void * used_c( void ) const { return &this->tab[ this->tab_size() ]; }
  /* set the hash at pos, which should be located using find() */
  void set( Int h,  size_t pos,  Value v ) {
    this->tab[ pos ].hash = h;
    this->tab[ pos ].val  = v;
    if ( ! this->test_set( pos, this->used() ) ) {
      if ( this->elem_count >= this->max_count )
        this->resize( this->tab_size() * 2 );
    }
  }
  void get( size_t pos,  Int &h,  Value &v ) const {
    h = this->tab[ pos ].hash;
    v = this->tab[ pos ].val;
  }
  /* room for {h,v} array + used bits */
  static size_t alloc_size( size_t sz ) {
    return ( sz * sizeof( Elem ) ) + IntHashUsage::bits_size( sz );
  }
  /* alloc, copy, delete, null argument is ok */
  void resize( size_t sz ) {
    if ( sz > 0 ) {
      size_t asz = IntHashTabX::alloc_size( sz );
      void * p   = ::malloc( asz );
      IntHashTabX yht( p, sz );
      if ( p != NULL ) {
        yht.copy( *this );
        this->move( yht );
      }
    }
  }
  /* copy from cpy into this, does not check for duplicate entries or resize */
  void copy( const IntHashTabX &cpy ) {
    copy_tab<IntHashTabX>( *this, cpy );
  }
  /* iterate through elements */
  bool first( size_t &pos ) const {
    pos = 0;
    return this->scan( pos, this->used_c() );
  }
  bool next( size_t &pos ) const {
    pos++;
    return this->scan( pos, this->used_c() );
  }
  /* find hash by scanning chain, return it's position and the value if found */
  bool find( Int h,  size_t &pos ) const {
    const void * u = this->used_c();
    for ( pos = h & this->tab_mask; ; pos = ( pos + 1 ) & this->tab_mask ) {
      if ( ! this->is_used( pos, u ) )
        return false;
      if ( this->tab[ pos ].hash == h )
        return true;
    }
  }
  bool find( Int h,  size_t &pos,  Value &val ) const {
    if ( ! this->find( h, pos ) )
      return false;
    val = this->tab[ pos ].val;
    return true;
  }
  /* udpate or insert hash */
  void upsert( Int h,  Value val ) {
    size_t pos;
    this->find( h, pos );
    this->set( h, pos, val );
  }
  /* remove at pos */
  void remove( size_t pos ) {
    remove_tab<IntHashTabX>( *this, pos );
    if ( this->elem_count < this->min_count )
      this->resize( this->tab_size() / 2 );
  }
  size_t mem_size( void ) const {
    return IntHashTabX::alloc_size( this->tab_size() );
  }
};
/* no value, just keys
 * usage:
 *
 * typedef IntHashTabU<uint> MyHT;
 * MyHT * ht = MyHT::resize( NULL );
 * ht->upsert( x );
 * MyHt::check_resize( ht );
 * if ( ht->find( x, pos ) ) {
 *   ht->remove( pos );
 *   MyHt::check_resize( ht );
 * }
 */
template <class Int>
struct IntHashTabU : public IntHashUsage
{
  struct Elem {
    Int hash;
  };
  Elem tab[ 2 ];    /* [ 2 ] to make it aligned on uint64_t */

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  IntHashTabU( size_t sz ) : IntHashUsage( sz ) {
    this->clear_all_elems( this->used() );
  }
  void * used( void ) { return &this->tab[ this->tab_size() ]; }
  const void * used_c( void ) const { return &this->tab[ this->tab_size() ]; }
  void clear_all( void ) {
    this->clear_all_elems( this->used() );
  }
  /* set the hash at pos, which should be located using find() */
  void set( Int h,  size_t pos ) {
    this->test_set( pos, this->used() );
    this->tab[ pos ].hash = h;
  }
  static void set_rsz( IntHashTabU *&xht,  Int h,  size_t pos ) {
    xht->set( h, pos );
    check_resize( xht );
  }
  void get( size_t pos,  Int &h ) const {
    h = this->tab[ pos ].hash;
  }
  /* room for {h,v} array + used bits */
  static size_t alloc_size( size_t sz ) {
    size_t n = sizeof( IntHashTabU );
    if ( sz > 2 )
      n += ( sz - 2 ) * sizeof( Elem );
    return n + IntHashUsage::bits_size( sz );
  }
  /* alloc, copy, delete, null argument is ok */
  static IntHashTabU *resize( IntHashTabU *xht ) {
    check_resize( xht );
    return xht;
  }
  static bool check_resize( IntHashTabU *&xht ) {
    return check_resize_tab<IntHashTabU>( xht );
  }
  static void size_ht( IntHashTabU *&xht, size_t count ) {
    size_tab<IntHashTabU>( xht, count );
  }
  /* copy from cpy into this, does not check for duplicate entries or resize */
  void copy( const IntHashTabU &cpy ) {
    copy_tab<IntHashTabU>( *this, cpy );
  }
  /* iterate through elements */
  bool first( size_t &pos ) const {
    pos = 0;
    return this->scan( pos, this->used_c() );
  }
  bool next( size_t &pos ) const {
    pos++;
    return this->scan( pos, this->used_c() );
  }
  /* find hash by scanning chain, return it's position and the value if found */
  bool find( Int h,  size_t &pos ) const {
    const void * u = this->used_c();
    for ( pos = h & this->tab_mask; ; pos = ( pos + 1 ) & this->tab_mask ) {
      if ( ! this->is_used( pos, u ) )
        return false;
      if ( this->tab[ pos ].hash == h )
        return true;
    }
  }
  /* udpate or insert hash */
  void upsert( Int h ) {
    size_t pos;
    this->find( h, pos );
    this->set( h, pos );
  }
  static void upsert_rsz( IntHashTabU *&xht,  Int h ) {
    xht->upsert( h );
    check_resize( xht );
  }
  /* remove at pos */
  void remove( size_t pos ) {
    remove_tab<IntHashTabU>( *this, pos );
  }
  size_t mem_size( void ) const {
    return IntHashTabU::alloc_size( this->tab_size() );
  }
};
#if __GNUC__ >= 11
#pragma GCC diagnostic pop
#endif
}
}
#endif
