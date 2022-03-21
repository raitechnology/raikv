#ifndef __rai_raikv__route_ht_h__
#define __rai_raikv__route_ht_h__

namespace rai {
namespace kv {

static const size_t TRAIL_VALLEN = 2;
struct RouteSub { /* trails data, string starts at value */
  uint32_t hash;   /* 32 bit hash value */
  uint16_t len;    /* max value size is 16 bits */
  char     value[ TRAIL_VALLEN ]; /* must be at leat 2 bytes for delete mark */
/*  bool equals( const void *s,  uint16_t l ) const {
    return l == this->len && ::memcmp( s, this->value, l ) == 0;
  }
  void copy( const void *s,  uint16_t l ) {
    ::memcpy( this->value, s, l );
  }*/
};

enum RouteFit {
  DOES_NOT_FIT = 0,
  FITS_ADJUST  = 1,
  FITS_OK      = 2
};

/* Data has DataTrail members, with value[] trailing */
template < class Data,
         void (*data_copy)( Data &, const void *, uint16_t ) = nullptr,
         bool (*data_equals)( const Data &, const void *, uint16_t ) = nullptr >
struct RouteHT {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  /* with avg 40 len entries: 2048 entries use 98k : sizeof( RouteHT<Data> ) */
  static const size_t HT_SHIFT   = 12,
                      HT_SIZE    = 1 << HT_SHIFT, /* 4096 */
                      HT_83FULL  = HT_SIZE / 6 * 5,
                      BLOCK_SIZE = 2 * HT_SIZE + 1984; /* 80k, 40b at 50% */

  struct Entry {
    uint16_t half; /* lower half of hash32, off == 0 means entry is unused */
    uint16_t off;  /* data off, &block[ BLOCK_SIZE - off ] */
  };
  /* entries are never reallocated, so uint16 is enough to count them */
  uint16_t free_off,  /* next offset free */
           count,     /* count of items inserted */
           rem_count, /* count of items removed */
           rem_size;  /* size of removed items */
  uint32_t min_hash_val,   /* min hash value stored */
           max_hash_val;   /* max hash value stored (inclusive) */
  Entry    entry[ HT_SIZE ];    /* ht index of data */
  uint64_t block[ BLOCK_SIZE ]; /* data items, aligned on uint64_t */

  RouteHT() {
    this->reset();
    this->min_hash_val = 0;
    this->max_hash_val = ~(uint32_t) 0;
  }
  /* if all items removed, reset instead of split */
  void reset( void ) {
    this->free_off  = 0;
    this->count     = 0;
    this->rem_count = 0;
    this->rem_size  = 0;
    ::memset( this->entry, 0, sizeof( this->entry ) );
  }
  /* dup this from cp */
  void copy( const RouteHT &cp ) {
    this->free_off     = cp.free_off;
    this->count        = cp.count;
    this->rem_count    = cp.rem_count;
    this->rem_size     = cp.rem_size;
    this->min_hash_val = cp.min_hash_val;
    this->max_hash_val = cp.max_hash_val;
    ::memcpy( this->entry, cp.entry, sizeof( this->entry ) );
    ::memcpy( &this->block[ BLOCK_SIZE - this->free_off ],
              &cp.block[ BLOCK_SIZE - this->free_off ],
              sizeof( this->block[ 0 ] ) * (size_t) this->free_off );
  }
  /* reorganize, compress space */
  void adjust( void ) {
    if ( this->count != this->rem_count ) {
      RouteHT<Data, data_copy, data_equals> x; /* temp copy */
      x.insert_all( *this );
      x.min_hash_val = this->min_hash_val;
      x.max_hash_val = this->max_hash_val;
      this->copy( x );
    }
    else {
      this->reset();
    }
  }

  /*static size_t ht_size( void ) { return HT_SIZE; }
  static size_t ht_shift( void ) { return HT_SHIFT; }*/
  /* number of int32 words needed for data + string length l */
  static size_t intsize( uint16_t value_len ) {
    return ( (size_t) value_len + ( sizeof( Data ) - TRAIL_VALLEN ) +
             sizeof( uint64_t ) - 1 ) / sizeof( uint64_t );
  }
  /* if entry string length l fits into ht */
  RouteFit fits( uint16_t value_len ) const {
    size_t xoff = (size_t) this->free_off + intsize( value_len );
    if ( (size_t) ( this->count - this->rem_count ) < HT_83FULL ) {
      if ( xoff <= BLOCK_SIZE ) /* fits without adjusting */
        return FITS_OK;
      xoff -= (size_t) this->rem_size;
      if ( xoff <= BLOCK_SIZE ) /* only fits after adjusting */
        return FITS_ADJUST;
    }
    return DOES_NOT_FIT; /* does not fit */
  }
  /* need a marker to iterate over (non-removed) data elems */
  static inline void mark_removed( Data *d ) {
    uint16_t val = 0;
    ::memcpy( d->value, &val, 2 ); /* value must be a string based key */
  }
  static inline bool is_removed( const Data *d ) {
    uint16_t val;
    ::memcpy( &val, d->value, 2 );
    return val == 0;
  }
  static void do_copy( Data &data,  const void *s,  uint16_t l ) {
    if ( data_copy != NULL )
      data_copy( data, s, l );
    else
      ::memcpy( data.value, s, l );
    if ( l == 0 )
      data.value[ 0 ] = 1; /* not removed */
  }
  /* put data at location in ht */
  Data *inplace( uint32_t h,  const void *s,  uint16_t l,  uint16_t i ) {
    Data * data;
    size_t next_off = (size_t) this->free_off + intsize( l );
    if ( next_off > BLOCK_SIZE )
      return NULL;
    this->free_off = (uint16_t) next_off;
    this->count++;
    this->entry[ i ].off  = this->free_off;
    this->entry[ i ].half = (uint16_t) h;
    data = (Data *) (void *) &this->block[ BLOCK_SIZE - this->free_off ];
    data->hash = h;
    data->len = l;
    do_copy( *data, s, l );
    return data;
  }
  /* resize data element, if possible */
  Data *resize( uint16_t i,  uint16_t new_sz ) {
    uint16_t off  = this->entry[ i ].off,
             new_off;
    Data   * data = (Data *) (void *) &this->block[ BLOCK_SIZE - off ],
           * new_data;
    uint16_t nz   = (uint16_t) intsize( new_sz ),
             dz   = (uint16_t) intsize( data->len );
    bool     mark;

    if ( off == this->free_off ) { /* adjust free off if at the end */
      if ( nz == dz ) {
        data->len = new_sz;
        return data;
      }
      if ( nz > dz ) { /* if growing */
        if ( (size_t) ( nz - dz ) + (size_t) off > BLOCK_SIZE )
          return NULL;
        this->free_off += ( nz - dz );
        new_off = this->free_off;
      }
      else { /* if reducing */
        this->free_off -= ( dz - nz );
        new_off = this->free_off;
      }
      mark = false;
    }
    else { /* move to end */
      if ( this->free_off + nz > BLOCK_SIZE )
        return NULL;
      this->free_off += nz;
      new_off = this->free_off;
      mark = true;
    }
    new_data = (Data *) (void *) &this->block[ BLOCK_SIZE - new_off ];
    if ( dz > nz )
      dz = nz;
    ::memmove( new_data, data, dz * sizeof( this->block[ 0 ] ) );
    new_data->len = new_sz;
    this->entry[ i ].off = new_off;
    if ( mark )
      mark_removed( data );
    return new_data;
  }
  /* insert data to ht, no dup check */
  Data *insert( uint32_t h,  const void *s,  uint16_t l,  uint16_t &j ) {
    uint16_t i = (uint16_t) ( h % HT_SIZE );

    while ( this->entry[ i ].off != 0 )
      i = ( i + 1 ) % HT_SIZE;
    j = i;
    return this->inplace( h, s, l, i );
  }
  /* find data and pos by hash */
  Data *locate_hash( uint32_t h,  uint16_t &pos ) const {
    pos = (uint16_t) ( h % HT_SIZE );
    return this->iterate_hash( h, pos );
  }
  Data *iterate_hash( uint32_t h,  uint16_t &pos ) const {
    Data   * data;
    uint16_t i = (uint16_t) ( pos % HT_SIZE );

    while ( this->entry[ i ].off != 0 ) {
      if ( this->entry[ i ].half == (uint16_t) h ) {
        data = (Data *) (void *)
               &this->block[ BLOCK_SIZE - this->entry[ i ].off ];
        if ( data->hash == h ) {
          pos = i;
          return data;
        }
      }
      i = ( i + 1 ) % HT_SIZE;
    }
    pos = i;
    return NULL;
  }
  uint16_t locate_data( const Data *data ) const {
    uint32_t h = data->hash;
    uint16_t i = (uint16_t) ( h % HT_SIZE );

    while ( this->entry[ i ].off != 0 ) {
      Data *data2 = (Data *) (void *)
             &this->block[ BLOCK_SIZE - this->entry[ i ].off ];
      if ( (void *) data == (void *) data2 )
        return i;
      i = ( i + 1 ) % HT_SIZE;
    }
    return HT_SIZE;
  }
  static bool test_equals( const Data &data,  const void *s,  uint16_t l ) {
    if ( data_equals != NULL )
      return data_equals( data, s, l );
    return l == data.len && ::memcmp( s, data.value, l ) == 0;
  }
  /* find data and position by key */
  Data *locate( uint32_t h,  const void *s,  uint16_t l, uint16_t &pos ) const {
    Data   * data;
    uint16_t i = (uint16_t) ( h % HT_SIZE );

    while ( this->entry[ i ].off != 0 ) {
      if ( this->entry[ i ].half == (uint16_t) h ) {
        data = (Data *) (void *)
               &this->block[ BLOCK_SIZE - this->entry[ i ].off ];
        if ( data->hash == h && test_equals( *data, s, l ) ) {
          pos = i;
          return data;
        }
      }
      i = ( i + 1 ) % HT_SIZE;
    }
    pos = i;
    return NULL;
  }
  /* locate with hash collision counter */
  Data *locate2( uint32_t h,  const void *s,  uint16_t l,
                 uint16_t &pos,  uint32_t &hcnt ) const {
    Data   * data,
           * found = NULL;
    uint16_t i = (uint16_t) ( h % HT_SIZE );
    hcnt = 0;
    while ( this->entry[ i ].off != 0 ) {
      if ( this->entry[ i ].half == (uint16_t) h ) {
        data = (Data *) (void *)
               &this->block[ BLOCK_SIZE - this->entry[ i ].off ];
        if ( data->hash == h ) {
          hcnt++;
          if ( found == NULL && test_equals( *data, s, l ) ) {
            pos = i;
            found = data;
          }
        }
      }
      i = ( i + 1 ) % HT_SIZE;
    }
    if ( found == NULL )
      pos = i;
    return found;
  }
  Data *get( uint16_t i ) {
    return (Data *) (void *) &this->block[ BLOCK_SIZE - this->entry[ i ].off ];
  }
  /* remove data by location */
  Data *remove( uint16_t i ) {
    return this->remove( i, this->get( i ) );
  }
  /* remove data by locating then removing */
  Data *remove( uint32_t h,  const void *s,  uint16_t l ) {
    Data   * data;
    uint16_t i;

    if ( (data = this->locate( h, s, l, i )) == NULL )
      return NULL;
    return this->remove( i, data );
  }
  /* remove data already located at pos i */
  Data *remove( uint16_t i,  Data *data ) {
    uint16_t j, off;

    this->rem_count++;
    this->rem_size += (uint16_t) intsize( data->len );
    mark_removed( data );
    this->entry[ i ].off = 0;
    for (;;) {
      i = ( i + 1 ) % HT_SIZE;
      if ( (off = this->entry[ i ].off) == 0 )
        break;
      j = this->entry[ i ].half % HT_SIZE;
      if ( i != j ) {
        this->entry[ i ].off = 0;
        while ( this->entry[ j ].off != 0 )
          j = ( j + 1 ) % HT_SIZE;
        this->entry[ j ].half = this->entry[ i ].half;
        this->entry[ j ].off  = off;
      }
    }
    return data;
  }
  /* init pos = 0, iterate through ht entries */
  Data *iter( uint16_t &pos ) {
    uint16_t i;
    for (;;) {
      if ( pos == HT_SIZE )
        return NULL;
      i = pos;
      pos++;
      if ( this->entry[ i ].off != 0 )
        return (Data *) (void *)
               &this->block[ BLOCK_SIZE - this->entry[ i ].off ];
    }
  }
  /* init off = free_off, iterate through data entries */
  Data *iter_data( uint16_t &off ) {
    for (;;) {
      if ( off == 0 )
        return NULL;
      Data   * data = (Data *) (void *) &this->block[ BLOCK_SIZE - off ];
      off -= (uint16_t) intsize( data->len );
      if ( ! is_removed( data ) )
        return data;
    }
  }
  /* copy and insert data to this */
  void copy_ins( Data *data ) {
    uint16_t i = (uint16_t) ( data->hash % HT_SIZE );
    size_t  sz;

    while ( this->entry[ i ].off != 0 )
      i = ( i + 1 ) % HT_SIZE;
    sz = intsize( data->len );
    this->free_off += (uint16_t) sz;
    this->count++;
    this->entry[ i ].off  = this->free_off;
    this->entry[ i ].half = (uint16_t) data->hash;
    ::memcpy( &this->block[ BLOCK_SIZE - this->free_off ],
              data, sz * sizeof( this->block[ 0 ] ) );
  }
  /* fetch data at entry[ i ] */
  Data *deref( uint16_t i ) {
    uint16_t off = this->entry[ i ].off;
    if ( off == 0 )
      return NULL;
    Data * data = (Data *) (void *) &this->block[ BLOCK_SIZE - off ];
    if ( ! is_removed( data ) )
      return data;
    return NULL;
  }
  /* split this into two, with half the entries in each table */
  void split( RouteHT &l ) { /* l = left, the lesseq hash values */
    RouteHT<Data, data_copy, data_equals> r; /* r = right, the geq hash values */
    uint32_t   piv,
               min_h = this->min_hash_val,
               max_h = this->max_hash_val;
    int        cmp;
    Data     * data;
    uint32_t * tmph = (uint32_t *) (void *) r.block;
    uint16_t   off, cnt = 0;
    /* bsearch the median value */
    for (;;) {
      piv = min_h + ( max_h - min_h ) / 2;
      if ( piv == min_h )
        break;

      int lteq = 0, gt = 0; /* counts lteq and gt piv */
      if ( cnt == 0 ) {
        off = this->free_off;
        while ( (data = this->iter_data( off )) != NULL ) {
          tmph[ cnt++ ] = data->hash; /* copy for better cache behavior */
          if ( data->hash > piv )
            gt++;
          else
            lteq++;
        }
      }
      else { /* second time through */
        for ( off = 0; off < cnt; off++ ) {
          if ( tmph[ off ] > piv )
            gt++;
          else
            lteq++;
        }
      }
      cmp = lteq - gt;
      if ( cmp > 0 )
        max_h = piv;
      else
        min_h = piv;
      if ( (uint32_t) ( cmp + 1 ) < 3 )
        break;
    }
    /* split values left and right, left is lteq, right is gt */
    off = this->free_off;
    while ( (data = this->iter_data( off )) != NULL ) {
      if ( data->hash > piv )
        r.copy_ins( data );
      else
        l.copy_ins( data );
    }
    l.min_hash_val = this->min_hash_val;
    l.max_hash_val = piv;
    r.min_hash_val = piv + 1;
    r.max_hash_val = this->max_hash_val;
    this->copy( r );
  }
  /* insert all entries in fr to this */
  void insert_all( RouteHT &fr ) {
    Data * data;
    for ( uint16_t off = fr.free_off; (data = fr.iter_data( off )) != NULL; )
      this->copy_ins( data );
  }
  /* merge two into one, if possible */
  bool merge( RouteHT &fr ) {
    if ( (uint32_t) this->free_off + (uint32_t) fr.free_off -
         ( (uint32_t) this->rem_size + (uint32_t) fr.rem_size ) > BLOCK_SIZE )
      return false;
    if ( (uint32_t) this->count + (uint32_t) fr.count -
         ( (uint32_t) this->rem_count + (uint32_t) fr.rem_count ) >= HT_83FULL )
      return false;

    RouteHT<Data, data_copy, data_equals> x; /* temp copy */
    x.insert_all( *this );
    x.insert_all( fr );
    x.min_hash_val = this->min_hash_val;
    if ( fr.min_hash_val < this->min_hash_val )
      x.min_hash_val = fr.min_hash_val;
    x.max_hash_val = this->max_hash_val;
    if ( fr.max_hash_val > this->max_hash_val )
      x.max_hash_val = fr.max_hash_val;
    this->copy( x );
    return true;
  }
};

struct RouteLoc {
  uint32_t i;
  uint16_t j;
  bool     is_new;

  void init( void ) { this->i = 0; this->j = 0; this->is_new = false; }
};

/* Data has DataTrail members, with value[] trailing */
template < class Data,
         void (*data_copy)( Data &, const void *, uint16_t ) = nullptr,
         bool (*data_equals)( const Data &, const void *, uint16_t ) = nullptr >
struct RouteVec {
                           /* ht vector, in order by max_hash_val[] */
  RouteHT<Data, data_copy, data_equals> ** vec;
  uint32_t * max_hash_val, /* vec[ k ], h > max_hash_val[ k-1 ] <= [ k ]*/
             vec_size;     /* count of vec[] */

  RouteVec() : vec( 0 ), max_hash_val( 0 ), vec_size( 0 ) {}
  RouteVec( RouteVec &v )
    : vec( v.vec ), max_hash_val( v.max_hash_val ), vec_size( v.vec_size ) {}

  void release( void ) {
    if ( this->vec_size > 0 ) {
      for ( uint32_t i = 0; i < this->vec_size; i++ )
        delete this->vec[ i ];
      ::free( this->vec );
      this->vec          = NULL;
      this->max_hash_val = NULL;
      this->vec_size     = 0;
    }
  }

  size_t pop_count( void ) const {
    size_t cnt = 0, rem = 0;
    for ( uint32_t i = 0; i < this->vec_size; i++ ) {
      cnt += this->vec[ i ]->count;
      rem += this->vec[ i ]->rem_count;
    }
    return cnt - rem;
  }

  /* find the vec[] that hash belongs */
  uint32_t bsearch( uint32_t h ) const {
    uint32_t size = this->vec_size, k = 0, piv;
    for (;;) {
      if ( size < 3 ) {
        if ( size >= 1 && h > this->max_hash_val[ k ] ) {
          k++;
          if ( size == 2 && h > this->max_hash_val[ k ] )
            k++;
        }
        return k;
      }
      piv = size / 2;
      if ( h <= this->max_hash_val[ k + piv ] )
        size = piv;
      else {
        size -= piv + 1;
        k    += piv + 1;
      }
    }
  }
  bool bsearch_init( RouteLoc &loc,  uint32_t h ) {
    loc.init();
    if ( this->vec_size == 0 ) {
      if ( ! this->init_ht() )
        return false;
    }
    if ( this->vec_size > 1 )
      loc.i = this->bsearch( h );
    return true;
  }
  bool fits( RouteLoc &loc,  uint32_t h,  uint16_t l ) {
    switch ( this->vec[ loc.i ]->fits( l ) ) {
      case DOES_NOT_FIT: /* split vec[ i ] into two, then insert */
        if ( ! this->split_ht( loc.i ) )
          return NULL;
        if ( h > this->max_hash_val[ loc.i ] )
          loc.i++;
        break;
      case FITS_ADJUST: /* adjust first, then insert */
        this->vec[ loc.i ]->adjust();
        break;
      case FITS_OK: /* put data at locate() position */
        return true;
    }
    return false;
  }
  /* add new data, may be duplicate */
  Data *insert( uint32_t h,  const void *s,  size_t l,  RouteLoc &loc ) {
    return this->insert( h, s, (uint16_t) l, loc );
  }
  Data *insert( uint32_t h,  const void *s,  uint16_t l,  RouteLoc &loc ) {
    if ( ! this->bsearch_init( loc, h ) )
      return NULL;
    loc.is_new = true;
    this->fits( loc, h, l );
    return this->vec[ loc.i ]->insert( h, s, l, loc.j );
  }
  /* add new data */
  Data *insert( uint32_t h,  const void *s,  size_t l ) {
    return this->insert( h, s, (uint16_t) l );
  }
  Data *insert( uint32_t h,  const void *s,  uint16_t l ) {
    RouteLoc loc;
    return this->insert( h, s, l, loc );
  }
  /* insert or find data if present */
  Data *upsert( uint32_t h,  const void *s,  size_t l,  RouteLoc &loc ) {
    return this->upsert( h, s, (uint16_t) l, loc );
  }
  Data *upsert( uint32_t h,  const void *s,  uint16_t l,  RouteLoc &loc ) {
    Data * data;
    if ( ! this->bsearch_init( loc, h ) )
      return NULL;
    data = this->vec[ loc.i ]->locate( h, s, l, loc.j );
    if ( data != NULL )
      return data;
    loc.is_new = true;
    if ( ! this->fits( loc, h, l ) )
      this->vec[ loc.i ]->locate( h, s, l, loc.j );
    return this->vec[ loc.i ]->inplace( h, s, l, loc.j );
  }
  Data *upsert2( uint32_t h,  const void *s,  size_t l,  RouteLoc &loc,
                 uint32_t &hcnt ) {
    return this->upsert2( h, s, (uint16_t) l, loc, hcnt );
  }
  Data *upsert2( uint32_t h,  const void *s,  uint16_t l,  RouteLoc &loc,
                 uint32_t &hcnt ) {
    Data * data;
    hcnt = 0;
    if ( ! this->bsearch_init( loc, h ) )
      return NULL;
    data = this->vec[ loc.i ]->locate2( h, s, l, loc.j, hcnt );
    if ( data != NULL )
      return data;
    loc.is_new = true;
    if ( ! this->fits( loc, h, l ) )
      this->vec[ loc.i ]->locate2( h, s, l, loc.j, hcnt );
    return this->vec[ loc.i ]->inplace( h, s, l, loc.j );
  }
  /* insert if not present */
  Data *upsert( uint32_t h,  const void *s,  size_t l ) {
    return this->upsert( h, s, (uint16_t) l );
  }
  Data *upsert( uint32_t h,  const void *s,  uint16_t l ) {
    RouteLoc loc;
    return this->upsert( h, s, l, loc );
  }
  /* resize a data element */
  Data *resize( uint32_t h,  const void *s,  size_t old_len,
                size_t new_len,  RouteLoc &loc ) {
    return this->resize( h, s, (uint16_t) old_len, (uint16_t) new_len, loc );
  }
  Data *resize( uint32_t h,  const void *s,  uint16_t old_len,
                uint16_t new_len,  RouteLoc &loc ) {
    Data *data = this->vec[ loc.i ]->resize( loc.j, new_len );
    if ( data != NULL )
      return data;

    switch ( this->vec[ loc.i ]->fits( new_len ) ) {
      case DOES_NOT_FIT: /* split vec[ i ] into two, then insert */
        if ( ! this->split_ht( loc.i ) )
          return NULL;
        if ( h > this->max_hash_val[ loc.i ] )
          loc.i++;
        break;
      default: /* adjust */
        this->vec[ loc.i ]->adjust();
        break;
    }
    this->vec[ loc.i ]->locate( h, s, old_len, loc.j );
    return this->vec[ loc.i ]->resize( loc.j, new_len );
  }
  /* find data by hash and value, fill in location */
  Data *find( uint32_t h,  const void *s,  size_t l,  RouteLoc &loc ) const {
    return this->find( h, s, (uint16_t) l, loc );
  }
  Data *find( uint32_t h,  const void *s,  uint16_t l,  RouteLoc &loc ) const {
    loc.init();
    if ( this->vec_size == 0 )
      return NULL;
    if ( this->vec_size > 1 )
      loc.i = this->bsearch( h );
    return this->vec[ loc.i ]->locate( h, s, l, loc.j );
  }
  /* find a data by hash and value */
  Data *find( uint32_t h,  const void *s,  size_t l ) const {
    return this->find( h, s, (uint16_t) l );
  }
  Data *find( uint32_t h,  const void *s,  uint16_t l ) const {
    RouteLoc loc;
    return this->find( h, s, l, loc );
  }
  Data *find2( uint32_t h,  const void *s,  size_t l,  RouteLoc &loc,
               uint32_t &hcnt ) const {
    return this->find2( h, s, (uint16_t) l, loc, hcnt );
  }
  Data *find2( uint32_t h,  const void *s,  uint16_t l,  RouteLoc &loc,
               uint32_t &hcnt ) const {
    loc.init();
    hcnt = 0;
    if ( this->vec_size == 0 )
      return NULL;
    if ( this->vec_size > 1 )
      loc.i = this->bsearch( h );
    return this->vec[ loc.i ]->locate2( h, s, l, loc.j, hcnt );
  }
  /* find first element by hash */
  Data *find_by_hash( uint32_t h,  RouteLoc &loc ) const {
    loc.init();
    if ( this->vec_size == 0 )
      return NULL;
    if ( this->vec_size > 1 )
      loc.i = this->bsearch( h );
    return this->vec[ loc.i ]->locate_hash( h, loc.j );
  }
  /* find next element by hash */
  Data *find_next_by_hash( uint32_t h,  RouteLoc &loc ) const {
    loc.j++;
    return this->vec[ loc.i ]->iterate_hash( h, loc.j );
  }
  /* locate first element that matches hash */
  Data *find_by_hash( uint32_t h ) const {
    RouteLoc loc;
    return this->find_by_hash( h, loc );
  }
  /* allocate the first block */
  bool init_ht( void ) {
    return this->split_ht( 0 );
  }
  /* when vec[] is full, split it */
  bool split_ht( uint32_t i ) {
    RouteVec<Data, data_copy, data_equals> tmp( *this );
    size_t sz = ( sizeof( uint32_t ) +
                  sizeof( RouteHT<Data, data_copy, data_equals> * ) ) *
                ( this->vec_size + 1 );
    void * p = ::malloc( sz ),
         * v = ::malloc( sizeof( RouteHT<Data, data_copy, data_equals> ) );
    uint32_t j;
    if ( p == NULL || v == NULL )
      return false;
    this->vec_size++;
    this->vec = (RouteHT<Data, data_copy, data_equals> **) p;
    this->max_hash_val = (uint32_t *) (void *) &this->vec[ this->vec_size ];
    for ( j = 0; j < i; j++ ) {
      this->vec[ j ] = tmp.vec[ j ];
      this->max_hash_val[ j ] = tmp.max_hash_val[ j ];
    }
    for ( j = i; j < tmp.vec_size; j++ ) {
      this->vec[ j + 1 ] = tmp.vec[ j ];
      this->max_hash_val[ j + 1 ] = tmp.max_hash_val[ j ];
    }
    if ( tmp.vec != NULL )
      ::free( tmp.vec );
    this->vec[ i ] = new ( v ) RouteHT<Data, data_copy, data_equals>();
    if ( i + 1 < this->vec_size )
      this->vec[ i + 1 ]->split( *this->vec[ i ] );
    this->max_hash_val[ i ] = this->vec[ i ]->max_hash_val;
    return true;
  }
  /* find data by hash, value and remove if found */
  bool remove( uint32_t h,  const void *s,  size_t l ) {
    return this->remove( h, s, (uint16_t) l );
  }
  bool remove( uint32_t h,  const void *s,  uint16_t l ) {
    Data   * data;
    uint32_t i = 0;
    if ( this->vec_size == 0 )
      return false;
    if ( this->vec_size > 1 )
      i = this->bsearch( h );
    if ( (data = this->vec[ i ]->remove( h, s, l )) != NULL )
      this->try_shrink( i );
    return data != NULL;
  }
  /* remove data at location and shrink vec[] */
  bool remove( RouteLoc &loc ) {
    Data * data;
    if ( (data = this->vec[ loc.i ]->remove( loc.j )) != NULL ) {
      this->try_shrink( loc.i );
    }
    return data != NULL;
  }
  /* return data from a find result */
  Data *get( RouteLoc &loc ) {
    return this->vec[ loc.i ]->get( loc.j );
  }
  /* try to merge vec[ i ] into vec[ i - 1 ] */
  void try_shrink( uint32_t i ) {
    if ( i > 0 &&
         this->vec[ i ]->rem_count * 2 > this->vec[ i ]->count &&
         this->vec[ i - 1 ]->merge( *this->vec[ i ] ) ) {
      delete this->vec[ i ];
      this->max_hash_val[ i - 1 ] = this->max_hash_val[ i ];
      this->vec_size -= 1;
      for ( ; i < this->vec_size; i++ ) {
        this->vec[ i ] = this->vec[ i + 1 ];
        this->max_hash_val[ i ] = this->max_hash_val[ i + 1 ];
      }
      uint32_t * ptr = (uint32_t *) (void *) &this->vec[ this->vec_size ];
      ::memmove( ptr, this->max_hash_val,
                 sizeof( uint32_t ) * this->vec_size );
      this->max_hash_val = ptr;
    }
  }
  /* find first data element */
  Data *first( uint32_t &v,  uint16_t &off ) {
    Data * data;
    for ( v = 0; v < this->vec_size; v++ ) {
      off = this->vec[ v ]->free_off;
      data = this->vec[ v ]->iter_data( off );
      if ( data != NULL )
        return data;
    }
    return NULL;
  }
  /* must use first() before next() */
  Data *next( uint32_t &v,  uint16_t &off ) {
    Data * data;
    for (;;) {
      if ( v >= this->vec_size )
        return NULL;
      data = this->vec[ v ]->iter_data( off );
      if ( data != NULL )
        return data;
      if ( ++v < this->vec_size )
        off = this->vec[ v ]->free_off;
    }
  }
  /* remove current data ptr and iterate to next */
  Data *remove_and_next( Data *old,  uint32_t &v,  uint16_t &off ) {
    uint16_t j = this->vec[ v ]->locate_data( old );
    this->vec[ v ]->remove( j, old );
    return this->next( v, off );
  }
  /* the RouteLoc refers to vec[ i ]->block[ j ], this is different
   * from find usage, which are vec[ i ]->entry[ j ] */
  Data *first( RouteLoc &loc ) {
    return this->first( loc.i, loc.j );
  }
  /* iterate by data location */
  Data *next( RouteLoc &loc ) {
    return this->next( loc.i, loc.j );
  }

  Data *remove_and_next( Data *old,  RouteLoc &loc) {
    return this->remove_and_next( old, loc.i, loc.j );
  }
};

}
}

#endif
