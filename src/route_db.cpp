#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/route_db.h>
#include <raikv/pattern_cvt.h>

using namespace rai;
using namespace kv;

RoutePublish &
RoutePDB::get_service( const char *svc,  uint32_t num ) noexcept
{
  if ( svc == NULL || ::strcmp( svc, "default" ) == 0 )
    return *this;
  size_t   len = ::strlen( svc );
  char   * s   = (char *) ::malloc( len + 10 );
  uint32_t h   = kv_crc_c( svc, len, num );
  if ( num > 0 ) {
    len = ::snprintf( s, len + 10, "%s.%x", svc, num );
  }
  else {
    ::memcpy( s, svc, len );
    s[ len ] = '\0';
  }
  RouteLoc loc;
  RouteService * rt = this->svc_db.upsert( h, s, len, loc );
  if ( loc.is_new ) {
    void * m = aligned_malloc( sizeof( RoutePublish ) );
    rt->sub_route = new ( m ) RoutePublish( this->poll, s );
  }
  return *rt->sub_route;
}

RouteDB::RouteDB() noexcept
{
  for ( size_t i = 0; i < sizeof( this->rt_hash ) /
                          sizeof( this->rt_hash[ 0 ] ); i++ ) {
    this->rt_hash[ i ] = UIntHashTab::resize( NULL );
    this->bloom_pref_count[ i ] = 0;
  }
  this->entry_count   = 0;
  this->pat_mask      = 0;
  this->rt_mask       = 0;
  this->bloom_mask    = 0;
  this->pat_bit_count = 0;

  for ( uint8_t j = 0; j < 64; j++ ) {
    SysWildSub w( NULL, j );
    this->pre_seed[ j ] = kv_crc_c( w.sub, w.len, 0 );
  }
}

RouteCache::RouteCache() noexcept
{
  this->ht         = RteCacheTab::resize( NULL );
  this->end        = 0;
  this->free       = 0;
  this->count      = 0;
  this->busy       = 0;
  this->need       = 0;
  this->is_invalid = false;
}

bool
RouteCache::reset( void ) noexcept
{
  if ( this->busy ) {
    this->need       = 0;
    this->is_invalid = true;
    return false;
  }
  this->ht->clear_all();
  this->end        = 0;
  this->free       = 0;
  this->count      = 0;
  this->busy       = 0;
  this->need       = 0;
  this->is_invalid = false;
  return true;
}

RouteZip::RouteZip() noexcept
{
  this->init();
}

void
RouteZip::init( void ) noexcept
{
  this->zht         = UIntHashTab::resize( NULL );
  this->code_end    = 0;
  this->code_free   = 0;
}

void
RouteZip::reset( void ) noexcept
{
  size_t i;
  delete this->zht;
  this->code_buf.reset();
  this->zroute_spc.reset();
  for ( i = 0; i < sizeof( this->route_spc ) /
                   sizeof( this->route_spc[ 0 ] ); i++ )
    this->route_spc[ i ].reset();
  for ( i = 0; i < sizeof( this->bloom_spc ) /
                   sizeof( this->bloom_spc[ 0 ] ); i++ )
    this->bloom_spc[ i ].reset();
  this->init();
}

uint32_t *
RouteZip::make_code_ref_space( uint32_t ecnt,  uint32_t &off ) noexcept
{
  uint32_t i = CodeRef::alloc_words( ecnt );
  this->code_buf.make( this->code_end + i );
  off = this->code_end;
  this->code_end += i;
  return &this->code_buf.ptr[ off ];
}

uint32_t
RouteZip::make_code_ref( uint32_t *code,  uint32_t ecnt,
                         uint32_t rcnt ) noexcept
{
  uint32_t seed, h, off;
  size_t   pos;
  for ( seed = 0; ; seed++ ) {
    h = CodeRef::hash_code( code, ecnt, seed );
    if ( ! this->zht->find( h, pos, off ) )
      break;
    CodeRef *p = (CodeRef *) (void *) &this->code_buf.ptr[ off ];
    if ( p->equals( code, ecnt ) ) {
      if ( p->ref++ == 0 ) /* previously free, now used */
        this->code_free -= p->word_size();
      return h;
    }
  }
  uint32_t * spc = this->make_code_ref_space( ecnt, off );
  new ( spc ) CodeRef( code, ecnt, rcnt, h );
  this->zht->set( h, pos, off );
  UIntHashTab::check_resize( this->zht );
  return h; /* unique route code */
}

void
RouteZip::gc_code_ref_space( void ) noexcept
{
  size_t    pos;
  uint32_t  i, j = 0, k, e;
  CodeRef * p;
  /* remove from zht */
  for ( i = 0; i < this->code_end; i += e ) {
    p = (CodeRef *) (void *) &this->code_buf.ptr[ i ];
    e = p->word_size();
    if ( this->zht->find( p->hash, pos, k ) ) {
      if ( p->ref == 0 )
        this->zht->remove( pos );
      else {
        if ( j != i ) {
          this->zht->set( p->hash, pos, j );
          ::memmove( &this->code_buf.ptr[ j ], p, e * sizeof( uint32_t ) );
        }
        j += e;
      }
    }
  }
  UIntHashTab::check_resize( this->zht );
  this->code_end  = j;
  this->code_free = 0;
}

uint32_t
rai::kv::bsearch_route( uint32_t r,  const uint32_t *routes, uint32_t size ) noexcept
{
  uint32_t k = 0, piv;
  for (;;) {
    if ( size < 4 ) {
      for ( ; size > 0 && r > routes[ k ]; size-- )
        k++;
      return k;
    }
    piv = size / 2;
    if ( r <= routes[ k + piv ] )
      size = piv;
    else {
      size -= piv + 1;
      k    += piv + 1;
    }
  }
}

uint32_t
rai::kv::insert_route( uint32_t r,  uint32_t *routes,  uint32_t rcnt ) noexcept
{
  uint32_t i = bsearch_route( r, routes, rcnt );
  if ( i < rcnt && r == routes[ i ] )
    return rcnt;
  for ( uint32_t j = rcnt++; j > i; j-- )
    routes[ j ] = routes[ j - 1 ];
  routes[ i ] = r;
  return rcnt;
}

uint32_t
rai::kv::delete_route( uint32_t r,  uint32_t *routes,  uint32_t rcnt ) noexcept
{
  uint32_t i = bsearch_route( r, routes, rcnt );
  if ( i == rcnt || r != routes[ i ] )
    return rcnt;
  for ( ; i < rcnt - 1; i++ )
    routes[ i ] = routes[ i + 1 ];
  return rcnt - 1;
}

uint32_t
rai::kv::merge_route( uint32_t *routes,  uint32_t count,
                      const uint32_t *merge,  uint32_t mcount ) noexcept
{
  uint32_t i = 0;
  for ( uint32_t j = 0; ; j++ ) {
    if ( j == mcount )
      return count;
    i += bsearch_route( merge[ j ], &routes[ i ], count - i );

    if ( i == count ) {
      do {
        routes[ count++ ] = merge[ j++ ];
      } while ( j < mcount );
      return count;
    }

    if ( merge[ j ] != routes[ i ] ) {
      for ( uint32_t k = count++; k > i; k-- )
        routes[ k ] = routes[ k - 1 ];
      routes[ i++ ] = merge[ j ];
    }
  }
}

uint32_t
RouteRef::decode( uint32_t val,  uint32_t add ) noexcept
{
  size_t pos;
  if ( this->rdb.zip.zht->find( val, pos, this->ptr.off ) ) {
    this->ptr.code_buf = &this->rdb.zip.code_buf;
    CodeRef *p = this->ptr.get();
    this->routes = this->route_spc.make( p->rcnt + add );
    this->rcnt   = DeltaCoder::decode_stream( p->ecnt, &p->code, 0,
                                              this->routes );
  }
  else {
    this->ptr.code_buf = NULL;
  }
  return this->rcnt;
}

void
RouteRef::val_to_coderef( uint32_t val ) noexcept
{
  size_t pos;
  if ( this->rdb.zip.zht->find( val, pos, this->ptr.off ) )
    this->ptr.code_buf = &this->rdb.zip.code_buf;
  else
    this->ptr.code_buf = NULL;
}

uint32_t
RouteRef::insert( uint32_t r ) noexcept
{
  if ( this->rcnt == 0 ) {
    this->routes = this->route_spc.make( 1 );
    this->routes[ this->rcnt++ ] = r;
  }
  else {
    this->rcnt = insert_route( r, this->routes, this->rcnt );
  }
  return this->rcnt;
}

uint32_t
RouteRef::remove( uint32_t r ) noexcept
{
  if ( this->rcnt <= 1 ) {
    if ( this->rcnt == 1 && this->routes[ 0 ] == r )
      this->rcnt = 0;
  }
  else {
    this->rcnt = delete_route( r, this->routes, this->rcnt );
  }
  return this->rcnt;
}

uint32_t
RouteRef::compress( void ) noexcept
{
  if ( this->rcnt <= 3 ) { /* most likely single value code, skips bsearch */
    uint32_t val = DeltaCoder::encode( this->rcnt, this->routes, 0 );
    if ( val != 0 )
      return val;
  }
  uint32_t * code = this->rdb.zip.zroute_spc.make( this->rcnt );
  uint32_t   ecnt = DeltaCoder::encode_stream( this->rcnt, this->routes, 0,
                                               code );
  if ( ecnt == 1 ) /* single value code */
    return code[ 0 ];
  return this->rdb.zip.make_code_ref( code, ecnt, this->rcnt );
}

void
RouteRef::deref( void ) noexcept
{
  CodeRef *p = this->ptr.get();
  if ( p != NULL && --p->ref == 0 ) {
    ptr.code_buf = NULL;
    this->rdb.zip.code_free += p->word_size();
    if ( this->rdb.zip.code_free * 2 > this->rdb.zip.code_end )
      this->rdb.zip.gc_code_ref_space();
  }
}
/* just find the first route of many, used to find a subject string from
 * the source of the subscripion */
uint32_t
RouteZip::decompress_one( uint32_t val ) noexcept
{
  if ( DeltaCoder::is_not_encoded( val ) ) { /* if is a multi value code */
    size_t   pos;
    uint32_t off;
    if ( ! this->zht->find( val, pos, off ) )
      return 0;
    CodeRef * p = (CodeRef *) (void *) &this->code_buf.ptr[ off ];
    val = p->code;
  }
  return DeltaCoder::decode_one( val, 0 );
}
/* how many routes exists for a hash */
uint32_t
RouteDB::get_route_count( uint16_t prefix_len,  uint32_t hash ) noexcept
{
  uint32_t rcnt = 0, *routes;
  if ( ! this->cache_find( prefix_len, hash, routes, rcnt ) ) {
    UIntHashTab * xht  = this->rt_hash[ prefix_len ];
    size_t        pos;
    uint32_t      val, off;
    if ( xht->find( hash, pos, val ) ) {
      if ( DeltaCoder::is_not_encoded( val ) ) {
        if ( this->zip.zht->find( val, pos, off ) ) {
          CodeRef *p = (CodeRef *) (void *) &this->zip.code_buf.ptr[ off ];
          rcnt = p->rcnt;
        }
      }
      else {
        rcnt = DeltaCoder::decode_length( val );
      }
    }
    if ( ! this->bloom_list.is_empty() )
      rcnt += this->get_bloom_count( prefix_len, hash );
  }
  return rcnt;
}
/* insert route for a subject hash */
uint32_t
RouteDB::add_route( uint16_t prefix_len,  uint32_t hash,  uint32_t r ) noexcept
{
  RouteRef      rte( *this, this->zip.route_spc[ prefix_len ] );
  UIntHashTab * xht = this->rt_hash[ prefix_len ];
  size_t        pos;
  uint32_t    * routes;
  uint32_t      val, rcnt = 0, xcnt;

  if ( ! this->cache_find( prefix_len, hash, routes, rcnt ) )
    routes = NULL;
  if ( xht->find( hash, pos, val ) )
    rcnt = rte.decompress( val, 1, routes, rcnt, this->bloom_list.is_empty() );
  xcnt = rte.insert( r );

  if ( rcnt == 0 ) { /* track which pattern lengths are used */
    this->add_prefix_len( prefix_len, true );
    this->entry_count++;
  }
  /* if a route was inserted */
  if ( xcnt != rcnt ) {
    if ( routes != NULL )
      this->cache_purge( prefix_len, hash );
    val = rte.compress();
    xht->set( hash, pos, val );
    if ( UIntHashTab::check_resize( xht ) )
      this->rt_hash[ prefix_len ] = xht;
    rte.deref(); /* free the old code, it is no longer referenced */
  }
  return xcnt; /* count of routes after inserting r */
}
/* delete a route for a subject hash */
uint32_t
RouteDB::del_route( uint16_t prefix_len,  uint32_t hash,  uint32_t r ) noexcept
{
  RouteRef      rte( *this, this->zip.route_spc[ prefix_len ] );
  UIntHashTab * xht = this->rt_hash[ prefix_len ];
  size_t        pos;
  uint32_t    * routes;
  uint32_t      val, rcnt = 0, xcnt;

  if ( ! this->cache_find( prefix_len, hash, routes, rcnt ) )
    routes = NULL;
  if ( ! xht->find( hash, pos, val ) )
    return 0;
  rcnt = rte.decompress( val, 0, routes, rcnt, this->bloom_list.is_empty() );
  xcnt = rte.remove( r );

  if ( xcnt != rcnt ) {
    if ( routes != NULL )
      this->cache_purge( prefix_len, hash );
    if ( xcnt == 0 ) { /* no more routes left */
      xht->remove( pos );
      if ( xht->is_empty() )
        this->del_prefix_len( prefix_len, true );
      this->entry_count--;
    }
    else { /* recompress and update hash */
      val = rte.compress();
      xht->set( hash, pos, val );
      if ( UIntHashTab::check_resize( xht ) )
        this->rt_hash[ prefix_len ] = xht;
    }
    rte.deref(); /* free old code buf */
  }
  return xcnt; /* count of routes after removing r */
}

uint32_t
RouteDB::get_route_slow( uint32_t hash,  uint32_t val,
                         uint32_t *&routes ) noexcept
{
  RouteRef rte( *this, this->zip.route_spc[ SUB_RTE ] );
  uint32_t rcnt = rte.decompress( val, 0 );
  routes = rte.routes;
  return this->get_bloom_route( hash, routes, rcnt );
}

uint32_t
RouteDB::get_bloom_route( uint32_t hash, uint32_t *&routes,
                          uint32_t merge_cnt ) noexcept
{
  RouteSpace & spc   = this->zip.bloom_spc[ SUB_RTE ];
  uint32_t     count = 0;

  for ( BloomRoute *b = this->bloom_list.hd; b != NULL; b = b->next ) {
    for ( uint32_t i = 0; i < b->nblooms; i++ ) {
      BloomRef * r = b->bloom[ i ];
      if ( r->pref_count[ SUB_RTE ] != 0 && r->bits->is_member( hash ) ) {
        uint32_t * p = spc.make( count + 1 );
        p[ count++ ] = b->r;
        break;
      }
    }
  }
  if ( count == 0 ) {
    this->cache_save( SUB_RTE, hash, routes, merge_cnt );
    return merge_cnt;
  }
  if ( merge_cnt > 0 ) {
    spc.make( count + merge_cnt );
    count = merge_route( spc.ptr, count, routes, merge_cnt );
  }
  this->cache_save( SUB_RTE, hash, spc.ptr, count );
  routes = spc.ptr;
  return count;
}

uint32_t
RouteDB::get_route_slow2( const char *sub,  uint16_t sublen,
                          uint16_t prefix_len,  uint64_t mask,  uint32_t hash,
                          uint32_t val,  uint32_t *&routes,
                          uint32_t subj_hash ) noexcept
{
  RouteRef rte( *this, this->zip.route_spc[ prefix_len ] );
  uint32_t rcnt = rte.decompress( val, 0 );
  routes = rte.routes;
  return this->get_bloom_route2( sub, sublen, prefix_len, mask, hash, routes,
                                 rcnt, subj_hash );
}

uint32_t
RouteDB::get_bloom_route2( const char *sub,  uint16_t sublen,
                           uint16_t prefix_len,  uint64_t mask,  uint32_t hash,
                           uint32_t *&routes,  uint32_t merge_cnt,
                           uint32_t subj_hash ) noexcept
{
  RouteSpace & spc        = this->zip.bloom_spc[ prefix_len ];
  uint32_t     count      = 0;
  bool         has_detail = false;

  if ( ( mask & this->bloom_mask ) != 0 ) {
    for ( BloomRoute *b = this->bloom_list.hd; b != NULL; b = b->next ) {
      if ( b->is_invalid )
        b->update_masks();

      if ( ( b->pref_mask & mask ) == 0 ) /* no prefix here */
        goto no_match;

      if ( ( b->detail_mask & mask ) == 0 ) { /* if no details match */
        for ( uint32_t i = 0; i < b->nblooms; i++ ) {
          BloomRef * r = b->bloom[ i ];
          if ( ( r->pref_mask & mask ) != 0 && r->bits->is_member( hash ) )
            goto match;
        }
        goto no_match;
      }
      /*printf( "-- %.*s %u:mask 0x%x pref 0x%x detail 0x%x, r %u\n",
               (int) sublen, sub, prefix_len,
               (uint32_t) mask, (uint32_t) b->pref_mask,
               (uint32_t) b->detail_mask , b->r );*/
      for ( uint32_t i = 0; i < b->nblooms; i++ ) {
        BloomRef * r = b->bloom[ i ];
        if ( ( r->pref_mask & mask ) != 0 &&
               r->bits->is_member( hash ) &&
               r->detail_matches( prefix_len, mask, hash, sub, sublen,
                                  subj_hash, has_detail ) )
          goto match;
      }
      if ( 0 ) {
      match:;
        uint32_t * p = spc.make( count + 1 );
        p[ count++ ] = b->r; /* route matches */
      }
      no_match:; /* next bloom route */
    }
  }
  if ( count == 0 ) {
    if ( ! has_detail )
      this->cache_save( prefix_len, hash, routes, merge_cnt );
    return merge_cnt;
  }
  if ( merge_cnt > 0 ) {
    spc.make( count + merge_cnt );
    count = merge_route( spc.ptr, count, routes, merge_cnt );
  }
  if ( ! has_detail )
    this->cache_save( prefix_len, hash, spc.ptr, count );
  routes = spc.ptr;
  return count;
}

void
BloomRoute::update_masks( void ) noexcept
{
  this->is_invalid  = false;
  this->pref_mask   = 0;
  this->detail_mask = 0;
  for ( uint32_t i = 0; i < this->nblooms; i++ ) {
    BloomRef * r = this->bloom[ i ];
    this->pref_mask   |= r->pref_mask;
    this->detail_mask |= r->detail_mask;
  }
}

uint32_t
RouteDB::get_bloom_count( uint16_t prefix_len,  uint32_t hash ) noexcept
{
  if ( this->bloom_pref_count[ prefix_len ] == 0 )
    return 0;
  BloomRoute * b;
  BloomRef   * r;
  uint32_t     rcnt = 0;
  for ( b = this->bloom_list.hd; b != NULL; b = b->next ) {
    for ( uint32_t i = 0; i < b->nblooms; i++ ) {
      r = b->bloom[ i ];
      if ( r->pref_count[ prefix_len ] != 0 && r->bits->is_member( hash ) ) {
        rcnt++;
        break;
      }
    }
  }
  return rcnt;
}

BloomRoute *
RouteDB::create_bloom_route( uint32_t r,  uint32_t *pref_count,
                             BloomBits *bits ) noexcept
{
  return this->create_bloom_route( r,
           this->create_bloom_ref( pref_count, bits ) );
}

BloomRoute *
RouteDB::create_bloom_route( uint32_t r,  uint32_t seed ) noexcept
{
  return this->create_bloom_route( r, this->create_bloom_ref( seed ) );
}

BloomRoute *
RouteDB::create_bloom_route( uint32_t r,  BloomRef *ref ) noexcept
{
  void       * m;
  BloomRoute * b = NULL;

  for ( BloomRoute *p = this->bloom_list.hd; ; p = p->next ) {
    if ( p == NULL ) {
      m = ::malloc( sizeof( BloomRoute ) );
      b = new ( m ) BloomRoute( r, *this );
      this->bloom_list.push_tl( b );
      break;
    }
    if ( p->r >= r ) {
      if ( p->r == r )
        b = p;
      else {
        m = ::malloc( sizeof( BloomRoute ) );
        b = new ( m ) BloomRoute( r, *this );
        this->bloom_list.insert_before( b, p );
      }
      break;
    }
  }
  b->add_bloom_ref( ref );
  return b;
}

BloomRef *
RouteDB::create_bloom_ref( uint32_t seed ) noexcept
{
  void * m = ::malloc( sizeof( BloomRef ) );
  return new ( m ) BloomRef( seed );
}

BloomRef *
RouteDB::create_bloom_ref( uint32_t *pref_count,  BloomBits *bits ) noexcept
{
  void * m = ::malloc( sizeof( BloomRef ) );
  return new ( m ) BloomRef( bits, pref_count );
}

BloomRef::BloomRef( uint32_t seed ) noexcept
{
  this->bits        = NULL;
  this->links       = NULL;
  this->details     = NULL;
  this->nlinks      = 0;
  this->ndetails    = 0;
  this->pref_mask   = 0;
  this->detail_mask = 0;
  ::memset( this->pref_count, 0, sizeof( this->pref_count ) );
  this->update_route( NULL, BloomBits::resize( NULL, seed ), NULL, 0 );
}

BloomRef::BloomRef( BloomBits *b,  const uint32_t *pref ) noexcept
{
  this->bits        = NULL;
  this->links       = NULL;
  this->details     = NULL;
  this->nlinks      = 0;
  this->ndetails    = 0;
  this->pref_mask   = 0;
  this->detail_mask = 0;
  ::memset( this->pref_count, 0, sizeof( this->pref_count ) );
  this->update_route( pref, b, NULL, 0 );
}

void
BloomRoute::add_bloom_ref( BloomRef *ref ) noexcept
{
  uint32_t n;
  this->invalid();
  n = this->nblooms + 1;
  this->bloom = (BloomRef **)
                ::realloc( this->bloom, sizeof( this->bloom[ 0 ] ) * n );
  this->bloom[ n - 1 ] = ref;
  this->nblooms = n;
  ref->add_link( this );

  uint16_t prefix_len;
  for ( prefix_len = 0; prefix_len < MAX_RTE; prefix_len++ ) {
    if ( ref->pref_count[ prefix_len ] != 0 ) {
      if ( this->rdb.bloom_pref_count[ prefix_len ]++ == 0 ) {
        if ( this->rdb.rt_hash[ prefix_len ]->is_empty() )
          this->rdb.add_prefix_len( prefix_len, false );
      }
    }
  }
}

void
BloomRef::unlink( bool del_empty_routes ) noexcept
{
  if ( this->nlinks == 0 )
    return;
  this->invalid();
  while ( this->nlinks > 0 ) {
    BloomRoute * b = this->links[ this->nlinks - 1 ];
    b->del_bloom_ref( this );
    if ( del_empty_routes && b->nblooms == 0 ) {
      b->rdb.bloom_list.pop( b );
      delete b;
    }
  }
}

void
BloomRef::zero( void ) noexcept
{
  ::memset( this->pref_count, 0, sizeof( this->pref_count ) );
  this->bits->zero();
  if ( this->ndetails > 0 ) {
    ::free( this->details );
    this->details  = NULL;
    this->ndetails = 0;
  }
}

void
BloomRef::add_link( BloomRoute *b ) noexcept
{
  uint32_t n = this->nlinks + 1;
  this->links = (BloomRoute **)
                ::realloc( this->links, sizeof( this->links[ 0 ] ) * n );
  this->links[ n - 1 ] = b;
  this->nlinks = n;
}

void
BloomRoute::del_bloom_ref( BloomRef *ref ) noexcept
{
  uint32_t i, n;
  this->invalid();
  n = this->nblooms;
  for ( i = n; ; i-- ) {
    if ( i == 0 )
      return;
    if ( this->bloom[ i - 1 ] == ref ) {
      if ( i != n )
        ::memmove( &this->bloom[ i - 1 ], &this->bloom[ i ],
                   sizeof( this->bloom[ 0 ] ) * ( n - i ) );
      this->nblooms = n - 1;
      ref->del_link( this );
      break;
    }
  }

  uint16_t prefix_len;
  for ( prefix_len = 0; prefix_len < MAX_RTE; prefix_len++ ) {
    if ( ref->pref_count[ prefix_len ] != 0 ) {
      if ( --this->rdb.bloom_pref_count[ prefix_len ] == 0 ) {
        this->rdb.del_prefix_len( prefix_len, false );
      }
    }
  }
}

void
BloomRef::del_link( BloomRoute *b ) noexcept
{
  uint32_t i, n = this->nlinks;
  for ( i = n; ; i-- ) {
    if ( i == 0 )
      return;
    if ( this->links[ i - 1 ] == b ) {
      if ( i != n )
        ::memmove( &this->links[ i - 1 ], &this->links[ i ],
                   sizeof( this->links[ 0 ] ) * ( n - i ) );
      this->nlinks = n - 1;
      break;
    }
  }
}

bool
BloomRef::has_link( uint32_t fd ) noexcept
{
  for ( uint32_t i = 0; i < this->nlinks; i++ )
    if ( this->links[ i ]->r == fd )
      return true;
  return false;
}

bool
BloomRef::add_route( uint16_t prefix_len,  uint32_t hash ) noexcept
{
  if ( this->pref_count[ prefix_len ]++ == 0 )
    this->ref_pref_count( prefix_len );
  this->bits->add( hash );
  this->invalid();
  return this->bits->test_resize();
}

bool
BloomRef::add_shard_route( uint16_t prefix_len,  uint32_t hash,
                           const ShardMatch &match ) noexcept
{
  this->details = (BloomDetail *) ::realloc( this->details,
                      sizeof( this->details[ 0 ] ) * ( this->ndetails + 1 ) );
  BloomDetail &d = this->details[ this->ndetails++ ];
  d.hash       = hash;
  d.prefix_len = prefix_len;
  d.init_shard( match );
  this->detail_mask |= (uint64_t) 1 << prefix_len;
  return this->add_route( prefix_len, hash );
}

bool
BloomRef::add_suffix_route( uint16_t prefix_len,  uint32_t hash,
                            const SuffixMatch &match ) noexcept
{
  this->details = (BloomDetail *) ::realloc( this->details,
                      sizeof( this->details[ 0 ] ) * ( this->ndetails + 1 ) );
  BloomDetail &d = this->details[ this->ndetails++ ];
  d.hash       = hash;
  d.prefix_len = prefix_len;
  d.init_suffix( match );
  this->detail_mask |= (uint64_t) 1 << prefix_len;
  return this->add_route( prefix_len, hash );
}

void
BloomRef::del_route( uint16_t prefix_len,  uint32_t hash ) noexcept
{
  if ( --this->pref_count[ prefix_len ] == 0 )
    this->deref_pref_count( prefix_len );
  this->bits->remove( hash );
  this->invalid();
}

void
BloomRef::del_shard_route( uint16_t prefix_len,  uint32_t hash,
                           const ShardMatch &match ) noexcept
{
  if ( prefix_len != SUB_RTE ) {
    uint64_t new_mask = 0;
    uint32_t n = this->ndetails,
             j = n;
    for ( uint32_t i = 0; i < n; i++ ) {
      if ( prefix_len == this->details[ i ].prefix_len &&
           hash       == this->details[ i ].hash &&
           this->details[ i ].shard_equals( match ) ) {
        j = i;
      }
      else {
        new_mask |= (uint64_t) 1 << this->details[ i ].prefix_len;
      }
    }
    if ( j < n ) {
      if ( j < n - 1 )
        ::memmove( &this->details[ j ], &this->details[ j + 1 ],
                   sizeof( this->details[ 0 ] ) * ( n - ( j + 1 ) ) );
      this->ndetails    = n - 1;
      this->detail_mask = new_mask;
      this->del_route( prefix_len, hash );
    }
  }
}

void
BloomRef::del_suffix_route( uint16_t prefix_len,  uint32_t hash,
                            const SuffixMatch &match ) noexcept
{
  if ( prefix_len != SUB_RTE ) {
    uint64_t new_mask = 0;
    uint32_t n = this->ndetails,
             j = n;
    for ( uint32_t i = 0; i < n; i++ ) {
      if ( prefix_len == this->details[ i ].prefix_len &&
           hash       == this->details[ i ].hash &&
           this->details[ i ].suffix_equals( match ) ) {
        j = i;
      }
      else {
        new_mask |= (uint64_t) 1 << this->details[ i ].prefix_len;
      }
    }
    if ( j < n ) {
      if ( j < n - 1 )
        ::memmove( &this->details[ j ], &this->details[ j + 1 ],
                   sizeof( this->details[ 0 ] ) * ( n - ( j + 1 ) ) );
      this->ndetails    = n - 1;
      this->detail_mask = new_mask;
      this->del_route( prefix_len, hash );
    }
  }
}

void
BloomRef::ref_pref_count( uint16_t prefix_len ) noexcept
{
  if ( prefix_len != SUB_RTE )
    this->pref_mask |= (uint64_t) 1 << prefix_len;
  for ( uint32_t i = 0; i < this->nlinks; i++ ) {
    if ( this->links[ i ]->rdb.bloom_pref_count[ prefix_len ]++ == 0 ) {
      if ( this->links[ i ]->rdb.rt_hash[ prefix_len ]->is_empty() )
        this->links[ i ]->rdb.add_prefix_len( prefix_len, false );
    }
  }
}

void
BloomRef::deref_pref_count( uint16_t prefix_len ) noexcept
{
  if ( prefix_len != SUB_RTE )
    this->pref_mask &= ~( (uint64_t) 1 << prefix_len );
  for ( uint32_t i = 0; i < this->nlinks; i++ ) {
    if ( --this->links[ i ]->rdb.bloom_pref_count[ prefix_len ] == 0 ) {
      this->links[ i ]->rdb.del_prefix_len( prefix_len, false );
    }
  }
}

void
BloomRoute::invalid( void ) noexcept
{
  this->rdb.cache.need       = 0;
  this->rdb.cache.is_invalid = true;
  this->is_invalid           = true;
}

void
BloomRef::invalid( void ) noexcept
{
  for ( uint32_t i = 0; i < this->nlinks; i++ ) {
    this->links[ i ]->rdb.cache.need       = 0;
    this->links[ i ]->rdb.cache.is_invalid = true;
    this->links[ i ]->is_invalid           = true;
  }
}

void
BloomRef::update_route( const uint32_t *pref_count,  BloomBits *bits,
                        BloomDetail *details,  uint32_t ndetails ) noexcept
{
  uint16_t prefix_len;
  if ( this->bits != NULL ) {
    if ( this->bits->count != 0 )
      this->invalid();
    delete this->bits;
    this->bits = bits;

    for ( prefix_len = 0; prefix_len < MAX_RTE; prefix_len++ ) {
      if ( this->pref_count[ prefix_len ] != pref_count[ prefix_len ] ) {
        if ( this->pref_count[ prefix_len ] == 0 )
          this->ref_pref_count( prefix_len );
        else if ( pref_count[ prefix_len ] == 0 )
          this->deref_pref_count( prefix_len );
        this->pref_count[ prefix_len ] = pref_count[ prefix_len ];
      }
    }
  }
  else {
    /*if ( bits == NULL )
      this->bits = BloomBits::resize( NULL );
    else*/
    this->bits = bits;

    if ( pref_count != NULL ) {
      ::memcpy( this->pref_count, pref_count, sizeof( this->pref_count ) );
      for ( prefix_len = 0; prefix_len < MAX_RTE; prefix_len++ ) {
        if ( pref_count[ prefix_len ] != 0 )
          this->ref_pref_count( prefix_len );
      }
    }
  }
  if ( this->ndetails != 0 )
    ::free( this->details );
  this->ndetails    = ndetails;
  this->details     = details;
  this->detail_mask = 0;
  for ( uint32_t i = 0; i < ndetails; i++ )
    this->detail_mask |= (uint64_t) 1 << details[ i ].prefix_len;
  if ( this->bits->count != 0 )
    this->invalid();
  /*printf( "update fd %d ndetails %u mask %lx\n",
          this->nlinks > 0 ? this->links[ 0 ]->r : -1,
          ndetails, this->detail_mask );*/
}

bool
BloomDetail::shard_endpoints( uint32_t shard,  uint32_t nshards,
                              uint32_t &start,  uint32_t &end ) noexcept
{
  uint64_t ratio = (uint64_t) 0xffffffffU / (uint64_t) nshards;
  if ( shard >= nshards )
    return false;
  if ( shard == 0 ) {
    start = 0;
    end   = (uint32_t) ( ratio - 1 );
  }
  else {
    start = (uint32_t) ( ratio * (uint64_t) shard );
    if ( shard + 1 == nshards )
      end = 0xffffffffU;
    else
      end = (uint32_t) ( ( ratio * (uint64_t) ( shard + 1 ) ) - 1 );
  }
  return true;
}

bool
BloomDetail::from_pattern( const PatternCvt &cvt ) noexcept
{
  if ( cvt.shard_total != 0 ) {
    if ( ! BloomDetail::shard_endpoints( cvt.shard_num, cvt.shard_total,
                                   this->u.shard.start, this->u.shard.end ) ) {
      fprintf( stderr, "bad shard\n" );
      return false;
    }
    this->detail_type = SHARD_MATCH;
  }
  else if ( cvt.suffixlen != 0 ) {
    this->u.suffix.len  = cvt.suffixlen;
    this->u.suffix.hash = kv_crc_c( cvt.suffix, cvt.suffixlen, 0 );
    this->detail_type = SUFFIX_MATCH;
  }
  else {
    this->detail_type = NO_DETAIL;
  }
  return true;
}

void
RouteDB::cache_save( uint16_t prefix_len,  uint32_t hash,
                     uint32_t *routes,  uint32_t rcnt ) noexcept
{
  if ( this->cache.is_invalid || this->cache.free * 2 > this->cache.end ||
       this->cache.end > RouteCache::MAX_CACHE ) {
    if ( ! this->cache.reset() )
      return;
  }
  uint64_t    h;
  uint32_t  * ptr;
  RteCacheVal val;

  if ( ! this->cache.busy ) { /* no refs, ok to realloc() */
    ptr = this->cache.spc.make( this->cache.end + rcnt + 1024 );
  }
  else {
    ptr = this->cache.spc.ptr; /* no realloc until ! busy */
    uint32_t n = this->cache.end + rcnt;
    if ( n > this->cache.spc.size ) {
      this->cache.need = n - this->cache.spc.size;
      if ( this->cache.need < 1024 )
        this->cache.need = 1024;
      return;
    }
  }
  val.rcnt = rcnt;
  val.off  = this->cache.end;
  this->cache.end += rcnt;
  ::memcpy( &ptr[ val.off ], routes, sizeof( routes[ 0 ] ) * rcnt );

  h = ( (uint64_t) prefix_len << 32 ) | (uint64_t) hash;
  this->cache.ht->upsert( h, val ); /* save rcnt, off at hash */
  this->cache.count++;

  if ( this->cache.ht->elem_count >= this->cache.ht->max_count )
    RteCacheTab::check_resize( this->cache.ht );
}

void
RouteDB::cache_need( void ) noexcept
{
  this->cache.spc.make( this->cache.end + this->cache.need );
  this->cache.need = 0;
}

void
RouteDB::cache_purge( uint16_t prefix_len,  uint32_t hash ) noexcept
{
  uint64_t    h = ( (uint64_t) prefix_len << 32 ) | (uint64_t) hash;
  size_t      pos;
  RteCacheVal val;

  if ( this->cache.ht->find( h, pos, val ) ) {
    this->cache.free += val.rcnt;
    this->cache.count--;

    if ( this->cache.is_invalid || this->cache.free * 2 > this->cache.end ||
         this->cache.end > RouteCache::MAX_CACHE ) {
      this->cache.reset();
      return;
    }
    this->cache.ht->remove( pos );
  }
}

void
RouteDB::add_prefix_len( uint16_t prefix_len,  bool is_rt_hash ) noexcept
{
  if ( prefix_len < SUB_RTE ) {
    uint64_t mask = (uint64_t) 1 << prefix_len;
    if ( is_rt_hash )
      this->rt_mask |= mask;
    else
      this->bloom_mask |= mask;
    if ( ( mask & this->pat_mask ) == 0 ) {
      this->pat_mask |= mask;
      this->update_prefix_len();
    }
  }
}

void
RouteDB::del_prefix_len( uint16_t prefix_len,  bool is_rt_hash ) noexcept
{
  if ( prefix_len < SUB_RTE ) {
    if ( this->bloom_pref_count[ prefix_len ] == 0 ) {
      uint64_t mask = (uint64_t) 1 << prefix_len;
      if ( is_rt_hash )
        this->rt_mask &= ~mask;
      else
        this->bloom_mask &= ~mask;
      if ( ( mask & this->pat_mask ) != 0 ) {
        if ( ( ( this->rt_mask | this->bloom_mask ) & mask ) == 0 ) {
          this->pat_mask &= ~mask;
          this->update_prefix_len();
        }
      }
    }
  }
}

void
RouteDB::update_prefix_len( void ) noexcept
{
  uint64_t m   = this->pat_mask;
  uint8_t  cnt = 0, bit = 0;
  while ( m != 0 ) {
    if ( ( m & 1 ) != 0 )
      this->prefix_len[ cnt++ ] = bit;
    m = ( m >> 1 );
    bit++;
  }
  this->pat_bit_count = cnt;
}

uint32_t
RouteDB::add_pattern_route_str( const char *str,  uint16_t len,
                                uint32_t r ) noexcept
{
  uint32_t seed = this->prefix_seed( len );
  return this->add_pattern_route( kv_crc_c( str, len, seed ), r, len );
}

uint32_t
RouteDB::del_pattern_route_str( const char *str,  uint16_t len,
                                uint32_t r ) noexcept
{
  uint32_t seed = this->prefix_seed( len );
  return this->del_pattern_route( kv_crc_c( str, len, seed ), r, len );
}

uint32_t
RouteDB::add_sub_route_str( const char *str,  uint16_t len,
                            uint32_t r ) noexcept
{
  return this->add_sub_route( kv_crc_c( str, len, 0 ), r );
}

uint32_t
RouteDB::del_sub_route_str( const char *str,  uint16_t len,
                            uint32_t r ) noexcept
{
  return this->del_sub_route( kv_crc_c( str, len, 0 ), r );
}
/* test if route exists for hash */
bool
RouteDB::is_member( uint16_t prefix_len,  uint32_t hash,  uint32_t r ) noexcept
{
  UIntHashTab * xht = this->rt_hash[ prefix_len ];
  size_t        pos;
  uint32_t      val;
  /* mostly used on low count route values, check xht val first */
  if ( ! xht->find( hash, pos, val ) )
    return false;

  if ( ! DeltaCoder::is_not_encoded( val ) ) {
    uint32_t base = 0;
    return DeltaCoder::test( val, r, base );
  }

  uint32_t * routes, rcnt;
  if ( this->cache_find( prefix_len, hash, routes, rcnt ) ) {
    uint32_t i = bsearch_route( r, routes, rcnt );
    if ( i < rcnt && r == routes[ i ] )
      return true;
    return false;
  }

  CodeRef * p;
  uint32_t  off;
  if ( this->zip.zht->find( val, pos, off ) ) {
    p = (CodeRef *) (void *) &this->zip.code_buf.ptr[ off ];
    return DeltaCoder::test_stream( p->ecnt, &p->code, 0, r );
  }
  return false;
}
