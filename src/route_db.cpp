#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/route_db.h>
#include <raikv/pattern_cvt.h>

using namespace rai;
using namespace kv;

RoutePublish &
RoutePDB::get_service( const char *svc,  uint32_t svc_num,
                       uint32_t rte_id ) noexcept
{
  if ( svc == NULL || ::strcmp( svc, "default" ) == 0 )
    return *this;
  size_t   len = ::strlen( svc );
  char   * s   = (char *) ::malloc( len + 10 );
  uint32_t h   = kv_crc_c( svc, len, svc_num );
  if ( svc_num > 0 ) {
    len = ::snprintf( s, len + 10, "%s.%x", svc, svc_num );
  }
  else {
    ::memcpy( s, svc, len );
    s[ len ] = '\0';
  }
  RouteLoc loc;
  RouteService * rt = this->svc_db.upsert( h, s, (uint16_t) len, loc );
  if ( loc.is_new ) {
    void * m = aligned_malloc( sizeof( RoutePublish ) );
    rt->sub_route = new ( m ) RoutePublish( this->poll, s, svc_num, rte_id );
  }
  return *rt->sub_route;
}

RouteDB::RouteDB( BloomDB &g_db ) noexcept
       : g_bloom_db( g_db ), entry_count( 0 ), pat_mask( 0 ), rt_mask( 0 ),
         pat_bit_count( 0 ), bloom_mem( 0 )
{
  for ( size_t i = 0; i < sizeof( this->rt_hash ) /
                          sizeof( this->rt_hash[ 0 ] ); i++ ) {
    this->rt_hash[ i ] = UIntHashTab::resize( NULL );
    this->bloom_pref_count[ i ] = 0;
  }
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
RouteZip::make_code_ref_space( uint32_t ecnt,  size_t &off ) noexcept
{
  size_t i = CodeRef::alloc_words( ecnt );
  this->code_buf.make( this->code_end + i );
  off = this->code_end;
  this->code_end += i;
  return &this->code_buf.ptr[ off ];
}

uint32_t
RouteZip::make_code_ref( uint32_t *code,  uint32_t ecnt,
                         uint32_t rcnt ) noexcept
{
  uint32_t seed, h, x;
  size_t   off, pos;
  for ( seed = 0; ; seed++ ) {
    h = CodeRef::hash_code( code, ecnt, seed );
    if ( ! this->zht->find( h, pos, x ) )
      break;
    off = x;
    CodeRef *p = (CodeRef *) (void *) &this->code_buf.ptr[ off ];
    if ( p->equals( code, ecnt ) ) {
      if ( p->ref++ == 0 ) /* previously free, now used */
        this->code_free -= p->word_size();
      return h;
    }
  }
  uint32_t * spc = this->make_code_ref_space( ecnt, off );
  new ( spc ) CodeRef( code, ecnt, rcnt, h );
  this->zht->set( h, pos, (uint32_t) off );
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
    e = (uint32_t) p->word_size();
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
RouteRef::find_coderef( uint32_t val ) noexcept
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
/* how many routes exists for a hash, excluding bloom */
uint32_t
RouteDB::get_route_count( uint16_t prefix_len,  uint32_t hash ) noexcept
{
  uint32_t rcnt = 0/*, *routes*/;
  /*if ( ! this->cache_find( prefix_len, hash, routes, rcnt ) ) {*/
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
    /*if ( ! this->bloom_list.is_empty() )
      rcnt += this->get_bloom_count( prefix_len, hash );*/
  /*}*/
  return rcnt;
}
/* insert route for a subject hash */
uint32_t
RouteDB::add_route( uint16_t prefix_len,  uint32_t hash,  uint32_t r ) noexcept
{
  RouteRef rte( *this, this->zip.route_spc[ prefix_len ] );
  return this->add_route( prefix_len, hash, r, rte );
}

uint32_t
RouteDB::add_route( uint16_t prefix_len,  uint32_t hash,  uint32_t r,
                    RouteRef &rte ) noexcept
{
  UIntHashTab * xht = this->rt_hash[ prefix_len ];
  size_t        pos, cpos = 0;
  uint32_t    * routes;
  uint32_t      val = 0, rcnt = 0, xcnt;
  bool          decompress_route = true,
                exists;

  exists = xht->find( hash, pos, val );
  if ( this->cache_find( prefix_len, hash, routes, rcnt, 0, cpos ) ) {
    /* if bloom routes exist, don't use cached route */
    if ( this->bloom_list.is_empty() ) {
      rte.copy_cached( routes, rcnt, 1 );
      decompress_route = false;
      if ( exists && DeltaCoder::is_not_encoded( val ) )
        rte.find_coderef( val );
    }
    else {
      rcnt = 0;
    }
    this->cache_purge( cpos );
  }
  if ( exists && decompress_route )
    rcnt = rte.decompress( val, 1 );
  xcnt = rte.insert( r ); /* add r to rte.routes[] */

  if ( rcnt == 0 ) { /* track which pattern lengths are used */
    if ( xht->is_empty() )
      this->add_prefix_len( prefix_len, true );
    this->entry_count++;
  }
  /* if a route was inserted */
  if ( xcnt != rcnt ) {
    val = rte.compress();
    xht->set( hash, pos, val ); /* update val in xht */
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
  RouteRef rte( *this, this->zip.route_spc[ prefix_len ] );
  return this->del_route( prefix_len, hash, r, rte );
}

uint32_t
RouteDB::del_route( uint16_t prefix_len,  uint32_t hash,  uint32_t r,
                    RouteRef &rte ) noexcept
{
  UIntHashTab * xht = this->rt_hash[ prefix_len ];
  size_t        pos, cpos = 0;
  uint32_t    * routes;
  uint32_t      val, rcnt = 0, xcnt;
  bool          decompress_route = true;

  if ( ! xht->find( hash, pos, val ) )
    return 0;
  if ( this->cache_find( prefix_len, hash, routes, rcnt, 0, cpos ) ) {
    /* if bloom routes exist, don't use cached route */
    if ( this->bloom_list.is_empty() ) {
      rte.copy_cached( routes, rcnt, 0 );
      decompress_route = false;
      if ( DeltaCoder::is_not_encoded( val ) )
        rte.find_coderef( val );
    }
    else {
      rcnt = 0;
    }
    this->cache_purge( cpos );
  }
  if ( decompress_route )
    rcnt = rte.decompress( val, 0 );
  xcnt = rte.remove( r ); /* remove r from rte.routes[] */
  /* if route was deleted */
  if ( xcnt != rcnt ) {
    if ( xcnt == 0 ) { /* no more routes left */
      xht->remove( pos ); /* remove val from xht */
      if ( xht->is_empty() )
        this->del_prefix_len( prefix_len, true );
      this->entry_count--;
    }
    else { /* recompress and update hash */
      val = rte.compress();
      xht->set( hash, pos, val ); /* update val in xht */
      if ( UIntHashTab::check_resize( xht ) )
        this->rt_hash[ prefix_len ] = xht;
    }
    rte.deref(); /* free old code buf */
  }
  return xcnt; /* count of routes after removing r */
}

uint32_t
RouteDB::ref_route( uint16_t prefix_len,  uint32_t hash,
                    RouteRef &rte ) noexcept
{
  UIntHashTab * xht = this->rt_hash[ prefix_len ];
  size_t        pos;
  uint32_t    * routes;
  uint32_t      val, rcnt = 0;

  if ( ! xht->find( hash, pos, val ) )
    return 0;
  /* if bloom routes exist, don't use cached route */
  if ( this->bloom_list.is_empty() ) {
    if ( this->cache_find( prefix_len, hash, routes, rcnt, 0, pos ) ) {
      rte.copy_cached( routes, rcnt, 0 );
      return rcnt;
    }
  }
  return rte.decompress( val, 0 );
}

uint32_t
RouteDB::get_route_slow( uint32_t hash,  uint32_t val,
                         uint32_t *&routes,  uint32_t shard ) noexcept
{
  RouteRef rte( *this, this->zip.route_spc[ SUB_RTE ] );
  uint32_t rcnt = rte.decompress( val, 0 );
  routes = rte.routes;
  return this->get_bloom_route( hash, routes, rcnt, shard );
}

uint32_t
RouteDB::get_bloom_route( uint32_t hash,  uint32_t *&routes,
                          uint32_t merge_cnt,  uint32_t shard ) noexcept
{
  RouteSpace & spc   = this->zip.bloom_spc[ SUB_RTE ];
  uint32_t     count = 0;

  for ( BloomRoute *b = this->bloom_list.hd( shard ); b != NULL; b = b->next ) {
    if ( b->in_list != shard + 1 )
      continue;
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
    this->cache_save( SUB_RTE, hash, routes, merge_cnt, shard );
    return merge_cnt;
  }
  if ( merge_cnt > 0 ) {
    spc.make( count + merge_cnt );
    count = merge_route( spc.ptr, count, routes, merge_cnt );
  }
  this->cache_save( SUB_RTE, hash, spc.ptr, count, shard );
  routes = spc.ptr;
  return count;
}

uint32_t
RouteDB::get_route_slow2( const char *sub,  uint16_t sublen,
                          uint16_t prefix_len,  uint64_t mask,  uint32_t hash,
                          uint32_t val,  uint32_t *&routes,
                          uint32_t subj_hash,  uint32_t shard ) noexcept
{
  RouteRef rte( *this, this->zip.route_spc[ prefix_len ] );
  uint32_t rcnt = rte.decompress( val, 0 );
  routes = rte.routes;
  return this->get_bloom_route2( sub, sublen, prefix_len, mask, hash, routes,
                                 rcnt, subj_hash, shard );
}

uint32_t
RouteDB::get_bloom_route2( const char *sub,  uint16_t sublen,
                           uint16_t prefix_len,  uint64_t mask,  uint32_t hash,
                           uint32_t *&routes,  uint32_t merge_cnt,
                           uint32_t subj_hash,  uint32_t shard ) noexcept
{
  RouteSpace & spc        = this->zip.bloom_spc[ prefix_len ];
  uint32_t     count      = 0;
  bool         has_detail = false;

  if ( ( mask & this->bloom_mask ) != 0 ) {
    for ( BloomRoute *b = this->bloom_list.hd( shard ); b != NULL; b = b->next){
      if ( b->in_list != shard + 1 )
        continue;

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
      this->cache_save( prefix_len, hash, routes, merge_cnt, shard );
    return merge_cnt;
  }
  if ( merge_cnt > 0 ) {
    spc.make( count + merge_cnt );
    count = merge_route( spc.ptr, count, routes, merge_cnt );
  }
  if ( ! has_detail )
    this->cache_save( prefix_len, hash, spc.ptr, count, shard );
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
RouteDB::get_bloom_count( uint16_t prefix_len,  uint32_t hash,
                          uint32_t shard ) noexcept
{
  if ( this->bloom_pref_count[ prefix_len ] == 0 )
    return 0;
  BloomRoute * b;
  BloomRef   * r;
  uint32_t     rcnt = 0;
  for ( b = this->bloom_list.hd( shard ); b != NULL; b = b->next ) {
    if ( b->in_list == shard + 1 ) {
      for ( uint32_t i = 0; i < b->nblooms; i++ ) {
        r = b->bloom[ i ];
        if ( r->pref_count[ prefix_len ] != 0 && r->bits->is_member( hash ) ) {
          rcnt++;
          break;
        }
      }
    }
  }
  return rcnt;
}

BloomRoute *
RouteDB::create_bloom_route( uint32_t r,  BloomRef *ref,
                             uint32_t shard ) noexcept
{
  BloomRoute * b = NULL;

  for ( BloomRoute *p = this->bloom_list.hd( shard ); ; p = p->next ) {
    if ( p == NULL ) {
      b = new ( this->alloc_bloom() ) BloomRoute( r, *this, shard + 1 );
      this->bloom_list.get_list( shard ).push_tl( b );
      break;
    }
    if ( p->r > r || ( p->r == r && p->in_list >= shard + 1 ) ) {
      if ( p->r == r && p->in_list == shard + 1 )
        b = p;
      else {
        b = new ( this->alloc_bloom() ) BloomRoute( r, *this, shard + 1 );
        this->bloom_list.get_list( shard ).insert_before( b, p );
      }
      break;
    }
  }
  if ( ref != NULL )
    b->add_bloom_ref( ref );
  return b;
}

void *
RouteDB::alloc_bloom( void ) noexcept
{
  static const size_t BLOOM_BLOCK_SIZE = 32,
                      alloc_sz = sizeof( BloomRoute ) * BLOOM_BLOCK_SIZE +
                                 sizeof( BloomRoute * );
  if ( this->bloom_mem == NULL ) {
    this->bloom_mem = ::malloc( alloc_sz );
    ::memset( this->bloom_mem, 0, alloc_sz );
  }
  BloomRoute * block = (BloomRoute *) this->bloom_mem;
  for (;;) {
    for ( size_t i = 0; i < BLOOM_BLOCK_SIZE; i++ ) {
      if ( block[ i ].in_list == 0 )
        return &block[ i ];
    }
    void ** next = (void **) &block[ BLOOM_BLOCK_SIZE ];
    if ( *next == NULL ) {
      *next = ::malloc( alloc_sz );
      ::memset( *next, 0, alloc_sz );
    }
    block = (BloomRoute *) *next;
  }
}

void
RouteDB::remove_bloom_route( BloomRoute *b ) noexcept
{
  if ( b->nblooms > 0 ) {
    fprintf( stderr, "bloom ref still exist in route %u\n", b->r );
    return;
  }
  if ( b->in_list ) {
    this->bloom_list.get_list( b->in_list - 1 ).pop( b );
    if ( b->bloom != NULL ) {
      ::free( b->bloom );
      b->bloom = NULL;
    }
    b->in_list = 0;
  }
}

BloomRef *
RouteDB::create_bloom_ref( uint32_t seed,  const char *nm,
                           BloomDB &db ) noexcept
{
  void * m = ::malloc( sizeof( BloomRef ) );
  return new ( m ) BloomRef( seed, nm, db );
}

BloomRef *
RouteDB::create_bloom_ref( uint32_t *pref_count,  BloomBits *bits,
                           const char *nm,  BloomDB &db ) noexcept
{
  void * m = ::malloc( sizeof( BloomRef ) );
  return new ( m ) BloomRef( bits, pref_count, nm, db );
}

BloomRef *
RouteDB::update_bloom_ref( const void *data,  size_t datalen,
                           uint32_t ref_num,  const char *nm,
                           BloomDB &db ) noexcept
{
  BloomRef * ref;
  uint32_t     pref[ MAX_RTE ];
  BloomCodec   code;
  BloomBits  * bits;
  void       * details;
  size_t       detail_size;

  bits = code.decode( pref, MAX_RTE, details, detail_size, data, datalen/4 );
  if ( bits == NULL )
    return NULL;

  if ( (ref = db[ ref_num ]) != NULL ) {
    ref->update_route( pref, bits, (BloomDetail *) details,
                       (uint32_t) ( detail_size / sizeof( BloomDetail ) ) );
  }
  else {
    void * m = ::malloc( sizeof( BloomRef ) );
    ref = new ( m ) BloomRef( bits, pref, nm, db, ref_num );
  }
  return ref;
}

BloomRef::BloomRef( uint32_t seed,  const char *nm,  BloomDB &db ) noexcept
        : bits( 0 ), links( 0 ), details( 0 ), pref_mask( 0 ),
          detail_mask( 0 ), nlinks( 0 ), ndetails( 0 ), ref_num( db.count ),
          bloom_db( db )
{
  ::strncpy( this->name, nm, sizeof( this->name ) );
  this->name[ sizeof( this->name ) - 1 ] = '\0';
  ::memset( this->pref_count, 0, sizeof( this->pref_count ) );
  db[ this->ref_num ] = this;
  this->update_route( NULL, BloomBits::resize( NULL, seed, 52 ), NULL, 0 );
}

BloomRef::BloomRef( BloomBits *b,  const uint32_t *pref,
                    const char *nm,  BloomDB &db,  uint32_t num ) noexcept
        : bits( 0 ), links( 0 ), details( 0 ), pref_mask( 0 ),
          detail_mask( 0 ), nlinks( 0 ), ndetails( 0 ), bloom_db( db )
{
  ::strncpy( this->name, nm, sizeof( this->name ) );
  this->name[ sizeof( this->name ) - 1 ] = '\0';
  ::memset( this->pref_count, 0, sizeof( this->pref_count ) );
  if ( (this->ref_num = num) == (uint32_t) -1 )
    this->ref_num = db.count;
  db[ this->ref_num ] = this;
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
  /*printf( "add_bloom_ref %s -> %s.%u (cnt=%u) (%lx)\n", ref->name,
          ((RoutePublish &) this->rdb).service_name, this->r, n,
          ref->pref_mask );*/

  uint16_t prefix_len;
  for ( prefix_len = 0; prefix_len < MAX_RTE; prefix_len++ ) {
    if ( ref->pref_count[ prefix_len ] != 0 ) {
      if ( this->rdb.bloom_pref_count[ prefix_len ]++ == 0 ) {
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
      b->rdb.remove_bloom_route( b );
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

BloomRef *
BloomRoute::del_bloom_ref( BloomRef *ref ) noexcept
{
  uint32_t i, n = this->nblooms;

  this->invalid();
  if ( ref == NULL ) {
    if ( n == 0 )
      return NULL;
    ref = this->bloom[ n - 1 ];
  }
  else {
    for ( i = n; ; i-- ) {
      if ( i == 0 )
        return NULL;
      if ( this->bloom[ i - 1 ] != ref )
        continue;
      if ( i != n )
        ::memmove( &this->bloom[ i - 1 ], &this->bloom[ i ],
                   sizeof( this->bloom[ 0 ] ) * ( n - i ) );
      break;
    }
  }
  this->nblooms = n - 1;
  ref->del_link( this );

  uint16_t prefix_len;
  for ( prefix_len = 0; prefix_len < MAX_RTE; prefix_len++ ) {
    if ( ref->pref_count[ prefix_len ] != 0 ) {
      if ( --this->rdb.bloom_pref_count[ prefix_len ] == 0 ) {
        this->rdb.del_prefix_len( prefix_len, false );
      }
    }
  }
  return ref;
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
      return;
    }
  }
}

BloomRoute *
BloomRef::get_bloom_by_fd( uint32_t fd,  uint32_t shard ) noexcept
{
  uint32_t i, n = this->nlinks;
  for ( i = n; ; i-- ) {
    if ( i == 0 )
      break;
    BloomRoute *b = this->links[ i - 1 ];
    if ( b->r == fd && b->in_list == shard + 1 )
      return b;
  }
  return NULL;
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
      /*if ( this->links[ i ]->rdb.rt_hash[ prefix_len ]->is_empty() )*/
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
    this->u.suffix.len  = (uint16_t) cvt.suffixlen;
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
                     uint32_t *routes,  uint32_t rcnt,
                     uint32_t shard ) noexcept
{
  if ( this->cache.is_invalid ) {
do_reset:;
    if ( ! this->cache.reset() )
      return;
  }
  uint64_t    h;
  uint32_t  * ptr;
  RteCacheVal val;
  size_t      n = this->cache.end + rcnt;

  if ( ! this->cache.busy ) { /* no refs, ok to realloc() */
    if ( n > RouteCache::MAX_CACHE )
      goto do_reset;
    ptr = this->cache.spc.make( n + 1024 );
  }
  else {
    ptr = this->cache.spc.ptr; /* no realloc until ! busy */
    if ( n > this->cache.spc.size ) {
      this->cache.need = n - this->cache.spc.size;
      if ( this->cache.need < 1024 )
        this->cache.need = 1024;
      return;
    }
  }
  val.rcnt = rcnt;
  val.off  = (uint32_t) this->cache.end;
  this->cache.end += rcnt;
  ::memcpy( &ptr[ val.off ], routes, sizeof( routes[ 0 ] ) * rcnt );

  h = ( (uint64_t) shard << 48 ) | ( (uint64_t) prefix_len << 32 ) |
        (uint64_t) hash;
  this->cache.ht->upsert( h, val ); /* save rcnt, off at hash */
  this->cache.count++;

  if ( this->cache.ht->elem_count >= this->cache.ht->max_count ) {
    if ( this->cache.ht->elem_count > RouteCache::MAX_CACHE )
      goto do_reset;
    RteCacheTab::check_resize( this->cache.ht );
  }
}

void
RouteDB::cache_need( void ) noexcept
{
  this->cache.spc.make( this->cache.end + this->cache.need );
  this->cache.need = 0;
}

void
RouteDB::cache_purge( size_t pos ) noexcept
{
  uint64_t    h;
  RteCacheVal val;
  this->cache.ht->get( pos, h, val );
  this->cache.free += val.rcnt;
  this->cache.count--;
#if 0
  if ( this->cache.is_invalid || this->cache.free * 2 > this->cache.end ||
       this->cache.end > RouteCache::MAX_CACHE ) {
    this->cache.reset();
    return;
  }
#endif
  this->cache.ht->remove( pos );
}
#if 0
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
#endif
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
  if ( this->cache_find( prefix_len, hash, routes, rcnt, 0, pos ) ) {
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
