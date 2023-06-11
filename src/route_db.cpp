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

uint32_t RouteGroup::pre_seed[ MAX_PRE ];

RouteGroup::RouteGroup( RouteCache &c,  RouteZip &z,  BloomGroup &b,
                        uint32_t gn ) noexcept
          : cache( c ), zip( z ), group_num( gn ), entry_count( 0 ),
            rt_mask( 0 ), bloom( b )
{
  for ( size_t i = 0; i < sizeof( this->rt_hash ) /
                          sizeof( this->rt_hash[ 0 ] ); i++ ) {
    this->rt_hash[ i ] = UIntHashTab::resize( NULL );
  }
  /* wildcard prefix: _SYS.W3. */
  if ( pre_seed[ MAX_PRE - 1 ] == 0 ) {
    for ( uint16_t j = 0; j < MAX_PRE; j++ ) {
      char   sub[ 16 ];
      CatPtr p( sub );
      p.s( "_SYS.W" ).u( j ).s( "." );
      pre_seed[ j ] = kv_crc_c( sub, p.len(), 0 );
    }
  }
}

RouteDB::RouteDB( BloomDB &g_db ) noexcept
       : RouteGroup( this->cache, this->zip, this->bloom_grp, 0 ),
         bloom_grp( this->zip ), g_bloom_db( g_db ), q_ht( 0 )
{
}

QueueName *
QueueNameDB::get_queue_name( const QueueName &qn ) noexcept
{
  uint32_t i;
  char   * q;
  if ( this->q_ht != NULL ) {
    size_t pos;
    if ( this->q_ht->find( qn.queue_hash, pos, i ) )
      return this->queue_name.ptr[ i ];
  }
  if ( qn.queue_len == 0 )
    return NULL;
  if ( this->q_ht == NULL )
    this->q_ht = UIntHashTab::resize( NULL );
  size_t l = this->queue_strlen;
  this->queue_str = (char *) ::realloc( this->queue_str, l + qn.queue_len + 1 );
  q = &this->queue_str[ l ];
  ::memcpy( q, qn.queue, qn.queue_len );
  q[ qn.queue_len ] = '\0';
  this->queue_strlen += qn.queue_len + 1;

  q = this->queue_str;
  for ( i = 0; i < this->queue_name.count; i++ ) {
    this->queue_name.ptr[ i ]->queue = q;
    q = &q[ this->queue_name.ptr[ i ]->queue_len + 1 ];
  }
  QueueName *p = new ( ::malloc( sizeof( QueueName ) ) )
    QueueName( q, qn.queue_len, qn.queue_hash );
  p->idx = this->queue_name.count;
  this->queue_name.push( p );
  this->q_ht->upsert_rsz( this->q_ht, qn.queue_hash, p->idx );
  return p;
}

QueueName *
QueueNameDB::get_queue_str( const char *name,  size_t len ) noexcept
{
  QueueName qn( name, len, kv_crc_c( name, len, 0 ) );
  return this->get_queue_name( qn );
}

QueueName *
QueueNameDB::get_queue_hash( uint32_t q_hash ) noexcept
{
  QueueName qn( NULL, 0, q_hash );
  return this->get_queue_name( qn );
}

void
QueueDB::init( RouteCache &c,  RouteZip &z,  BloomGroup &b,  QueueName *qn,
               uint32_t gn ) noexcept
{
  void * m = ::malloc( sizeof( RouteGroup ) );
  qn->refs++;
  this->q_name      = qn;
  this->route_group = new ( m ) RouteGroup( c, z, b, gn );
  this->next_route  = 0;
}

RouteGroup &
RouteDB::get_queue_group( const QueueName &qn ) noexcept
{
  size_t   pos;
  uint32_t i;
  if ( this->q_ht != NULL && this->q_ht->find( qn.queue_hash, pos, i ) )
    return *this->queue_db.ptr[ i ].route_group;

  i = this->queue_db.count;
  QueueName * q_ptr = this->g_bloom_db.q_db.get_queue_name( qn );
  QueueDB   & el    = this->queue_db.push();
  el.init( this->cache, this->zip, this->bloom, q_ptr, i );
  if ( this->q_ht == NULL )
    this->q_ht = UIntHashTab::resize( NULL );
  this->q_ht->upsert_rsz( this->q_ht, qn.queue_hash, i );
  return *el.route_group;
}

RouteCache::RouteCache() noexcept
{
  this->ht         = RteCacheTab::resize( NULL );
  this->end        = 0;
  this->free       = 0;
  this->count      = 0;
  this->busy       = 0;
  this->need       = 0;
  this->hit_cnt    = 0;
  this->miss_cnt   = 0;
  this->max_cnt    = 0;
  this->max_size   = 0;
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
  this->zht        = UIntHashTab::resize( NULL );
  this->code_end   = 0;
  this->code_free  = 0;
  this->route_free = 0;
}

void
RouteZip::reset( void ) noexcept
{
  delete this->zht;
  this->code_buf.reset();
  this->zroute_spc.reset();
  for ( int i = 0; i < 64; i++ )
    this->route_temp[ i ].reset();
  this->init();
}

RouteSpace &
RouteZip::create_extra_spc( uint16_t &ref ) noexcept
{
  size_t i = 0;
  for ( ; i < this->extra_temp.size; i++ ) {
    if ( this->extra_temp.ptr[ i ] == NULL ||
         ! this->extra_temp.ptr[ i ]->used )
      break;
  }
  if ( i == this->extra_temp.size )
    this->extra_temp.make( i + 1, true );
  if ( this->extra_temp.ptr[ i ] == NULL ) {
    this->extra_temp.ptr[ i ] =
      new ( ::malloc( sizeof( RouteSpaceRef ) ) ) RouteSpaceRef();
  }
  this->extra_temp.ptr[ i ]->used = true;
  ref = 64 + (uint16_t) i;
  return *this->extra_temp.ptr[ i ];
}

void
RouteZip::release_extra_spc( uint16_t ref ) noexcept
{
  this->extra_temp.ptr[ ref - 64 ]->used = false;
}

void
RouteRefCount::deref( RouteDB &rdb ) noexcept
{
  rdb.zip.route_free &= ~this->ref_mask;
  rdb.cache.busy     -= this->cache_ref;
  if ( this->extra_cnt > 0 ) {
    uint32_t i, max_bit = 64 * this->extra_cnt;
    UIntBitSet set( this->ref_extra );
    for ( bool b = set.first( i, max_bit ); b; b = set.next( i, max_bit ) )
      rdb.zip.release_extra_spc( 64 + i );
  }
}

void
RouteRefCount::set_ref_extra( uint16_t spc_ref ) noexcept
{
  if ( this->extra_cnt == 0 ) {
    ::memset( this->ref_extra, 0, sizeof( this->ref_extra ) );
    this->extra_cnt =
      sizeof( this->ref_extra ) / sizeof( this->ref_extra[ 0 ] );
  }
  uint32_t max_bit = 64 * this->extra_cnt;
  if ( spc_ref >= 64 && spc_ref < 64 + max_bit )
    this->ref_extra[ spc_ref / 64 - 1 ] |= (uint64_t) 1 << ( spc_ref % 64 );
}

void
RouteRefCount::merge_ref_extra( RouteRefCount &refs ) noexcept
{
  if ( refs.extra_cnt != 0 ) {
    if ( this->extra_cnt == 0 ) {
      ::memset( this->ref_extra, 0, sizeof( this->ref_extra ) );
      this->extra_cnt =
        sizeof( this->ref_extra ) / sizeof( this->ref_extra[ 0 ] );
    }
    for ( uint32_t i = 0; i < this->extra_cnt; i++ )
      this->ref_extra[ i ] |= refs.ref_extra[ i ];
  }
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

static int cmp_uint( const uint32_t &x,  const uint32_t &y ) {
  return (int) x - (int) y;
}

uint32_t
rai::kv::bsearch_route( uint32_t r,  const uint32_t *routes,
                        uint32_t size ) noexcept
{
  if ( size < 24 ) {
    for ( ; size > 0; size-- ) {
      if ( routes[ size - 1 ] < r )
        return size;
    }
    return 0;
  }
  return lower_bound( routes, r, size, cmp_uint );
}

static int cmp_qref( const QueueRef &x,  const QueueRef &y ) {
  return (int) x.r - (int) y.r;
}

uint32_t
rai::kv::bsearch_route( uint32_t r,  const QueueRef *routes,
                        uint32_t size ) noexcept
{
  if ( size < 24 ) {
    for ( ; size > 0; size-- ) {
      if ( routes[ size - 1 ].r < r )
        return size;
    }
    return 0;
  }
  QueueRef x = { r, 0 };
  return lower_bound( routes, x, size, cmp_qref );
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
  uint32_t i = 0, j = 0, k;
  while ( i < count && j < mcount ) {
    if ( routes[ i ] <= merge[ j ] ) {
      if ( routes[ i ] == merge[ j ] )
        j++;
      i++;
    }
    else {
      k = j + 1;
      while ( k < mcount && routes[ i ] > merge[ k ] )
        k++;
      uint32_t len = k - j, dest = count + len, src = count;
      while ( src > i )
        routes[ --dest ] = routes[ --src ];
      count += len;
      do {
        routes[ i++ ] = merge[ j++ ];
      } while ( j < k );
    }
  }
  while ( j < mcount )
    routes[ count++ ] = merge[ j++ ];
  return count;
}

uint32_t
rai::kv::merge_route2( uint32_t *dest,  const uint32_t *routes,  uint32_t count,
                       const uint32_t *merge,  uint32_t mcount ) noexcept
{
  uint32_t i = 0, j = 0, k = 0;
  while ( i < count && j < mcount ) {
    if ( routes[ i ] <= merge[ j ] ) {
      dest[ k ] = routes[ i++ ];
      if ( dest[ k ] == merge[ j ] )
        j++;
      k++;
    }
    else {
      dest[ k++ ] = merge[ j++ ];
    }
  }
  while ( i < count )
    dest[ k++ ] = routes[ i++ ];
  while ( j < mcount )
    dest[ k++ ] = merge[ j++ ];
  return k;
}

uint32_t
rai::kv::merge_route2( QueueRef *dest,  const QueueRef *routes,  uint32_t count,
                       const QueueRef *merge,  uint32_t mcount ) noexcept
{
  uint32_t i = 0, j = 0, k = 0;
  while ( i < count && j < mcount ) {
    if ( routes[ i ].r <= merge[ j ].r ) {
      dest[ k ] = routes[ i++ ];
      if ( dest[ k ].r == merge[ j ].r )
        dest[ k ].refcnt += merge[ j++ ].refcnt;
      k++;
    }
    else {
      dest[ k++ ] = merge[ j++ ];
    }
  }
  while ( i < count )
    dest[ k++ ] = routes[ i++ ];
  while ( j < mcount )
    dest[ k++ ] = merge[ j++ ];
  return k;
}

uint32_t
RouteRef::decode( uint32_t val,  uint32_t add ) noexcept
{
  size_t pos;
  if ( this->zip.zht->find( val, pos, this->ptr.off ) ) {
    this->ptr.code_buf = &this->zip.code_buf;
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
  if ( this->zip.zht->find( val, pos, this->ptr.off ) )
    this->ptr.code_buf = &this->zip.code_buf;
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
  uint32_t * code = this->zip.zroute_spc.make( this->rcnt );
  uint32_t   ecnt = DeltaCoder::encode_stream( this->rcnt, this->routes, 0,
                                               code );
  if ( ecnt == 1 ) /* single value code */
    return code[ 0 ];
  return this->zip.make_code_ref( code, ecnt, this->rcnt );
}

void
RouteRef::deref_coderef( void ) noexcept
{
  CodeRef *p = this->ptr.get();
  if ( p != NULL && --p->ref == 0 ) {
    ptr.code_buf = NULL;
    this->zip.code_free += p->word_size();
    if ( this->zip.code_free * 2 > this->zip.code_end )
      this->zip.gc_code_ref_space();
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
RouteGroup::get_route_count( uint16_t prefix_len,  uint32_t hash ) noexcept
{
  uint32_t rcnt = 0;
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
  return rcnt;
}
/* insert route for a subject hash */
uint32_t
RouteGroup::add_route( uint16_t prefix_len,  uint32_t hash,
                       uint32_t r ) noexcept
{
  RouteRef rte( this->zip, prefix_len );
  return this->add_route( prefix_len, hash, r, rte );
}

uint32_t
RouteGroup::add_route( uint16_t prefix_len,  uint32_t hash,  uint32_t r,
                       RouteRef &rte ) noexcept
{
  UIntHashTab * xht = this->rt_hash[ prefix_len ];
  size_t        pos, cpos = 0;
  uint32_t    * routes;
  uint32_t      val = 0, rcnt = 0, xcnt;
  bool          exists;

  exists = xht->find( hash, pos, val );
  if ( this->cache_find( prefix_len, hash, routes, rcnt, 0, cpos ) )
    this->cache_purge( cpos );
  if ( exists )
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
    rte.deref_coderef(); /* free the old code, it is no longer referenced */
  }
  return xcnt; /* count of routes after inserting r */
}
/* delete a route for a subject hash */
uint32_t
RouteGroup::del_route( uint16_t prefix_len,  uint32_t hash,
                       uint32_t r ) noexcept
{
  RouteRef rte( this->zip, prefix_len );
  return this->del_route( prefix_len, hash, r, rte );
}

uint32_t
RouteGroup::del_route( uint16_t prefix_len,  uint32_t hash,  uint32_t r,
                       RouteRef &rte ) noexcept
{
  UIntHashTab * xht = this->rt_hash[ prefix_len ];
  size_t        pos, cpos = 0;
  uint32_t    * routes;
  uint32_t      val, rcnt = 0, xcnt;

  if ( ! xht->find( hash, pos, val ) )
    return 0;
  if ( this->cache_find( prefix_len, hash, routes, rcnt, 0, cpos ) )
    this->cache_purge( cpos );
  rcnt = rte.decompress( val, 0 );
  xcnt = rte.remove( r ); /* remove r from rte.routes[] */
  /* if route was deleted */
  if ( xcnt != rcnt ) {
    if ( xcnt == 0 ) { /* no more routes left */
      xht->remove( pos ); /* remove val from xht */
      if ( xht->is_empty() )
        this->del_prefix_len( prefix_len, true );
      this->entry_count--;
      /*if ( this->entry_count == 0 ) {
        printf( "rg %u empty\n", this->group_num );
      }*/
    }
    else { /* recompress and update hash */
      val = rte.compress();
      xht->set( hash, pos, val ); /* update val in xht */
      if ( UIntHashTab::check_resize( xht ) )
        this->rt_hash[ prefix_len ] = xht;
    }
    rte.deref_coderef(); /* free old code buf */
  }
  return xcnt; /* count of routes after removing r */
}

uint32_t
RouteGroup::ref_route( uint16_t prefix_len,  uint32_t hash,
                       RouteRef &rte ) noexcept
{
  UIntHashTab * xht = this->rt_hash[ prefix_len ];
  size_t        pos;
  uint32_t      val;

  if ( ! xht->find( hash, pos, val ) )
    return 0;
  return rte.decompress( val, 0 );
}

void
RouteGroup::get_route_slow( RouteLookup &look,  uint32_t val ) noexcept
{
  RouteRef rte( this->zip, SUB_RTE );
  look.rcount = rte.decompress( val, 0 );
  look.routes = rte.routes;
  look.add_ref( rte );
  return this->get_bloom_route( look );
}

void
RouteGroup::get_route_slow2( RouteLookup &look,  uint16_t prefix_len,
                             uint32_t hash,  uint32_t val ) noexcept
{
  RouteRef rte( this->zip, prefix_len );
  look.rcount = rte.decompress( val, 0 );
  look.routes = rte.routes;
  look.add_ref( rte );
  return this->get_bloom_route2( look, prefix_len, hash );
}

static int cmp_prefix( const BloomDetail &el,  const BloomDetailHash &v ) {
  if ( el.prefix_len == v.prefix_len )
    return (int) (uint32_t) el.hash - (int) (uint32_t) v.hash;
  return (int) (uint32_t) el.prefix_len - (int) (uint32_t) v.prefix_len;
}

template <class Match>
bool
BloomRef::detail_matches( Match &args,  uint16_t prefix_len,
                          uint32_t hash,  bool &has_detail ) const noexcept
{
  uint32_t j = 0, n = this->ndetails, detail_not_matched = 0;
  BloomDetail * d = this->details;
  BloomDetailHash v = { hash, prefix_len };

  j = lower_bound( d, v, n, cmp_prefix );
  for ( ; j < n; j++ ) {
    if ( d[ j ].hash != hash || d[ j ].prefix_len != prefix_len )
      break;
    has_detail = true;
    if ( d[ j ].match( args.sub, args.sublen, args.subj_hash ) )
      return true;
    detail_not_matched++; /* hash match, but match filtered */
  }
  /* no more details left, is a match */
  if ( detail_not_matched > 0 ) {
    /* if has a regular prefix match */
    if ( this->bits->ht_refs( args.subj_hash ) >= detail_not_matched ) {
      has_detail = false; /* override since prefix matches */
      return true;
    }
    return false;
  }
  return true;
}

bool
BloomRef::queue_matches( RouteLookup &look,  uint16_t prefix_len,
                         uint32_t hash,  QueueMatch &m ) const noexcept
{
  uint32_t j = 0, n = this->ndetails;
  BloomDetail   * d = this->details;
  BloomDetailHash v = { hash, prefix_len };

  j = lower_bound( d, v, n, cmp_prefix );
  m.refcnt = 0;
  for ( ; j < n; j++ ) {
    if ( d[ j ].hash != hash || d[ j ].prefix_len != prefix_len )
      break;
    if ( m.xhash == 0 ) {
      uint16_t sublen = ( prefix_len < SUB_RTE ? prefix_len : look.sublen );
      m.xhash = QueueMatch::hash2( look.sub, sublen, hash );
    }
    if ( d[ j ].queue_equals( m ) )
      m.refcnt += this->details[ j ].u.queue.refcnt;
  }
  return m.refcnt > 0;
}

void
RouteGroup::get_bloom_route( RouteLookup &look ) noexcept
{
  if ( look.queue_hash != 0 )
    return this->bloom.get_queue( look );
  bool has_detail = this->bloom.get_route( look );
  if ( ! has_detail )
    this->cache_save( SUB_RTE, look.subj_hash, look.routes, look.rcount,
                      look.shard );
}

void
RouteGroup::get_bloom_route2( RouteLookup &look,  uint16_t prefix_len,
                              uint32_t hash ) noexcept
{
  if ( look.queue_hash != 0 )
    return this->bloom.get_queue2( look, prefix_len, hash );
  bool has_detail = this->bloom.get_route2( look, prefix_len, hash );
  if ( ! has_detail )
    this->cache_save( prefix_len, hash, look.routes, look.rcount, look.shard );
}

bool
BloomGroup::get_route( RouteLookup &look ) noexcept
{
  RouteRef     rte( this->zip, SUB_RTE + 16 );
  RouteSpace & spc        = rte.route_spc;
  uint32_t     count      = 0,
               hash       = look.subj_hash;
  bool         has_detail = false;

  for ( BloomRoute *b = this->list.hd( look.shard ); b != NULL; b = b->next ) {
    if ( b->in_list != look.shard + 1 )
      continue;

    if ( b->is_invalid )
      b->update_masks();

    if ( ! b->has_subs )
      continue;

    for ( uint32_t i = 0; i < b->nblooms; i++ ) {
      BloomRef * r = b->bloom[ i ];
      if ( r->pref_count[ SUB_RTE ] != 0 && r->bits->is_member( hash ) &&
           ( ! r->sub_detail ||
             r->detail_matches( look, SUB_RTE, hash, has_detail ) ) ) {
        uint32_t * p = spc.make( count + 1 );
        p[ count++ ] = b->r;
        break;
      }
    }
  }
  if ( count > 0 ) {
    if ( look.rcount > 0 ) {
      spc.make( count + look.rcount );
      count = merge_route( spc.ptr, count, look.routes, look.rcount );
    }
    look.rcount = count;
    look.routes = spc.ptr;
    look.add_ref( rte );
  }
  return has_detail;
}

bool
BloomGroup::get_route2( RouteLookup &look,  uint16_t prefix_len,
                        uint32_t hash ) noexcept
{
  RouteRef     rte( this->zip, prefix_len + 16 );
  RouteSpace & spc         = rte.route_spc;
  uint64_t     prefix_mask = look.mask;
  uint32_t     count       = 0;
  bool         has_detail  = false;

  if ( ( this->mask & mask ) != 0 ) {
    BloomRoute *b = this->list.hd( look.shard );
    for ( ; b != NULL; b = b->next ){
      if ( b->in_list != look.shard + 1 )
        continue;

      if ( b->is_invalid )
        b->update_masks();

      if ( ( b->pref_mask & prefix_mask ) == 0 )
        continue;

      if ( ( b->detail_mask & mask ) == 0 ) {
        for ( uint32_t i = 0; i < b->nblooms; i++ ) {
          BloomRef * r = b->bloom[ i ];
          if ( ( r->pref_mask & prefix_mask ) != 0 &&
               r->bits->is_member( hash ) )
            goto match;
        }
        continue;
      }
      /*printf( "-- %.*s %u:mask 0x%x pref 0x%x detail 0x%x, r %u\n",
               (int) sublen, sub, prefix_len,
               (uint32_t) mask, (uint32_t) b->pref_mask,
               (uint32_t) b->detail_mask , b->r );*/
      for ( uint32_t i = 0; i < b->nblooms; i++ ) {
        BloomRef * r = b->bloom[ i ];
        if ( ( r->pref_mask & prefix_mask ) != 0 && r->bits->is_member( hash ) &&
             ( ( r->detail_mask & prefix_mask ) == 0 ||
               r->detail_matches( look, prefix_len, hash, has_detail ) ) )
          goto match;
      }
      if ( 0 ) {
    match:;
        uint32_t * p = spc.make( count + 1 );
        p[ count++ ] = b->r; /* route matches */
      }
    }
  }
  if ( count > 0 ) {
    if ( look.rcount > 0 ) {
      spc.make( count + look.rcount );
      count = merge_route( spc.ptr, count, look.routes, look.rcount );
    }
    look.rcount = count;
    look.routes = spc.ptr;
    look.add_ref( rte );
  }
  return has_detail;
}

uint32_t
rai::kv::merge_queue( QueueRef *routes,  uint32_t count,
                      const uint32_t *merge,  uint32_t mcount ) noexcept
{
  uint32_t i = 0, j = 0, k;
  while ( i < count && j < mcount ) {
    if ( routes[ i ].r <= merge[ j ] ) {
      if ( routes[ i ].r == merge[ j ] ) {
        routes[ i ].refcnt++;
        j++;
      }
      i++;
    }
    else {
      k = j + 1;
      while ( k < mcount && routes[ i ].r > merge[ k ] )
        k++;
      uint32_t len = k - j, dest = count + len, src = count;
      while ( src > i )
        routes[ --dest ] = routes[ --src ];
      count += len;
      do {
        routes[ i ].r = merge[ j++ ];
        routes[ i++ ].refcnt = 1;
      } while ( j < k );
    }
  }
  while ( j < mcount ) {
    routes[ count ].r = merge[ j++ ];
    routes[ count++ ].refcnt = 1;
  }
  return count;
}

uint32_t
rai::kv::merge_queue2( QueueRef *routes,  uint32_t count,
                       const QueueRef *merge,  uint32_t mcount ) noexcept
{
  uint32_t i = 0, j = 0, k;
  while ( i < count && j < mcount ) {
    if ( routes[ i ].r <= merge[ j ].r ) {
      if ( routes[ i ].r == merge[ j ].r ) {
        routes[ i ].refcnt += merge[ j ].refcnt;
        j++;
      }
      i++;
    }
    else {
      k = j + 1;
      while ( k < mcount && routes[ i ].r > merge[ k ].r )
        k++;
      uint32_t len = k - j, dest = count + len, src = count;
      while ( src > i )
        routes[ --dest ] = routes[ --src ];
      count += len;
      do {
        routes[ i++ ] = merge[ j++ ];
      } while ( j < k );
    }
  }
  while ( j < mcount )
    routes[ count++ ] = merge[ j++ ];
  return count;
}

void
BloomGroup::get_queue( RouteLookup &look ) noexcept
{
  RouteRef     rte( this->zip, SUB_RTE + 16 );
  RouteSpace & spc   = rte.route_spc;
  uint32_t     count = 0,
               hash  = look.subj_hash;
  QueueMatch   m     = { look.queue_hash, 0, 0 };

  for ( BloomRoute *b = this->list.hd( look.shard ); b != NULL;
        b = b->next ) {
    if ( b->in_list != look.shard + 1 )
      continue;

    if ( b->is_invalid )
      b->update_masks();

    if ( ! b->has_subs || b->queue_cnt == 0 )
      continue;

    QueueRef * p = NULL;
    for ( uint32_t i = 0; i < b->nblooms; i++ ) {
      BloomRef * r = b->bloom[ i ];
      if ( r->queue_cnt != 0 && r->pref_count[ SUB_RTE ] != 0 &&
           r->bits->is_member( hash ) &&
           r->queue_matches( look, SUB_RTE, hash, m ) ) {
        if ( p == NULL ) {
          p = (QueueRef *) spc.make( ( count + 1 ) * 2 );
          p[ count ].r = b->r;
          p[ count++ ].refcnt = m.refcnt;
        }
        else {
          p[ count - 1 ].refcnt += m.refcnt;
        }
      }
    }
  }
  if ( count > 0 ) {
    if ( look.rcount > 0 ) {
      spc.make( ( count + look.rcount ) * 2 );
      count = merge_queue( (QueueRef *) spc.ptr, count, look.routes,
                           look.rcount );
    }
    look.rcount  = count;
    look.routes  = NULL;
    look.qroutes = (QueueRef *) spc.ptr;
    look.add_ref( rte );
  }
}

void
BloomGroup::get_queue2( RouteLookup &look,  uint16_t prefix_len,
                        uint32_t hash ) noexcept
{
  RouteRef     rte( this->zip, prefix_len + 16 );
  RouteSpace & spc         = rte.route_spc;
  uint64_t     prefix_mask = look.mask;
  uint32_t     count       = 0;
  QueueMatch   m           = { look.queue_hash, 0, 0 };

  if ( ( this->mask & prefix_mask ) != 0 ) {
    BloomRoute *b = this->list.hd( look.shard );
    for ( ; b != NULL; b = b->next ){
      if ( b->in_list != look.shard + 1 )
        continue;

      if ( b->is_invalid )
        b->update_masks();

      if ( ( b->pref_mask & prefix_mask ) == 0 || b->queue_cnt == 0 )
        continue;

      QueueRef * p = NULL;
      for ( uint32_t i = 0; i < b->nblooms; i++ ) {
        BloomRef * r = b->bloom[ i ];
        if ( r->queue_cnt != 0 && ( r->detail_mask & prefix_mask ) != 0 &&
             r->bits->is_member( hash ) &&
             r->queue_matches( look, prefix_len, hash, m ) ) {
          if ( p == NULL ) {
            p = (QueueRef *) spc.make( ( count + 1 ) * 2 );
            p[ count ].r = b->r;
            p[ count++ ].refcnt = m.refcnt;
          }
          else {
            p[ count - 1 ].refcnt += m.refcnt;
          }
        }
      }
    }
  }
  if ( count > 0 ) {
    if ( look.rcount > 0 ) {
      spc.make( ( count + look.rcount ) * 2 );
      count = merge_queue( (QueueRef *) spc.ptr, count, look.routes,
                           look.rcount );
    }
    look.rcount  = count;
    look.routes  = NULL;
    look.qroutes = (QueueRef *) spc.ptr;
    look.add_ref( rte );
  }
}

uint16_t
BloomMatch::test_prefix( BloomMatchArgs &args,  const BloomRef &bloom,
                         uint16_t prefix_len ) noexcept
{
  bool has_det;
  if ( prefix_len == SUB_RTE ) {
    if ( bloom.pref_count[ SUB_RTE ] != 0 &&
         bloom.bits->is_member( args.subj_hash ) &&
         ( ! bloom.sub_detail ||
             bloom.detail_matches( args, SUB_RTE, args.subj_hash, has_det ) ) )
      return SUB_RTE;
    return MAX_RTE;
  }
  if ( prefix_len < this->max_pref ) {
    if ( test_prefix_mask( bloom.pref_mask, prefix_len ) ) {
      uint32_t prefix_hash = this->get_prefix_hash( args, prefix_len );
      if ( bloom.bits->is_member( prefix_hash ) &&
           ( ! test_prefix_mask( bloom.detail_mask, prefix_len ) ||
            bloom.detail_matches( args, prefix_len, prefix_hash, has_det ) ) )
        return prefix_len;
    }
  }
  return MAX_RTE;
}

void
BloomRoute::update_masks( void ) noexcept
{
  this->is_invalid  = false;
  this->pref_mask   = 0;
  this->detail_mask = 0;
  this->queue_cnt   = 0;
  this->sub_detail  = false;
  this->has_subs    = false;
  for ( uint32_t i = 0; i < this->nblooms; i++ ) {
    BloomRef * r = this->bloom[ i ];
    this->pref_mask   |= r->pref_mask;
    this->detail_mask |= r->detail_mask;
    this->sub_detail  |= r->sub_detail;
    this->queue_cnt   += r->queue_cnt;
    this->has_subs    |= ( r->pref_count[ SUB_RTE ] != 0 );
  }
}

uint32_t
RouteDB::get_bloom_count( uint16_t prefix_len,  uint32_t hash,
                          uint32_t shard ) noexcept
{
  if ( this->bloom.pref_count[ prefix_len ] == 0 )
    return 0;
  BloomRoute * b;
  BloomRef   * r;
  uint32_t     rcnt = 0;
  for ( b = this->bloom.list.hd( shard ); b != NULL; b = b->next ) {
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

  for ( BloomRoute *p = this->bloom.list.hd( shard ); ; p = p->next ) {
    if ( p == NULL ) {
      void * m = this->g_bloom_db.bloom_mem.alloc( sizeof( BloomRoute ) );
      b = new ( m ) BloomRoute( r, *this, shard + 1 );
      this->bloom.list.get_list( shard ).push_tl( b );
      break;
    }
    if ( p->r > r || ( p->r == r && p->in_list >= shard + 1 ) ) {
      if ( p->r == r && p->in_list == shard + 1 )
        b = p;
      else {
        void * m = this->g_bloom_db.bloom_mem.alloc( sizeof( BloomRoute ) );
        b = new ( m ) BloomRoute( r, *this, shard + 1 );
        this->bloom.list.get_list( shard ).insert_before( b, p );
      }
      break;
    }
  }
  if ( ref != NULL )
    b->add_bloom_ref( ref );
  return b;
}

void
RouteDB::remove_bloom_route( BloomRoute *b ) noexcept
{
  if ( b->nblooms > 0 ) {
    fprintf( stderr, "bloom ref still exist in route %u\n", b->r );
    return;
  }
  if ( b->in_list ) {
    this->bloom.list.get_list( b->in_list - 1 ).pop( b );
    if ( b->bloom != NULL ) {
      ::free( b->bloom );
      b->bloom = NULL;
    }
    b->in_list = 0;
    this->g_bloom_db.bloom_mem.release( b, sizeof( BloomRoute ) );
  }
}

BloomRef *
RouteDB::create_bloom_ref( uint32_t seed,  const char *nm,
                           BloomDB &db ) noexcept
{
  void * m = this->g_bloom_db.bloom_mem.alloc( sizeof( BloomRef ) );
  return new ( m ) BloomRef( seed, nm, db );
}

BloomRef *
RouteDB::create_bloom_ref( uint32_t *pref_count,  BloomBits *bits,
                           const char *nm,  BloomDB &db ) noexcept
{
  void * m = this->g_bloom_db.bloom_mem.alloc( sizeof( BloomRef ) );
  return new ( m ) BloomRef( bits, pref_count, nm, db );
}

BloomRef *
RouteDB::update_bloom_ref( const void *data,  size_t datalen,
                           uint32_t ref_num,  const char *nm,
                           BloomDB &db ) noexcept
{
  BloomRef   * ref;
  uint32_t     pref[ MAX_RTE ];
  BloomCodec   code;
  BloomBits  * bits;
  void       * details;
  size_t       detail_size;
  void       * queue;
  size_t       queue_size;

  bits = code.decode( pref, MAX_RTE, details, detail_size, queue,
                      queue_size, data, datalen/4 );
  if ( bits == NULL )
    return NULL;

  if ( (ref = db[ ref_num ]) != NULL ) {
    ref->update_route( pref, bits, (BloomDetail *) details,
                       (uint32_t) ( detail_size / sizeof( BloomDetail ) ) );
  }
  else {
    void * m = this->g_bloom_db.bloom_mem.alloc( sizeof( BloomRef ) );
    ref = new ( m ) BloomRef( bits, pref, nm, db, ref_num );
  }
  return ref;
}

void
RouteDB::remove_bloom_ref( BloomRef *ref ) noexcept
{
  if ( ref->ref_num < this->g_bloom_db.count &&
       this->g_bloom_db[ ref->ref_num ] == ref ) {
    this->g_bloom_db[ ref->ref_num ] = NULL;
    ref->ref_num = -1;
    this->g_bloom_db.bloom_mem.release_if_alloced( ref, sizeof( BloomRef ) );
  }
}

BloomRef::BloomRef( uint32_t seed,  const char *nm,  BloomDB &db ) noexcept
        : bits( 0 ), links( 0 ), details( 0 ), pref_mask( 0 ),
          detail_mask( 0 ), nlinks( 0 ), ndetails( 0 ), ref_num( db.count ),
          bloom_db( db ), sub_detail( false )
{
  size_t len = ::strlen( nm );
  len = min_int( len, sizeof( this->name ) - 1 );
  ::memcpy( this->name, nm, len );
  this->name[ len ] = '\0';
  ::memset( this->pref_count, 0, sizeof( this->pref_count ) );
  db[ this->ref_num ] = this;
  this->update_route( NULL, BloomBits::resize( NULL, seed, 52 ), NULL, 0 );
}

BloomRef::BloomRef( BloomBits *b,  const uint32_t *pref,
                    const char *nm,  BloomDB &db,  uint32_t num ) noexcept
        : bits( 0 ), links( 0 ), details( 0 ), pref_mask( 0 ),
          detail_mask( 0 ), nlinks( 0 ), ndetails( 0 ),
          queue_cnt( 0 ), bloom_db( db ), sub_detail( false )
{
  size_t len = ::strlen( nm );
  len = min_int( len, sizeof( this->name ) - 1 );
  ::memcpy( this->name, nm, len );
  this->name[ len ] = '\0';
  ::memset( this->pref_count, 0, sizeof( this->pref_count ) );
  if ( (this->ref_num = num) == (uint32_t) -1 )
    this->ref_num = db.count;
  db[ this->ref_num ] = this;
  this->update_route( pref, b, NULL, 0 );
}

void
BloomRoute::add_bloom_ref( BloomRef *ref ) noexcept
{
  this->invalid();
  uint32_t n = this->nblooms;
  size_t osz = n * sizeof( this->bloom[ 0 ] ),
         nsz = (n+1) * sizeof( this->bloom[ 0 ] );
  this->bloom = (BloomRef **)
    this->rdb.g_bloom_db.bloom_mem.resize( this->bloom, osz, nsz );
  this->bloom[ n++ ] = ref;
  this->nblooms = n;
  ref->add_link( this );
  /*printf( "add_bloom_ref %s -> %s.%u (cnt=%u) (%lx)\n", ref->name,
          ((RoutePublish &) this->rdb).service_name, this->r, n,
          ref->pref_mask );*/

  uint16_t prefix_len;
  for ( prefix_len = 0; prefix_len < MAX_RTE; prefix_len++ ) {
    if ( ref->pref_count[ prefix_len ] != 0 ) {
      if ( this->rdb.bloom.pref_count[ prefix_len ]++ == 0 ) {
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
  this->pref_mask   = 0;
  this->detail_mask = 0;
  this->queue_cnt   = 0;
  this->sub_detail  = false;
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
  size_t osz = n * sizeof( this->bloom[ 0 ] ),
         nsz = (n-1) * sizeof( this->bloom[ 0 ] );
  this->nblooms = n - 1;
  this->bloom   = (BloomRef **)
    this->rdb.g_bloom_db.bloom_mem.resize( this->bloom, osz, nsz );
  ref->del_link( this );

  uint16_t prefix_len;
  for ( prefix_len = 0; prefix_len < MAX_RTE; prefix_len++ ) {
    if ( ref->pref_count[ prefix_len ] != 0 ) {
      if ( --this->rdb.bloom.pref_count[ prefix_len ] == 0 ) {
        this->rdb.del_prefix_len( prefix_len, false );
      }
    }
  }
  return ref;
}

void
BloomRef::add_link( BloomRoute *b ) noexcept
{
  uint32_t n = this->nlinks;
  size_t osz = n * sizeof( this->links[ 0 ] ),
         nsz = (n+1) * sizeof( this->links[ 0 ] );
  this->links = (BloomRoute **)
    this->bloom_db.bloom_mem.resize( this->links, osz, nsz );
  this->links[ n ] = b;
  this->nlinks = n + 1;
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
      size_t nsz = (n-1) * sizeof( this->links[ 0 ] ),
             osz = n * sizeof( this->links[ 0 ] );
      this->links = (BloomRoute **)
        this->bloom_db.bloom_mem.resize( this->links, osz, nsz );
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

BloomDetail &
BloomRef::add_detail( uint16_t prefix_len,  uint32_t hash ) noexcept
{
  BloomDetail * d = this->details;
  uint32_t      n = this->ndetails++;
  d = (BloomDetail *) ::realloc( d, sizeof( d[ 0 ] ) * ( n + 1 ) );
  this->details = d;

  for ( ; ; n -= 1 ) {
    if ( n == 0 || ( d[ n - 1 ].prefix_len < prefix_len ||
         ( d[ n - 1 ].prefix_len == prefix_len &&
           d[ n - 1 ].hash <= hash ) ) )
      break;
    d[ n ].copy( d[ n - 1 ] );
  }
  if ( prefix_len < SUB_RTE )
    this->detail_mask |= (uint64_t) 1 << prefix_len;
  else
    this->sub_detail = true;
  return d[ n ];
}

bool
BloomRef::add_shard_route( uint16_t prefix_len,  uint32_t hash,
                           const ShardMatch &match ) noexcept
{
  BloomDetail &a = this->add_detail( prefix_len, hash );
  a.hash       = hash;
  a.prefix_len = prefix_len;
  a.init_shard( match );
  return this->add_route( prefix_len, hash );
}

bool
BloomRef::add_suffix_route( uint16_t prefix_len,  uint32_t hash,
                            const SuffixMatch &match ) noexcept
{
  BloomDetail &a = this->add_detail( prefix_len, hash );
  a.hash       = hash;
  a.prefix_len = prefix_len;
  a.init_suffix( match );
  return this->add_route( prefix_len, hash );
}

bool
BloomRef::add_queue_route( uint16_t prefix_len,  uint32_t hash,
                           const QueueMatch &match ) noexcept
{
  BloomDetail * d = this->details;
  uint32_t      i,     /* first prefix that matches */
                j = 0, /* exact match */
                n = this->ndetails;
  BloomDetailHash v = { hash, prefix_len };
  i = lower_bound( d, v, n, cmp_prefix );
  for ( j = i; j < n; j++ ) {
    if ( d[ j ].hash != hash || d[ j ].prefix_len != prefix_len )
      break;
    if ( d[ j ].queue_equals( match ) ) {
      d[ j ].init_queue( match );
      return false;
    }
  }
  BloomDetail &a = this->add_detail( prefix_len, hash );
  a.hash       = hash;
  a.prefix_len = prefix_len;
  a.init_queue( match );
  this->queue_cnt++;
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

template <class Match>
void
BloomRef::del_detail( uint16_t prefix_len,  uint32_t hash,  const Match &match,
               bool ( BloomDetail::*equals )( const Match &m ) const ) noexcept
{
  BloomDetail * d = this->details;
  uint32_t      i,     /* first prefix that matches */
                j = 0, /* exact match */
                n = this->ndetails;
  BloomDetailHash v = { hash, prefix_len };
  bool matched = false;

  i = lower_bound( d, v, n, cmp_prefix );
  for ( j = i; j < n; j++ ) {
    if ( d[ j ].hash != hash || d[ j ].prefix_len != prefix_len )
      break;
    if ( ( d[ j ].*equals )( match ) ) {
      matched = true;
      break;
    }
  }
  if ( matched ) {
    if ( d[ j ].detail_type == QUEUE_MATCH )
      this->queue_cnt--;
    if ( j < --n )
      ::memmove( &this->details[ j ], &this->details[ j + 1 ],
                 sizeof( this->details[ 0 ] ) * ( n - j ) );
    this->ndetails = n;
    if ( ! ( ( j > 0 && d[ j - 1 ].prefix_len == prefix_len ) ||
             ( j < n && d[ j ].prefix_len == prefix_len ) ) ) {
      if ( prefix_len < SUB_RTE )
        this->detail_mask &= ~( (uint64_t) 1 << prefix_len );
      else
        this->sub_detail = false;
    }
    this->del_route( prefix_len, hash );
  }
}

void
BloomRef::del_shard_route( uint16_t prefix_len,  uint32_t hash,
                           const ShardMatch &match ) noexcept
{
  this->del_detail<ShardMatch>( prefix_len, hash, match,
                                &BloomDetail::shard_equals );
}

void
BloomRef::del_suffix_route( uint16_t prefix_len,  uint32_t hash,
                            const SuffixMatch &match ) noexcept
{
  this->del_detail<SuffixMatch>( prefix_len, hash, match,
                                 &BloomDetail::suffix_equals );
}

void
BloomRef::del_queue_route( uint16_t prefix_len,  uint32_t hash,
                           const QueueMatch &match ) noexcept
{
  this->del_detail<QueueMatch>( prefix_len, hash, match,
                                &BloomDetail::queue_equals );
}

void
BloomRef::ref_pref_count( uint16_t prefix_len ) noexcept
{
  if ( prefix_len != SUB_RTE )
    this->pref_mask |= (uint64_t) 1 << prefix_len;
  for ( uint32_t i = 0; i < this->nlinks; i++ ) {
    if ( this->links[ i ]->rdb.bloom.pref_count[ prefix_len ]++ == 0 ) {
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
    if ( --this->links[ i ]->rdb.bloom.pref_count[ prefix_len ] == 0 ) {
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
  bool had_subs = false;
  if ( this->bits != NULL ) {
    had_subs = ( this->bits->count != 0 );
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
  this->sub_detail  = false;
  this->queue_cnt   = 0;
  for ( uint32_t i = 0; i < ndetails; i++ ) {
    if ( details[ i ].prefix_len < SUB_RTE )
      this->detail_mask |= (uint64_t) 1 << details[ i ].prefix_len;
    else
      this->sub_detail = true;
    if ( details[ i ].detail_type == QUEUE_MATCH )
      this->queue_cnt++;
  }
  if ( had_subs || this->bits->count != 0 )
    this->invalid();
  /*printf( "update fd %d ndetails %u mask %lx\n",
          this->nlinks > 0 ? this->links[ 0 ]->r : -1,
          ndetails, this->detail_mask );*/
}

void
BloomRef::encode( BloomCodec &code ) noexcept
{
  const char  * q_str = NULL;
  size_t        q_len = 0;
  BloomDetail * d = this->details;
  CatMalloc     cat;

  if ( this->queue_cnt > 0 ) {
    uint32_t      i;
    size_t        len = 0;
    BitSpace      q_bits;
    QueueNameDB & q_db = this->bloom_db.q_db;
    QueueName   * qn;
    for ( i = 0; i < this->ndetails; i++ ) {
      if ( d[ i ].detail_type == QUEUE_MATCH ) {
        qn = q_db.get_queue_hash( d[ i ].u.queue.qhash );
        if ( qn != NULL && ! q_bits.test_set( qn->idx ) )
          len += qn->queue_len + 1;
      }
    }
    if ( len == q_db.queue_strlen ) {
      q_str = q_db.queue_str;
      q_len = q_db.queue_strlen;
    }
    else {
      cat.resize( len );
      for ( bool b = q_bits.first( i ); b; b = q_bits.next( i ) ) {
        qn = q_db.get_queue_idx( i );
        cat.x( qn->queue, qn->queue_len ).c( 0 );
      }
      q_str = cat.start;
      q_len = cat.len();
    }
  }
  code.encode( this->pref_count, MAX_RTE, d, this->ndetails * sizeof( d[ 0 ] ),
               q_str, q_len, *this->bits );
}

bool
BloomRef::decode( const void *data,  size_t datalen,
                  QueueNameArray &q_ar ) noexcept
{
  uint32_t    pref[ MAX_RTE ];
  BloomCodec  code;
  BloomBits * bits;
  void      * details     = NULL;
  size_t      detail_size = 0;
  void      * queue       = NULL;
  size_t      queue_size  = 0;

  bits = code.decode( pref, MAX_RTE, details, detail_size,
                      queue, queue_size, data, datalen/4 );
  if ( bits == NULL )
    return false;
  if ( queue_size > 0 ) {
    const char  * name = (const char *) queue,
                * end = &name[ queue_size ];
    QueueNameDB & q_db = this->bloom_db.q_db;
    for (;;) {
      size_t len = 0;
      while ( &name[ len ] < end && name[ len ] != '\0' )
        len++;
      if ( len == 0 )
        break;
      q_ar.push( q_db.get_queue_str( name, len ) );
      name = &name[ len + 1 ];
      if ( name >= end )
        break;
    }
  }
  this->update_route( pref, bits, (BloomDetail *) details,
                      (uint32_t) ( detail_size / sizeof( BloomDetail ) ) );
  return true;
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
RouteGroup::cache_save( uint16_t prefix_len,  uint32_t hash,
                        uint32_t *routes,  uint32_t rcnt,
                        uint32_t shard ) noexcept
{
  if ( this->cache.is_invalid ) {
do_reset:;
    if ( ! this->cache.reset() )
      return;
  }
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

  uint64_t h = ( (uint64_t) this->group_num << 48 ) |
               ( (uint64_t) shard << 40 ) |
               ( (uint64_t) prefix_len << 32 ) | (uint64_t) hash;
  this->cache.ht->upsert( h, val ); /* save rcnt, off at hash */
  this->cache.count++;

  if ( this->cache.ht->elem_count >= this->cache.ht->max_count ) {
    if ( this->cache.ht->elem_count > RouteCache::MAX_CACHE )
      goto do_reset;
    if ( this->cache.ht->elem_count >= this->cache.max_cnt )
      this->cache.max_cnt = this->cache.ht->elem_count;
    if ( this->cache.end >= this->cache.max_size )
      this->cache.max_size = this->cache.end;
    RteCacheTab::check_resize( this->cache.ht );
  }
}

void
RouteGroup::cache_need( void ) noexcept
{
  this->cache.spc.make( this->cache.end + this->cache.need );
  this->cache.need = 0;
}

void
RouteGroup::cache_purge( size_t pos ) noexcept
{
  uint64_t    h;
  RteCacheVal val;
  this->cache.ht->get( pos, h, val );
  this->cache.free += val.rcnt;
  this->cache.count--;
  this->cache.ht->remove( pos );
}

void
RouteGroup::add_prefix_len( uint16_t prefix_len,  bool is_rt_hash ) noexcept
{
  if ( prefix_len < SUB_RTE ) {
    uint64_t mask = (uint64_t) 1 << prefix_len;
    if ( is_rt_hash )
      this->rt_mask |= mask;
    else
      this->bloom.mask |= mask;
  }
}

void
RouteGroup::del_prefix_len( uint16_t prefix_len,  bool is_rt_hash ) noexcept
{
  if ( prefix_len < SUB_RTE ) {
    uint64_t mask = (uint64_t) 1 << prefix_len;
    if ( is_rt_hash )
      this->rt_mask &= ~mask;
    else
      this->bloom.mask &= ~mask;
  }
}

uint32_t
RouteGroup::add_pattern_route_str( const char *str,  uint16_t len,
                                   uint32_t r ) noexcept
{
  uint32_t seed = this->prefix_seed( len );
  return this->add_pattern_route( kv_crc_c( str, len, seed ), r, len );
}

uint32_t
RouteGroup::del_pattern_route_str( const char *str,  uint16_t len,
                                   uint32_t r ) noexcept
{
  uint32_t seed = this->prefix_seed( len );
  return this->del_pattern_route( kv_crc_c( str, len, seed ), r, len );
}

uint32_t
RouteGroup::add_sub_route_str( const char *str,  uint16_t len,
                               uint32_t r ) noexcept
{
  return this->add_sub_route( kv_crc_c( str, len, 0 ), r );
}

uint32_t
RouteGroup::del_sub_route_str( const char *str,  uint16_t len,
                               uint32_t r ) noexcept
{
  return this->del_sub_route( kv_crc_c( str, len, 0 ), r );
}
/* test if route exists for hash */
bool
RouteGroup::is_member( uint16_t prefix_len,  uint32_t hash,
                       uint32_t r ) noexcept
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
