#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/route_db.h>

using namespace rai;
using namespace kv;

RouteDB::RouteDB() noexcept
{
  for ( size_t i = 0; i < sizeof( this->rt_hash ) /
                          sizeof( this->rt_hash[ 0 ] ); i++ )
    this->rt_hash[ i ] = UIntHashTab::resize( NULL );
  this->pat_mask = 0;

  for ( uint8_t j = 0; j < 64; j++ ) {
    SysWildSub w( NULL, j );
    this->pre_seed[ j ] = kv_crc_c( w.sub, w.len, 0 );
  }
}

static uint32_t *
make_space( uint32_t i,  uint32_t &size,  uint32_t *&ptr,
            uint32_t *static_spc )
{
  if ( i > size ) {
    uint32_t * old_ptr = ptr;
    size_t     old_siz = size;
    if ( ptr == static_spc )
      ptr = NULL;
    i    = kv::align<uint32_t>( i, 256 );
    ptr  = (uint32_t *) ::realloc( ptr, i * sizeof( uint32_t ) );
    size = i;
    /* each space container has preallocated static space */
    if ( old_ptr == static_spc )
      ::memcpy( ptr, static_spc, old_siz * sizeof( uint32_t ) );
  }
  return ptr;
}

RouteZip::RouteZip() noexcept
{
  this->init();
}

void
RouteZip::init( void ) noexcept
{
  this->zht            = UIntHashTab::resize( NULL );
  this->code_buf       = this->code_buf_spc; /* temp buffers */
  this->code_spc_ptr   = this->code_spc;
  this->route_spc_ptr  = this->route_spc;
  this->code_end       = 0;
  this->code_size      = INI_SPC * 4;
  this->code_free      = 0;
  this->code_spc_size  = INI_SPC;
  this->route_spc_size = INI_SPC;
}

void
RouteZip::reset( void ) noexcept
{
  delete this->zht;
  if ( this->code_buf != this->code_buf_spc )
    ::free( this->code_buf );
  if ( this->code_spc_ptr != this->code_spc )
    ::free( this->code_spc_ptr );
  if ( this->route_spc_ptr != this->route_spc )
    ::free( this->route_spc_ptr );
  for ( size_t i = 0; i < sizeof( this->push_route_spc ) /
                          sizeof( this->push_route_spc[ 0 ] ); i++ )
    this->push_route_spc[ i ].reset();
  this->init();
}

uint32_t *
RouteZip::make_route_space( uint32_t i ) noexcept
{
  return make_space( i, this->route_spc_size, this->route_spc_ptr,
                     this->route_spc );
}

uint32_t *
RouteZip::make_push_route_space( uint8_t n,  uint32_t i ) noexcept
{
  return make_space( i, this->push_route_spc[ n ].size,
                     this->push_route_spc[ n ].ptr,
                     this->push_route_spc[ n ].spc );
}

uint32_t *
RouteZip::make_code_space( uint32_t i ) noexcept
{
  return make_space( i, this->code_spc_size, this->code_spc_ptr,
                     this->code_spc );
}

uint32_t *
RouteZip::make_code_ref_space( uint32_t i,  uint32_t &off ) noexcept
{
  make_space( this->code_end + i, this->code_size, this->code_buf,
              this->code_buf_spc );
  off = this->code_end;
  this->code_end += i;
  return &this->code_buf[ off ];
}

void
RouteZip::gc_code_ref_space( void ) noexcept
{
  size_t    pos;
  uint32_t  i, j = 0, k, e;
  CodeRef * p;
  /* remove from zht */
  for ( i = 0; i < this->code_end; i += e ) {
    p = (CodeRef *) (void *) &this->code_buf[ i ];
    e = p->word_size();
    if ( this->zht->find( p->hash, pos, k ) ) {
      if ( p->ref == 0 )
        this->zht->remove( pos );
      else {
        if ( j != i ) {
          this->zht->set( p->hash, pos, j );
          ::memmove( &this->code_buf[ j ], p, e * sizeof( uint32_t ) );
        }
        j += e;
      }
    }
  }
  if ( this->zht->need_resize() )
    this->zht = UIntHashTab::resize( this->zht );
  this->code_end  = j;
  this->code_free = 0;
}

uint32_t
RouteZip::insert_route( uint32_t r,  uint32_t *routes,  uint32_t rcnt )
{
  uint32_t i = 0;
  while ( i < rcnt && r > routes[ i ] )
    i++;
  if ( i < rcnt && r == routes[ i ] )
    return rcnt;
  for ( uint32_t j = rcnt++; j > i; j-- )
    routes[ j ] = routes[ j - 1 ];
  routes[ i ] = r;
  return rcnt;
}

uint32_t
RouteZip::delete_route( uint32_t r,  uint32_t *routes,  uint32_t rcnt )
{
  uint32_t i = 0;
  while ( i < rcnt && r > routes[ i ] )
    i++;
  if ( i == rcnt || r != routes[ i ] )
    return rcnt;
  for ( ; i < rcnt - 1; i++ )
    routes[ i ] = routes[ i + 1 ];
  return rcnt - 1;
}

uint32_t
RouteZip::compress_routes( uint32_t *routes,  uint32_t rcnt ) noexcept
{
  CodeRef  * p;
  uint32_t * code, ecnt;
  size_t     pos;
  uint32_t   h, val, seed;

  if ( rcnt < 5 ) { /* most likely single value code, skips bsearch */
    h = DeltaCoder::encode( rcnt, routes, 0 );
    if ( h != 0 )
      return h;
  }
  code = this->make_code_space( rcnt );
  ecnt = DeltaCoder::encode_stream( rcnt, routes, 0, code );
  if ( ecnt == 1 ) /* single value code */
    return code[ 0 ];

  seed = 0; /* multi value code, use zht */
find_next_hash:;
  h = CodeRef::hash_code( code, ecnt, seed );
  if ( this->zht->find( h, pos, val ) ) {
    p = (CodeRef *) (void *) &this->code_buf[ val ];
    if ( ! p->equals( code, ecnt ) ) { /* hash needs to be unique */
      seed++;
      goto find_next_hash;
    }
    if ( p->ref++ == 0 ) /* previously free, now used */
      this->code_free -= p->word_size();
  }
  else { /* new code ref */
    uint32_t * spc =
      this->make_code_ref_space( CodeRef::alloc_words( ecnt ), val );
    new ( spc ) CodeRef( code, ecnt, rcnt, h /* , seed */ );
    this->zht->set( h, pos, val ); /* val is offset of the code ref */
    if ( this->zht->need_resize() )
      this->zht = UIntHashTab::resize( this->zht );
  }
  return h; /* unique route code */
}
/* fetch the routes and decompress */
uint32_t
RouteZip::decompress_routes( uint32_t r,  uint32_t *&routes,
                             CodeRef *&p ) noexcept
{
  size_t   pos;
  uint32_t rcnt, val;

  if ( DeltaCoder::is_not_encoded( r ) ) { /* if is a multi value code */
    if ( this->zht->find( r, pos, val ) ) {
      p      = (CodeRef *) (void *) &this->code_buf[ val ];
      routes = this->make_route_space( p->rcnt + 1 ); /* in case insert */
      rcnt   = DeltaCoder::decode_stream( p->ecnt, &p->code, 0, routes );
    }
    else { /* no route exists */
      routes = NULL;
      rcnt   = 0;
      p      = NULL;
    }
  }
  else { /* single value code */
    routes = this->make_route_space( MAX_DELTA_CODE_LENGTH + 1 );
    rcnt   = DeltaCoder::decode( r, routes, 0 );
    p      = NULL;
  }
  return rcnt;
}
/* push for multiple decodings for multiple wildcard matches */
uint32_t
RouteZip::push_decompress_routes( uint8_t n,  uint32_t r,
                                  uint32_t *&routes ) noexcept
{
  CodeRef * p;
  size_t    pos;
  uint32_t  rcnt, val;

  if ( DeltaCoder::is_not_encoded( r ) ) { /* if is a multi value code */
    if ( this->zht->find( r, pos, val ) ) {
      p      = (CodeRef *) (void *) &this->code_buf[ val ];
      routes = this->make_push_route_space( n, p->rcnt );
      rcnt   = DeltaCoder::decode_stream( p->ecnt, &p->code, 0, routes );
    }
    else { /* no route exists */
      routes = NULL;
      rcnt   = 0;
    }
  }
  else { /* single value code */
    routes = this->make_push_route_space( n, MAX_DELTA_CODE_LENGTH );
    rcnt   = DeltaCoder::decode( r, routes, 0 );
  }
  return rcnt;
}
/* just find the first route of many, used to find a subject string from
 * the source of the subscripion */
uint32_t
RouteZip::decompress_one( uint32_t r ) noexcept
{
  if ( DeltaCoder::is_not_encoded( r ) ) { /* if is a multi value code */
    CodeRef * p;
    size_t    pos;
    uint32_t  val;
    if ( this->zht->find( r, pos, val ) ) {
      p = (CodeRef *) (void *) &this->code_buf[ val ];
      r = p->code;
    }
    else { /* no route exists */
      return 0;
    }
  }
  return DeltaCoder::decode_one( r, 0 );
}
/* how many routes exists for a hash */
uint32_t
RouteDB::get_route_count( uint16_t prefix_len,  uint32_t hash ) noexcept
{
  UIntHashTab * xht = this->rt_hash[ prefix_len ];
  size_t        pos;
  uint32_t      val, off;
  if ( ! xht->find( hash, pos, val ) )
    return 0;
  if ( DeltaCoder::is_not_encoded( val ) ) {
    if ( this->zht->find( val, pos, off ) ) {
      CodeRef *p = (CodeRef *) (void *) &this->code_buf[ off ];
      return p->rcnt;
    }
  }
  return DeltaCoder::decode_length( val );
}
/* insert route for a subject hash */
uint32_t
RouteDB::add_route( uint16_t prefix_len,  uint32_t hash,  uint32_t r ) noexcept
{
  UIntHashTab * xht = this->rt_hash[ prefix_len ];
  size_t        pos;
  uint32_t      val, rcnt = 0, xcnt = 0;
  uint32_t    * routes, tmp_route;
  CodeRef     * p = NULL;
  /* find and decompress routes */
  if ( xht->find( hash, pos, val ) ) {
    rcnt = this->decompress_routes( val, routes, p );
    if ( rcnt > 0 )
      xcnt = insert_route( r, routes, rcnt );
  }
  /* if no routes exist */
  if ( rcnt == 0 ) { /* new route */
    xcnt      = 1;
    tmp_route = r;
    routes    = &tmp_route;
    /* track which pattern lengths are used */
    if ( prefix_len < SUB_RTE )
      this->pat_mask |= (uint64_t) 1 << prefix_len;
  }
  /* compress in insert routes */
  if ( xcnt != rcnt ) {
    val = this->compress_routes( routes, xcnt );
    xht->set( hash, pos, val );
    if ( xht->need_resize() )
      this->rt_hash[ prefix_len ] = UIntHashTab::resize( xht );
    /* free the old code, it is no longer referenced */
    this->deref_codep( p );
  }
  return xcnt; /* count of routes after inserting r */
}
/* delete a route for a subject hash */
uint32_t
RouteDB::del_route( uint16_t prefix_len,  uint32_t hash,  uint32_t r ) noexcept
{
  UIntHashTab * xht = this->rt_hash[ prefix_len ];
  size_t        pos;
  uint32_t      val, rcnt = 0, xcnt = 0;
  uint32_t    * routes;
  CodeRef     * p = NULL;
  /* find and dcompress routes */
  if ( xht->find( hash, pos, val ) ) {
    rcnt = this->decompress_routes( val, routes, p );
    xcnt = delete_route( r, routes, rcnt );
    /* if the routes changed */
    if ( rcnt != xcnt ) {
      /* if there are any routes left */
      if ( xcnt > 0 ) {
        val = this->compress_routes( routes, xcnt );
        xht->set( hash, pos, val );
      }
      else {
        xht->remove( pos );
        if ( prefix_len < SUB_RTE ) {
          /* track which pattern lengths are used */
          if ( xht->is_empty() )
            this->pat_mask &= ~( (uint64_t) 1 << prefix_len );
        }
        if ( xht->need_resize() )
          this->rt_hash[ prefix_len ] = UIntHashTab::resize( xht );
      }
      this->deref_codep( p );
    }
  }
  return xcnt; /* count of routes after deleting r */
}
#if 0
/* for patterns, track how many pattern routes exist for a prefix length */
uint32_t
RouteDB::add_pattern_route( uint32_t hash,  uint32_t r,
                            uint16_t pre_len ) noexcept
{
  uint32_t xcnt = this->add_route( hash, r );
  if ( pre_len > 63 )
    pre_len = 63;
  if ( this->pre_count[ pre_len ]++ == 0 )
    this->pat_mask |= ( (uint64_t) 1 << pre_len );
  return xcnt;
}
/* remove a pattern */
uint32_t
RouteDB::del_pattern_route( uint32_t hash,  uint32_t r,
                            uint16_t pre_len ) noexcept
{
  uint32_t xcnt = this->del_route( hash, r );
  if ( pre_len > 63 )
    pre_len = 63;
  if ( --this->pre_count[ pre_len ] == 0 )
    this->pat_mask &= ~( (uint64_t) 1 << pre_len );
  return xcnt;
}
#endif
/* test if route exists for hash */
bool
RouteDB::is_member( uint16_t prefix_len,  uint32_t hash,
                    uint32_t x ) noexcept
{
  UIntHashTab * xht = this->rt_hash[ prefix_len ];
  size_t        pos;
  uint32_t      r;

  if ( ! xht->find( hash, pos, r ) )
    return false;

  if ( DeltaCoder::is_not_encoded( r ) ) { /* if is a multi value code */
    CodeRef * p;
    uint32_t  val;
    if ( this->zht->find( r, pos, val ) ) {
      p = (CodeRef *) (void *) &this->code_buf[ val ];
      return DeltaCoder::test_stream( p->ecnt, &p->code, 0, x );
    }
    return false;
  }
  /* single value code */
  uint32_t base = 0;
  return DeltaCoder::test( r, x, base );
}
