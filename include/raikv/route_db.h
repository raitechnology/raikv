#ifndef __rai_raikv__route_db_h__
#define __rai_raikv__route_db_h__

#include <raikv/uint_ht.h>
#include <raikv/delta_coder.h>
#include <raikv/key_hash.h>
#include <raikv/util.h>
#include <raikv/array_space.h>
#include <raikv/bit_set.h>
#include <raikv/bloom.h>
#include <raikv/prio_queue.h>
#include <raikv/dlinklist.h>
#include <raikv/route_ht.h>
#include <raikv/kv_msg.h>

struct sockaddr;

namespace rai {
namespace kv {

#define SYS_WILD_PREFIX "_SYS.W"

struct SysWildSub { /* wildcard prefix: _SYS.W3.XYZ */
  size_t len;
  char   sub[ sizeof( SYS_WILD_PREFIX ) - 1 + 4 + 64 ];

  SysWildSub( const char *s,  uint8_t prefix_len ) {
    size_t  i = sizeof( SYS_WILD_PREFIX ) - 1;
    uint8_t j = prefix_len;
    ::memcpy( this->sub, SYS_WILD_PREFIX, i );
    if ( prefix_len > 9 ) { /* max 63 */
      this->sub[ i++ ] = ( j / 10 ) + '0';
      j %= 10;
    }
    this->sub[ i++ ] = j + '0';
    this->sub[ i++ ] = '.'; 
    if ( s != NULL ) {
      ::memcpy( &this->sub[ i ], s, prefix_len );
      i += prefix_len;
    }
    this->sub[ i ] = '\0';
    this->len = i;
  }
};

typedef ArraySpace<uint32_t, 128> RouteSpace;

struct CodeRef { /* refs to a route space, which is a list of fds */
  uint32_t hash, /* hash of the route */
           ref,  /* how many refs to this route (multiple subs) */
           ecnt, /* encoded number of ints needed for route (zipped) */
           rcnt, /* count of elems in a route (not encoded) */
           code; /* the routes code */

  CodeRef( uint32_t *co,  uint32_t ec,  uint32_t rc,  uint32_t h ) :
    hash( h ), ref( 1 ), ecnt( ec ), rcnt( rc ) {
    ::memcpy( &this->code, co, sizeof( co[ 0 ] ) * ec );
  }
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  bool equals( const uint32_t *c,  uint32_t ec ) const {
    const uint32_t *c2 = &this->code;
    if ( ec != this->ecnt ) return false;
    for (;;) {
      if ( *c++ != *c2++ )
        return false;
      if ( --ec == 0 )
        return true;
    }
  }
  static uint32_t hash_code( const uint32_t *code,  uint32_t ecnt,
                             uint32_t seed ) {
    return kv_crc_c( code, ecnt * sizeof( code[ 0 ] ), seed ) & 0x7fffffffU;
  }
  static size_t alloc_words( uint32_t ecnt ) {
    return sizeof( CodeRef ) / sizeof( uint32_t ) + ( ecnt - 1 );
  }
  size_t word_size( void ) const {
    return alloc_words( this->ecnt );
  }
};

struct CodeRefPtr {
  RouteSpace * code_buf;
  uint32_t     off;

  CodeRefPtr() : code_buf( 0 ), off( 0 ) {}

  CodeRef *get( void ) const {
    if ( this->code_buf != NULL )
      return (CodeRef *) (void *) &this->code_buf->ptr[ this->off ];
    return NULL;
  }
};

struct BloomRoute;
struct BloomRef;
typedef kv::DLinkList<BloomRoute> BloomList;

struct RouteDB;
struct RouteRef {
  RouteDB    & rdb;       /* zipped routes */
  RouteSpace & route_spc; /* space to unzip and modify route arrays */
  CodeRefPtr   ptr;       /* if route is updated, no longer need old route */
  uint32_t   * routes,    /* the set of routes allocated from route_spc */
               rcnt;      /* num elems in routes[] */

  RouteRef( RouteDB &db,  RouteSpace &spc )
    : rdb( db ), route_spc( spc ), routes( 0 ), rcnt( 0 ) {}

  /* val is the route id, decompress with add space for inserting */
  uint32_t decompress( uint32_t val,  uint32_t add,  uint32_t *cached = NULL,
                       uint32_t cache_rcnt = 0,  bool use_cached = false ) {
    if ( use_cached && cached != NULL ) { /* if already cached, use that instead */
      this->routes = this->route_spc.make( cache_rcnt + add );
      this->rcnt   = cache_rcnt;
      ::memcpy( this->routes, cached, sizeof( cached[ 0 ] ) * cache_rcnt );
      if ( DeltaCoder::is_not_encoded( val ) )
        this->val_to_coderef( val );
    }
    else { /* decompress val */
      if ( DeltaCoder::is_not_encoded( val ) ) /* if is a multi value code */
        return this->decode( val, add );
      /* single value code, could contain up to 15 routes */
      this->routes = this->route_spc.make( MAX_DELTA_CODE_LENGTH + add );
      this->rcnt   = DeltaCoder::decode( val, this->routes, 0 );
    }
    return this->rcnt;
  }
  /* convert val to coderef, if need to deref it */
  void val_to_coderef( uint32_t val ) noexcept;
  uint32_t decode( uint32_t val,  uint32_t add ) noexcept; /* stream of codes */
  uint32_t insert( uint32_t r ) noexcept; /* insert r into routes[] */
  uint32_t remove( uint32_t r ) noexcept; /* remove r from routes[] */
  uint32_t compress( void ) noexcept;     /* compress routes and merge zipped */
  void deref( void ) noexcept;            /* deref code ref, could gc */
};

/* find r in routes[] */
uint32_t bsearch_route( uint32_t r, const uint32_t *routes, uint32_t size ) noexcept;
/* insert r in routes[] if not already there */
uint32_t insert_route( uint32_t r, uint32_t *routes, uint32_t rcnt ) noexcept;
/* delete r from routes[] if found */
uint32_t delete_route( uint32_t r, uint32_t *routes, uint32_t rcnt ) noexcept;
/* merge arrays */
uint32_t merge_route( uint32_t *routes,  uint32_t count,
                      const uint32_t *merge,  uint32_t mcount ) noexcept;

/* ptr to route[] array */
struct RteCacheVal {
  uint32_t rcnt,
           off;
};
/* table of [prefix|hash] -> [rcnt|off], for caching decompressed routes */
typedef IntHashTabT<uint64_t, RteCacheVal> RteCacheTab;

struct RouteCache {
  static const uint32_t MAX_CACHE = 256 * 1024; /* max routes in cache */
  RouteSpace    spc;        /* cache space */
  RteCacheTab * ht;         /* hash into spc */
  uint32_t      end,        /* end array spot */
                free,       /* free count for gc */
                count,      /* count of elems <= MAX_CACHE */
                busy,       /* busy, no realloc because of references */
                need;       /* need this amount of space */
  bool          is_invalid; /* full or updated, no more entries allowed */
  RouteCache() noexcept;
  bool reset( void ) noexcept;
};

static const uint16_t MAX_PRE = 64, /* wildcard prefix routes */
                      SUB_RTE = 64, /* non-wildcard routes */
                      MAX_RTE = 65; /* subject + wildcard */
/* Use DeltaCoder to compress routes, manage hash -> route list */
struct RouteZip {
  UIntHashTab  * zht;             /* code ref hash -> code_buf.ptr[] offset */
  size_t         code_end,        /* end of code_buf.ptr[] list */
                 code_free;       /* amount free between 0 -> code_end */
  RouteSpace     code_buf,        /* space for code ref db */
                 zroute_spc,      /* space for compressing */
                 route_spc[ MAX_RTE ], /* for each wildcard */
                 bloom_spc[ MAX_RTE ];
  RouteZip() noexcept;
  void init( void ) noexcept;
  void reset( void ) noexcept;

  uint32_t *make_code_ref_space( uint32_t ecnt,  uint32_t &off ) noexcept;
  uint32_t make_code_ref( uint32_t *code,  uint32_t ecnt,
                          uint32_t rcnt ) noexcept;
  void gc_code_ref_space( void ) noexcept;
  uint32_t decompress_one( uint32_t val ) noexcept;
};

/* Map a subscription hash to a route list using RouteZip */
struct RouteDB {
  RouteCache    cache;               /* the route arrays indexed by ht */
  BloomList     bloom_list;
  uint32_t      bloom_pref_count[ MAX_RTE ];
  uint32_t      entry_count;         /* counter of route/hashes indexed */
  RouteZip      zip;
  UIntHashTab * rt_hash[ MAX_RTE ];  /* route hash -> code | ref hash */
  uint32_t      pre_seed[ MAX_PRE ]; /* hash seeds for each prefix */
  uint64_t      pat_mask,            /* mask of subject prefixes, up to 64 */
                rt_mask,
                bloom_mask;
  uint8_t       prefix_len[ MAX_PRE ]; /* current set of prefix lengths */
  uint8_t       pat_bit_count;      /* how many bits in pat_mask are set */

  RouteDB() noexcept;

  /* modify the bits of the prefixes used */
  void add_prefix_len( uint16_t prefix_len,  bool is_rt_hash ) noexcept;
  void del_prefix_len( uint16_t prefix_len,  bool is_rt_hash ) noexcept;
  void update_prefix_len( void ) noexcept;

  /* scatter prefix lengths to keylen[] and prefix seeds to hash[] */
  uint8_t setup_prefix_hash( const char *s,  size_t s_len,
                             const char *key[ MAX_PRE ],
                             size_t keylen[ MAX_PRE ],
                             uint32_t hash[ MAX_PRE ] ) {
    for ( uint8_t i = 0; i < this->pat_bit_count; i++ ) {
      uint8_t len = this->prefix_len[ i ];
      if ( len > s_len )
        return i;
      key[ i ]    = s;
      keylen[ i ] = len;
      hash[ i ]   = this->pre_seed[ len ];
    }
    return this->pat_bit_count;
  }
  uint32_t prefix_seed( size_t prefix_len ) const {
    if ( prefix_len < MAX_PRE )
      return this->pre_seed[ prefix_len ];
    return this->pre_seed[ MAX_PRE - 1 ];
  }
  uint32_t add_route( uint16_t prefix_len,  uint32_t hash,
                      uint32_t r ) noexcept;
  uint32_t del_route( uint16_t prefix_len,  uint32_t hash,
                      uint32_t r ) noexcept;
  bool is_member( uint16_t prefix_len,  uint32_t hash,  uint32_t r ) noexcept;

  bool is_sub_member( uint32_t hash,  uint32_t r ) {
    return this->is_member( SUB_RTE, hash, r );
  }
  uint32_t add_pattern_route_str( const char *str,  uint16_t len,
                                  uint32_t r ) noexcept;
  uint32_t add_pattern_route( uint32_t hash, uint32_t r, uint16_t prefix_len ) {
    return this->add_route( prefix_len, hash, r );
  }
  uint32_t del_pattern_route_str( const char *str,  uint16_t len,
                                  uint32_t r ) noexcept;
  uint32_t del_pattern_route( uint32_t hash, uint32_t r, uint16_t prefix_len ) {
    return this->del_route( prefix_len, hash, r );
  }
  uint32_t add_sub_route_str( const char *str,  uint16_t len,
                              uint32_t r ) noexcept;
  uint32_t add_sub_route( uint32_t hash, uint32_t r ) {
    return this->add_route( SUB_RTE, hash, r );
  }
  uint32_t del_sub_route_str( const char *str,  uint16_t len,
                              uint32_t r ) noexcept;
  uint32_t del_sub_route( uint32_t hash, uint32_t r ) {
    return this->del_route( SUB_RTE, hash, r );
  }
  /* get_[sub_]route() functions are usually the hot path */
  uint32_t get_route( const char *sub,  uint16_t sublen,
                      uint16_t prefix_len,  uint32_t hash,
                      uint32_t *&routes,  uint32_t &ref,
                      uint32_t subj_hash ) {
    uint32_t rcnt, val;
    size_t   pos;
    if ( this->cache_find( prefix_len, hash, routes, rcnt ) ) {
      this->cache.busy++;
      ref++;
      return rcnt;
    }
    uint64_t mask = (uint64_t) 1 << prefix_len;
    if ( ( this->rt_mask & mask ) != 0 ) {
      if ( this->rt_hash[ prefix_len ]->find( hash, pos, val ) )
        this->get_route_slow2( sub, sublen, prefix_len, mask, hash, val, routes,
                               subj_hash );
    }
    return this->get_bloom_route2( sub, sublen, prefix_len, mask, hash, routes,
                                   0, subj_hash );
  }
  uint32_t get_sub_route( uint32_t hash,  uint32_t *&routes,  uint32_t &ref ) {
    uint32_t rcnt, val;
    size_t   pos;
    if ( this->cache_find( SUB_RTE, hash, routes, rcnt ) ) {
      this->cache.busy++;
      ref++;
      return rcnt;
    }
    if ( this->rt_hash[ SUB_RTE ]->find( hash, pos, val ) )
      return this->get_route_slow( hash, val, routes );
    return this->get_bloom_route( hash, routes, 0 );
  }
  uint32_t get_route_slow( uint32_t hash, uint32_t val,
                           uint32_t *&routes ) noexcept;
  uint32_t get_bloom_route( uint32_t hash,  uint32_t *&routes,
                            uint32_t merge_cnt ) noexcept;
  uint32_t get_route_slow2( const char *sub,  uint16_t sublen,
                            uint16_t prefix_len,  uint64_t mask,  uint32_t hash,
                            uint32_t val,  uint32_t *&routes,
                            uint32_t subj_hash ) noexcept;
  uint32_t get_bloom_route2( const char *sub,  uint16_t sublen,
                             uint16_t prefix_len,  uint64_t mask, uint32_t hash,
                             uint32_t *&routes,  uint32_t merge_cnt,
                             uint32_t subj_hash ) noexcept;
  uint32_t get_route_count( uint16_t prefix_len,  uint32_t hash ) noexcept;

  uint32_t get_sub_route_count( uint32_t hash ) {
    return this->get_route_count( SUB_RTE, hash );
  }
  BloomRoute * create_bloom_route( uint32_t r,  uint32_t *pref_count,
                                   BloomBits *bits ) noexcept;
  BloomRoute * create_bloom_route( uint32_t r,  uint32_t seed ) noexcept;
  BloomRoute * create_bloom_route( uint32_t r,  BloomRef *ref ) noexcept;
  static BloomRef * create_bloom_ref( uint32_t *pref_count,
                                      BloomBits *bits ) noexcept;
  static BloomRef * create_bloom_ref( uint32_t seed ) noexcept;
  void update_bloom_route( uint32_t *pref_count,  BloomBits *bits,
                           BloomRoute *b ) noexcept;
  void release_bloom_route( BloomRoute *b ) noexcept;
  uint32_t get_bloom_count( uint16_t prefix_len,  uint32_t hash ) noexcept;

  bool cache_find( uint16_t prefix_len,  uint32_t hash,
                   uint32_t *&routes,  uint32_t &rcnt ) {
    if ( this->cache.is_invalid )
      return false;
    if ( ! this->cache.busy && this->cache.need )
      this->cache_need();
    uint64_t    h = ( (uint64_t) prefix_len << 32 ) | (uint64_t) hash;
    size_t      pos;
    RteCacheVal val;

    if ( ! this->cache.ht->find( h, pos, val ) )
      return false;
    rcnt   = val.rcnt;
    routes = &this->cache.spc.ptr[ val.off ];
    return true;
  }
  void cache_save( uint16_t prefix_len,  uint32_t hash,
                   uint32_t *routes,  uint32_t rcnt ) noexcept;
  void cache_purge( uint16_t prefix_len,  uint32_t hash ) noexcept;
  void cache_need( void ) noexcept;
};

struct BloomRoute;

struct SuffixMatch {
  uint32_t hash;  /* hash of suffix */
  uint16_t len;   /* len of suffix, ex. 7: SUB.*.SUFFIX */
};
struct ShardMatch {
  uint32_t start, /* start hash */
           end;   /* end hash, inclusive, start <= hash <= end */
};
enum DetailType {
  NO_DETAIL    = 0,
  SUFFIX_MATCH = 1,
  SHARD_MATCH  = 2
};
struct PatternCvt;
struct BloomDetail {
  uint32_t hash;
  uint16_t prefix_len,
           detail_type;
  union {
    SuffixMatch suffix; /* match wildcard suffix */
    ShardMatch  shard;  /* match shard hash */
  } u;
  void init_suffix( const SuffixMatch &match ) {
    this->detail_type = SUFFIX_MATCH;
    this->u.suffix    = match;
  }
  void init_shard( const ShardMatch &match ) {
    this->detail_type = SHARD_MATCH;
    this->u.shard     = match;
  }
  void init_none( void ) {
    this->detail_type = NO_DETAIL;
  }
  bool suffix_equals( const SuffixMatch &match ) const {
    return this->detail_type   == SUFFIX_MATCH &&
           this->u.suffix.hash == match.hash &&
           this->u.suffix.len  == match.len;
  }
  bool shard_equals( const ShardMatch &match ) const {
    return this->detail_type   == SHARD_MATCH &&
           this->u.shard.start == match.start &&
           this->u.shard.end   == match.end;
  }
  bool match( const char *sub,  uint16_t sublen,  uint32_t subj_hash ) const {
    if ( this->detail_type == SUFFIX_MATCH ) {
      if ( this->prefix_len + this->u.suffix.len > sublen )
        return false;
      if ( this->u.suffix.len == 0 )
        return true;
      uint32_t h = kv_crc_c( &sub[ sublen - this->u.suffix.len ],
                             this->u.suffix.len, 0 );
      return h == this->u.suffix.hash;
    }
    if ( this->detail_type == SHARD_MATCH )
      return subj_hash >= this->u.shard.start && subj_hash <= this->u.shard.end;
    return true;
  }
  static bool shard_endpoints( uint32_t shard,  uint32_t nshards,
                               uint32_t &start,  uint32_t &end ) noexcept;
  bool from_pattern( const PatternCvt &cvt ) noexcept;
};

struct BloomRef {
  BloomBits   * bits;
  BloomRoute ** links;
  BloomDetail * details;
  uint64_t      pref_mask,
                detail_mask;
  uint32_t      nlinks,
                ndetails,
                pref_count[ MAX_RTE ];

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  BloomRef( uint32_t seed ) noexcept;
  BloomRef( BloomBits *b,  const uint32_t *pref ) noexcept;
  void unlink( bool del_empty_routes ) noexcept;
  void zero( void ) noexcept;
  void add_link( BloomRoute *b ) noexcept;
  void del_link( BloomRoute *b ) noexcept;
  bool has_link( uint32_t fd ) noexcept;
  void ref_pref_count( uint16_t prefix_len ) noexcept;
  void deref_pref_count( uint16_t prefix_len ) noexcept;
  void invalid( void ) noexcept;
  bool add_route( uint16_t prefix_len,  uint32_t hash ) noexcept;
  bool add_shard_route( uint16_t prefix_len,  uint32_t hash,
                        const ShardMatch &match ) noexcept;
  bool add_suffix_route( uint16_t prefix_len,  uint32_t hash,
                         const SuffixMatch &match ) noexcept;
  void del_route( uint16_t prefix_len,  uint32_t hash ) noexcept;
  void del_shard_route( uint16_t prefix_len,  uint32_t hash,
                        const ShardMatch &match ) noexcept;
  void del_suffix_route( uint16_t prefix_len,  uint32_t hash,
                         const SuffixMatch &match ) noexcept;
  void update_route( const uint32_t *pref_count,  BloomBits *bits,
                     BloomDetail *details,  uint32_t ndetails ) noexcept;

  bool add( uint32_t hash ) {
    return this->add_route( SUB_RTE, hash );
  }
  void del( uint32_t hash ) {
    return this->del_route( SUB_RTE, hash );
  }
  void encode( BloomCodec &code ) {
    code.encode( this->pref_count, MAX_RTE,
                 this->details, this->ndetails * sizeof( this->details[ 0 ] ),
                 *this->bits );
  }
  bool decode( const void *data,  size_t datalen ) {
    uint32_t     pref[ MAX_RTE ];
    BloomCodec   code; 
    BloomBits  * bits;
    void       * details;
    size_t       detail_size;
    bits = code.decode( pref, MAX_RTE, details, detail_size, data, datalen/4 );
    if ( bits == NULL )
      return false;
    this->update_route( pref, bits, (BloomDetail *) details,
                        detail_size / sizeof( BloomDetail ) );
    return true;
  }
  bool detail_matches( uint16_t prefix_len,  uint64_t mask, uint32_t hash,
                       const char *sub,  uint16_t sublen,
                       uint32_t subj_hash,  bool &has_detail ) const {
    if ( ( this->detail_mask & mask ) == 0 ) /* no detail */
      return true;
    uint32_t detail_not_matched = 0;
    for ( uint32_t j = 0; j < this->ndetails; j++ ) {
      if ( this->details[ j ].prefix_len == prefix_len &&
           this->details[ j ].hash == hash ) {
        has_detail = true;
        if ( this->details[ j ].match( sub, sublen, subj_hash ) )
          return true;
        detail_not_matched++; /* match filtered */
      }
    }
    /* no more details left, is a match */
    if ( detail_not_matched > 0 )
      return false;
    return true;
  }
};

struct BloomMatch {
  uint64_t pref_mask;
  uint16_t max_pref;
  uint32_t pref_hash[ MAX_PRE ];

  bool match_sub( uint32_t subj_hash, const char *sub,  uint16_t sublen,
                  const BloomRef &bloom ) {
    if ( bloom.pref_count[ SUB_RTE ] != 0 && bloom.bits->is_member( subj_hash ))
      return true;
    for ( uint16_t i = 0; i < this->max_pref; i++ ) {
      uint64_t mask = (uint64_t) 1 << i;
      if ( ( bloom.pref_mask & mask ) != 0 ) {
        if ( ( this->pref_mask & mask ) == 0 ) {
          this->pref_mask |= mask;
          this->pref_hash[ i ] = kv_crc_c( sub, i, this->pref_hash[ i ] );
        }
        bool has_detail;
        if ( bloom.bits->is_member( this->pref_hash[ i ] ) &&
             bloom.detail_matches( i, mask, this->pref_hash[ i ], sub, sublen,
                                   subj_hash, has_detail ) )
          return true;
      }
    }
    return false;
  }
  void init_match( uint16_t sublen,  const uint32_t *pref ) {
    this->max_pref = sublen + 1;
    this->pref_mask = 0;
    if ( this->max_pref > MAX_PRE )
      this->max_pref = MAX_PRE;
    ::memcpy( this->pref_hash, pref, sizeof( pref[ 0 ] ) * this->max_pref );
  }
  static size_t match_size( uint16_t sublen ) {
    uint16_t max_pref = sublen + 1;
    if ( max_pref > MAX_PRE )
      max_pref = MAX_PRE;
    size_t len = sizeof( BloomMatch ) -
                 ( sizeof( uint32_t ) * ( MAX_PRE - max_pref ) );
    return ( len + 7 ) & ~(size_t) 7;
  }
};

struct BloomRoute {
  BloomRoute * next,
             * back;
  RouteDB    & rdb;
  BloomRef  ** bloom;
  uint32_t     r,
               nblooms,
               pref_mask,
               detail_mask;
  bool         is_invalid;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  BloomRoute( uint32_t fd,  RouteDB &db )
    : next( 0 ), back( 0 ), rdb( db ), bloom( 0 ), r( fd ), nblooms( 0 ),
      pref_mask( 0 ), detail_mask( 0 ), is_invalid( true ) {}
  ~BloomRoute() {
    if ( this->bloom != NULL )
      ::free( this->bloom );
  }

  void add_bloom_ref( BloomRef *ref ) noexcept;
  void del_bloom_ref( BloomRef *ref ) noexcept;
  void invalid( void ) noexcept;
  void update_masks( void ) noexcept;
};

struct EvPublish;
struct RoutePublishData {   /* structure for publish queue heap */
  uint32_t prefix : 7,  /* prefix size (how much of subject matches) */
           rcount : 25, /* count of routes (32 million max (fd)) */
           hash,        /* hash of prefix (is subject hash if prefix=64) */
         * routes;      /* routes for this hash */

  void set( uint8_t pref,  uint32_t rcnt,  uint32_t h,  uint32_t *r ) {
    this->prefix = pref;
    this->rcount = rcnt;
    this->hash   = h;
    this->routes = r;
  }
  /* 64 is used for subject route, make that 0, prefixes[0..63] += 1 */
  static inline uint32_t prefix_cmp( uint32_t p ) {
    return ( ( ( p & 63 ) << 1 ) + 1 ) | ( p >> 6 );
  }
  static bool is_greater( RoutePublishData *r1,  RoutePublishData *r2 ) {
    if ( r1->routes[ 0 ] > r2->routes[ 0 ] ) /* order by fd */
      return true;
    if ( r1->routes[ 0 ] < r2->routes[ 0 ] )
      return false;
    return prefix_cmp( r1->prefix ) > prefix_cmp( r2->prefix );
  }
};

struct RoutePublishCache {
  RouteDB        & rdb;
  RoutePublishData rpd[ MAX_RTE ];
  uint32_t         n,
                   ref;

  RoutePublishCache( RouteDB &db,  const char *subject,
                     size_t subject_len,  uint32_t subj_hash )
      : rdb( db ), n( 0 ), ref( 0 ) {
    uint32_t * routes;
    uint32_t   rcount = db.get_sub_route( subj_hash, routes, this->ref );

    if ( rcount > 0 )
      this->rpd[ this->n++ ].set( SUB_RTE, rcount, subj_hash, routes );

    const char * key[ MAX_PRE ];
    size_t       keylen[ MAX_PRE ];
    uint32_t     hash[ MAX_PRE ];
    uint32_t     k = 0,
                 x = db.setup_prefix_hash( subject, subject_len, key, keylen,
                                           hash );
    /* prefix len 0 matches all */
    if ( x > 0 && keylen[ 0 ] == 0 ) {
      rcount = db.get_route( subject, subject_len, 0, hash[ 0 ], routes,
                             this->ref, subj_hash );
      if ( rcount > 0 )
        this->rpd[ this->n++ ].set( 0, rcount, hash[ 0 ], routes );
      k = 1;
    }
    /* the rest of the prefixes are hashed */
    if ( k < x ) {
      kv_crc_c_array( (const void **) &key[ k ], &keylen[ k ], &hash[ k ], x-k);
      do {
        rcount = db.get_route( subject, subject_len, keylen[ k ], hash[ k ],
                               routes, this->ref, subj_hash );
        if ( rcount > 0 )
          this->rpd[ this->n++ ].set( keylen[ k ], rcount, hash[ k ], routes );
      } while ( ++k < x );
    }
  }

  RoutePublishCache( RouteDB &db,  const char *subject,
                     size_t subject_len,  uint32_t subj_hash,
                     uint8_t pref_cnt,  KvPrefHash *ph )
     : rdb( db ), n( 0 ), ref( 0 ) {
    uint32_t * routes;
    uint32_t   rcount = db.get_sub_route( subj_hash, routes, this->ref );

    if ( rcount > 0 )
      this->rpd[ this->n++ ].set( SUB_RTE, rcount, subj_hash, routes );

    const char * key[ MAX_PRE ];
    size_t       keylen[ MAX_PRE ];
    uint32_t     hash[ MAX_PRE ];
    uint8_t      j = 0, k = 0,
                 x = db.setup_prefix_hash( subject, subject_len, key, keylen,
                                           hash );
    /* prefix len 0 matches all */
    if ( x > 0 && keylen[ 0 ] == 0 ) {
      rcount = db.get_route( subject, subject_len, 0, hash[ 0 ], routes,
                             this->ref, subj_hash );
      if ( rcount > 0 )
        this->rpd[ this->n++ ].set( 0, rcount, hash[ 0 ], routes );
      k = 1;
    }
    /* process the precomputed hashes */
    while ( k < x && j < pref_cnt ) {
      if ( ph[ j ].pref == keylen[ k ] ) {
        uint32_t h = ph[ j ].get_hash();
        rcount = db.get_route( subject, subject_len, keylen[ k ], h, routes,
                               this->ref, subj_hash );
        if ( rcount > 0 )
          this->rpd[ this->n++ ].set( keylen[ k ], rcount, h, routes );
        k++;
      }
      j++;
    }
    /* the rest of the prefixes are hashed */
    if ( k < x ) {
      kv_crc_c_array( (const void **) &key[ k ], &keylen[ k ], &hash[ k ], x-k);
      do {
        rcount = db.get_route( subject, subject_len, keylen[ k ], hash[ k ],
                               routes, this->ref, subj_hash );
        if ( rcount > 0 )
          this->rpd[ this->n++ ].set( keylen[ k ], rcount, hash[ k ], routes );
      } while ( ++k < x );
    }
  }

  ~RoutePublishCache() {
    this->rdb.cache.busy -= this->ref;
  }
};

/* queue for publishes, to merge multiple sub matches into one pub, for example:
 *   SUB* -> 1, 7
 *   SUBJECT* -> 1, 3, 7
 *   SUBJECT_MATCH -> 1, 3, 8
 * The publish to SUBJECT_MATCH has 3 matches for 1 and 2 matches for 7 and 3
 * The heap sorts by route id (fd) to merge the publishes to 1, 3, 7, 8 */
typedef struct kv::PrioQueue<RoutePublishData *, RoutePublishData::is_greater>
 RoutePublishQueue;

struct PeerData;
struct PeerMatchArgs {
  int64_t      id;       /* each peer is assigned a unique id */
  const char * ip,       /* the ip address to match */
             * type;     /* the type of peer to match (pubsub, normal, etc) */
  size_t       ip_len,   /* len of ip */
               type_len; /* len of type */
  bool         skipme;   /* true if don't match the client's peer */
  PeerMatchArgs( const char *t = NULL,  size_t l = 0 )
   : id( 0 ), ip( 0 ), type( t ), ip_len( 0 ), type_len( l ), skipme( false ) {}
  PeerMatchArgs &set_type( const char *t,  size_t l ) {
    this->type     = t;
    this->type_len = l;
    return *this;
  }
};

struct PeerStats {
  uint64_t bytes_recv,
           bytes_sent,
           accept_cnt,
           msgs_recv,
           msgs_sent;
  PeerStats() : bytes_recv( 0 ), bytes_sent( 0 ), accept_cnt( 0 ),
                msgs_recv( 0 ), msgs_sent( 0 ) {}
  void zero( void ) {
    this->bytes_recv = 0;
    this->bytes_sent = 0;
    this->accept_cnt = 0;
    this->msgs_recv  = 0;
    this->msgs_sent  = 0;
  }
};
#if 0
struct PeerOps { /* interface for the peers */
  PeerOps() {}   /* list prints the peer, kill shuts the connection down */
  virtual int client_list( PeerData &,  char *,  size_t ) { return 0; }
  virtual bool client_kill( PeerData & )                  { return false; }
  virtual bool match( PeerData &,  PeerMatchArgs & )      { return true; }
  virtual void client_stats( PeerData &,  PeerStats & )   { return; }
  virtual void retired_stats( PeerData &,  PeerStats & )  { return; }
};
#endif
struct PeerMatchIter {
  PeerData      & me,  /* the client using this connection is this */
                * p;   /* the current position in the list */
  PeerMatchArgs & ka;  /* the match args to apply to the list */
  PeerMatchIter( PeerData &m,  PeerMatchArgs &a )
    : me( m ), p( 0 ), ka( a ) {}

  PeerData *first( void ) noexcept;
  PeerData *next( void ) noexcept;
  size_t length( void ) {
    size_t cnt = 0;
    if ( this->first() != NULL ) {
      do { cnt++; } while ( this->next() != NULL );
    }
    return cnt;
  }
};

static inline void
set_strlen64( char buf[ 64 ],  const char *s,  size_t len )
{
  if ( len == 0 ) {
    buf[ 0 ] = '\0';
  }
  else if ( len < 63 ) {
    ::memcpy( buf, s, len );
    ::memset( &buf[ len ], 0, 63 - len );
  }
  else {
    ::memcpy( buf, s, 63 );
    len = 0;
  }
  buf[ 63 ] = (char) len;
}

static inline size_t get_strlen64( const char *buf )
{
  if ( buf[ 0 ] == '\0' )
    return 0;
  if ( buf[ 63 ] == '\0' )
    return 63;
  return (size_t) (uint8_t) buf[ 63 ];
}

struct PeerAddrStr {
  char buf[ 64 ];

  PeerAddrStr() {
    this->buf[ 0 ] = '\0';
    this->buf[ 63 ] = '\0';
  }
  void set_addr( const struct sockaddr *sa ) noexcept;

  void init_ctx( uint32_t ctx_id ) {
    ::memcpy( this->buf, "ctx:", 4 );
    int i = 4;
    if ( ctx_id >= 1000 )
      this->buf[ i++ ] = ( ctx_id / 1000 ) % 10 + '0';
    if ( ctx_id >= 100 )
      this->buf[ i++ ] = ( ctx_id / 100 ) % 10 + '0';
    if ( ctx_id >= 10 )
      this->buf[ i++ ] = ( ctx_id / 10 ) % 10 + '0';
    this->buf[ i++ ] = ctx_id % 10 + '0';
    this->buf[ i ] = '\0';
    this->buf[ 63 ] = (char) i;
  }
  void set_address( const struct sockaddr *sa ) {
    this->buf[ 0 ] = '\0';
    this->buf[ 63 ] = '\0';
    if ( sa != NULL )
      this->set_addr( sa );
  }
  bool set_sock_addr( int fd ) noexcept;
  bool set_peer_addr( int fd ) noexcept;
};

struct PeerData {
  PeerData   * next,       /* dbl link list */
             * back;
  int32_t      fd,         /* fildes */
               pad;        /* 64 * 3 */
  uint64_t     id,         /* identifies peer */
               start_ns,   /* start time */
               active_ns;  /* last read time */
  const char * kind;       /* what protocol type */
  char         name[ 64 ]; /* name assigned to peer */
  PeerAddrStr  peer_address; /* ip4 1.2.3.4:p, ip6 [ab::cd]:p, other */

  PeerData() : next( 0 ), back( 0 ) {
    this->init_peer( -1, NULL, NULL );
  }
  /* no address, attached directly to shm */
  void init_ctx( int fildes,  uint32_t ctx_id,  const char *k ) {
    this->init_peer( fildes, NULL, k );
    this->peer_address.init_ctx( ctx_id );
  }
  void set_addr( const struct sockaddr *sa ) {
    this->peer_address.set_address( sa );
  }
  /* connected via ip address */
  void init_peer( int fildes,  const sockaddr *sa,  const char *k ) {
    this->fd = fildes;
    this->id = 0;
    this->start_ns = this->active_ns = 0;
    this->kind = k;
    this->name[ 0 ] = '\0';
    this->name[ 63 ] = '\0';
    this->peer_address.set_address( sa );
  }
  void set_peer_address( const char *s,  size_t len ) {
    set_strlen64( this->peer_address.buf, s, len );
  }
  void set_name( const char *s,  size_t len ) {
    set_strlen64( this->name, s, len );
  }
  size_t get_peer_address_strlen( void ) const {
    return get_strlen64( this->peer_address.buf );
  }
  size_t get_name_strlen( void ) const {
    return get_strlen64( this->name );
  }
  virtual int client_list( char *,  size_t ) { return 0; }
  virtual bool client_kill( void )           { return false; }
  virtual bool match( PeerMatchArgs & )      { return true; }
  virtual void client_stats( PeerStats & )   { return; }
  virtual void retired_stats( PeerStats & )  { return; }
};

struct RouteNotify {
  RouteNotify * next,
              * back;
  uint8_t       in_notify;
  RouteNotify() : next( 0 ), back( 0 ), in_notify( 0 ) {}

  virtual void on_sub( uint32_t h,  const char *sub,  size_t len,
                       uint32_t fd,  uint32_t rcnt,  char src_type,
                       const char *rep,  size_t rlen ) noexcept;
  virtual void on_unsub( uint32_t h,  const char *sub,  size_t len,
                         uint32_t fd,  uint32_t rcnt,
                         char src_type ) noexcept;
  virtual void on_psub( uint32_t h,  const char *pattern,  size_t len,
                        const char *prefix,  uint8_t prefix_len,
                        uint32_t fd,  uint32_t rcnt,
                        char src_type ) noexcept;
  virtual void on_punsub( uint32_t h,  const char *pattern,  size_t len,
                          const char *prefix,  uint8_t prefix_len,
                          uint32_t fd,  uint32_t rcnt,
                          char src_type ) noexcept;
  virtual void on_reassert( uint32_t fd,  RouteVec<RouteSub> &sub_db,
                            RouteVec<RouteSub> &pat_db ) noexcept;
};

struct KvPrefHash;
struct EvPoll;
struct RoutePublish : public RouteDB {
  EvPoll  & poll;
  kv::DLinkList<RouteNotify> notify_list;
  const char * service_name;
  /* this should be moved to a notify_list */
  uint32_t keyspace, /* route of __keyspace@N__ subscribes active */
           keyevent, /* route of __keyevent@N__ subscribes active */
           listblkd, /* route of __listblkd@N__ subscribes active */
           zsetblkd, /* route of __zsetblkd@N__ subscribes active */
           strmblkd, /* route of __strmblkd@N__ subscribes active */
           monitor;  /* route of __monitor_@N__ subscribes active */
  uint16_t key_flags; /* bits set for key subs above:
                         EKF_KEYSPACE_FWD | EKF_KEYEVENT_FWD |
                         EKF_LISTBLKD_NOT | EKF_ZSETBLKD_NOT |
                         EKF_STRMBLKD_NOT | EKF_MONITOR */

  bool forward_msg( EvPublish &pub ) noexcept;
  bool forward_except( EvPublish &pub,  const BitSpace &fdexcpt ) noexcept;
  bool forward_msg( EvPublish &pub,  uint32_t *rcount_total,  uint8_t pref_cnt,
                    KvPrefHash *ph ) noexcept;
  bool forward_some( EvPublish &pub, uint32_t *routes, uint32_t rcnt ) noexcept;
  bool forward_not_fd( EvPublish &pub,  uint32_t not_fd ) noexcept;
  bool forward_not_fd2( EvPublish &pub,  uint32_t not_fd,
                        uint32_t not_fd2 ) noexcept;
  bool forward_all( EvPublish &pub, uint32_t *routes, uint32_t rcnt ) noexcept;
  bool forward_set( EvPublish &pub,  const BitSpace &fdset ) noexcept;
  bool forward_set_not_fd( EvPublish &pub,  const BitSpace &fdset,
                           uint32_t not_fd ) noexcept;
  bool forward_to( EvPublish &pub,  uint32_t fd ) noexcept;
  bool hash_to_sub( uint32_t r,  uint32_t h,  char *key,
                    size_t &keylen ) noexcept;
  void update_keyspace_route( uint32_t &val,  uint16_t bit,  int add,
                              uint32_t fd ) noexcept;
  void update_keyspace_count( const char *sub,  size_t len,  int add,
                              uint32_t fd ) noexcept;
  /* notify subscription start / stop to listeners */
  void notify_sub( uint32_t h,  const char *sub,  size_t len,
                   uint32_t fd,  uint32_t rcnt,  char src_type,
                   const char *rep = NULL,  size_t rlen = 0 ) noexcept;
  void notify_unsub( uint32_t h,  const char *sub,  size_t len,
                     uint32_t fd,  uint32_t rcnt,  char src_type ) noexcept;
  void notify_psub( uint32_t h,  const char *pattern,  size_t len,
                    const char *prefix,  uint8_t prefix_len,
                    uint32_t fd,  uint32_t rcnt,  char src_type ) noexcept;
  void notify_punsub( uint32_t h,  const char *pattern,  size_t len,
                     const char *prefix,  uint8_t prefix_len,
                     uint32_t fd,  uint32_t rcnt,  char src_type ) noexcept;
  void notify_reassert( uint32_t fd,  RouteVec<RouteSub> &sub_db,
                        RouteVec<RouteSub> &pat_db ) noexcept;
  void add_route_notify( RouteNotify &x ) {
    if ( ! x.in_notify ) {
      x.in_notify = 1;
      this->notify_list.push_tl( &x );
    }
  }
  void remove_route_notify( RouteNotify &x ) {
    if ( x.in_notify ) {
      x.in_notify = 0;
      this->notify_list.pop( &x );
    }
  }
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  RoutePublish( EvPoll &p,  const char *svc )
    : poll( p ), service_name( svc ),
      keyspace( 0 ), keyevent( 0 ), listblkd( 0 ), zsetblkd( 0 ),
      strmblkd( 0 ), monitor( 0 ), key_flags( 0 ) {}
};

/* callbacks cannot disappear between epochs, fd based connections that can
 * close between epochs should use fd based timers with unique timer ids */
struct EvTimerCallback {
  virtual bool timer_cb( uint64_t timer_id,  uint64_t event_id ) noexcept;
};

struct EvTimerQueue;
struct TimerQueue {
  EvTimerQueue * queue;

  TimerQueue() : queue( 0 ) {} /* created in EvPoll::init() */
  /* arm a timer by cb */
  bool add_timer_seconds( EvTimerCallback &tcb,  uint32_t secs,
                          uint64_t timer_id,  uint64_t event_id ) noexcept;
  bool add_timer_millis( EvTimerCallback &tcb,  uint32_t msecs,
                         uint64_t timer_id,  uint64_t event_id ) noexcept;
  bool add_timer_micros( EvTimerCallback &tcb,  uint32_t usecs,
                         uint64_t timer_id,  uint64_t event_id ) noexcept;
  bool add_timer_nanos( EvTimerCallback &tcb,  uint32_t nsecs,
                        uint64_t timer_id,  uint64_t event_id ) noexcept;
  /* arm a timer by fd */
  bool add_timer_seconds( int32_t id,  uint32_t secs,  uint64_t timer_id,
                          uint64_t event_id ) noexcept;
  bool add_timer_millis( int32_t id,  uint32_t msecs,  uint64_t timer_id,
                         uint64_t event_id ) noexcept;
  bool add_timer_micros( int32_t id,  uint32_t usecs,  uint64_t timer_id,
                         uint64_t event_id ) noexcept;
  bool add_timer_nanos( int32_t id,  uint32_t nsecs,  uint64_t timer_id,
                        uint64_t event_id ) noexcept;
  bool remove_timer( int32_t id,  uint64_t timer_id,
                     uint64_t event_id ) noexcept;
  /* when a timer expires, these are updated */
  uint64_t current_monotonic_time_ns( void ) noexcept;
  uint64_t current_time_ns( void ) noexcept;
};

struct RouteService {
  RoutePublish * sub_route;
  uint32_t       hash;
  uint16_t       len;
  char           value[ TRAIL_VALLEN ];
};

struct RoutePDB : public RoutePublish {
  TimerQueue           & timer;
  RouteVec<RouteService> svc_db;
  RoutePDB( EvPoll &p ) noexcept;
  RoutePublish & get_service( const char *svc,  uint32_t num = 0 ) noexcept;
};

}
}
#endif
