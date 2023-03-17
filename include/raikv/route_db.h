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
#include <raikv/balloc.h>
#include <raikv/pattern_cvt.h>

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
struct BloomList {
  kv::DLinkList<BloomRoute> list[ 4 ];
  BloomList() {}
  bool is_empty( void ) const {
    for ( int i = 0; i < 4; i++ )
      if ( ! this->list[ i ].is_empty() )
        return false;
    return true;
  }
  bool is_empty( uint32_t shard ) const {
    return this->list[ shard & 3 ].is_empty();
  }
  BloomRoute *hd( uint32_t shard ) {
    return this->list[ shard & 3 ].hd;
  }
  kv::DLinkList<BloomRoute> & get_list( uint32_t shard ) {
    return this->list[ shard & 3 ];
  }
};

struct RouteDB;
struct RouteRef {
  RouteDB    & rdb;       /* zipped routes */
  RouteSpace & route_spc; /* space to unzip and modify route arrays */
  CodeRefPtr   ptr;       /* if route is updated, no longer need old route */
  uint32_t   * routes,    /* the set of routes allocated from route_spc */
               rcnt;      /* num elems in routes[] */

  RouteRef( RouteDB &db,  RouteSpace &spc )
    : rdb( db ), route_spc( spc ), routes( 0 ), rcnt( 0 ) {}

  void copy_cached( uint32_t *cached,  uint32_t cache_rcnt,  uint32_t add ) {
    this->routes = this->route_spc.make( cache_rcnt + add );
    this->rcnt   = cache_rcnt;
    ::memcpy( this->routes, cached, sizeof( cached[ 0 ] ) * cache_rcnt );
  }
  /* val is the route id, decompress with add space for inserting */
  uint32_t decompress( uint32_t val,  uint32_t add ) {
    if ( DeltaCoder::is_not_encoded( val ) ) /* if is a multi value code */
      return this->decode( val, add );
    /* single value code, could contain up to 15 routes */
    this->routes = this->route_spc.make( MAX_DELTA_CODE_LENGTH + add );
    this->rcnt   = DeltaCoder::decode( val, this->routes, 0 );
    return this->rcnt;
  }
  /* convert val to coderef, if need to deref it */
  void find_coderef( uint32_t val ) noexcept;
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
  size_t        end,        /* end array spot */
                free,       /* free count for gc */
                count,      /* count of elems <= MAX_CACHE */
                busy,       /* busy, no realloc because of references */
                need;       /* need this amount of space */
  uint64_t      hit_cnt,
                miss_cnt,
                max_cnt,
                max_size;
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

  uint32_t *make_code_ref_space( uint32_t ecnt,  size_t &off ) noexcept;
  uint32_t make_code_ref( uint32_t *code,  uint32_t ecnt,
                          uint32_t rcnt ) noexcept;
  void gc_code_ref_space( void ) noexcept;
  uint32_t decompress_one( uint32_t val ) noexcept;
};

/* Map a subscription hash to a route list using RouteZip */
struct BloomDB : public ArrayCount<BloomRef *, 128> {
  BallocList<8, 16*1024> bloom_mem;
};

struct RouteDB {
  RouteCache    cache;               /* the route arrays indexed by ht */
  BloomList     bloom_list;
  uint32_t      bloom_pref_count[ MAX_RTE ];
  BloomDB     & g_bloom_db;
  size_t        entry_count;         /* counter of route/hashes indexed */
  RouteZip      zip;
  UIntHashTab * rt_hash[ MAX_RTE ];  /* route hash -> code | ref hash */
  uint32_t      pre_seed[ MAX_PRE ]; /* hash seeds for each prefix */
  uint64_t      pat_mask,            /* mask of subject prefixes, up to 64 */
                rt_mask,
                bloom_mask;
  uint8_t       prefix_len[ MAX_PRE ]; /* current set of prefix lengths */
  uint8_t       pat_bit_count;      /* how many bits in pat_mask are set */

  RouteDB( BloomDB &g_db ) noexcept;

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
  uint32_t add_route( uint16_t prefix_len,  uint32_t hash,
                      uint32_t r,  RouteRef &rte ) noexcept;
  uint32_t del_route( uint16_t prefix_len,  uint32_t hash,
                      uint32_t r,  RouteRef &rte ) noexcept;
  uint32_t ref_route( uint16_t prefix_len,  uint32_t hash,
                      RouteRef &rte ) noexcept;
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
                      uint32_t subj_hash,  uint32_t shard ) {
    uint32_t rcnt, val;
    size_t   pos;
    if ( this->cache_find( prefix_len, hash, routes, rcnt, shard, pos ) ) {
      this->cache.busy++;
      ref++;
      return rcnt;
    }
    uint64_t mask = (uint64_t) 1 << prefix_len;
    if ( shard == 0 && ( this->rt_mask & mask ) != 0 ) {
      if ( this->rt_hash[ prefix_len ]->find( hash, pos, val ) )
        return this->get_route_slow2( sub, sublen, prefix_len, mask, hash, val,
                                      routes, subj_hash, 0 );
    }
    return this->get_bloom_route2( sub, sublen, prefix_len, mask, hash, routes,
                                   0, subj_hash, shard );
  }
  uint32_t get_sub_route( uint32_t hash,  uint32_t *&routes,  uint32_t &ref,
                          uint32_t shard ) {
    uint32_t rcnt, val;
    size_t   pos;
    if ( this->cache_find( SUB_RTE, hash, routes, rcnt, shard, pos ) ) {
      this->cache.busy++;
      ref++;
      return rcnt;
    }
    if ( shard == 0 && this->rt_hash[ SUB_RTE ]->find( hash, pos, val ) )
      return this->get_route_slow( hash, val, routes, 0 );
    return this->get_bloom_route( hash, routes, 0, shard );
  }
  uint32_t get_route_slow( uint32_t hash, uint32_t val,
                           uint32_t *&routes,  uint32_t shard ) noexcept;
  uint32_t get_bloom_route( uint32_t hash,  uint32_t *&routes,
                            uint32_t merge_cnt,  uint32_t shard ) noexcept;
  uint32_t get_route_slow2( const char *sub,  uint16_t sublen,
                            uint16_t prefix_len,  uint64_t mask,  uint32_t hash,
                            uint32_t val,  uint32_t *&routes,
                            uint32_t subj_hash,  uint32_t shard ) noexcept;
  uint32_t get_bloom_route2( const char *sub,  uint16_t sublen,
                             uint16_t prefix_len,  uint64_t mask, uint32_t hash,
                             uint32_t *&routes,  uint32_t merge_cnt,
                             uint32_t subj_hash,  uint32_t shard ) noexcept;
  uint32_t get_route_count( uint16_t prefix_len,  uint32_t hash ) noexcept;

  uint32_t get_sub_route_count( uint32_t hash ) {
    return this->get_route_count( SUB_RTE, hash );
  }
  BloomRoute * create_bloom_route( uint32_t r,  BloomRef *ref,
                                   uint32_t shard ) noexcept;
  void remove_bloom_route( BloomRoute *b ) noexcept;

  BloomRef * create_bloom_ref( uint32_t *pref_count,  BloomBits *bits,
                               const char *nm,  BloomDB &db ) noexcept;
  BloomRef * create_bloom_ref( uint32_t seed,  const char *nm,
                               BloomDB &db ) noexcept;
  BloomRef * update_bloom_ref( const void *data,  size_t datalen,
                               uint32_t ref_num,  const char *nm,
                               BloomDB &db ) noexcept;
  void remove_bloom_ref( BloomRef *ref ) noexcept;
  uint32_t get_bloom_count( uint16_t prefix_len,  uint32_t hash,
                            uint32_t shard ) noexcept;
  bool cache_find( uint16_t prefix_len,  uint32_t hash,
                   uint32_t *&routes,  uint32_t &rcnt,
                   uint32_t shard,  size_t &pos ) {
    if ( ! this->cache.is_invalid ) {
      if ( ! this->cache.busy && this->cache.need )
        this->cache_need();
      uint64_t    h = ( (uint64_t) shard << 48 ) |
                      ( (uint64_t) prefix_len << 32 ) | (uint64_t) hash;
      RteCacheVal val;

      if ( this->cache.ht->find( h, pos, val ) ) {
        rcnt   = val.rcnt;
        routes = &this->cache.spc.ptr[ val.off ];
        this->cache.hit_cnt++;
        return true;
      }
    }
    this->cache.miss_cnt++;
    return false;
  }
  void cache_save( uint16_t prefix_len,  uint32_t hash,
                   uint32_t *routes,  uint32_t rcnt,  uint32_t shard ) noexcept;
  void cache_purge( size_t pos ) noexcept;
#if 0
  void cache_purge( uint16_t prefix_len,  uint32_t hash ) noexcept;
#endif
  void cache_need( void ) noexcept;
};

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
                pref_count[ MAX_RTE ],
                ref_num;
  BloomDB     & bloom_db;
  char          name[ 32 ];

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void * ) {}

  BloomRef( uint32_t seed,  const char *nm,  BloomDB &db ) noexcept;
  BloomRef( BloomBits *b,  const uint32_t *pref,  const char *nm,
            BloomDB &db,  uint32_t num = (uint32_t) -1 ) noexcept;
  void unlink( bool del_empty_routes ) noexcept;
  void zero( void ) noexcept;
  void add_link( BloomRoute *b ) noexcept;
  void del_link( BloomRoute *b ) noexcept;
  BloomRoute *get_bloom_by_fd( uint32_t fd,  uint32_t shard ) noexcept;
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
  /*void notify_update( void ) noexcept;*/

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
                        (uint32_t) ( detail_size / sizeof( BloomDetail ) ) );
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
  BloomRef  ** bloom;       /* the blooms that are filtering this route */
  uint32_t     r,           /* the fd to route for blooms */
               nblooms;     /* count of bloom[] */
  uint64_t     pref_mask,   /* prefix mask for blooms */
               detail_mask; /* shard/suffix mask */
  uint32_t     in_list;     /* whether in bloom_list and what shard */
  bool         is_invalid;  /* whether to recalculate masks after sub ob */

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void * ) {} /* no delete, in rdb.bloom_mem */

  BloomRoute( uint32_t fd,  RouteDB &db,  uint32_t lst )
    : next( 0 ), back( 0 ), rdb( db ), bloom( 0 ), r( fd ), nblooms( 0 ),
      pref_mask( 0 ), detail_mask( 0 ), in_list( lst ), is_invalid( true ) {}

  void add_bloom_ref( BloomRef *ref ) noexcept;
  BloomRef *del_bloom_ref( BloomRef *ref ) noexcept;
  void invalid( void ) noexcept;
  void update_masks( void ) noexcept;
  void remove_if_empty( void ) {
    if ( this->nblooms == 0 )
      this->rdb.remove_bloom_route( this );
  }
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
#if 0
  static bool is_greater( RoutePublishData *r1,  RoutePublishData *r2 ) {
    if ( r1->routes[ 0 ] > r2->routes[ 0 ] ) /* order by fd */
      return true;
    if ( r1->routes[ 0 ] < r2->routes[ 0 ] )
      return false;
    return prefix_cmp( r1->prefix ) > prefix_cmp( r2->prefix );
  }
#endif
};

struct EvPoll;
struct RoutePublishCache;
enum { /* BPData::flags */
  BP_FORWARD = 1,
  BP_NOTIFY  = 2,
  BP_IN_LIST = 4
};
struct BPData {
  uint16_t bp_state,
           bp_flags;
  uint32_t bp_fd;
  uint64_t bp_id;
  BPData * next, * back;
  BPData() : bp_state( 0 ), bp_flags( 0 ) {}
  virtual void on_write_ready( void ) noexcept;
  bool bp_fwd( void ) const { return ( this->bp_flags & BP_FORWARD ) != 0; }
  bool bp_in_list( void ) const { return ( this->bp_flags & BP_IN_LIST ) != 0; }
  bool bp_notify( void ) const { return ( this->bp_flags & BP_NOTIFY ) != 0; }
  bool has_back_pressure( EvPoll &poll,  uint32_t fd ) noexcept;
  bool test_back_pressure_one( EvPoll &poll,  RoutePublishData &rpd ) noexcept;
  bool test_back_pressure_64( EvPoll &poll,  RoutePublishData *rpd,
                              uint32_t n ) noexcept;
  bool test_back_pressure_n( EvPoll &poll,  RoutePublishData *rpd,
                             uint32_t n ) noexcept;
  bool test_back_pressure( EvPoll &poll,  RoutePublishCache &cache ) noexcept;

};

struct RoutePublishCache {
  RouteDB        & rdb;
  RoutePublishData rpd[ MAX_RTE ];
  uint32_t         n,
                   ref;

  RoutePublishCache( RouteDB &db,  const char *subject,
                     size_t subject_len,  uint32_t subj_hash,  uint32_t shard )
      : rdb( db ), n( 0 ), ref( 0 ) {
    uint32_t * routes;
    uint32_t   rcount = db.get_sub_route( subj_hash, routes, this->ref, shard );

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
      rcount = db.get_route( subject, (uint16_t) subject_len, 0, hash[ 0 ],
                             routes, this->ref, subj_hash, shard );
      if ( rcount > 0 )
        this->rpd[ this->n++ ].set( 0, rcount, hash[ 0 ], routes );
      k = 1;
    }
    /* the rest of the prefixes are hashed */
    if ( k < x ) {
      kv_crc_c_array( (const void **) &key[ k ], &keylen[ k ], &hash[ k ], x-k);
      do {
        rcount = db.get_route( subject, (uint16_t) subject_len,
                               (uint16_t) keylen[ k ], hash[ k ], routes,
                               this->ref, subj_hash, shard );
        if ( rcount > 0 )
          this->rpd[ this->n++ ].set( (uint8_t) keylen[ k ], rcount, hash[ k ],
                                      routes );
      } while ( ++k < x );
    }
  }

  ~RoutePublishCache() {
    this->rdb.cache.busy -= this->ref;
  }
};
#if 0
/* queue for publishes, to merge multiple sub matches into one pub, for example:
 *   SUB* -> 1, 7
 *   SUBJECT* -> 1, 3, 7
 *   SUBJECT_MATCH -> 1, 3, 8
 * The publish to SUBJECT_MATCH has 3 matches for 1 and 2 matches for 7 and 3
 * The heap sorts by route id (fd) to merge the publishes to 1, 3, 7, 8 */
typedef struct kv::PrioQueue<RoutePublishData *, RoutePublishData::is_greater>
 RoutePublishQueue;
#endif
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
           msgs_sent,
           active_ns,
           read_ns;
  PeerStats() : bytes_recv( 0 ), bytes_sent( 0 ), accept_cnt( 0 ),
                msgs_recv( 0 ), msgs_sent( 0 ), active_ns( 0 ), read_ns( 0 ) {}
  void zero( void ) {
    this->bytes_recv = 0;
    this->bytes_sent = 0;
    this->accept_cnt = 0;
    this->msgs_recv  = 0;
    this->msgs_sent  = 0;
    this->active_ns  = 0;
    this->read_ns    = 0;
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
struct EvSocket;
struct PeerMatchIter {
  EvSocket      & me,  /* the client using this connection is this */
                * p;   /* the current position in the list */
  PeerMatchArgs & ka;  /* the match args to apply to the list */
  PeerMatchIter( EvSocket &m,  PeerMatchArgs &a )
    : me( m ), p( 0 ), ka( a ) {}

  EvSocket *first( void ) noexcept;
  EvSocket *next( void ) noexcept;
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
  size_t len( void ) const {
    return get_strlen64( this->buf );
  }

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
  EvSocket   * next,       /* dbl link list, active and free list */
             * back;
  int32_t      fd;         /* fildes */
  uint32_t     route_id;   /* 64 * 3 */
  uint64_t     /*id,         * identifies peer */
               start_ns,   /* start time */
               active_ns,  /* last write time */
               read_ns;    /* last read time */
  const char * kind;       /* what protocol type */
  char         name[ 64 ]; /* name assigned to peer */
  PeerAddrStr  peer_address; /* ip4 1.2.3.4:p, ip6 [ab::cd]:p, other */

  PeerData() : next( 0 ), back( 0 ) {
    this->init_peer( -1, -1, NULL, NULL );
  }
  /* no address, attached directly to shm */
  void init_ctx( int fildes,  uint32_t rte_id,  uint32_t ctx_id,
                 const char *k ) {
    this->init_peer( fildes, rte_id, NULL, k );
    this->peer_address.init_ctx( ctx_id );
  }
  void set_addr( const struct sockaddr *sa ) {
    this->peer_address.set_address( sa );
  }
  /* connected via ip address */
  void init_peer( int fildes,  uint32_t rte_id,  const sockaddr *sa,
                  const char *k ) {
    this->fd = fildes;
    this->route_id = rte_id;
    /*this->id = 0;*/
    this->start_ns = this->active_ns = this->read_ns = 0;
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

struct NotifySub {
  const char * subject,
             * reply;
  uint16_t     subject_len,
               reply_len;
  uint32_t     subj_hash,
               src_fd,
               sub_count;
  EvSocket   * src;
  RouteRef   * ref;
  BloomRef   * bref;
  uint8_t      hash_collision;
  char         src_type;

  NotifySub( const char *s,  size_t slen,
             const char *r,  size_t rlen,
             uint32_t shash,  uint32_t fd,
             bool coll,  char t,  EvSocket *source = NULL ) :
    subject( s ), reply( r ), subject_len( (uint16_t) slen ),
    reply_len( (uint16_t) rlen ), subj_hash( shash ), src_fd( fd ),
    sub_count( 0 ), src( source ), ref( 0 ), bref( 0 ), hash_collision( coll ),
    src_type( t ) {}

  NotifySub( const char *s,  size_t slen,
             uint32_t shash,  uint32_t fd,
             bool coll,  char t,  EvSocket *source = NULL ) :
    subject( s ), reply( 0 ), subject_len( (uint16_t) slen ), reply_len( 0 ),
    subj_hash( shash ), src_fd( fd ), sub_count( 0 ), src( source ), ref( 0 ),
    bref( 0 ), hash_collision( coll ), src_type( t ) {}
};

struct NotifyPattern {
  const PatternCvt & cvt;
  const char       * pattern,
                   * reply;
  uint16_t           pattern_len,
                     reply_len;
  uint32_t           prefix_hash,
                     src_fd,
                     sub_count;
  EvSocket         * src;
  RouteRef         * ref;
  BloomRef         * bref;
  uint8_t            hash_collision;
  char               src_type;

  NotifyPattern( const PatternCvt &c,
                 const char *s,  size_t slen,
                 const char *r,  size_t rlen,
                 uint32_t phash,  uint32_t fd,
                 bool coll,  char t,  EvSocket *source = NULL ) :
    cvt( c ), pattern( s ), reply( r ), pattern_len( (uint16_t) slen ),
    reply_len( (uint16_t) rlen ), prefix_hash( phash ), src_fd( fd ),
    sub_count( 0 ), src( source ), ref( 0 ), bref( 0 ), hash_collision( coll ),
    src_type( t ) {}

  NotifyPattern( const PatternCvt &c,
                 const char *s,  size_t slen,
                 uint32_t phash,  uint32_t fd,
                 bool coll,  char t,  EvSocket *source = NULL ) :
    cvt( c ), pattern( s ), reply( 0 ), pattern_len( (uint16_t) slen ),
    reply_len( 0 ), prefix_hash( phash ), src_fd( fd ), sub_count( 0 ),
    src( source ), ref( 0 ), bref( 0 ), hash_collision( coll ), src_type( t ) {}
};

struct RoutePublish;
struct RouteNotify {
  RoutePublish & sub_route;
  RouteNotify  * next,
               * back;
  uint8_t        in_notify;
  char           notify_type;

  RouteNotify( RoutePublish &p ) : sub_route( p ), next( 0 ), back( 0 ),
                                   in_notify( 0 ), notify_type( 0 ) {}
  virtual void on_sub( NotifySub &sub ) noexcept;
  virtual void on_resub( NotifySub &sub ) noexcept;
  virtual void on_unsub( NotifySub &sub ) noexcept;
  virtual void on_psub( NotifyPattern &pat ) noexcept;
  virtual void on_repsub( NotifyPattern &pat ) noexcept;
  virtual void on_punsub( NotifyPattern &pat ) noexcept;
  virtual void on_reassert( uint32_t fd,  SubRouteDB &sub_db,
                            SubRouteDB &pat_db ) noexcept;
  virtual void on_bloom_ref( BloomRef &ref ) noexcept;
  virtual void on_bloom_deref( BloomRef &ref ) noexcept;
};
#if 0
struct RedisKeyspaceNotify : public RouteNotify {
  /* this should be moved to raids */
  uint32_t keyspace, /* route of __keyspace@N__ subscribes active */
           keyevent, /* route of __keyevent@N__ subscribes active */
           listblkd, /* route of __listblkd@N__ subscribes active */
           zsetblkd, /* route of __zsetblkd@N__ subscribes active */
           strmblkd, /* route of __strmblkd@N__ subscribes active */
           monitor;  /* route of __monitor_@N__ subscribes active */
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  RedisKeyspaceNotify( RoutePublish &p ) : RouteNotify( p ), keyspace( 0 ),
    keyevent( 0 ), listblkd( 0 ), zsetblkd( 0 ), strmblkd( 0 ), monitor( 0 ) {
    this->notify_type = 'R';
  }
  void update_keyspace_route( uint32_t &val,  uint16_t bit,
                              int add,  uint32_t fd ) noexcept;
  void update_keyspace_count( const char *sub,  size_t len,
                              int add,  uint32_t fd ) noexcept;
  virtual void on_sub( NotifySub &sub ) noexcept;
  virtual void on_unsub( NotifySub &sub ) noexcept;
  virtual void on_psub( NotifyPattern &pat ) noexcept;
  virtual void on_punsub( NotifyPattern &pat ) noexcept;
  virtual void on_reassert( uint32_t fd,  RouteVec<RouteSub> &sub_db,
                            RouteVec<RouteSub> &pat_db ) noexcept;
};
#endif
struct KvPubSub; /* manages pubsub through kv shm */
struct HashTab;  /* shm ht */
struct EvShm;    /* shm context */
/*struct RedisKeyspaceNotify;*/

struct RoutePublish : public RouteDB {
  EvPoll  & poll;
  HashTab * map;             /* the shm data store */

  DLinkList<RouteNotify> notify_list;
  KvPubSub             * pubsub;   /* cross process pubsub */
  void                 * keyspace; /* update sub_route.key_flags */

  const char * service_name;
  uint32_t     svc_id,
               route_id,  /* id used by endpoints to identify route */
               ctx_id,    /* this thread context */
               dbx_id;    /* the db context */
  uint16_t     key_flags; /* bits set for key subs above:
                         EKF_KEYSPACE_FWD | EKF_KEYEVENT_FWD |
                         EKF_LISTBLKD_NOT | EKF_ZSETBLKD_NOT |
                         EKF_STRMBLKD_NOT | EKF_MONITOR */
  PeerStats    peer_stats; /* retired sockets, updated in process_close() */

  int init_shm( EvShm &shm ) noexcept;

  void do_notify_sub( NotifySub &sub ) {
    for ( RouteNotify *p = this->notify_list.hd; p != NULL; p = p->next )
      p->on_sub( sub );
  }
  void do_notify_unsub( NotifySub &sub ) {
    for ( RouteNotify *p = this->notify_list.hd; p != NULL; p = p->next )
      p->on_unsub( sub );
  }
  void do_notify_resub( NotifySub &sub ) {
    for ( RouteNotify *p = this->notify_list.hd; p != NULL; p = p->next )
      p->on_resub( sub );
  }
  void do_notify_psub( NotifyPattern &pat ) {
    for ( RouteNotify *p = this->notify_list.hd; p != NULL; p = p->next )
      p->on_psub( pat );
  }
  void do_notify_punsub( NotifyPattern &pat ) {
    for ( RouteNotify *p = this->notify_list.hd; p != NULL; p = p->next )
      p->on_punsub( pat );
  }
  void do_notify_repsub( NotifyPattern &pat ) {
    for ( RouteNotify *p = this->notify_list.hd; p != NULL; p = p->next )
      p->on_repsub( pat );
  }
  void do_notify_bloom_ref( BloomRef &ref ) {
    for ( RouteNotify *p = this->notify_list.hd; p != NULL; p = p->next )
      p->on_bloom_ref( ref );
  }
  void do_notify_bloom_deref( BloomRef &ref ) {
    for ( RouteNotify *p = this->notify_list.hd; p != NULL; p = p->next )
      p->on_bloom_deref( ref );
  }
#if 0
  void resolve_collisions( NotifySub &sub,  RouteRef &rte ) noexcept;
  void resolve_pcollisions( NotifyPattern &pat,  RouteRef &rte ) noexcept;
#endif
  void add_sub( NotifySub &sub ) {
    RouteRef rte( *this, this->zip.route_spc[ SUB_RTE ] );
    if ( sub.hash_collision == 0 )
      sub.sub_count = this->add_route( SUB_RTE, sub.subj_hash, sub.src_fd, rte );
    else if ( ! this->notify_list.is_empty() )
      sub.sub_count = this->ref_route( SUB_RTE, sub.subj_hash, rte );
    else
      sub.sub_count = 1;
    /*if ( sub.sub_count > 1 )
      this->resolve_collisions( sub, rte );*/
    sub.ref = &rte;
    this->do_notify_sub( sub );
    sub.ref = NULL;
  }

  void del_sub( NotifySub &sub ) {
    RouteRef rte( *this, this->zip.route_spc[ SUB_RTE ] );
    if ( sub.hash_collision == 0 )
      sub.sub_count = this->del_route( SUB_RTE, sub.subj_hash, sub.src_fd, rte );
    else if ( ! this->notify_list.is_empty() )
      sub.sub_count = this->ref_route( SUB_RTE, sub.subj_hash, rte );
    else
      sub.sub_count = 0;
    /*if ( sub.sub_count > 0 )
      this->resolve_collisions( sub, rte );*/
    sub.ref = &rte;
    this->do_notify_unsub( sub );
    sub.ref = NULL;
  }

  void notify_sub( NotifySub &sub ) {
    this->do_notify_resub( sub );
  }

  void add_pat( NotifyPattern &pat ) {
    uint16_t prefix_len = (uint16_t) pat.cvt.prefixlen;
    RouteRef rte( *this, this->zip.route_spc[ prefix_len ] );
    if ( pat.hash_collision == 0 )
      pat.sub_count = this->add_route( prefix_len, pat.prefix_hash, pat.src_fd,
                                       rte );
    else if ( ! this->notify_list.is_empty() )
      pat.sub_count = this->ref_route( prefix_len, pat.prefix_hash, rte );
    else
      pat.sub_count = 1;
    /*if ( pat.sub_count > 1 )
      this->resolve_pcollisions( pat, rte );*/
    pat.ref = &rte;
    this->do_notify_psub( pat );
    pat.ref = NULL;
  }

  void del_pat( NotifyPattern &pat ) {
    uint16_t prefix_len = (uint16_t) pat.cvt.prefixlen;
    RouteRef rte( *this, this->zip.route_spc[ prefix_len ] );
    if ( pat.hash_collision == 0 )
      pat.sub_count = this->del_route( prefix_len, pat.prefix_hash, pat.src_fd,
                                       rte );
    else if ( ! this->notify_list.is_empty() )
      pat.sub_count = this->ref_route( prefix_len, pat.prefix_hash, rte ) - 1;
    else
      pat.sub_count = 0;
    /*if ( pat.sub_count > 0 )
      this->resolve_pcollisions( pat, rte );*/
    pat.ref = &rte;
    this->do_notify_punsub( pat );
    pat.ref = NULL;
  }

  void notify_pat( NotifyPattern &pat ) {
    this->do_notify_repsub( pat );
  }

  bool forward_msg( EvPublish &pub,  BPData *data = NULL ) noexcept;
  bool forward_with_cnt( EvPublish &pub,  uint32_t &rcnt,
                         BPData *data = NULL ) noexcept;
  bool forward_except( EvPublish &pub,  const BitSpace &fdexcpt,
                       BPData *data = NULL ) noexcept;
  bool forward_except_with_cnt( EvPublish &pub,  const BitSpace &fdexcpt,
                                uint32_t &rcnt,  BPData *data = NULL ) noexcept;
  bool forward_set_with_cnt( EvPublish &pub,  const BitSpace &fdset,
                             uint32_t &rcnt,  BPData *data = NULL ) noexcept;
  bool forward_some( EvPublish &pub, uint32_t *routes, uint32_t rcnt,
                     BPData *data = NULL ) noexcept;
  bool forward_not_fd( EvPublish &pub,  uint32_t not_fd,
                       BPData *data = NULL ) noexcept;
  bool forward_not_fd2( EvPublish &pub,  uint32_t not_fd,
                        uint32_t not_fd2,  BPData *data = NULL ) noexcept;
#if 0
  bool forward_all( EvPublish &pub, uint32_t *routes, uint32_t rcnt,
                    BPData *data = NULL ) noexcept;
#endif
  bool forward_set_no_route( EvPublish &pub,  const BitSpace &fdset ) noexcept;
  bool forward_set_no_route_not_fd( EvPublish &pub,  const BitSpace &fdset,
                                    uint32_t not_fd ) noexcept;
  bool forward_to( EvPublish &pub,  uint32_t fd, BPData *data = NULL ) noexcept;
  bool hash_to_sub( uint32_t r,  uint32_t h,  char *key,
                    size_t &keylen ) noexcept;
  void notify_reassert( uint32_t fd,  SubRouteDB &sub_db,
                        SubRouteDB &pat_db ) noexcept;
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

  RoutePublish( EvPoll &p,  const char *svc,  uint32_t svc_num,
                uint32_t rte_id ) noexcept;
};

/* callbacks cannot disappear between epochs, fd based connections that can
 * close between epochs should use fd based timers with unique timer ids */
struct EvTimerCallback {
  virtual bool timer_cb( uint64_t timer_id,  uint64_t event_id ) noexcept;
};

enum TimerUnits {
  IVAL_SECS   = 0,
  IVAL_MILLIS = 1,
  IVAL_MICROS = 2,
  IVAL_NANOS  = 3
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
  bool add_timer_units( EvTimerCallback &tcb,  uint32_t val,  TimerUnits units,
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
  bool add_timer_units( int32_t id,  uint32_t val,  TimerUnits units,
                        uint64_t timer_id,  uint64_t event_id ) noexcept;
  bool remove_timer( int32_t id,  uint64_t timer_id,
                     uint64_t event_id ) noexcept;
  bool remove_timer_cb( EvTimerCallback &tcb,  uint64_t timer_id,
                        uint64_t event_id ) noexcept;
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
  RoutePublish & get_service( const char *svc,  uint32_t svc_num,
                              uint32_t rte_id ) noexcept;
};

}
}
#endif
