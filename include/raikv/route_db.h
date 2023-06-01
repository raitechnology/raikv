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

struct RouteSpace : ArraySpace<uint32_t, 128> {};

struct RouteSpaceRef : public RouteSpace {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  bool used;
  RouteSpaceRef() : used( false ) {}
};

struct RouteSpaceTemp : public ArraySpace<RouteSpaceRef *, 4> {};

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

struct QueueRef {
  uint32_t r;
  uint32_t refcnt;
  bool operator==( uint32_t i ) const { return i == this->r; }
};
/* find r in routes[] */
uint32_t bsearch_route( uint32_t r, const uint32_t *routes, uint32_t size ) noexcept;
uint32_t bsearch_route( uint32_t r, const QueueRef *routes, uint32_t size ) noexcept;
/* insert r in routes[] if not already there */
uint32_t insert_route( uint32_t r, uint32_t *routes, uint32_t rcnt ) noexcept;
/* delete r from routes[] if found */
uint32_t delete_route( uint32_t r, uint32_t *routes, uint32_t rcnt ) noexcept;
/* merge arrays */
uint32_t merge_route( uint32_t *routes,  uint32_t count,
                      const uint32_t *merge,  uint32_t mcount ) noexcept;
uint32_t merge_route2( uint32_t *dest,  const uint32_t *routes,  uint32_t count,
                       const uint32_t *merge,  uint32_t mcount ) noexcept;
uint32_t merge_route2( QueueRef *dest,  const QueueRef *routes,  uint32_t count,
                       const QueueRef *merge,  uint32_t mcount ) noexcept;
uint32_t merge_queue( QueueRef *routes,  uint32_t count,
                      const uint32_t *merge,  uint32_t mcount ) noexcept;
uint32_t merge_queue2( QueueRef *routes,  uint32_t count,
                       const QueueRef *merge,  uint32_t mcount ) noexcept;

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

static const uint16_t MAX_PRE      = 64, /* wildcard prefix routes 0 -> 63 */
                      SUB_RTE      = 64, /* non-wildcard routes 64 */
                      MAX_RTE      = 65,
                      NO_RTSPC_REF = 0xffffU;
/* Use DeltaCoder to compress routes, manage hash -> route list */
struct RouteZip {
  UIntHashTab  * zht;             /* code ref hash -> code_buf.ptr[] offset */
  size_t         code_end,        /* end of code_buf.ptr[] list */
                 code_free;       /* amount free between 0 -> code_end */
  RouteSpace     code_buf,        /* space for code ref db */
                 zroute_spc;      /* space for compressing */
  uint64_t       route_free;      /* bits indicating route_spc[] free */
  RouteSpace     route_temp[ 64 ];/* working space for merging routes */
  RouteSpaceTemp extra_temp;      /* if run out of route_temp[] space */

  RouteZip() noexcept;
  RouteSpace & ref_route_spc( uint16_t prefix_len,  uint16_t &ref ) {
    for ( ; ; prefix_len++ ) {
      ref = prefix_len % 64;
      uint64_t mask = (uint64_t) 1 << ref;
      if ( ( this->route_free & mask ) == 0 ) {
        this->route_free |= mask;
        return this->route_temp[ ref ];
      }
      if ( prefix_len >= 128 )
        return this->create_extra_spc( ref );
    }
  }
  RouteSpace & route_spc( uint16_t prefix_len ) {
    return this->route_temp[ prefix_len % 64 ];
  }
  void release_route_spc( uint16_t ref ) {
    if ( ref < 64 )
      this->route_free &= ~( (uint64_t) 1 << ref );
    else if ( ref != NO_RTSPC_REF )
      this->release_extra_spc( ref );
  }
  RouteSpace &create_extra_spc( uint16_t &ref ) noexcept;
  void release_extra_spc( uint16_t ref ) noexcept;
  void init( void ) noexcept;
  void reset( void ) noexcept;

  uint32_t *make_code_ref_space( uint32_t ecnt,  size_t &off ) noexcept;
  uint32_t make_code_ref( uint32_t *code,  uint32_t ecnt,
                          uint32_t rcnt ) noexcept;
  void gc_code_ref_space( void ) noexcept;
  uint32_t decompress_one( uint32_t val ) noexcept;
};

struct RouteRef {
  RouteZip   & zip;       /* zipped routes */
  RouteSpace & route_spc; /* space to unzip and modify route arrays */
  CodeRefPtr   ptr;       /* if route is updated, no longer need old route */
  uint32_t   * routes,    /* the set of routes allocated from route_spc */
               rcnt;      /* num elems in routes[] */
  uint16_t     spc_ref;

  RouteRef( RouteZip &z,  uint16_t prefix_len )
    : zip( z ), route_spc( z.ref_route_spc( prefix_len, this->spc_ref ) ),
      routes( 0 ), rcnt( 0 ) {}
  ~RouteRef() {
    this->zip.release_route_spc( this->spc_ref );
  }

  void copy( const uint32_t *a,  uint32_t acnt ) {
    this->routes = this->route_spc.make( acnt );
    this->rcnt   = acnt;
    for ( uint32_t i = 0; i < acnt; i++ )
      this->routes[ i ] = a[ i ];
  }
  void merge( const uint32_t *b, uint32_t bcnt ) {
    this->routes = this->route_spc.make( this->rcnt + bcnt );
    this->rcnt   = merge_route( this->routes, this->rcnt, b, bcnt );
  }
#if 0
  void merge2( const uint32_t *a,  uint32_t acnt,  const uint32_t *b,
               uint32_t bcnt ) {
    this->routes = this->route_spc.make( acnt + bcnt );
    this->rcnt   = merge_route2( this->routes, a, acnt, b, bcnt );
  }
#endif
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
  void deref_coderef( void ) noexcept;    /* deref code ref, could gc */
};

struct QueueName {
  const char * queue;
  uint32_t     queue_len,
               queue_hash,
               refs,
               idx;
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  QueueName( const char *q,  uint32_t qlen,  uint32_t qhash ) :
    queue( q ), queue_len( qlen ), queue_hash( qhash ), refs( 0 ), idx( 0 ) {}
  bool equals( const QueueName &qn ) const {
    if ( qn.queue_hash == this->queue_hash ) {
      if ( qn.queue_len == 0 ||
           ( qn.queue_len == this->queue_len &&
             ::memcmp( qn.queue, this->queue, qn.queue_len ) == 0 ) )
        return true;
    }
    return false;
  }
};

typedef ArrayCount<QueueName *, 4> QueueNameArray;

struct QueueNameDB {
  char         * queue_str;
  size_t         queue_strlen;
  QueueNameArray queue_name;
  UIntHashTab  * q_ht;

  QueueNameDB() : queue_str( 0 ), queue_strlen( 0 ), q_ht( 0 ) {}
  QueueName *get_queue_name( const QueueName &qn ) noexcept;
  QueueName *get_queue_str( const char *name,  size_t len ) noexcept;
  QueueName *get_queue_hash( uint32_t q_hash ) noexcept;
  QueueName *get_queue_idx( uint32_t idx ) {
    return this->queue_name.ptr[ idx ];
  }
};
/* Map a subscription hash to a route list using RouteZip */
struct BloomDB : public ArrayCount<BloomRef *, 128> {
  BallocList<8, 16*1024> bloom_mem;
  QueueNameDB q_db;
};

struct RouteDB;
struct RouteRefCount {
  uint64_t ref_mask;
  uint32_t cache_ref,
           extra_cnt;
  uint64_t ref_extra[ 8 ];
  RouteRefCount() : ref_mask( 0 ), cache_ref( 0 ), extra_cnt( 0 ) {}

  void add_ref( RouteRef &ref ) {
    if ( ref.spc_ref < 64 )
      this->ref_mask |= (uint64_t) 1 << ref.spc_ref;
    else
      this->set_ref_extra( ref.spc_ref );
    ref.spc_ref = NO_RTSPC_REF;
  }
  void add_refs( RouteRefCount &refs ) {
    this->ref_mask |= refs.ref_mask;
    this->cache_ref += refs.cache_ref;
    if ( refs.extra_cnt != 0 )
      this->merge_ref_extra( refs );
  }
  void set_ref_extra( uint16_t spc_ref ) noexcept;
  void merge_ref_extra( RouteRefCount &refs ) noexcept;
  void deref( RouteDB &rdb ) noexcept;
};

struct RouteLookup {
  const char  * sub;        /* find routes for subject */
  uint16_t      sublen;     /* length of sub */
  uint32_t    * routes;     /* routes found for a prefix */
  QueueRef    * qroutes;
  uint64_t      mask;       /* current get_route prefix mask */
  uint32_t      rcount,     /* count of routes */
                subj_hash,  /* hash of sub */
                shard,      /* which shard the route lookup is using */
                prefix_cnt, /* how many prefixes are matched */
                queue_hash;
  size_t        keylen[ MAX_PRE ]; /* length of prefix */
  uint32_t      hash[ MAX_PRE ];   /* hash of prefix */
  uint64_t      prefix_mask;/* all prefix mask bits */
  RouteRefCount rte_ref;    /* route, rcount ref counters */

  RouteLookup( const char *s,  uint16_t slen,  uint32_t h,  uint32_t sh )
    : sub( s ), sublen( slen ), routes( 0 ),  qroutes( 0 ), mask( 0 ),
      rcount( 0 ), subj_hash( h ), shard( sh ), prefix_cnt( 0 ),
      queue_hash( 0 ), prefix_mask( 0 ) {}

  void add_ref( RouteRef &ref ) { this->rte_ref.add_ref( ref ); }
  void deref( RouteDB &rdb )  { this->rte_ref.deref( rdb ); }
  void setup_prefix_hash( uint64_t pat_mask ) noexcept;
};

static inline bool test_prefix_mask( uint64_t mask,  uint16_t prefix_len ) {
  return ( mask & ( (uint64_t) 1 << prefix_len ) ) != 0;
}

struct BloomGroup {
  RouteZip & zip;                   /* route ids compressed */
  BloomList  list;                  /* list of bloom filters*/
  uint64_t   mask;                  /* bloom filter subject prefixes */
  uint32_t   pref_count[ MAX_RTE ]; /* count of prefix subjects */

  BloomGroup( RouteZip &z ) : zip( z ), mask( 0 ) {
    ::memset( this->pref_count, 0, sizeof( this->pref_count ) );
  }
  bool get_route( RouteLookup &look ) noexcept;
  bool get_route2( RouteLookup &look, uint16_t prefix_len,
                   uint32_t hash ) noexcept;
  void get_queue( RouteLookup &look ) noexcept;
  void get_queue2( RouteLookup &look,  uint16_t prefix_len,
                   uint32_t hash ) noexcept;
};

struct RouteGroup {
  static uint32_t pre_seed[ MAX_PRE ]; /* hash seeds for each prefix */
  RouteCache  & cache;                 /* the route arrays indexed by ht */
  RouteZip    & zip;                   /* route ids compressed */
  UIntHashTab * rt_hash[ MAX_RTE ];    /* ht for each prefix */
  uint32_t      group_num,             /* which route group number */
                entry_count;           /* count of rt_hash subjects */
  uint64_t      rt_mask;               /* subjects subscribed mask */
  BloomGroup  & bloom;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void * ) {}
  RouteGroup( RouteCache &c,  RouteZip &z,  BloomGroup &b,  uint32_t gn ) noexcept;
  /* modify the bits of the prefixes used */
  void add_prefix_len( uint16_t prefix_len,  bool is_rt_hash ) noexcept;
  void del_prefix_len( uint16_t prefix_len,  bool is_rt_hash ) noexcept;
  uint64_t pat_mask( void ) const { return this->bloom.mask | this->rt_mask; }

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
  void get_route( uint16_t prefix_len,  uint32_t hash,
                  RouteLookup &look ) {
    uint32_t val;
    size_t   pos;
    look.rcount = 0;
    look.routes = NULL;
    look.mask   = (uint64_t) 1 << prefix_len;
    if ( this->cache_find( prefix_len, hash, look.routes, look.rcount,
                           look.shard, pos ) ) {
      this->cache.busy++;
      look.rte_ref.cache_ref++;
      return;
    }
    if ( look.shard == 0 && ( this->rt_mask & look.mask ) != 0 ) {
      if ( this->rt_hash[ prefix_len ]->find( hash, pos, val ) )
        return this->get_route_slow2( look, prefix_len, hash, val );
    }
    return this->get_bloom_route2( look, prefix_len, hash );
  }
  void get_sub_route( RouteLookup &look ) {
    size_t   pos;
    uint32_t val;
    look.rcount = 0;
    look.routes = NULL;
    look.mask   = 0;
    if ( this->cache_find( SUB_RTE, look.subj_hash, look.routes, look.rcount,
                           look.shard, pos ) ) {
      this->cache.busy++;
      look.rte_ref.cache_ref++;
      return;
    }
    if ( look.shard == 0 ) {
      if ( this->rt_hash[ SUB_RTE ]->find( look.subj_hash, pos, val ) )
        return this->get_route_slow( look, val );
    }
    return this->get_bloom_route( look );
  }
  void get_route_slow( RouteLookup &look,  uint32_t val ) noexcept;
  void get_bloom_route( RouteLookup &look ) noexcept;
  void get_route_slow2( RouteLookup &look,  uint16_t prefix_len,  uint32_t hash,
                        uint32_t val ) noexcept;
  void get_bloom_route2( RouteLookup &look,  uint16_t prefix_len,
                         uint32_t hash ) noexcept;
  uint32_t get_route_count( uint16_t prefix_len,  uint32_t hash ) noexcept;

  uint32_t get_sub_route_count( uint32_t hash ) {
    return this->get_route_count( SUB_RTE, hash );
  }
  uint32_t prefix_seed( size_t prefix_len ) const {
    if ( prefix_len < MAX_PRE )
      return this->pre_seed[ prefix_len ];
    return this->pre_seed[ MAX_PRE - 1 ];
  }
  bool cache_find( uint16_t prefix_len,  uint32_t hash,
                   uint32_t *&routes,  uint32_t &rcnt,
                   uint32_t shard,  size_t &pos ) {
    if ( ! this->cache.is_invalid ) {
      if ( ! this->cache.busy && this->cache.need )
        this->cache_need();
      uint64_t h = ( (uint64_t) this->group_num << 48 ) |
                   ( (uint64_t) shard << 40 ) |
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

  void cache_need( void ) noexcept;
};

struct QueueDB {
  RouteGroup * route_group;
  QueueName  * q_name;
  uint32_t     next_route;

  bool equals( const char *q,  uint32_t qlen,  uint32_t qhash ) const {
    QueueName qn( q, qlen, qhash );
    return this->q_name->equals( qn );
  }
  void init( RouteCache &c,  RouteZip &z,  BloomGroup &b,  QueueName *qn,
             uint32_t gn ) noexcept;
};

struct RouteDB : public RouteGroup {
  RouteCache             cache;
  RouteZip               zip;
  BloomGroup             bloom_grp;
  BloomDB              & g_bloom_db;
  ArrayCount<QueueDB, 4> queue_db;
  UIntHashTab          * q_ht;

  RouteDB( BloomDB &g_db ) noexcept;

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
  RouteGroup &get_queue_group( const QueueName &qn ) noexcept;
  RouteGroup &get_queue_group( const char *queue,  uint32_t queue_len,
                               uint32_t queue_hash ) {
    QueueName qn( queue, queue_len, queue_hash );
    return this->get_queue_group( qn );
  }
};

struct SuffixMatch {
  uint32_t hash;  /* hash of suffix */
  uint16_t len;   /* len of suffix, ex. 7: SUB.*.SUFFIX */
};
struct ShardMatch {
  uint32_t start, /* start hash */
           end;   /* end hash, inclusive, start <= hash <= end */
};
struct QueueMatch {
  uint32_t qhash, /* hash of queue */
           refcnt,/* how many subscribers */
           xhash;
  static uint32_t hash2( const char *sub,  uint16_t sublen,  uint32_t seed ) {
    return seed ^ kv_djb( sub, sublen );
  }
};
enum DetailType {
  NO_DETAIL    = 0,
  SUFFIX_MATCH = 1,
  SHARD_MATCH  = 2,
  QUEUE_MATCH  = 3
};

struct BloomDetailHash {
  uint32_t hash;
  uint16_t prefix_len;
};

struct BloomDetail {
  uint32_t hash;
  uint16_t prefix_len,
           detail_type;
  union {
    SuffixMatch suffix; /* match wildcard suffix */
    ShardMatch  shard;  /* match shard hash */
    QueueMatch  queue;
  } u;
  void copy( const BloomDetail &det ) {
    this->hash        = det.hash;
    this->prefix_len  = det.prefix_len;
    this->detail_type = det.detail_type;
    if ( det.detail_type == SUFFIX_MATCH )
      this->u.suffix = det.u.suffix;
    else if ( det.detail_type == SHARD_MATCH )
      this->u.shard = det.u.shard;
    else if ( det.detail_type == QUEUE_MATCH )
      this->u.queue = det.u.queue;
  }
  void init_suffix( const SuffixMatch &match ) {
    this->detail_type = SUFFIX_MATCH;
    this->u.suffix    = match;
  }
  void init_shard( const ShardMatch &match ) {
    this->detail_type = SHARD_MATCH;
    this->u.shard     = match;
  }
  void init_queue( const QueueMatch &match ) {
    this->detail_type = QUEUE_MATCH;
    this->u.queue     = match;
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
  bool queue_equals( const QueueMatch &match ) const {
    return this->detail_type   == QUEUE_MATCH &&
           this->u.queue.qhash == match.qhash &&
           this->u.queue.xhash == match.xhash;
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
    if ( this->detail_type == QUEUE_MATCH )
      return false;
    return true;
  }
  static bool shard_endpoints( uint32_t shard,  uint32_t nshards,
                               uint32_t &start,  uint32_t &end ) noexcept;
  bool from_pattern( const PatternCvt &cvt ) noexcept;
};

struct BloomMatchArgs;
struct BloomRef {
  BloomBits   * bits;
  BloomRoute ** links;
  BloomDetail * details;
  uint64_t      pref_mask,
                detail_mask;
  uint32_t      nlinks,
                ndetails,
                pref_count[ MAX_RTE ],
                ref_num,
                queue_cnt;
  BloomDB     & bloom_db;
  bool          sub_detail;
  char          name[ 31 ];

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
  BloomDetail & add_detail( uint16_t prefix_len,  uint32_t hash ) noexcept;
  bool add_shard_route( uint16_t prefix_len,  uint32_t hash,
                        const ShardMatch &match ) noexcept;
  bool add_suffix_route( uint16_t prefix_len,  uint32_t hash,
                         const SuffixMatch &match ) noexcept;
  bool add_queue_route( uint16_t prefix_len,  uint32_t hash,
                        const QueueMatch &match ) noexcept;
  void del_route( uint16_t prefix_len,  uint32_t hash ) noexcept;
  template <class Match>
  void del_detail( uint16_t prefix_len,  uint32_t hash,  const Match &match,
               bool ( BloomDetail::*equals )( const Match &m ) const ) noexcept;
  void del_shard_route( uint16_t prefix_len,  uint32_t hash,
                        const ShardMatch &match ) noexcept;
  void del_suffix_route( uint16_t prefix_len,  uint32_t hash,
                         const SuffixMatch &match ) noexcept;
  void del_queue_route( uint16_t prefix_len,  uint32_t hash,
                        const QueueMatch &match ) noexcept;
  void update_route( const uint32_t *pref_count,  BloomBits *bits,
                     BloomDetail *details,  uint32_t ndetails ) noexcept;
  /*void notify_update( void ) noexcept;*/

  bool add( uint32_t hash ) {
    return this->add_route( SUB_RTE, hash );
  }
  void del( uint32_t hash ) {
    return this->del_route( SUB_RTE, hash );
  }
  void encode( BloomCodec &code ) noexcept;
  bool decode( const void *data,  size_t datalen,
               QueueNameArray &q_ar ) noexcept;
  template <class Match>
  bool detail_matches( Match &look,  uint16_t prefix_len,
                       uint32_t hash,  bool &has_detail ) const noexcept;
  bool queue_matches( RouteLookup &look,  uint16_t prefix_len,  uint32_t hash,
                      QueueMatch &m ) const noexcept;
};

struct BloomMatchArgs {
  const char * sub;
  uint32_t     subj_hash,
               queue_hash;
  uint16_t     sublen;

  BloomMatchArgs( uint32_t h, const char *s,  uint16_t l )
    : sub( s ), subj_hash( h ), queue_hash( 0 ), sublen( l ) {
    if ( h == 0 )
      this->subj_hash = kv_crc_c( this->sub, this->sublen, 0 );
  }
};

struct BloomMatch {
  uint64_t pref_mask;
  uint16_t max_pref;
  uint32_t pref_hash[ MAX_PRE ];

  bool match_sub( BloomMatchArgs &args,  const BloomRef &bloom ) {
    return this->sub_prefix( args, bloom ) != MAX_RTE;
  }
  uint16_t sub_prefix( BloomMatchArgs &args,  const BloomRef &bloom ) {
    uint16_t test = this->test_prefix( args, bloom, SUB_RTE );
    for ( uint16_t prefix_len = 0; ; prefix_len++ ) {
      if ( test != MAX_RTE || prefix_len == this->max_pref )
        return test;
      test = this->test_prefix( args, bloom, prefix_len );
    }
  }
  uint16_t test_prefix( BloomMatchArgs &args,  const BloomRef &bloom,
                        uint16_t prefix_len ) noexcept;

  uint32_t get_prefix_hash( BloomMatchArgs &args,  uint16_t prefix_len ) {
    if ( ! test_prefix_mask( this->pref_mask, prefix_len ) ) {
      this->pref_mask |= (uint64_t) 1 << prefix_len;
      this->pref_hash[ prefix_len ] = kv_crc_c( args.sub, prefix_len,
                                           RouteGroup::pre_seed[ prefix_len ] );
    }
    return this->pref_hash[ prefix_len ];
  }
  void init_match( uint16_t sublen ) {
    this->max_pref  = min_int<uint16_t>( sublen + 1, MAX_PRE );
    this->pref_mask = 0;
  }
  static size_t match_size( uint16_t sublen ) {
    uint16_t max_pref = min_int<uint16_t>( sublen + 1, MAX_PRE );
    size_t   len = sizeof( BloomMatch ) -
                   ( sizeof( uint32_t ) * ( MAX_PRE - max_pref ) );
    return align<size_t>( len, 8 );
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
  uint32_t     in_list,     /* whether in bloom_list and what shard */
               queue_cnt;
  uint8_t      sub_detail;
  bool         has_subs,
               is_invalid;  /* whether to recalculate masks after sub ob */

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void * ) {} /* no delete, in rdb.bloom_mem */

  BloomRoute( uint32_t fd,  RouteDB &db,  uint32_t lst )
    : next( 0 ), back( 0 ), rdb( db ), bloom( 0 ), r( fd ), nblooms( 0 ),
      pref_mask( 0 ), detail_mask( 0 ), in_list( lst ),
      queue_cnt( 0 ), sub_detail( 0 ), has_subs( false ), is_invalid( true ) {}

  void add_bloom_ref( BloomRef *ref ) noexcept;
  BloomRef *del_bloom_ref( BloomRef *ref ) noexcept;
  void invalid( void ) noexcept;
  void update_masks( void ) noexcept;
  void remove_if_empty( void ) {
    if ( this->nblooms == 0 )
      this->rdb.remove_bloom_route( this );
  }
};

struct EvPoll;
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
};

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
  size_t len63 = ( len < 63 ? len : 63 );
  if ( s != NULL ) ::memcpy( buf, s, len63 );
  ::memset( &buf[ len63 ], 0, 63 - len63 );
  buf[ 63 ] = (char) (uint8_t) ( len63 < 63 ? len63 : 0 );
}

static inline size_t
get_strlen64( const char *buf )
{
  if ( buf[ 0 ] == '\0' ) return 0;
  if ( buf[ 63 ] == '\0' ) return 63;
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

struct PeerId {
  uint64_t start_ns; /* start time */
  int32_t  fd;
  uint32_t route_id; /* upper layer route */

  bool equals( const PeerId &pid ) const {
    return pid.start_ns == this->start_ns &&
           pid.fd       == this->fd       &&
           pid.route_id == this->route_id;
  }
};

struct PeerData : public PeerId {
  EvSocket   * next,       /* dbl link list, active and free list */
             * back;
  uint64_t     active_ns,  /* last write time */
               read_ns;    /* last read time */
  const char * kind;       /* what protocol type */
  char         name[ 64 ]; /* name assigned to peer */
  PeerAddrStr  peer_address; /* ip4 1.2.3.4:p, ip6 [ab::cd]:p, other */

  PeerData() : next( 0 ), back( 0 ) {
    this->init_peer( 0, -1, -1, NULL, NULL );
  }
  /* no address, attached directly to shm */
  void init_ctx( uint64_t ns,  int fildes,  uint32_t rte_id,  uint32_t ctx_id,
                 const char *k ) {
    this->init_peer( ns, fildes, rte_id, NULL, k );
    this->peer_address.init_ctx( ctx_id );
  }
  void set_addr( const struct sockaddr *sa ) {
    this->peer_address.set_address( sa );
  }
  /* connected via ip address */
  void init_peer( uint64_t ns,  int32_t fildes,  uint32_t rte_id,
                  const sockaddr *sa,  const char *k ) {
    this->start_ns   = ns;
    this->fd         = fildes;
    this->route_id   = rte_id;
    this->active_ns  = 0;
    this->read_ns    = 0;
    this->kind       = k;
    this->name[ 0 ]  = '\0';
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

enum {
  NOTIFY_ADD_SUB  = 0,
  NOTIFY_REM_SUB  = 1,
  NOTIFY_ADD_REF  = 2,
  NOTIFY_REM_REF  = 3,
  NOTIFY_IS_QUEUE = 4
};

static inline uint8_t get_notify_type( uint8_t notify_type ) {
  return ( notify_type & 0x3 );
}
static inline bool is_notify_queue( uint8_t notify_type ) {
  return ( notify_type & NOTIFY_IS_QUEUE ) != 0;
}

struct NotifySub {
  const char   * subject,
               * reply;
  uint16_t       subject_len,
                 reply_len;
  uint32_t       subj_hash,
                 sub_count;
  const PeerId & src;
  RouteRef     * refp;
  BloomRef     * bref;
  uint8_t        hash_collision;
  char           src_type;  /* C:console, M:session, V:rv, 'R:redis, K:kv */
  uint8_t        notify_type; /* addsub, addref, remref, remsub */

  NotifySub( const char *s,  size_t slen, const char *r,  size_t rlen,
             uint32_t shash,  bool coll,  char t,  const PeerId &source ) :
    subject( s ), reply( r ), subject_len( (uint16_t) slen ),
    reply_len( (uint16_t) rlen ), subj_hash( shash ),
    sub_count( 0 ), src( source ), refp( 0 ), bref( 0 ), hash_collision( coll ),
    src_type( t ), notify_type( 0 ) {}

  NotifySub( const char *s,  size_t slen, uint32_t shash,  bool coll,
             char t,  const PeerId &source ) :
    subject( s ), reply( 0 ), subject_len( (uint16_t) slen ), reply_len( 0 ),
    subj_hash( shash ), sub_count( 0 ), src( source ), refp( 0 ),
    bref( 0 ), hash_collision( coll ), src_type( t ), notify_type( 0 ) {}
};

struct NotifyQueue : public NotifySub {
  const char * queue;
  uint16_t     queue_len;
  uint32_t     queue_hash;

  NotifyQueue( const char *s,  size_t slen, const char *r,  size_t rlen,
               uint32_t shash,  bool coll,  char t,  const PeerId &source,
               const char *q,  size_t qlen,  uint32_t qhash ) :
    NotifySub( s, slen, r, rlen, shash, coll, t, source ),
    queue( q ), queue_len( qlen ), queue_hash( qhash ) {}
};

struct NotifyPattern {
  const PatternCvt & cvt;
  const char       * pattern,
                   * reply;
  uint16_t           pattern_len,
                     reply_len;
  uint32_t           prefix_hash,
                     sub_count;
  const PeerId     & src;
  RouteRef         * refp;
  BloomRef         * bref;
  uint8_t            hash_collision;
  char               src_type;  /* C:console, M:session, V:rv, 'R:redis, K:kv */
  uint8_t            notify_type; /* addsub, addref, remref, remsub */

  NotifyPattern( const PatternCvt &c,
                 const char *s,  size_t slen,  const char *r,  size_t rlen,
                 uint32_t phash,  bool coll,  char t,  const PeerId &source ) :
    cvt( c ), pattern( s ), reply( r ), pattern_len( (uint16_t) slen ),
    reply_len( (uint16_t) rlen ), prefix_hash( phash ),
    sub_count( 0 ), src( source ), refp( 0 ), bref( 0 ), hash_collision( coll ),
    src_type( t ), notify_type( 0 ) {}

  NotifyPattern( const PatternCvt &c,  const char *s,  size_t slen,
                 uint32_t phash,  bool coll,  char t,  const PeerId &source ) :
    cvt( c ), pattern( s ), reply( 0 ), pattern_len( (uint16_t) slen ),
    reply_len( 0 ), prefix_hash( phash ), sub_count( 0 ),
    src( source ), refp( 0 ), bref( 0 ), hash_collision( coll ),
    src_type( t ), notify_type( 0 ) {}
};

struct NotifyPatternQueue : public NotifyPattern {
  const char * queue;
  uint16_t     queue_len;
  uint32_t     queue_hash;

  NotifyPatternQueue( const PatternCvt &c,  const char *s,  size_t slen,
                      uint32_t phash,  bool coll,  char t,
                      const PeerId &source,
                      const char *q,  size_t qlen,  uint32_t qhash ) :
    NotifyPattern( c, s, slen, phash, coll, t, source ),
    queue( q ), queue_len( qlen ), queue_hash( qhash ) {}
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

  virtual void on_sub_q( NotifyQueue &sub ) noexcept;
  virtual void on_resub_q( NotifyQueue &sub ) noexcept;
  virtual void on_unsub_q( NotifyQueue &sub ) noexcept;

  virtual void on_psub_q( NotifyPatternQueue &pat ) noexcept;
  virtual void on_repsub_q( NotifyPatternQueue &pat ) noexcept;
  virtual void on_punsub_q( NotifyPatternQueue &pat ) noexcept;

  virtual void on_reassert( uint32_t fd,  SubRouteDB &sub_db,
                            SubRouteDB &pat_db ) noexcept;
  virtual void on_bloom_ref( BloomRef &ref ) noexcept;
  virtual void on_bloom_deref( BloomRef &ref ) noexcept;
};

struct KvPubSub; /* manages pubsub through kv shm */
struct HashTab;  /* shm ht */
struct EvShm;    /* shm context */
struct EvPublish;
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
  /* BloomRef */
  void do_notify_bloom_ref( BloomRef &ref ) {
    for ( RouteNotify *p = this->notify_list.hd; p != NULL; p = p->next )
      p->on_bloom_ref( ref );
  }
  void do_notify_bloom_deref( BloomRef &ref ) {
    for ( RouteNotify *p = this->notify_list.hd; p != NULL; p = p->next )
      p->on_bloom_deref( ref );
  }
  typedef uint32_t (RouteGroup::*mod_route_t)( uint16_t, uint32_t, uint32_t,
                                               RouteRef & );
  template <class Notify>
  void do_notify( RouteGroup &grp,  uint16_t prefix_len, uint32_t prefix_hash,
                  Notify &sub,  int type,  mod_route_t mod_route,
                  void (RouteNotify::*cb)( Notify & ) ) {
    RouteRef rte( grp.zip, prefix_len );
    if ( mod_route != NULL && sub.hash_collision == 0 )
      sub.sub_count = (grp.*mod_route)( prefix_len, prefix_hash,
                                        sub.src.fd, rte );
    if ( ! this->notify_list.is_empty() ) {
      if ( mod_route != NULL && sub.hash_collision != 0 )
        sub.sub_count = grp.ref_route( prefix_len, prefix_hash, rte );
      sub.refp = &rte;
      sub.notify_type = type;
      for ( RouteNotify *p = this->notify_list.hd; p != NULL; p = p->next )
        (p->*cb)( sub );
      sub.refp = NULL;
    }
  }
  /* NotifySub */
  void do_snotify( NotifySub &sub,  int type,  mod_route_t mod_route,
                   void (RouteNotify::*cb)( NotifySub & ) ) {
    this->do_notify( *this, SUB_RTE, sub.subj_hash, sub, type, mod_route, cb );
  }
  void add_sub( NotifySub &sub ) {
    sub.sub_count = 1;
    this->do_snotify( sub, NOTIFY_ADD_SUB, &RouteGroup::add_route,
                      &RouteNotify::on_sub );
  }
  void del_sub( NotifySub &sub ) {
    sub.sub_count = 0;
    this->do_snotify( sub, NOTIFY_REM_SUB, &RouteGroup::del_route,
                      &RouteNotify::on_unsub );
  }
  void do_notify_sub( NotifySub &sub ) {
    sub.sub_count = 1;
    this->do_snotify( sub, NOTIFY_ADD_SUB, NULL, &RouteNotify::on_sub );
  }
  void do_notify_unsub( NotifySub &sub ) {
    sub.sub_count = 0;
    this->do_snotify( sub, NOTIFY_REM_SUB, NULL, &RouteNotify::on_unsub );
  }
  void do_notify_sub_q( NotifyQueue &sub ) {
    this->do_qnotify( sub, NOTIFY_ADD_SUB, NULL, &RouteNotify::on_sub_q );
  }
  void do_notify_unsub_q( NotifyQueue &sub ) {
    this->do_qnotify( sub, NOTIFY_REM_SUB, NULL, &RouteNotify::on_unsub_q );
  }
  void do_notify_psub_q( NotifyPatternQueue &pat ) {
    this->do_pqnotify( pat, NOTIFY_ADD_SUB, NULL, &RouteNotify::on_psub_q );
  }
  void do_notify_punsub_q( NotifyPatternQueue &pat ) {
    this->do_pqnotify( pat, NOTIFY_REM_SUB, NULL, &RouteNotify::on_punsub_q );
  }
  void notify_sub( NotifySub &sub ) {
    this->do_snotify( sub, NOTIFY_ADD_REF, NULL, &RouteNotify::on_resub );
  }
  void notify_unsub( NotifySub &sub ) {
    this->do_snotify( sub, NOTIFY_REM_REF, NULL, &RouteNotify::on_resub );
  }
  /* NotifyPattern */
  void do_pnotify( NotifyPattern &pat,  int type,  mod_route_t mod_route,
                   void (RouteNotify::*cb)( NotifyPattern & ) ) {
    uint16_t prefix_len = (uint16_t) pat.cvt.prefixlen;
    this->do_notify( *this, prefix_len, pat.prefix_hash, pat, type,
                     mod_route, cb );
  }
  void add_pat( NotifyPattern &pat ) {
    pat.sub_count = 1;
    this->do_pnotify( pat, NOTIFY_ADD_SUB, &RouteGroup::add_route,
                      &RouteNotify::on_psub );
  }
  void del_pat( NotifyPattern &pat ) {
    pat.sub_count = 0;
    this->do_pnotify( pat, NOTIFY_REM_SUB, &RouteGroup::del_route,
                      &RouteNotify::on_punsub );
  }
  void do_notify_psub( NotifyPattern &pat ) {
    pat.sub_count = 1;
    this->do_pnotify( pat, NOTIFY_ADD_SUB, NULL, &RouteNotify::on_psub );
  }
  void do_notify_punsub( NotifyPattern &pat ) {
    pat.sub_count = 0;
    this->do_pnotify( pat, NOTIFY_REM_SUB, NULL, &RouteNotify::on_punsub );
  }
  void notify_pat( NotifyPattern &pat ) {
    this->do_pnotify( pat, NOTIFY_ADD_REF, NULL, &RouteNotify::on_repsub );
  }
  void notify_unpat( NotifyPattern &pat ) {
    this->do_pnotify( pat, NOTIFY_REM_REF, NULL, &RouteNotify::on_repsub );
  }
  /* NotifyQueue */
  void do_qnotify( NotifyQueue &sub,  int type,  mod_route_t mod_route,
                   void (RouteNotify::*cb)( NotifyQueue & ) ) {
    RouteGroup & grp = this->get_queue_group( sub.queue, sub.queue_len,
                                              sub.queue_hash );
    this->do_notify( grp, SUB_RTE, sub.subj_hash, sub,
                     type | NOTIFY_IS_QUEUE, mod_route, cb );
  }
  void add_sub_queue( NotifyQueue &sub ) {
    sub.sub_count = 1;
    this->do_qnotify( sub, NOTIFY_ADD_SUB, &RouteGroup::add_route,
                      &RouteNotify::on_sub_q );
  }
  void del_sub_queue( NotifyQueue &sub ) {
    sub.sub_count = 0;
    this->do_qnotify( sub, NOTIFY_REM_SUB, &RouteGroup::del_route,
                      &RouteNotify::on_unsub_q );
  }
  void notify_sub_queue( NotifyQueue &sub ) {
    this->do_qnotify( sub, NOTIFY_ADD_REF, NULL, &RouteNotify::on_resub_q );
  }
  void notify_unsub_queue( NotifyQueue &sub ) {
    this->do_qnotify( sub, NOTIFY_REM_REF, NULL, &RouteNotify::on_resub_q );
  }
  /* NotifyPatternQueue */
  void do_pqnotify( NotifyPatternQueue &pat,  int type,  mod_route_t mod_route,
                   void (RouteNotify::*cb)( NotifyPatternQueue & ) ) {
    RouteGroup & grp = this->get_queue_group( pat.queue, pat.queue_len,
                                              pat.queue_hash );
    uint16_t prefix_len = (uint16_t) pat.cvt.prefixlen;
    this->do_notify( grp, prefix_len, pat.prefix_hash, pat,
                     type | NOTIFY_IS_QUEUE, mod_route, cb );
  }
  void add_pat_queue( NotifyPatternQueue &pat ) {
    pat.sub_count = 1;
    this->do_pqnotify( pat, NOTIFY_ADD_SUB, &RouteGroup::add_route,
                       &RouteNotify::on_psub_q );
  }
  void del_pat_queue( NotifyPatternQueue &pat ) {
    pat.sub_count = 0;
    this->do_pqnotify( pat, NOTIFY_REM_SUB, &RouteGroup::del_route,
                       &RouteNotify::on_punsub_q );
  }
  void notify_pat_queue( NotifyPatternQueue &pat ) {
    this->do_pqnotify( pat, NOTIFY_ADD_REF, NULL, &RouteNotify::on_repsub_q );
  }
  void notify_unpat_queue( NotifyPatternQueue &pat ) {
    this->do_pqnotify( pat, NOTIFY_REM_REF, NULL, &RouteNotify::on_repsub_q );
  }
  /* publishing methods */
  bool forward_msg( EvPublish &pub,  BPData *data = NULL ) noexcept;
  bool forward_with_cnt( EvPublish &pub,  uint32_t &rcnt,
                         BPData *data = NULL ) noexcept;
  bool forward_except( EvPublish &pub,  const BitSpace &fdexcpt,
                       BPData *data = NULL ) noexcept;
  bool forward_except_with_cnt( EvPublish &pub,  const BitSpace &fdexcpt,
                                uint32_t &rcnt,  BPData *data = NULL ) noexcept;
  bool forward_set( EvPublish &pub,  const BitSpace &fdset,
                    BPData *data = NULL ) noexcept;
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
  /* convert a hash to a subject */
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
