#ifndef __rai_raikv__route_db_h__
#define __rai_raikv__route_db_h__

#include <raikv/uint_ht.h>
#include <raikv/delta_coder.h>
#include <raikv/key_hash.h>
#include <raikv/util.h>
#include <raikv/prio_queue.h>

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

struct CodeRef { /* refs to a route space, which is a list of fds */
  uint32_t hash, /* hash of the route */
           ref,  /* how many refs to this route (multiple subs) */
           ecnt, /* encoded number of ints needed to represent route (zipped) */
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

struct EvPublish;
struct RoutePublishData {   /* structure for publish queue heap */
  unsigned int prefix : 7,  /* prefix size (how much of subject matches) */
               rcount : 25; /* count of routes (32 million max (fd)) */
  uint32_t     hash;        /* hash of prefix (is subject hash if prefix=64) */
  uint32_t   * routes;      /* routes for this hash */

  static inline uint32_t precmp( uint32_t p ) {
    return ( ( ( p & 63 ) << 1 ) + 1 ) | ( p >> 6 );
  }
  static bool is_greater( RoutePublishData *r1,  RoutePublishData *r2 ) {
    if ( r1->routes[ 0 ] > r2->routes[ 0 ] )
      return true;
    if ( r1->routes[ 0 ] < r2->routes[ 0 ] )
      return false;
    return precmp( r1->prefix ) > precmp( r2->prefix );
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
  PeerMatchArgs() : id( 0 ), ip( 0 ), type( 0 ), ip_len( 0 ), type_len( 0 ),
                    skipme( false ) {}
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

struct PeerData {
  PeerData   * next,       /* dbl link list */
             * back;
  int32_t      fd,         /* fildes */
               pad;        /* 64 * 3 */
  uint64_t     id,         /* identifies peer */
               start_ns,   /* start time */
               active_ns;  /* last read time */
  const char * kind;       /* what protocol type */
  char         name[ 64 ], /* name assigned to peer */
               peer_address[ 64 ]; /* ip4 1.2.3.4:p, ip6 [ab::cd]:p, other */

  PeerData() : next( 0 ), back( 0 ) {
    this->init_peer( -1, NULL, NULL );
  }
  /* no address, attached directly to shm */
  void init_ctx( int fildes,  uint32_t ctx_id,  const char *k ) {
    this->init_peer( fildes, NULL, k );
    ::memcpy( this->peer_address, "ctx:", 4 );
    int i = 4;
    if ( ctx_id >= 1000 )
      this->peer_address[ i++ ] = ( ctx_id / 1000 ) % 10 + '0';
    if ( ctx_id >= 100 )
      this->peer_address[ i++ ] = ( ctx_id / 100 ) % 10 + '0';
    if ( ctx_id >= 10 )
      this->peer_address[ i++ ] = ( ctx_id / 10 ) % 10 + '0';
    this->peer_address[ i++ ] = ctx_id % 10 + '0';
    this->peer_address[ i ] = '\0';
    this->peer_address[ 63 ] = (char) i;
  }
  /* connected via ip address */
  void init_peer( int fildes,  const sockaddr *sa,  const char *k ) {
    this->fd = fildes;
    this->id = 0;
    this->start_ns = this->active_ns = 0;
    this->kind = k;
    this->name[ 0 ] = '\0';
    this->peer_address[ 0 ] = '\0';
    if ( sa != NULL )
      this->set_addr( sa );
  }

  void set_addr( const sockaddr *sa ) noexcept;

  void set_peer_address( const char *s,  size_t len ) {
    this->set_strlen64( this->peer_address, s, len );
  }
  void set_name( const char *s,  size_t len ) {
    this->set_strlen64( this->name, s, len );
  }
  void set_strlen64( char *buf,  const char *s,  size_t len ) {
    if ( len < 63 ) {
      ::memcpy( buf, s, len );
      ::memset( &buf[ len ], 0, 63 - len );
    }
    else {
      ::memcpy( buf, s, 63 );
      len = 0;
    }
    buf[ 63 ] = (char) len;
  }
  size_t get_peer_address_strlen( void ) const {
    return this->get_strlen64( this->peer_address );
  }
  size_t get_name_strlen( void ) const {
    return this->get_strlen64( this->name );
  }
  size_t get_strlen64( const char *buf ) const {  /* strlen( peer_address ) */
    if ( buf[ 0 ] == '\0' )
      return 0;
    if ( buf[ 63 ] == '\0' )
      return 63;
    return (size_t) (uint8_t) buf[ 63 ];
  }
  virtual int client_list( char *,  size_t ) { return 0; }
  virtual bool client_kill( void )           { return false; }
  virtual bool match( PeerMatchArgs & )      { return true; }
  virtual void client_stats( PeerStats & )   { return; }
  virtual void retired_stats( PeerStats & )  { return; }
};

struct KvPrefHash;
struct RoutePublish {
  int32_t keyspace_cnt, /* count of __keyspace@N__ subscribes active */
          keyevent_cnt, /* count of __keyevent@N__ subscribes active */
          listblkd_cnt, /* count of __listblkd@N__ subscribes active */
          zsetblkd_cnt, /* count of __zsetblkd@N__ subscribes active */
          strmblkd_cnt, /* count of __strmblkd@N__ subscribes active */
          monitor__cnt; /* count of __monitor_@N__ subscribes active */
  uint16_t key_flags;    /* bits set for key subs above (EKF_KEYSPACE_FWD|..) */
  bool forward_msg( EvPublish &pub,  uint32_t *rcount_total,  uint8_t pref_cnt,
                    KvPrefHash *ph ) noexcept;
  bool hash_to_sub( uint32_t r,  uint32_t h,  char *key,
                    size_t &keylen ) noexcept;
  void update_keyspace_count( const char *sub,  size_t len,  int add ) noexcept;
  void notify_sub( uint32_t h,  const char *sub,  size_t len,
                   uint32_t sub_id,  uint32_t rcnt,  char src_type,
                   const char *rep = NULL,  size_t rlen = 0 ) noexcept;
  void notify_unsub( uint32_t h,  const char *sub,  size_t len,
                     uint32_t sub_id,  uint32_t rcnt,  char src_type ) noexcept;
  void notify_psub( uint32_t h,  const char *pattern,  size_t len,
                    const char *prefix,  uint8_t prefix_len,
                    uint32_t sub_id,  uint32_t rcnt,  char src_type ) noexcept;
  void notify_punsub( uint32_t h,  const char *pattern,  size_t len,
                     const char *prefix,  uint8_t prefix_len,
                     uint32_t sub_id,  uint32_t rcnt,  char src_type ) noexcept;
  bool add_timer_seconds( int id,  uint32_t ival,  uint64_t timer_id,
                          uint64_t event_id ) noexcept;
  bool add_timer_millis( int id,  uint32_t ival,  uint64_t timer_id,
                         uint64_t event_id ) noexcept;
  bool remove_timer( int id,  uint64_t timer_id,  uint64_t event_id ) noexcept;

  RoutePublish() : keyspace_cnt( 0 ), keyevent_cnt( 0 ), listblkd_cnt( 0 ),
                   zsetblkd_cnt( 0 ), strmblkd_cnt( 0 ), monitor__cnt( 0 ),
                   key_flags( 0 ) {}
};

struct RouteDB {
  static const uint32_t INI_SPC = 16;
  struct PushRouteSpc {
    uint32_t   size;
    uint32_t * ptr;
    uint32_t   spc[ INI_SPC ];
    PushRouteSpc() : size( INI_SPC ) {
      this->ptr = this->spc;
    }
  };

  RoutePublish & rte;
  DeltaCoder     dc;             /* code          -> route list */
  UIntHashTab  * xht,            /* route hash    -> code | code ref hash */
               * zht;            /* code ref hash -> code buf offset */
  uint64_t       pat_mask;       /* mask of subject prefixes, up to 64 */
  uint32_t     * code_buf,       /* list of code ref, which is array of code */
               * code_spc_ptr,   /* temporary code space */
               * route_spc_ptr,  /* temporary route space */
                 code_end,       /* end of code_buf[] list */
                 code_size,      /* size of code_buf[] */
                 code_free,      /* amount free between 0 -> code_end */
                 code_spc_size,  /* size of code_spc_ptr */
                 route_spc_size, /* size of route_spc_ptr */
                 code_buf_spc[ INI_SPC * 4 ], /* initial code_buf[] */
                 code_spc[ INI_SPC ],         /* initial code_spc_ptr[] */
                 route_spc[ INI_SPC ],        /* initial code_route_ptr[] */
                 pre_seed[ 64 ],  /* hash seed of the prefix: _SYS.W<N>. */
                 pre_count[ 64 ]; /* count of subjects sharing a prefix N */
  PushRouteSpc   push_route_spc[ 64 ];

  RouteDB( RoutePublish &rp ) : rte( rp ), xht( 0 ), zht( 0 ), code_buf( 0 ),
              code_end( 0 ), code_size( INI_SPC * 4 ), code_free( 0 ),
              code_spc_size( INI_SPC ), route_spc_size( INI_SPC ) {
    this->code_buf      = this->code_buf_spc;
    this->code_spc_ptr  = this->code_spc;
    this->route_spc_ptr = this->route_spc;
    this->init_prefix_seed();
  }

  void init_prefix_seed( void ) noexcept;

  bool first_hash( uint32_t &pos,  uint32_t &h,  uint32_t &v ) {
    if ( this->xht != NULL && this->xht->first( pos ) ) {
      this->xht->get( pos, h, v );
      return true;
    }
    return false;
  }
  bool next_hash( uint32_t &pos,  uint32_t &h,  uint32_t &v ) {
    if ( this->xht->next( pos ) ) {
      this->xht->get( pos, h, v );
      return true;
    }
    return false;
  }
  uint32_t *make_route_space( uint32_t i ) noexcept;

  uint32_t *make_push_route_space( uint8_t n,  uint32_t i ) noexcept;

  uint32_t *make_code_space( uint32_t i ) noexcept;

  uint32_t *make_code_ref_space( uint32_t i,  uint32_t &off ) noexcept;

  void gc_code_ref_space( void ) noexcept;

  uint32_t compress_routes( uint32_t *routes,  uint32_t rcnt ) noexcept;

  uint32_t decompress_routes( uint32_t r,  uint32_t *&routes,
                              bool deref ) noexcept;
  uint32_t push_decompress_routes( uint8_t n,  uint32_t r,
                                   uint32_t *&routes ) noexcept;
  uint32_t decompress_one( uint32_t r ) noexcept;

  uint32_t add_route( uint32_t hash,  uint32_t r ) noexcept;

  uint32_t del_route( uint32_t hash,  uint32_t r ) noexcept;

  uint32_t add_pattern_route( uint32_t hash,  uint32_t r,
                              uint16_t pre_len ) noexcept;
  uint32_t del_pattern_route( uint32_t hash,  uint32_t r,
                              uint16_t pre_len ) noexcept;
  uint32_t prefix_seed( size_t prefix_len ) {
    if ( prefix_len > 63 )
      return this->pre_seed[ 63 ];
    return this->pre_seed[ prefix_len ];
  }
  bool is_member( uint32_t hash,  uint32_t x ) noexcept;

  uint32_t get_route( uint32_t hash,  uint32_t *&routes ) {
    uint32_t pos, val;
    if ( this->xht != NULL && this->xht->find( hash, pos, val ) )
      return this->decompress_routes( val, routes, false );
    return 0;
  }
  uint32_t push_get_route( uint8_t n,  uint32_t hash,  uint32_t *&routes ) {
    uint32_t pos, val;
    if ( this->xht != NULL && this->xht->find( hash, pos, val ) )
      return this->push_decompress_routes( n, val, routes );
    return 0;
  }
  uint32_t get_route_count( uint32_t hash ) noexcept;
};

}
}
#endif
