#ifndef __rai_raikv__kv_pubsub_h__
#define __rai_raikv__kv_pubsub_h__

#include <raikv/ev_net.h>

#ifndef _MSC_VER
#define KVPS_USE_UNIX_SOCKET
#endif

#ifdef KVPS_USE_UNIX_SOCKET
#include <raikv/ev_unix.h>
#define KVPS_LISTEN EvUnixListen
#else
#include <raikv/ev_tcp.h>
#define KVPS_LISTEN EvTcpListen
#endif
#include <raikv/route_ht.h>
#include <raikv/dlinklist.h>
#include <raikv/pattern_cvt.h>
#include <raikv/bit_set.h>

namespace rai {
namespace kv {

enum PsSubType {
  PS_SUB     = 0,
  PS_PATTERN = 1
};

struct PsSub {
  uint32_t hash;
  uint8_t  type,
           fmt;
  uint16_t len;
  char     value[ 4 ];
};

struct PsSubTab : public RouteVec<PsSub> {
  uint64_t   time_ns;
  uint32_t * sub_upd;
  PsSubTab() : time_ns( 0 ), sub_upd( 0 ) {}
  bool load( void *data,  uint32_t data_len ) noexcept;
  bool recover_lost_subs( void ) noexcept;
  virtual void * new_vec_data( uint32_t id,  size_t sz ) noexcept;
  virtual void free_vec_data( uint32_t id,  void *p,  size_t sz ) noexcept;
  void *get_vec_data( uint32_t id ) noexcept;
  void unmap_vec_data( void ) noexcept;
};

#define KVPS_CTRL_NAME_SIZE ( 64 - ( 2 * 8 + 4 * 4 ) )

struct PsCtrlCtx {
  AtomUInt64 key;         /* a time_ns spin lock */
  uint64_t   time_ns;     /* unizue time prefix used for connect and shm */
  uint32_t   pid,         /* pid of ctx */
             serial,      /* order of new ctx */
             sub_upd[ 2 ];/* last id and update count */
  uint8_t    name[ KVPS_CTRL_NAME_SIZE ];
  void zero( void ) {
    this->time_ns      = 0;
    this->pid          = 0;
    this->serial       = 0;
    this->sub_upd[ 0 ] = 0;
    this->sub_upd[ 1 ] = 0;
    ::memset( this->name, 0, sizeof( this->name ) );
  }
};

struct PsGuard {
  PsCtrlCtx & ctx;
  uint64_t    save;
  bool        locked;
  PsGuard( PsCtrlCtx &el,  uint64_t time_ns ) : ctx( el ), locked( true ) {
    this->save = this->lock( el, time_ns );
  }
  ~PsGuard() {
    this->unlock();
  }
  void test_unlock( uint64_t val ) {
    if ( ! this->locked ) return;
    this->locked = false;
    PsGuard::unlock( this->ctx, val );
  }
  void unlock( void )          { this->test_unlock( this->save ); }
  void release( void )         { this->test_unlock( 0 ); }
  void mark( uint64_t marker ) { this->test_unlock( marker ); }
  static uint64_t lock( PsCtrlCtx &el,  uint64_t time_ns ) noexcept;
  static void     unlock( PsCtrlCtx &el,  uint64_t val ) noexcept;
};

static const size_t   KVPS_CTRL_SIZE     = 128 * 64 + 64;
static const uint32_t KVPS_CTRL_CTX_SIZE = 128;
struct PsCtrlFile {
  char       magic[ 16 ];     /* PsCtrlFile */
  AtomUInt64 ipc_token;       /* if shm attached */
  AtomUInt32 next_serial,     /* serial number of new context */
             next_ctx,        /* try to find ctr[] at next */
             ctrl_ready_spin, /* wait until ctrl is initialized */
             init_ctx_spin;   /* serialize new context additions */
  uint8_t    unused[ 64 - ( 16 + 8 + 4 * 4 ) ];
  PsCtrlCtx  ctx[ KVPS_CTRL_CTX_SIZE ];

  void check_dead_pids( uint32_t &dead,  uint32_t &alive ) noexcept;
};

#if __cplusplus >= 201103L
  /* 1024b align */
  static_assert( KVPS_CTRL_SIZE == sizeof( PsCtrlFile ), "ps ctrl file" );
  static_assert( 64 == sizeof( PsCtrlCtx ), "ps ctrl ctx" );
#endif

struct KvPubSub;
struct KvMsgIn;

struct KvPubSubPeer : public EvConnection {
  RoutePublish & sub_route;
  KvPubSub     & me;
  PsCtrlFile   & ctrl;
  BloomRoute   * bloom_rt;
  BloomDB        bloom_db;
  uint64_t       time_ns,
                 sub_seqno;
  uint32_t       ctx_id;
  bool           is_shutdown;
  KvPubSubPeer * next, * back;

  void * operator new( size_t, void *ptr ) { return ptr; }
  KvPubSubPeer( EvPoll &p,  uint8_t st,  KvPubSub &m ) noexcept;

  void assert_subs( void *data,  uint32_t data_len ) noexcept;
  uint32_t iter_sub_tab( PsSubTab &sub_tab,  bool add ) noexcept;
  void drop_bloom_refs( void ) noexcept;
  void drop_sub_tab( void ) noexcept;
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual void process_shutdown( void ) noexcept;
  virtual bool on_msg( EvPublish &pub ) noexcept;
  void hello_msg( KvMsgIn &msg ) noexcept;
  void bloom_msg( KvMsgIn &msg ) noexcept;
  void bloom_del_msg( KvMsgIn &msg ) noexcept;
  void bye_msg( KvMsgIn &msg ) noexcept;
  void on_sub_msg( KvMsgIn &msg ) noexcept;
  void on_unsub_msg( KvMsgIn &msg ) noexcept;
  void do_sub_msg( KvMsgIn &msg,  bool is_sub ) noexcept;
  void on_psub_msg( KvMsgIn &msg ) noexcept;
  void on_punsub_msg( KvMsgIn &msg ) noexcept;
  void do_psub_msg( KvMsgIn &msg,  bool is_sub ) noexcept;
  void fwd_msg( KvMsgIn &msg ) noexcept;
};

struct KvPeerList : public DLinkList<KvPubSubPeer> {};
struct KvMsg;
struct KvEst;

struct KvPubSub : public KVPS_LISTEN, public RouteNotify {
  RoutePublish & sub_route;
  PsCtrlFile   & ctrl;
  BitSpace       peer_set;
  uint64_t       init_ns,
                 sub_seqno;
  uint32_t       serial,
                 ctx_id;
  PsSubTab       sub_tab;
  KvPeerList     peer_list;
  ArraySpace<char, 16 * 1024> msg_buf;
  const char  *  ipc_name;
  uint64_t       ipc_token;
  uint8_t        peer_sock_type;
  char           ctx_name[ KVPS_CTRL_NAME_SIZE ];

  void * operator new( size_t, void *ptr ) { return ptr; }
  KvPubSub( RoutePublish &sr,  PsCtrlFile &cf,  const char *ipc_nm,
            uint64_t ipc_tok,  const char *ctx_name ) noexcept;
  static KvPubSub *create( RoutePublish &sr,  const char *ipc_name,
                           uint64_t ipc_token,  const char *ctx_name ) noexcept;
  virtual EvSocket *accept( void ) noexcept;
  virtual void release( void ) noexcept;

  bool init( void ) noexcept;
  void send_hello( KvPubSubPeer &c ) noexcept;
  void bcast_msg( KvMsg &m ) noexcept;
  KvMsg &get_msg_buf( KvEst &e, int mtype ) noexcept;
  bool attach_ctx( void ) noexcept;
  void detach_ctx( void ) noexcept;
  /* RouteNotify */
  virtual void on_sub( NotifySub &sub ) noexcept;
  virtual void on_unsub( NotifySub &sub ) noexcept;
  void do_on_sub( NotifySub &sub,  int mtype ) noexcept;
  virtual void on_psub( NotifyPattern &pat ) noexcept;
  virtual void on_punsub( NotifyPattern &pat ) noexcept;
  void do_on_psub( NotifyPattern &pat,  int mtype ) noexcept;
  bool add_sub( uint32_t h,  const char *sub,  uint16_t sublen,
                PsSubType t,  PatternFmt f = (PatternFmt) 0 ) noexcept;
  bool rem_sub( uint32_t h,  const char *sub,  uint16_t sublen,
                PsSubType t,  PatternFmt f = (PatternFmt) 0 ) noexcept;
  virtual void on_reassert( uint32_t fd,  RouteVec<RouteSub> &sub_db,
                            RouteVec<RouteSub> &pat_db ) noexcept;
  virtual void on_bloom_ref( BloomRef &ref ) noexcept;
  virtual void on_bloom_deref( BloomRef &ref ) noexcept;
};

enum KvMsgType {
  KV_MSG_HELLO     = 0,
  KV_MSG_BLOOM     = 1,
  KV_MSG_BLOOM_DEL = 2,
  KV_MSG_BYE       = 3,
  KV_MSG_ON_SUB    = 4,
  KV_MSG_ON_PSUB   = 5,
  KV_MSG_ON_UNSUB  = 6,
  KV_MSG_ON_PUNSUB = 7,
  KV_MSG_FWD       = 8,
  KV_MSG_MAX       = 9
};

#define kv_dispatch_msg { \
  &KvPubSubPeer::hello_msg, \
  &KvPubSubPeer::bloom_msg, \
  &KvPubSubPeer::bloom_del_msg, \
  &KvPubSubPeer::bye_msg, \
  &KvPubSubPeer::on_sub_msg, \
  &KvPubSubPeer::on_psub_msg, \
  &KvPubSubPeer::on_unsub_msg, \
  &KvPubSubPeer::on_punsub_msg, \
  &KvPubSubPeer::fwd_msg \
}
#define kv_msg_name { \
  "hello", "bloom", "bloom_del", "bye", "on_sub", "on_psub", "on_unsub", \
  "on_punsub", "fwd" \
}

enum KvFieldType {
  KV_FLD_CTX_ID    = 0,
  KV_FLD_TIME_NS   = 1,
  KV_FLD_SUB_SEQNO = 2,
  KV_FLD_SUBJECT   = 3,
  KV_FLD_REPLY     = 4,
  KV_FLD_SUBJ_HASH = 5,
  KV_FLD_SUB_COUNT = 6,
  KV_FLD_HASH_COLL = 7,
  KV_FLD_PATTERN   = 8,
  KV_FLD_PAT_FMT   = 9,
  KV_FLD_MSG_ENC   = 10,
  KV_FLD_DATA      = 11,
  KV_FLD_REF_NUM   = 12,
  KV_FLD_NAME      = 13,
  KV_FLD_MAX       = 14
};

/* KvMsg :
 * <len 4 bytes><magic 0xab><KvMsgType 1 byte>
 * <4 bits field len><4 bits KvFieldType><field data>
 * ...
 * <len + 4>
 */
struct KvMsg {
  uint32_t off;
  char     d[ 1024 ];

  void * operator new( size_t, void *ptr ) { return ptr; }
  KvMsg( int t ) : off( 2 ) {
    this->d[ 0 ] = (char) 0xab;
    this->d[ 1 ] = (char) t;
  }
  const void * msg( void ) const { return &this->off; }
  size_t len( void ) const { return this->off + 4; }

  static uint8_t len_byte( uint32_t len,  bool var ) {
    uint8_t vflag = ( var ? 0x40 : 0 );
    if ( len == 4 ) return 0x10 | vflag;
    if ( len == 2 ) return 0x20 | vflag;
    if ( len == 8 ) return 0x30 | vflag;
    /* if len == 1, byte == 0 */
    return vflag;
  }
  KvMsg & u( KvFieldType t,  const void *p,  uint32_t len ) {
    this->d[ this->off ] = (char) ( (uint8_t) t | len_byte( len, false ) );
    ::memcpy( &this->d[ this->off + 1 ], p, len );
    this->off += len + 1;
    return *this;
  }
  KvMsg & v32( KvFieldType t,  uint32_t sz,  const void *var ) {
    this->d[ this->off ] = (char) ( (uint8_t) t | len_byte( 4, true ) );
    ::memcpy( &this->d[ this->off + 1 ], &sz, 4 );
    ::memcpy( &this->d[ this->off + 5 ], var, sz );
    this->off += 5 + sz;
    return *this;
  }
  KvMsg & v16( KvFieldType t,  uint16_t sz,  const void *var ) {
    this->d[ this->off ] = (char) ( (uint8_t) t | len_byte( 2, true ) );
    ::memcpy( &this->d[ this->off + 1 ], &sz, 2 );
    ::memcpy( &this->d[ this->off + 3 ], var, sz );
    this->off += 3 + sz;
    return *this;
  }
  KvMsg & ctx_id( uint32_t i )    { return this->u( KV_FLD_CTX_ID, &i, 4 ); }
  KvMsg & time_ns( uint64_t t )   { return this->u( KV_FLD_TIME_NS, &t, 8 ); }
  KvMsg & sub_seqno( uint64_t n ) { return this->u( KV_FLD_SUB_SEQNO, &n, 8 ); }
  KvMsg & subject( const char *s,  uint16_t len ) {
    return this->v16( KV_FLD_SUBJECT, len, s );
  }
  KvMsg & reply( const void *s,  uint16_t len ) {
    return this->v16( KV_FLD_REPLY, len, s );
  }
  KvMsg & subj_hash( uint32_t h ) { return this->u( KV_FLD_SUBJ_HASH, &h, 4 ); }
  KvMsg & sub_count( uint32_t n ) { return this->u( KV_FLD_SUB_COUNT, &n, 4 ); }
  KvMsg & hash_coll( uint8_t c )  { return this->u( KV_FLD_HASH_COLL, &c, 1 ); }
  KvMsg & pattern( const char *s,  uint16_t len ) {
    return this->v16( KV_FLD_PATTERN, len, s );
  }
  KvMsg & pat_fmt( uint8_t f )    { return this->u( KV_FLD_PAT_FMT, &f, 1 ); }
  KvMsg & msg_enc( uint32_t e )   { return this->u( KV_FLD_MSG_ENC, &e, 4 ); }
  KvMsg & data( const void *s,  uint32_t len ) {
    return this->v32( KV_FLD_DATA, len, s );
  }
  KvMsg & ref_num( uint32_t n )   { return this->u( KV_FLD_REF_NUM, &n, 4 ); }
  KvMsg & name( const char *s,  uint32_t len ) {
    return this->v16( KV_FLD_NAME, len, s );
  }
  void *ptr( KvFieldType t,  uint32_t len ) {
    this->d[ this->off ] = (char) ( (uint8_t) t | len_byte( 4, true ) );
    ::memcpy( &this->d[ this->off + 1 ], &len, 4 );
    char * ptr = &this->d[ this->off + 5 ];
    this->off += 5 + len;
    return ptr;
  }
};

struct KvEst {
  uint32_t off;

  KvEst() : off( 2 ) {}
  size_t len( void ) const { return this->off + 4; }

  KvEst & u( uint32_t len )       { this->off += len + 1; return *this; }
  KvEst & ctx_id( void )          { return this->u( 4 ); }
  KvEst & time_ns( void )         { return this->u( 8 ); }
  KvEst & sub_seqno( void )       { return this->u( 8 ); }
  KvEst & subject( uint16_t len ) { return this->u( len + 2 ); }
  KvEst & reply( uint16_t len )   { return this->u( len + 2 ); }
  KvEst & subj_hash( void )       { return this->u( 4 ); }
  KvEst & sub_count( void )       { return this->u( 4 ); }
  KvEst & hash_coll( void)        { return this->u( 1 ); }
  KvEst & pattern( uint16_t len ) { return this->u( len + 2 ); }
  KvEst & pat_fmt( void )         { return this->u( 1 ); }
  KvEst & msg_enc( void )         { return this->u( 4 ); }
  KvEst & data( uint32_t len )    { return this->u( len + 4 ); }
  KvEst & ref_num( void )         { return this->u( 4 ); }
  KvEst & name( uint32_t len )    { return this->u( len + 2 ); }
};

enum KvMsgErr {
  KV_NOT_ENOUGH   = -1,
  KV_MSG_OK       = 0,
  KV_BAD_MAGIC    = 1,
  KV_BAD_MSG_TYPE = 2,
  KV_BAD_FLD_TYPE = 3,
  KV_BAD_FLD_LEN  = 4
};

struct KvMsgIn {
  uint32_t     fld_set, len, missing;
  KvMsgType    type;
  const char * fld[ KV_FLD_MAX ];
  uint32_t     fld_size[ KV_FLD_MAX ];

  KvMsgIn() : fld_set( 0 ), len( 0 ), missing( 0 ) {}

  KvMsgErr decode( const char *msg,  uint32_t msglen ) noexcept;

  void print( void ) noexcept;

  bool is_set( int i ) const {
    return ( this->fld_set & ( 1U << i ) ) != 0;
  }
  template <class T>
  T get( int i ) {
    if ( this->is_set( i ) && this->fld_size[ i ] == sizeof( T ) ) {
      T ival;
      ::memcpy( &ival, this->fld[ i ], sizeof( T ) );
      return ival;
    }
    this->missing++;
    return 0;
  }
  const char * get_field( int i,  uint32_t &len ) {
    if ( this->is_set( i ) ) {
      len = this->fld_size[ i ];
      return this->fld[ i ];
    }
    this->missing |= ( 1U << i );
    len = 0;
    return NULL;
  }
  bool is_field_missing( void ) {
    if ( this->missing == 0 )
      return false;
    this->missing_error();
    return true;
  }
  void decode_ftype( const char *msg,  uint8_t &ftype,  uint32_t &fsize ) {
    ftype = (uint8_t) msg[ 0 ] & 0xf;
    fsize = (uint8_t) msg[ 0 ] & 0x30;

    if      ( fsize == 0x10 ) fsize = 4;
    else if ( fsize == 0x20 ) fsize = 2;
    else if ( fsize == 0x30 ) fsize = 8;
    else                      fsize = 1;
  }
  bool decode_vlen( const char *msg,  uint32_t fsize,  uint32_t &vlen ) {
    vlen = 0;
    if ( fsize == 2 ) {
      uint16_t vlen16;
      ::memcpy( &vlen16, &msg[ 1 ], 2 );
      vlen = vlen16;
    }
    else if ( fsize == 4 )
      ::memcpy( &vlen, &msg[ 1 ], 4 );
    else if ( fsize == 1 )
      vlen = (uint32_t) (uint8_t) msg[ 1 ];
    else
      return false;
    return true;
  }
  void missing_error( void ) noexcept;
};

}
}
#endif
