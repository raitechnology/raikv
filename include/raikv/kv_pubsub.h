#ifndef __rai_raikv__kv_pubsub_h__
#define __rai_raikv__kv_pubsub_h__

#include <raikv/shm_ht.h>
#include <raikv/key_buf.h>
#include <raikv/pipe_buf.h>
#include <raikv/stream_buf.h>
#include <raikv/ev_net.h>
#include <raikv/cube_route.h>
#include <raikv/route_ht.h>

namespace rai {
namespace kv {

struct EvPublish;
struct RouteDB;

static const uint32_t KV_CTX_INBOX_COUNT = 1;

enum KvMsgType {
  KV_MSG_HELLO    = 0, /* new session started, bcast hello */
  KV_MSG_BYE      = 1, /* session ended, bcast bye */
  KV_MSG_SUB      = 2, /* subscription started, bcast sub */
  KV_MSG_UNSUB    = 3, /* subscription ended, bcast unsub */
  KV_MSG_PSUB     = 4, /* pattern subscription started, bcast psub */
  KV_MSG_PUNSUB   = 5, /* pattern subscription ended, bcast punsub */
  KV_MSG_PUBLISH  = 6, /* publish a message, only to subscriptions */
  KV_MSG_FRAGMENT = 7  /* publish a msg fragment, only to subscriptions */
};

static const uint8_t KV_MAX_MSG_TYPE = 8;

struct KvMsg {
  uint32_t seqno_w[ 2 ];      /* seqno at src */
  uint32_t size;              /* size including header */
  uint8_t  src,               /* src = ctx_id */
           dest_start,        /* the recipient */
           dest_end,          /* if > start, forward msg to others */
           msg_type;          /* what class of message (sub, unsub, pub) */

  static const char * msg_type_string( uint8_t msg_type ) noexcept;
  const char * msg_type_string( void ) const noexcept;
  void print( void ) noexcept;
  void print_sub( void ) noexcept;

  uint64_t get_seqno( void ) const {
    uint64_t seq;
    ::memcpy( &seq, this->seqno_w, sizeof( seq ) );
    return seq;
  }
  void set_seqno( uint64_t seq ) {
    ::memcpy( this->seqno_w, &seq, sizeof( seq ) );
  }
  bool is_valid( uint32_t sz ) const {
    return sz >= this->size &&
           this->msg_type < KV_MAX_MSG_TYPE &&
           ( this->src | this->dest_start | this->dest_end ) < KV_MAX_CTX_ID;
  }
};

struct KvBacklog {
  KvBacklog      * next, * back;
  size_t           cnt;
  KvMsg         ** vec;
  kv::msg_size_t * siz;
  uint64_t         vec_size;

  void * operator new( size_t, void *ptr ) { return ptr; }
  KvBacklog( size_t n,  KvMsg **v,  kv::msg_size_t *sz,  uint64_t vsz )
    : next( 0 ), back( 0 ), cnt( n ), vec( v ), siz( sz ), vec_size( vsz ) {}
};

struct KvMcastKey {
  uint64_t        hash1, /* hash of mcast name: _SYS.MC */
                  hash2;
  kv::KeyFragment kbuf;  /* the key name (_SYS.MC) */

  void * operator new( size_t, void *ptr ) { return ptr; }
  KvMcastKey( kv::KeyCtx &kctx,  const char *name,  uint16_t namelen ) {
    this->kbuf.keylen = namelen;
    ::memcpy( this->kbuf.u.buf, name, namelen );
    kv::HashSeed hs;
    kctx.ht.hdr.get_hash_seed( kctx.db_num, hs );
    hs.hash( this->kbuf, this->hash1, this->hash2 );
  }
};

struct KvInboxKey : public kv::KeyCtx {
  uint64_t        hash1,            /* hash of inbox name */
                  hash2,
                  ibx_pos,
                  ibx_seqno,
                  read_cnt,
                  read_msg,
                  read_size;
  kv::ValueCtr    ibx_value_ctr;    /* the value serial ctr, track change */
  kv::KeyFragment ibx_kbuf;         /* the inbox name (_SYS.XX) XX=ctx_id hex */

  KvInboxKey( kv::KeyCtx &kctx,  const char *name,  uint16_t namelen )
    : kv::KeyCtx( kctx ), hash1( 0 ), hash2( 0 ),
      ibx_pos( 0 ), ibx_seqno( 0 ), read_cnt( 0 ), read_msg( 0 ),
      read_size( 0 ) {
    this->ibx_value_ctr.zero();
    this->ibx_kbuf.keylen = namelen;
    ::memcpy( this->ibx_kbuf.u.buf, name, namelen );
    kv::HashSeed hs;
    kctx.ht.hdr.get_hash_seed( kctx.db_num, hs );
    hs.hash( this->ibx_kbuf, this->hash1, this->hash2 );
  }
};

typedef kv::DLinkList<KvBacklog> KvBacklogList;

struct KvMsgQueue {
  KvMsgQueue  * next, * back;     /* links when have backlog msgs */
  kv::PipeBuf * snd_pipe;
  uint64_t      hash1,            /* hash of inbox name */
                hash2,
                high_water_size,  /* current high water send size */
                src_seqno,        /* src sender seqno */
                pub_size,         /* total size sent */
                pub_cnt,          /* total msg vectors sent */
                pub_msg,          /* total msgs sent */
                backlog_size,     /* size of messages in backlog */
                backlog_cnt,      /* count of messages in backlog */
                backlog_progress_ns, /* if backlog cleared some msgs */
                backlog_progress_cnt,
                backlog_progress_size,
                signal_cnt;       /* number of signals sent */
  uint32_t      ibx_num;
  bool          need_signal;      /* need to kill() recver to wake them */
  KvBacklogList kv_backlog;       /* kv backlog for this queue */
  KvBacklogList pipe_backlog;     /* pipe backlog for this queue */
  kv::WorkAllocT< 4096 > tmp;     /* msg queue mem when overflow */
  kv::KeyFragment kbuf;           /* the inbox name (_SYS.XX) XX=ctx_id hex */
  
  void * operator new( size_t, void *ptr ) { return ptr; }
  KvMsgQueue( kv::KeyCtx &kctx,  const char *name,  uint16_t namelen,
              uint32_t n )
    : next( 0 ), back( 0 ), snd_pipe( 0 ), high_water_size( 128 * 1024 ),
      src_seqno( 0 ), pub_size( 0 ),
      pub_cnt( 0 ), pub_msg( 0 ), backlog_size( 0 ), backlog_cnt( 0 ),
      backlog_progress_ns( 0 ), backlog_progress_cnt( 0 ),
      backlog_progress_size( 0 ), signal_cnt( 0 ), ibx_num( n ),
      need_signal( true ) {
    this->kbuf.keylen = namelen;
    ::memcpy( this->kbuf.u.buf, name, namelen );
    kv::HashSeed hs;
    kctx.ht.hdr.get_hash_seed( kctx.db_num, hs );
    hs.hash( this->kbuf, this->hash1, this->hash2 );
  }
};

typedef union {
  uint8_t  b[ 8 ];
  uint64_t w;
} range_t;

struct KvRouteCache {
  CubeRoute128 cr;
  range_t range;
  size_t cnt;
};

struct KvMsgList {
  KvMsgList * next, * back;
  range_t     range;
  uint32_t    off, cnt;
  KvMsg       msg;

  void init_route( void ) {
    this->range.w = 0;
    this->off     = 0;
    this->cnt     = 0;
  }
};

struct KvPrefHash {
  uint8_t pref;
  uint8_t hash[ 4 ];

  uint32_t get_hash( void ) const {
    uint32_t h;
    ::memcpy( &h, this->hash, sizeof( uint32_t ) );
    return h;
  }
  void set_hash( uint32_t h ) {
    ::memcpy( this->hash, &h, sizeof( uint32_t ) );
  }
};

struct KvSubMsg : public KvMsg {
  uint32_t hash,     /* hash of subject */
           msg_size; /* size of message data */
  uint16_t sublen,   /* length of subject, not including null char */
           replylen; /* length of reply, not including null char */
  uint8_t  code,     /* 'K' */
           msg_enc;  /* MD msg encoding type */
  char     buf[ 2 ]; /* subject\0\reply\0 */

/* | 0x98    0x04    0x00    0x00    0x00    0x00    0x00    0x00 |
 * | seqno                                                        |
 * | 0x64    0x00    0x00    0x00 |  0x04 |  0x03  | 0x03 |  0x06 |
 * | size                         |  src  | dstart | dend |  type |
 * | 0x22    0x6d    0x9d    0xef |  0x36    0x00    0x00    0x00 |
 * | hash                         |  msg_size                     |
 * | 0x04    0x00 |  0x00    0x00 |  0x70 |  0x02  | 0x70    0x69
 * | sublen       |  replylen     |  code | msg_enc| subject   
 *   0x6e    0x67 |  0x00 |  0x00 |  0x4b  | 0x01  | 0x40  | 0x22
 *   ping                 | reply |  stype | prefc | prefix| hash
 *   0x6d    0x9d    0xef    0x00    0x99    0x04    0x00    0x00
 *   hash                 | align |  data ->
 *   0x00    0x00    0x00    0x00    0x00    0x00    0x00    0x00
 *   0x00    0x00    0x00    0x00    0x00    0x00    0x00    0x00
 */

/* 00   02 00 00 00  00 00 00 00 <- seqno
   08   40 00 00 00              <- size
        02 01 01 07              <- src, dstart, dend, type (7 = publish)
   10   22 6d 9d ef              <- hash
        04 00 00 00              <- msg size
   18   04 00                    <- sublen
   2a   00 00                    <- replylen
   2c   70 02                    <- code, msg_enc
   2e   70 69 6e 67  00          <- ping
   23   00                       <- reply
   24   4b                       <- src type (K)
   25   01                       <- prefix cnt
   26   40                       <- prefix 64 = full (0-63 = partial)
   27   22 6d 9d ef              <- hash of prefix (dup of hash)
   2b   00                       <- align
   2c   01 00 00 00              <- msg_data (4 byte aligned) */
  char * subject( void ) {
    return this->buf;
  }
  char * reply( void ) {
    return &this->buf[ this->sublen + 1 ];
  }
  /* src type + prefix hashes + msg data */
  uint8_t * trail( void ) {
#if __GNUC__ >= 7
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
#endif
    return (uint8_t *) &this->buf[ this->sublen + 1 + this->replylen + 1 ];
#if __GNUC__ >= 7
#pragma GCC diagnostic pop
#endif
  }
  void set_subject( const char *s,  uint16_t len ) {
    this->sublen = len;
    ::memcpy( this->buf, s, len );
    this->buf[ len ] = '\0';
  }
  void set_reply( const char *s,  uint16_t len ) {
    this->replylen = len;
    ::memcpy( &this->buf[ this->sublen + 1 ], s, len );
    this->buf[ this->sublen + 1 + len ] = '\0';
  }
  char src_type( void ) {
    char * ptr = (char *) this->trail();
    return ptr[ 0 ];
  }
  void set_src_type( char t ) {
#if __GNUC__ >= 7
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstringop-overflow"
#endif
    char * ptr = (char *) this->trail();
    ptr[ 0 ] = t;
#if __GNUC__ >= 7
#pragma GCC diagnostic pop
#endif
  }
  void * get_msg_data( void ) {
    uint32_t i;
    i = this->size - kv::align<uint32_t>( this->msg_size, sizeof( uint32_t ) );
    return &((char *) (void *) this)[ i ];
  }
  uint64_t get_frag_off( void ) const {
    uint64_t off;
    uint32_t i;
    i = this->size - kv::align<uint32_t>( this->msg_size, sizeof( uint32_t ) );
    ::memcpy( &off, &((char *) (void *) this)[ i - sizeof( uint64_t ) ],
              sizeof( uint64_t ) );
    return off;
  }
  void set_msg_data( const void *p,  uint32_t len ) {
    uint32_t i = this->size - kv::align<uint32_t>( len, sizeof( uint32_t ) );
    this->msg_size = len;
    ::memcpy( &((char *) (void *) this)[ i ], p, len );
  }
  void set_frag_data( const void *p,  uint32_t len,  uint64_t off ) {
    uint32_t i = this->size - kv::align<uint32_t>( len, sizeof( uint32_t ) );
    this->msg_size = len;
    ::memcpy( &((char *) (void *) this)[ i ], p, len );
    ::memcpy( &((char *) (void *) this)[ i - sizeof( uint64_t ) ],
              &off, sizeof( uint64_t ) );
  }
  uint8_t get_prefix_cnt( void ) {
    uint8_t * ptr = this->trail();
    return ptr[ 1 ];
  }
  void set_prefix_cnt( uint8_t cnt ) {
#if __GNUC__ >= 7
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstringop-overflow"
#endif
    uint8_t * ptr = this->trail();
    ptr[ 1 ] = cnt;
#if __GNUC__ >= 7
#pragma GCC diagnostic pop
#endif
  }
  KvPrefHash * prefix_array( void ) {
    uint8_t * ptr = this->trail();
    return (KvPrefHash *) (void *) &ptr[ 2 ];
  }
  KvPrefHash & prefix_hash( uint8_t i ) {
    return this->prefix_array()[ i ];
  }
  static size_t hdr_size( size_t sublen,  size_t replylen,  uint8_t pref_cnt ) {
    return kv::align<size_t>(
      sizeof( KvSubMsg ) - 2 + sublen + 1 + replylen + 1
      + 1 /* src */ + 1 /* pref_cnt */
      + (size_t) pref_cnt * 5 /* pref + hash */, sizeof( uint32_t ) );
  }
  static size_t frag_hdr_size( size_t sublen,  uint8_t pref_cnt ) {
    return hdr_size( sublen, 0, pref_cnt );
  }
  static size_t calc_size( size_t sublen,  size_t replylen,  size_t msg_size,
                           uint8_t pref_cnt ) {
    return hdr_size( sublen, replylen, pref_cnt ) +
           kv::align<size_t>( msg_size, sizeof( uint32_t ) );
  }
};

static inline bool is_kv_bcast( uint8_t msg_type ) {
  return msg_type < KV_MSG_PUBLISH; /* all others flood the network */
}

struct KvSubRoute {
  uint32_t hash;
  uint8_t  rt_bits[ sizeof( CubeRoute128 ) ];
  uint16_t len;
  char     value[ 2 ];
  bool equals( const void *s,  uint16_t l ) const {
    return l == this->len && ::memcmp( s, this->value, l ) == 0;
  }
  void copy( const void *s,  uint16_t l ) {
    ::memcpy( this->value, s, l );
  }
};

struct KvSubTab : public RouteVec<KvSubRoute> {};

struct KvSubNotifyList {
  KvSubNotifyList * next, * back;
  bool in_list;
  virtual void on_sub( KvSubMsg &submsg ) noexcept;
  KvSubNotifyList() : next( 0 ), back( 0 ), in_list( false ) {}
};

enum KvPubSubFlag {
  KV_DO_NOTIFY     = 1, /* whether to notify external processes of inbox msgs */
  KV_INITIAL_SCAN  = 2, /* set after initial scan is complete */
  KV_READ_INBOX    = 4  /* set when reading inbox */
};

struct KvFragAsm {
  uint64_t first_seqno;
  uint64_t frag_count;
  uint64_t msg_size;
  uint64_t buf_size;
  uint8_t  buf[ 8 ];
};

struct KvPubSub : public EvSocket {
  static const uint8_t  EV_KV_PUBSUB = 9;
  static const uint16_t KV_NO_SIGUSR = 1;
  uint16_t     ctx_id;           /* my endpoint */
  uint16_t     flags;            /* KvPubSubFlags above */
  uint32_t     dbx_id;           /* db xref */
  kv::HashSeed hs;               /* seed for db */
  uint64_t     next_seqno,       /* next seqno of msg sent */
               timer_id,         /* my timer id */
               timer_cnt,        /* count of timer expires */
               time_ns,          /* current coarse time */
               send_size,        /* amount sent to other inboxes */
               send_cnt,         /* count of msgs sent, includes sendq */
               route_size,       /* size of msgs routed from send queue */
               route_cnt,        /* count of msgs routed from send queue*/
               sigusr_recv_cnt,
               inbox_msg_cnt,
               pipe_msg_cnt,
               mc_pos;           /* hash position of the mcast route */
  kv::ValueCtr mc_value_ctr;     /* the value serial ctr, track change */
  CubeRoute128 mc_cr;            /* the current mcast route */
  range_t      mc_range;         /* the range of current mcast */
  size_t       mc_cnt;           /* count of bits in mc_range */
  KvRouteCache rte_cache[ 256 ]; /* cache of cube routes */

  KvSubTab     sub_tab;          /* subject route table to shm */
  kv::KeyCtx   kctx,             /* a kv context for send/recv msgs */
               rt_kctx;          /* a kv context for route lookup */

  CubeRoute128 dead_cr,          /* clean up old route inboxes */
               pipe_cr,          /* routes that have pipe connected */
               subscr_cr;        /* routes that have subscribes incoming*/

  KvInboxKey  * rcv_inbox[ KV_CTX_INBOX_COUNT ];
  kv::PipeBuf * rcv_pipe[ KV_MAX_CTX_ID ];
  KvMsgQueue  * snd_inbox[ KV_MAX_CTX_ID ]; /* _SYS.IBX.xx : ctx send queues */
  KvMcastKey  & mcast;                   /* _SYS.MC : ctx_ids to shm network */
  KvFragAsm   * frag_asm[ KV_MAX_CTX_ID ];  /* fragment asm per source */
  /*uint64_t      last_seqno[ KV_MAX_CTX_ID ];*/

  kv::DLinkList<KvMsgList>       sendq;       /* sendq is to the network */
  kv::DLinkList<KvSubNotifyList> sub_notifyq; /* notify for subscribes */
  kv::DLinkList<KvMsgQueue>      backlogq;    /* ibx which have pending sends */

  kv::WorkAllocT< 256 >   rt_wrk,     /* wrk for rt_kctx kv */
                          wrk,        /* wrk for kctx, send, mcast */
                          ib_wrk;     /* wrk for inbox recv */ 
  kv::WorkAllocT< 65536 > snd_wrk;    /* for pending sends to shm */

  void * operator new( size_t, void *ptr ) { return ptr; }
  KvPubSub( EvPoll &p,  int sock,  void *mcptr,  const char *mc,  size_t mclen,
            uint32_t xid )
      : EvSocket( p, EV_KV_PUBSUB ),
        ctx_id( p.ctx_id ), flags( KV_DO_NOTIFY ),
        dbx_id( xid ), next_seqno( 0 ),
        timer_id( (uint64_t) EV_KV_PUBSUB << 56 ), timer_cnt( 0 ),
        time_ns( p.current_coarse_ns() ),
        send_size( 0 ), send_cnt( 0 ), route_size( 0 ), route_cnt( 0 ),
        sigusr_recv_cnt( 0 ), inbox_msg_cnt( 0 ), pipe_msg_cnt( 0 ),
        mc_pos( 0 ), mc_cnt( 0 ), kctx( *p.map, xid ),
        rt_kctx( *p.map, xid ),
        mcast( *(new ( mcptr ) KvMcastKey( this->kctx, mc, mclen )) ) {
    this->mc_value_ctr.zero();
    this->mc_cr.zero();
    this->mc_range.w = 0;
    ::memset( (void *) this->rte_cache, 0, sizeof( this->rte_cache ) );
    this->dead_cr.zero();
    this->pipe_cr.zero();
    this->subscr_cr.zero();
    ::memset( this->rcv_inbox, 0, sizeof( this->rcv_inbox ) );
    ::memset( this->rcv_pipe, 0, sizeof( this->rcv_pipe ) );
    ::memset( this->snd_inbox, 0, sizeof( this->snd_inbox ) );
    ::memset( this->frag_asm, 0, sizeof( this->frag_asm ) );
    /* ::memset( this->last_seqno, 0, sizeof( this->last_seqno ) ); */
    this->PeerData::init_peer( sock, NULL, "kv" );
    this->kctx.ht.hdr.get_hash_seed( this->kctx.db_num, this->hs );
  }

  static KvPubSub *create( EvPoll &p,  uint8_t db_num ) noexcept;
  void print_backlog( void ) noexcept;
  bool is_dead( uint32_t id ) const noexcept;
  bool get_set_mcast( CubeRoute128 &result_cr ) noexcept;
  bool register_mcast( void ) noexcept;
  void clear_mcast_dead_routes( void ) noexcept;
  void force_unsubscribe( void ) noexcept;
  void check_peer_health( void ) noexcept;
  bool unregister_mcast( void ) noexcept;
  enum UpdateEnum { DEACTIVATE = 0, ACTIVATE = 1, USE_FIND = 2 };
  bool update_mcast_sub( const char *sub,  size_t len,  int flags ) noexcept;
  bool update_mcast_route( void ) noexcept;
  bool push_backlog( KvMsgQueue &ibx,  KvBacklogList &list,  size_t cnt,
                     KvMsg **vec,  kv::msg_size_t *siz,
                     uint64_t vec_size ) noexcept;
  bool send_pipe_backlog( KvMsgQueue &ibx ) noexcept;
  bool send_kv_backlog( KvMsgQueue &ibx ) noexcept;
  bool pipe_msg( KvMsgQueue &ibx,  KvMsg &msg ) noexcept;
  bool send_kv_msg( KvMsgQueue &ibx,  KvMsg &msg,  bool backitup ) noexcept;
  bool pipe_vec( size_t cnt,  KvMsg **vec,  kv::msg_size_t *siz,
                 KvMsgQueue &ibx,  uint64_t vec_size ) noexcept;
  bool send_kv_vec( size_t cnt,  KvMsg **vec,  kv::msg_size_t *siz,
                    KvMsgQueue &ibx,  uint64_t vec_size ) noexcept;
  KvMsg *create_kvmsg( KvMsgType mtype,  size_t sz ) noexcept;
  void create_kvpublish( uint32_t h,  const char *sub,  size_t sublen,
                         const uint8_t *pref,  const uint32_t *hash,
                         uint8_t pref_cnt,  const char *reply, size_t rlen,
                         const void *msgdata,  size_t msgsz,
                         uint8_t code,  uint8_t msg_enc ) noexcept;
  KvSubMsg *create_kvsubmsg( uint32_t h,  const char *sub,  size_t len,
                             char src_type,  KvMsgType mtype,  const char *rep,
                             size_t rlen ) noexcept;
  KvSubMsg *create_kvpsubmsg( uint32_t h,  const char *pattern,  size_t sublen,
                              const char *prefix,  uint8_t prefix_len,
                              char src_type,  KvMsgType mtype ) noexcept;
  void do_sub( uint32_t h,  const char *sub,  size_t sublen,
               uint32_t sub_id,  uint32_t rcnt,  char src_type,
               const char *rep = NULL,  size_t rlen = 0 ) noexcept;
  void do_unsub( uint32_t h,  const char *sub,  size_t sublen,
                 uint32_t sub_id,  uint32_t rcnt,  char src_type ) noexcept;
  void do_psub( uint32_t h,  const char *pattern,  size_t patlen,
                const char *prefix,  uint8_t prefix_len,
                uint32_t sub_id,  uint32_t rcnt,  char src_type ) noexcept;
  void do_punsub( uint32_t h,  const char *pattern,  size_t patlen,
                  const char *prefix,  uint8_t prefix_len,
                  uint32_t sub_id,  uint32_t rcnt,  char src_type ) noexcept;
  void forward_sub( KvSubMsg &submsg ) noexcept;
  void scan_ht( void ) noexcept;
  size_t resolve_routes( CubeRoute128 &used ) noexcept;
  /*void check_seqno( void ) noexcept;*/
  void write_send_queue_fast( void ) noexcept;
  void write_send_queue_slow( void ) noexcept;
  void notify_peers( CubeRoute128 &used ) noexcept;
  bool check_backlog_warning( KvMsgQueue &ibx ) noexcept;
  bool get_sub_mcast( const char *sub,  size_t len,
                      CubeRoute128 &cr ) noexcept;
  bool route_msg( KvMsg &msg ) noexcept;
  bool make_rcv_pipe( size_t src ) noexcept;
  bool make_snd_pipe( size_t src ) noexcept;
  void open_pipe( size_t src ) noexcept;
  void close_pipe( size_t src ) noexcept;
  void close_rcv_pipe( size_t src ) noexcept;
  size_t read_inbox( bool read_until_empty ) noexcept;
  size_t read_inbox2( KvInboxKey &ibx,  bool read_until_empty ) noexcept;
  void publish_status( KvMsgType mtype ) noexcept;
  /* EvSocket */
  virtual void write( void ) noexcept final;
  virtual void read( void ) noexcept final;
  virtual void process( void ) noexcept final;
  virtual bool busy_poll( void ) noexcept final;
  virtual void release( void ) noexcept final;
  virtual bool timer_expire( uint64_t tid, uint64_t eid ) noexcept final;
  virtual bool hash_to_sub( uint32_t h, char *k, size_t &klen ) noexcept final;
  virtual bool on_msg( EvPublish &pub ) noexcept final;
  virtual void process_shutdown( void ) noexcept final;
  virtual void process_close( void ) noexcept final;
};

struct KvHexDump {
  char line[ 80 ];
  uint32_t boff, hex, ascii;
  uint64_t stream_off;

  KvHexDump();
  void reset( void );
  void flush_line( void );
  static char hex_char( uint8_t x );
  void init_line( void );
  uint32_t fill_line( const void *ptr,  uint64_t off,  uint64_t len );
  static void dump_hex( const void *ptr,  uint64_t size );
};

}
}

#endif
