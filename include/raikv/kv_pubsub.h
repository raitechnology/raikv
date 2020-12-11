#ifndef __rai_raikv__kv_pubsub_h__
#define __rai_raikv__kv_pubsub_h__

#include <raikv/shm_ht.h>
#include <raikv/key_buf.h>
#include <raikv/pipe_buf.h>
#include <raikv/stream_buf.h>
#include <raikv/ev_net.h>
#include <raikv/cube_route.h>
#include <raikv/route_ht.h>
#include <raikv/kv_msg.h>

namespace rai {
namespace kv {

struct EvPublish;
struct RouteDB;

static const uint32_t KV_CTX_INBOX_COUNT = 1;

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

struct KvRouteCache {
  CubeRoute128 cr;
  range_t range;
  size_t cnt;
};

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
#if 0
struct KvSubNotifyList {
  KvSubNotifyList * next, * back;
  bool in_list;
  virtual void on_sub( uint32_t src_fd,  uint32_t rcnt,
                       KvSubMsg &submsg ) noexcept;
  KvSubNotifyList() : next( 0 ), back( 0 ), in_list( false ) {}
};
#endif
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

struct KvPubSub : public EvSocket, public KvSendQueue, public RouteNotify {
  static const uint8_t  EV_KV_PUBSUB = 9;
  static const uint16_t KV_NO_SIGUSR = 1;
  uint16_t     ctx_id;           /* my endpoint */
  uint16_t     flags;            /* KvPubSubFlags above */
  uint32_t     dbx_id;           /* db xref */
  kv::HashSeed hs;               /* seed for db */
  uint64_t     timer_id,         /* my timer id */
               timer_cnt,        /* count of timer expires */
               time_ns,          /* current coarse time */
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

  /*kv::DLinkList<KvSubNotifyList> sub_notifyq;  notify for subscribes */
  kv::DLinkList<KvMsgQueue>      backlogq;    /* ibx which have pending sends */

  kv::WorkAllocT< 256 > rt_wrk,     /* wrk for rt_kctx kv */
                        wrk,        /* wrk for kctx, send, mcast */
                        ib_wrk;     /* wrk for inbox recv */ 

  void * operator new( size_t, void *ptr ) { return ptr; }
  KvPubSub( EvPoll &p,  int sock,  void *mcptr,  const char *mc,  size_t mclen,
            uint32_t xid )
      : EvSocket( p, p.register_type( "kv_pubsub" ) ),
        KvSendQueue( p.map->hdr.create_stamp, p.ctx_id ),
        ctx_id( p.ctx_id ), flags( KV_DO_NOTIFY ),
        dbx_id( xid ), timer_id( (uint64_t) EV_KV_PUBSUB << 56 ),
        timer_cnt( 0 ), time_ns( p.current_coarse_ns() ),
        route_size( 0 ), route_cnt( 0 ),
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
    p.add_route_notify( *this );
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
  /* RouteNotify */
  virtual void on_sub( uint32_t h,  const char *sub,  size_t sublen,
                       uint32_t src_fd,  uint32_t rcnt,  char src_type,
                       const char *rep,  size_t rlen ) noexcept;
  virtual void on_unsub( uint32_t h,  const char *sub,  size_t sublen,
                         uint32_t src_fd,  uint32_t rcnt,
                         char src_type ) noexcept;
  virtual void on_psub( uint32_t h,  const char *pattern,  size_t patlen,
                        const char *prefix,  uint8_t prefix_len,
                        uint32_t src_fd,  uint32_t rcnt,
                        char src_type ) noexcept;
  virtual void on_punsub( uint32_t h,  const char *pattern,  size_t patlen,
                          const char *prefix,  uint8_t prefix_len,
                          uint32_t src_fd,  uint32_t rcnt,
                          char src_type ) noexcept;
  /*void forward_sub( uint32_t src_fd,  uint32_t rcnt,
                    KvSubMsg &submsg ) noexcept;*/
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

}
}

#endif
