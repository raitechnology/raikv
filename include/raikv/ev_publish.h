#ifndef __rai_raikv__ev_publish_h__
#define __rai_raikv__ev_publish_h__

namespace rai {
namespace kv {

enum EvPubType {
  PUB_TYPE_NORMAL   = 0,  /* not special */
  PUB_TYPE_FRAGMENT = 1,  /* with fragment info */
  PUB_TYPE_ROUTING  = 2,  /* forward with routing */
  PUB_TYPE_SERIAL   = 3,  /* serialize, don't shard */
  PUB_TYPE_KEYSPACE = 4,  /* redis keyspace notification */
  PUB_TYPE_MONITOR  = 5,  /* redis monitor command */
  PUB_TYPE_KV       = 6,  /* kv internal publish */
  PUB_TYPE_INBOX    = 7,  /* extra inbox info */
  PUB_TYPE_QUEUE    = 128 /* forward queue publish */
};

enum EvPubStatus {
  /* > 0 is count of messages lost when sequence skipped */
  EV_PUB_NORMAL  = 0,  /* in sequence */
  EV_MAX_LOSS    = 0x7fff, /* <= MAX_LOSS is message loss */
  EV_PUB_START   = 0x8000, /* new stream started (from unique publisher) */
  EV_PUB_CYCLE   = 0x8001, /* stream cycle, new start */
  EV_PUB_RESTART = 0x8002  /* stream interrupted, restarted */
};

struct RoutePublish;
struct PeerId;
struct EvPublish {
  const char   * subject;     /* target subject */
  const void   * reply,       /* if rpc style pub w/reply */
               * msg;         /* the message attached */
  RoutePublish & sub_route;   /* source if publish */
  const PeerId & src_route;   /* fd of sender */
  uint16_t       subject_len, /* length of subject */
                 reply_len;   /* length of reply */
  uint32_t       msg_len,     /* length of msg */
                 subj_hash,   /* crc of subject */
                 msg_enc,     /* the type of message (ex: MD_STRING) */
                 shard,       /* route to shard */
                 hdr_len,     /* hdr prefix of msg */
                 suf_len;     /* suffix len of msg */
  uint8_t        publish_type,/* type of pub above */
                 prefix_cnt;  /* count of prefix[] */
  uint16_t       pub_status;  /* EvPubStatus, if msg loss */
  uint32_t       pub_host,    /* host id of publish */
               * hash;        /* the prefix hashes which match */
  uint8_t      * prefix;      /* the prefixes which match */
  uint64_t       cnt;         /* publish counter */

  void * operator new( size_t, void *ptr ) { return ptr; }
  EvPublish( const char *subj,  size_t subj_len,
             const void *repl,  size_t repl_len,
             const void *mesg,  size_t mesg_len,
             RoutePublish &sub_rt,  const PeerId &src,  uint32_t shash,
             uint32_t msg_encoding,  EvPubType pub_type = PUB_TYPE_NORMAL,
             uint16_t status = 0,  uint32_t host = 0,
             uint64_t counter = 0 )
    : subject( subj ), reply( repl ), msg( mesg ),
      sub_route( sub_rt ), src_route( src ), subject_len( (uint16_t) subj_len ),
      reply_len( (uint16_t) repl_len ), msg_len( (uint32_t) mesg_len ), subj_hash( shash ),
      msg_enc( msg_encoding ), shard( 0 ), hdr_len( 0 ), suf_len( 0 ),
      publish_type( pub_type ), prefix_cnt( 0 ), pub_status( status ),
      pub_host( host ), hash( 0 ), prefix( 0 ), cnt( counter ) {}

  EvPublish( const EvPublish &p )
    : subject( p.subject ), reply( p.reply ), msg( p.msg ),
      sub_route( p.sub_route ), src_route( p.src_route ), subject_len( p.subject_len ),
      reply_len( p.reply_len ), msg_len( p.msg_len ), subj_hash( p.subj_hash ),
      msg_enc( p.msg_enc ), shard( p.shard ), hdr_len( p.hdr_len ), suf_len( p.suf_len ),
      publish_type( p.publish_type ), prefix_cnt( 0 ), pub_status( p.pub_status ),
      pub_host( p.pub_host ), hash( 0 ), prefix( 0 ), cnt( p.cnt ) {}

  bool is_pub_type( EvPubType t ) const { return ( this->publish_type & 0x7f ) == t; }
  bool is_queue_pub( void ) const { return ( this->publish_type & PUB_TYPE_QUEUE ) != 0; }
};

struct EvPubTmp {
  EvPublish & pub;
  EvPubTmp( EvPublish &p,  uint32_t * hsh,  uint8_t * pre,  uint8_t pcnt = 0 )
      : pub( p ) {
    p.hash       = hsh;
    p.prefix     = pre;
    p.prefix_cnt = pcnt;
  }
  ~EvPubTmp() {
    this->pub.hash       = NULL;
    this->pub.prefix     = NULL;
    this->pub.prefix_cnt = 0;
  }
};

}
}
#endif
