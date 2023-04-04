#ifndef __rai_raikv__ev_publish_h__
#define __rai_raikv__ev_publish_h__

namespace rai {
namespace kv {

/*
 * pub_type is a ascii letter enumeration:
 *
 * publish ('a' -> 'z')
 * p  :  publish     no specific meaning
 * d  :  drop        remove subject from cache
 * i  :  initial     add value in cache
 * r  :  recap       update after possible msg loss
 * s  :  status      transient status of subject stream
 * u  :  update      update existing, does not cause a cached item
 * v  :  verify      update that acts as initial when not cached
 * x  :  snap        rpc reply
 * z  :  dictionary  rpc reply
 *
 * l  :  list        list update
 * e  :  set         set update
 * h  :  hash        hash update
 * o  :  sorted set  sorted set update
 *
 * session (' ' -> '/')
 * !  :  session  info
 * +  :  session  start
 * -  :  session  stop
 * /  :  session  subscriptions
 * #  :  publish  start / stop
 *
 * meta ('0' -> '@')
 * 0  :  reject    reject ping
 * 1  :  ack       reply to ping
 * @  :  quality   heartbeat
 * ?  :  ping      ping req, respond with ack reply or reject
 * :  :  keyspace  a cache key operation       __keyspace@db__:key oper
 *                                             __listblkd@db__:key oper
 *                                             __zsetblkd@db__:key oper
 *                                             __strmblkd@db__:key oper
 * ;  :  keyevent  a cache operation on a key  __keyevent@db__:oper key
 * <  :  monitor   a cache command             __monitor_@db__ msg
 *
 * subscriptions ('A' -> 'Z')
 * C  :  cancel     subcription
 * L  :  listen     for updates
 * S  :  subscribe  initial and updates
 * X  :  snap       rpc request
 * Z  :  dictionary rpc request
 */

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
                 hdr_len;     /* hdr prefix of msg */
  uint8_t        pub_type,    /* type of publish above */
                 prefix_cnt;  /* count of prefix[] */
  uint16_t       pub_status;  /* EvPubStatus, if msg loss */
  uint32_t       pub_host,    /* host id of publish */
               * hash;        /* the prefix hashes which match */
  uint8_t      * prefix;      /* the prefixes which match */
  uint64_t       cnt;         /* publish counter */

  EvPublish( const char *subj,  size_t subj_len,
             const void *repl,  size_t repl_len,
             const void *mesg,  size_t mesg_len,
             RoutePublish &sub_rt,  const PeerId &src,  uint32_t shash,
             uint32_t msg_encoding,  uint8_t publish_type,
             uint16_t status = 0,  uint32_t host = 0,
             uint64_t counter = 0 )
    : subject( subj ), reply( repl ), msg( mesg ),
      sub_route( sub_rt ), src_route( src ), subject_len( (uint16_t) subj_len ),
      reply_len( (uint16_t) repl_len ), msg_len( (uint32_t) mesg_len ), subj_hash( shash ),
      msg_enc( msg_encoding ), shard( 0 ), hdr_len( 0 ),
      pub_type( publish_type ), prefix_cnt( 0 ), pub_status( status ),
      pub_host( host ), hash( 0 ), prefix( 0 ), cnt( counter ) {}

  EvPublish( const EvPublish &p )
    : subject( p.subject ), reply( p.reply ), msg( p.msg ),
      sub_route( p.sub_route ), src_route( p.src_route ), subject_len( p.subject_len ),
      reply_len( p.reply_len ), msg_len( p.msg_len ), subj_hash( p.subj_hash ),
      msg_enc( p.msg_enc ), shard( p.shard ), hdr_len( p.hdr_len ),
      pub_type( p.pub_type ), prefix_cnt( 0 ), pub_status( p.pub_status ),
      pub_host( p.pub_host ), hash( 0 ), prefix( 0 ), cnt( p.cnt ) {}
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
