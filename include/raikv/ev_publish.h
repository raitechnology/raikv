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

struct EvPublish {
  const char * subject;        /* target subject */
  const void * reply,          /* if rpc style pub w/reply */
             * msg;            /* the message attached */
  size_t       subject_len,    /* length of subject */
               reply_len,      /* length of reply */
               msg_len;        /* length of msg */
  uint32_t     subj_hash,      /* crc of subject */
               src_route;      /* fd of sender */
  uint8_t      msg_len_digits, /* len of msg_len_buf ascii (may be 0) */
               msg_enc,        /* the type of message (ex: MD_STRING) */
               pub_type;       /* type of publish above */
  const char * msg_len_buf;    /* msg_len to string */
  uint32_t   * hash;           /* the prefix hashes which match */
  uint8_t    * prefix;         /* the prefixes which match */
  uint8_t      prefix_cnt;     /* count of these */

  EvPublish( const char *subj,  size_t subj_len,
             const void *repl,  size_t repl_len,
             const void *mesg,  size_t mesg_len,
             uint32_t src,  uint32_t hash,
             const char *msg_len_ptr,
             uint8_t msg_len_digs,
             uint8_t msg_encoding,  uint8_t publish_type )
    : subject( subj ), reply( repl ), msg( mesg ),
      subject_len( subj_len ), reply_len( repl_len ),
      msg_len( mesg_len ), subj_hash( hash ), src_route( src ),
      msg_len_digits( msg_len_digs ), msg_enc( msg_encoding ),
      pub_type( publish_type ), msg_len_buf( msg_len_ptr ),
      hash( 0 ), prefix( 0 ), prefix_cnt( 0 ) {}

  EvPublish( const EvPublish &p )
    : subject( p.subject ), reply( p.reply ), msg( p.msg ),
      subject_len( p.subject_len ), reply_len( p.reply_len ),
      msg_len( p.msg_len ), subj_hash( p.subj_hash ), src_route( p.src_route ),
      msg_len_digits( p.msg_len_digits ), msg_enc( p.msg_enc ),
      pub_type( p.pub_type ), msg_len_buf( p.msg_len_buf ),
      hash( 0 ), prefix( 0 ), prefix_cnt( 0 ) {}
};

}
}
#endif
