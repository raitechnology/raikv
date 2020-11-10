#ifndef __rai_raikv__kv_msg_h__
#define __rai_raikv__kv_msg_h__

#include <raikv/dlinklist.h>
#include <raikv/work.h>

namespace rai {
namespace kv {

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

static const uint8_t  KV_MAX_MSG_TYPE    = 8;
static const uint16_t KV_MAX_DESTINATION = 128; /* == KV_MAX_CTX_ID */
static const uint32_t MAX_KV_MSG_SIZE    = 64 * 1024 - 512; /* > fragments */

struct KvMsg {
  /* 51 seqno bits gives 7 years at 10 million/sec */
  static const uint64_t KV_MAGIC      = (uint64_t) 0x8db8 << 48;
  static const uint64_t KV_SEQNO_MASK = ( (uint64_t) 1 << 51 ) - 1;
  uint32_t seqno_w[ 2 ], /* seqno at src */
           stamp_w[ 2 ]; /* instance create at src */
  uint32_t size;         /* size including header */
  uint8_t  src,          /* src = ctx_id */
           dest_start,   /* the recipient */
           dest_end,     /* if > start, forward msg to others */
           msg_type;     /* what class of message (sub, unsub, pub) */
/* seqno      : 1
   stamp      : 0x1645fa4e80a77cad
   size       : 24
   src        : 11
   dest_start : 0
   dest_end   : 128
   msg_type   : hello
       |      seqno       |magic |        stamp           |
    0   01 00 00 00  00 00 b8 8d  ad 7c a7 80  4e fa 45 16
       |    size   |src|st|en|ty|
   10   18 00 00 00  0b 00 80 00 */
  void init( uint64_t seqno,  uint64_t stamp,  uint32_t sz,  uint8_t send_src,
             uint8_t mtype ) {
    this->set_seqno_stamp( seqno, stamp );
    this->size       = sz;
    this->src        = send_src;
    this->dest_start = 0;
    this->dest_end   = KV_MAX_DESTINATION;
    this->msg_type   = mtype;
  }
  static const char * msg_type_string( uint8_t msg_type ) noexcept;
  const char * msg_type_string( void ) const noexcept;
  void print( void ) noexcept;
  void print_sub( void ) noexcept;

  uint64_t get_seqno( void ) const {
    uint64_t seq;
    ::memcpy( &seq, this->seqno_w, sizeof( seq ) );
    return seq & KV_SEQNO_MASK;
  }
  uint64_t get_stamp( void ) const {
    uint64_t stamp;
    ::memcpy( &stamp, this->stamp_w, sizeof( stamp ) );
    return stamp;
  }
  void set_seqno_stamp( uint64_t seq,  uint64_t stamp ) {
    seq |= KV_MAGIC;
    ::memcpy( this->seqno_w, &seq, sizeof( this->seqno_w ) );
    ::memcpy( this->stamp_w, &stamp, sizeof( this->stamp_w ) );
  }
  bool is_valid( uint32_t sz ) const {
    if ( sz < this->size ||
         this->msg_type >= KV_MAX_MSG_TYPE )
      return false;
    uint64_t magic;
    ::memcpy( &magic, this->seqno_w, sizeof( magic ) );
    return ( magic & ~KV_SEQNO_MASK ) == KV_MAGIC;
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

typedef union {
  uint8_t  b[ 8 ];
  uint64_t w;
} range_t;

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

struct KvSendQueue {
  kv::WorkAllocT< 65536 >  snd_wrk;    /* for pending sends to shm */
  kv::DLinkList<KvMsgList> sendq;      /* sendq is to the network */
  uint64_t                 stamp,
                           next_seqno, /* incr for each message */
                           send_size,  /* incr for each send */
                           send_cnt;   /* incr for each send >= next_seqno */
  uint16_t                 send_src;   /* src of sender (ctx_id) */

  KvSendQueue( uint64_t ns,  uint16_t src )
    : stamp( ns ), next_seqno( 0 ), send_size( 0 ), send_cnt( 0 ),
      send_src( src ) {}

  KvMsg * create_kvmsg( KvMsgType mtype,  size_t sz ) noexcept {
    if ( sz > MAX_KV_MSG_SIZE )
      return NULL;
    KvMsgList * l = (KvMsgList *)
      this->snd_wrk.alloc( align<size_t>( sizeof( KvMsgList ) + sz, 8 ) );
    KvMsg & msg = l->msg;

    l->init_route();
    msg.init( ++this->next_seqno, this->stamp, sz, this->send_src, mtype );
    this->sendq.push_tl( l );
    this->send_size += sz;
    this->send_cnt++;
    return &msg;
  }

  void create_kvpublish( uint32_t h,  const char *sub,  size_t sublen,
                         const uint8_t *pref,  const uint32_t *hash,
                         uint8_t pref_cnt,  const char *reply,
                         size_t rlen,  const void *msgdata,  size_t msgsz,
                         uint8_t code,  uint8_t msg_enc ) noexcept;
  KvSubMsg * create_kvsubmsg( uint32_t h,  const char *sub,  size_t sublen,
                              char src_type,  KvMsgType mtype,  const char *rep,
                              size_t rlen ) noexcept;
  KvSubMsg * create_kvpsubmsg( uint32_t h,  const char *pattern,  size_t patlen,
                               const char *prefix,  uint8_t prefix_len,
                               char src_type,  KvMsgType mtype ) noexcept;
};

static inline bool is_kv_bcast( uint8_t msg_type ) {
  return msg_type < KV_MSG_PUBLISH; /* all others flood the network */
}

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
