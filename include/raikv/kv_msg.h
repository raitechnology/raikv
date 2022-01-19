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
  void set_seqno( uint64_t seq ) {
    seq |= KV_MAGIC;
    ::memcpy( this->seqno_w, &seq, sizeof( this->seqno_w ) );
  }
  void set_stamp( uint64_t stamp ) {
    ::memcpy( this->stamp_w, &stamp, sizeof( this->stamp_w ) );
  }
  void set_seqno_stamp( uint64_t seq,  uint64_t stamp ) {
    this->set_seqno( seq );
    this->set_stamp( stamp );
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
           msg_size, /* size of message data */
           msg_enc;  /* msg encoding */
  uint16_t sublen,   /* length of subject, not including null char */
           replylen, /* length of reply, not including null char */
           code;     /* pub type */
  char     buf[ 2 ]; /* subject\0\reply\0 */

/*  seqno      : 3
    stamp      : 0x1646ad58b35d4e8d
    size       : 96
    src        : 8
    dest_start : 0
    dest_end   : 128
    msg_type   : sub
    hash       : 3c354a65
    msg_size   : 0
    sublen     : 14
    prefix_cnt : 0
    replylen   : 38
    subject()  : RSF.REC.INTC.O
    reply()    : _INBOX.0A040416.97B655FB19E6F10E32A0.3
       |      seqno       |magic |        stamp           |
   0   03 00 00 00  00 00 b8 8d  8d 4e 5d b3  58 ad 46 16           N] X F   
       |    size   |src|st|en|ty|  hash      |msg_size    |
  10   60 00 00 00  08 00 80 02  65 4a 35 3c  00 00 00 00  `       eJ5<      
       |slen|rlen  |co|ml|  subject ...
  20   0e 00 26 00  4c 00 52 53  46 2e 52 45  43 2e 49 4e    & L RSF.REC.IN  
       subject ...    |  reply ...
  30   54 43 2e 4f  00 5f 49 4e  42 4f 58 2e  30 41 30 34  TC.O _INBOX.0A04  
                 reply ...
  40   30 34 31 36  2e 39 37 42  36 35 35 46  42 31 39 45  0416.97B655FB19E  
                                            |srt|pc|align
  50   36 46 31 30  45 33 32 41  30 2e 33 00  56 00 00 00  6F10E32A0.3 V     */
  char * subject( void ) {
    return this->buf;
  }
  const char * subj( void ) const {
    return this->buf;
  }
  char * reply( void ) {
    return &this->buf[ this->sublen + 1 ];
  }
  bool subject_equals( const char *s,  size_t len ) const {
    return (size_t) this->sublen == len && ::memcmp( this->buf, s, len ) == 0;
  }
  bool subject_equals( const KvSubMsg &submsg ) const {
    return this->subject_equals( submsg.buf, submsg.sublen );
  }
  bool reply_equals( const char *s,  size_t len ) const {
    return (size_t) this->replylen == len &&
                    ::memcmp( &this->buf[ this->sublen + 1 ], s, len ) == 0;
  }
  bool reply_equals( const KvSubMsg &submsg ) const {
    return this->reply_equals( &submsg.buf[ submsg.sublen + 1 ],
                               submsg.replylen );
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
  void * get_msg_data( void ) const {
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

struct KvFragAsm {
  uint64_t first_seqno;
  uint64_t frag_count;
  uint64_t msg_size;
  uint64_t buf_size;
  uint8_t  buf[ 8 ];

  static KvFragAsm *merge( KvFragAsm *&fragp,  const KvSubMsg &msg ) noexcept;
  static void release( KvFragAsm *&frag ) noexcept;
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

  KvSubMsg * copy_kvsubmsg( const KvSubMsg &cpy ) {
    return (KvSubMsg *) this->copy_kvmsg( cpy );
  }
  KvMsg * copy_kvmsg( const KvMsg &cpy ) {
    KvMsgList * l = (KvMsgList *)
      this->snd_wrk.alloc( align<size_t>( sizeof( KvMsgList ) + cpy.size, 8 ) );
    KvMsg & msg = l->msg;

    l->init_route();
    ::memcpy( &msg, &cpy, cpy.size );
    this->sendq.push_tl( l );
    this->send_size += cpy.size;
    this->send_cnt++;
    return &msg;
  }

  KvMsg * create_kvmsg( KvMsgType mtype,  size_t sz ) {
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
                         size_t replylen,  const void *msgdata,  size_t msgsz,
                         uint16_t code,  uint32_t msg_enc,
                         const size_t max_msg_size = MAX_KV_MSG_SIZE ) noexcept;
  KvSubMsg * create_kvsubmsg( uint32_t h,  const char *sub,  size_t sublen,
                              char src_type,  KvMsgType mtype,  uint8_t code,
                              const char *rep,  size_t replylen ) noexcept;
  KvSubMsg * create_kvpsubmsg( uint32_t h,  const char *pattern,  size_t patlen,
                               const char *prefix,  uint8_t prefix_len,
                               char src_type,  KvMsgType mtype,
                               uint8_t code ) noexcept;
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
