#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>

#include <raikv/util.h>
#include <raikv/kv_msg.h>

using namespace rai;
using namespace kv;

void
KvSendQueue::create_kvpublish( uint32_t h,  const char *sub,  size_t sublen,
                               const uint8_t *pref,  const uint32_t *hash,
                               uint8_t pref_cnt,  const char *reply,
                               size_t replylen,  const void *msgdata,
                               size_t msgsz,  uint8_t code,
                               uint8_t msg_enc,
                               const size_t max_msg_size ) noexcept
{
  size_t hsz = KvSubMsg::hdr_size( sublen, replylen, pref_cnt ),
         asz = kv::align<size_t>( msgsz, sizeof( uint32_t ) ),
         fsz, off, left, frag_msgsz, afrag;
  KvSubMsg * msg;
  uint8_t i;

  if ( hsz + asz > max_msg_size ) {
    if ( hsz >= max_msg_size ) /* shouldn't be this big */
      return;
    const uint8_t * fragdata = &((const uint8_t *) msgdata)[ msgsz ];
    asz = max_msg_size - hsz;
    fsz = KvSubMsg::frag_hdr_size( sublen, pref_cnt );
    off = msgsz;
    /* split off fragments under the msg size limit */
    for ( left = msgsz - asz; left > 0; left -= frag_msgsz ) {
      const size_t MAX_FRAG_SIZE = max_msg_size - sizeof( uint64_t );
      frag_msgsz = left;
      afrag = kv::align<size_t>( left, sizeof( uint32_t ) );
      if ( afrag + fsz > MAX_FRAG_SIZE ) {
        frag_msgsz = MAX_FRAG_SIZE - fsz;
        afrag = frag_msgsz;
      }
      msg = (KvSubMsg *)
        this->create_kvmsg( KV_MSG_FRAGMENT, fsz + afrag + sizeof( uint64_t ) );
      msg->hash    = h;
      msg->code    = code;
      msg->msg_enc = msg_enc;
      msg->set_subject( sub, sublen );
      msg->set_reply( NULL, 0 );
      msg->set_src_type( 'K' );
      msg->set_prefix_cnt( pref_cnt );
      for ( i = 0; i < pref_cnt; i++ ) {
        KvPrefHash &pf = msg->prefix_hash( i );
        pf.pref = pref[ i ];
        pf.set_hash( hash[ i ] );
      }
      fragdata = &fragdata[ -frag_msgsz ];
      off     -= frag_msgsz;
      msg->set_frag_data( fragdata, frag_msgsz, off );
    }
    msgsz = asz;
  }
  /* the final frag or only frag will have a subject */
  msg = (KvSubMsg *) this->create_kvmsg( KV_MSG_PUBLISH, hsz + asz );
  msg->hash    = h;
  msg->code    = code;
  msg->msg_enc = msg_enc;
  msg->set_subject( sub, sublen );
  msg->set_reply( reply, replylen );
  msg->set_src_type( 'K' );
  msg->set_prefix_cnt( pref_cnt );
  for ( i = 0; i < pref_cnt; i++ ) {
    KvPrefHash &pf = msg->prefix_hash( i );
    pf.pref = pref[ i ];
    pf.set_hash( hash[ i ] );
  }
  msg->set_msg_data( msgdata, msgsz );
}

KvSubMsg *
KvSendQueue::create_kvsubmsg( uint32_t h,  const char *sub,  size_t sublen,
                              char src_type,  KvMsgType mtype,  uint8_t code,
                              const char *rep,  size_t replylen ) noexcept
{
  size_t     sz  = KvSubMsg::calc_size( sublen, replylen, 0, 0 );
  KvSubMsg * msg = (KvSubMsg *) this->create_kvmsg( mtype, sz );
  if ( msg != NULL ) {
    msg->hash     = h;
    msg->msg_size = 0;
    msg->code     = code;
    msg->msg_enc  = 0;
    msg->set_subject( sub, sublen );
    msg->set_reply( rep, replylen );
    msg->set_src_type( src_type );
    msg->set_prefix_cnt( 0 );
  }
  return msg;
}

KvSubMsg *
KvSendQueue::create_kvpsubmsg( uint32_t h,  const char *pattern,  size_t patlen,
                               const char *prefix,  uint8_t prefix_len,
                               char src_type,  KvMsgType mtype,
                               uint8_t code ) noexcept
{
  size_t     sz  = KvSubMsg::calc_size( patlen, prefix_len, 0, 1 );
  KvSubMsg * msg = (KvSubMsg *) this->create_kvmsg( mtype, sz );
  if ( msg != NULL ) {
    msg->hash     = h;
    msg->msg_size = 0;
    msg->code     = code;
    msg->msg_enc  = 0;
    msg->set_subject( pattern, patlen );
    msg->set_reply( prefix, prefix_len );
    msg->set_src_type( src_type );
    msg->set_prefix_cnt( 1 );
    KvPrefHash & ph = msg->prefix_hash( 0 );
    ph.pref = prefix_len;
    ph.set_hash( h );
  }
  return msg;
}

const char *
KvMsg::msg_type_string( uint8_t msg_type ) noexcept
{
  switch ( (KvMsgType) msg_type ) {
    case KV_MSG_HELLO:    return "hello";
    case KV_MSG_BYE:      return "bye";
    case KV_MSG_SUB:      return "sub";
    case KV_MSG_UNSUB:    return "unsub";
    case KV_MSG_PSUB:     return "psub";
    case KV_MSG_PUNSUB:   return "punsub";
    case KV_MSG_PUBLISH:  return "publish";
    case KV_MSG_FRAGMENT: return "fragment";
  }
  return "unknown";
}

const char *
KvMsg::msg_type_string( void ) const noexcept
{
  return KvMsg::msg_type_string( this->msg_type );
}

void
KvMsg::print( void ) noexcept
{
  printf( "\r\nseqno      : %lu\r\n"
          "stamp      : 0x%lx\r\n"
          "size       : %u\r\n"
          "src        : %u\r\n"
          "dest_start : %u\r\n"
          "dest_end   : %u\r\n"
          "msg_type   : %s\r\n",
    this->get_seqno(), this->get_stamp(), this->size, this->src,
    this->dest_start, this->dest_end, msg_type_string( (KvMsgType)
    this->msg_type ) );

  if ( this->msg_type >= KV_MSG_SUB && this->msg_type <= KV_MSG_FRAGMENT ) {
    KvSubMsg &sub = (KvSubMsg &) *this;
    uint8_t prefix_cnt = sub.get_prefix_cnt();
    printf( "hash       : %x\r\n"
            "msg_size   : %u\r\n"
            "sublen     : %u\r\n"
            "code       : %u\r\n"
            "msg_enc    : %u\r\n"
            "prefix_cnt : %u\r\n"
            "replylen   : %u\r\n"
            "subject()  : %s\r\n"
            "reply()    : %s\r\n",
      sub.hash, sub.msg_size, sub.sublen, sub.code,
      sub.msg_enc, prefix_cnt,
      sub.replylen, sub.subject(), sub.reply() );
    if ( prefix_cnt > 0 ) {
      for ( uint8_t i = 0; i < prefix_cnt; i++ ) {
        KvPrefHash &pf = sub.prefix_hash( i );
        printf( "pf[ %u ] : %u, %x\r\n", i, pf.pref, pf.get_hash() );
      }
    }
    if ( this->msg_type == KV_MSG_FRAGMENT ) {
      printf( "msg_off    : %lu\r\n", sub.get_frag_off() );
    }
#if 0
    if ( this->msg_type >= KV_MSG_PUBLISH ) {
      printf( "msg_data() : %.*s\r\n", sub.msg_size,
                                       (char *) sub.get_msg_data() );
    }
#endif
  }
  KvHexDump::dump_hex( this, this->size );
}

void
KvMsg::print_sub( void ) noexcept
{
  if ( this->msg_type >= KV_MSG_SUB && this->msg_type <= KV_MSG_FRAGMENT) {
    KvSubMsg &sub = (KvSubMsg &) *this;
    printf( "ctx(%u) %s %s\n", this->src,
                               msg_type_string( (KvMsgType) this->msg_type ),
                               sub.subject() );
  }
}

KvFragAsm *
KvFragAsm::merge( KvFragAsm *&fragp,  const KvSubMsg &msg ) noexcept
{
  KvFragAsm *frag = fragp;
  uint64_t seqno = msg.get_seqno();
  if ( msg.msg_type == KV_MSG_FRAGMENT ) {
    size_t   off   = msg.get_frag_off(),
             size  = off + msg.msg_size;

    if ( frag == NULL || frag->frag_count == 0 ) {
    create_new_frag:;
      frag = (KvFragAsm *) ::realloc( frag, sizeof( KvFragAsm ) + size );
      frag->buf_size    = size;
      frag->first_seqno = seqno;
      frag->frag_count  = 1;
      frag->msg_size    = size;
    }
    /* check if in sequence with a previous frag from src */
    else {
      if ( seqno - frag->frag_count == frag->first_seqno ) {
        if ( size > frag->msg_size ) {
          fprintf( stderr,
            "bad fragment(%.*s), off %lu + %u > %lu\n",
            (int) msg.sublen, msg.subj(), off, msg.msg_size,
            frag->msg_size );
          goto create_new_frag;
        }
        frag->frag_count++;
      }
      else {
        fprintf( stderr,
          "bad fragment(%.*s), seqno %lu - frag_count %lu != first %lu\n",
          (int) msg.sublen, msg.subj(), seqno, frag->frag_count,
          frag->first_seqno );
        goto create_new_frag;
      }
    }
    ::memcpy( &frag->buf[ off ], msg.get_msg_data(), msg.msg_size );
    fragp = frag;
    return NULL;
  }
  if ( frag == NULL || frag->msg_size < msg.msg_size ) {
    fprintf( stderr,
      "bad fragment(%.*s), frag->msg_size %lu < msg_size %u\n",
      (int) msg.sublen, msg.subj(), frag == NULL ? 0 : frag->msg_size,
      msg.msg_size );
  }
  else if ( seqno - frag->frag_count != frag->first_seqno ) {
    fprintf( stderr,
      "bad fragment(%.*s), seqno %lu - frag_count %lu != first %lu\n",
      (int) msg.sublen, msg.subj(), seqno, frag->frag_count,
      frag->first_seqno );
  }
  else {
    ::memcpy( frag->buf, msg.get_msg_data(), msg.msg_size );
    fragp = frag;
    frag->frag_count = 0;
    return frag;
  }
  if ( frag != NULL )
    ::free( frag );
  fragp = NULL;
  return NULL;
}

void
KvFragAsm::release( KvFragAsm *&fragp ) noexcept
{
  KvFragAsm *frag = fragp;
  if ( frag != NULL ) {
    fragp = NULL;
    ::free( frag );
  }
}

