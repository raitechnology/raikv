#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
#include <unistd.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/un.h>
#else
#include <raikv/win.h>
#endif
#include <errno.h>

#include <raikv/os_file.h>
#include <raikv/ev_publish.h>
#include <raikv/kv_pubsub.h>

using namespace rai;
using namespace kv;
uint32_t rai::kv::kv_ps_debug = 0;

static const char KVPS_MAGIC[] = "PsCtrlFile.1";

KvPubSub::KvPubSub( RoutePublish &sr,  PsCtrlFile &cf,  const char *ipc_nm,
                    uint64_t ipc_tok,  const char *nm ) noexcept
  : KVPS_LISTEN( sr.poll, "pubsub_listen", "pusub_conn" ),
    RouteNotify( sr ), sub_route( sr ), ctrl( cf ), init_ns( 0 ),
    sub_seqno( 1 ), serial( 0 ), ctx_id( KVPS_CTRL_CTX_SIZE ),
    ipc_name( ipc_nm ), ipc_token( ipc_tok ),
    peer_sock_type( sr.poll.register_type( "kv_pub_sub_peer" ) )
{
  size_t len = ::strlen( nm );
  if ( len > sizeof( this->ctx_name ) - 1 )
    len = sizeof( this->ctx_name ) - 1;
  ::memset( this->ctx_name, 0, sizeof( this->ctx_name ) );
  ::memcpy( this->ctx_name, nm, len );
}

KvPubSubPeer::KvPubSubPeer( EvPoll &p,  uint8_t st,  KvPubSub &m ) noexcept
  : EvConnection( p, st ), sub_route( m.sub_route ), me( m ),
    ctrl( m.ctrl ), bloom_rt( 0 ), time_ns( 0 ), sub_seqno( 0 ),
    ctx_id( KVPS_CTRL_CTX_SIZE ), is_shutdown( false ), next( 0 ), back( 0 )
{
}

KvPubSub *
KvPubSub::create( RoutePublish &sr,  const char *ipc_name,
                  uint64_t ipc_token,  const char *ctx_name ) noexcept
{
  if ( ipc_name == NULL )
    ipc_name = "raikv";

  MapFile map( ipc_name, sizeof( PsCtrlFile ) );
  int fl = MAP_FILE_SHM | MAP_FILE_RDWR | MAP_FILE_CREATE | MAP_FILE_NOUNMAP;
  if ( ! map.open( fl ) ) {
    perror( ipc_name );
    return NULL;
  }
  if ( map.map_size != sizeof( PsCtrlFile ) ) {
    fprintf( stderr, "kv ctrl file %s incorrect size\n", ipc_name );
    map.no_unmap = false;
    map.close();
    return NULL;
  }
  PsCtrlFile * ctrl = (PsCtrlFile *) map.map;
  if ( map.is_new ) {
    ::memcpy( ctrl->magic, KVPS_MAGIC, sizeof( KVPS_MAGIC ) );
    ctrl->next_serial = 1;
    ctrl->ipc_token   = ipc_token;
    kv_release_fence();
    ctrl->ctrl_ready_spin.add( 1 );
  }
  else {
    while ( ctrl->ctrl_ready_spin.add( 0 ) == 0 )
      kv_sync_pause();
  }

  bool ret = true;
  while ( ctrl->init_ctx_spin.xchg( 1 ) != 0 )
    kv_sync_pause();

  KvPubSub * ps = NULL;
  bool magic_ok  = ::memcmp( ctrl->magic, KVPS_MAGIC, sizeof( KVPS_MAGIC ))== 0,
       ipc_ok    = true,
       reset_ipc = false;
  uint32_t dead  = 0,
           alive = 0;
  if ( ! magic_ok )
    ret = false;

  if ( ret ) {
    ctrl->check_dead_pids( dead, alive );
    if ( ctrl->ipc_token == 0 ) {
      if ( ipc_token != 0 )
        ctrl->ipc_token = ipc_token;
    }
    if ( ipc_token != 0 && ipc_token != ctrl->ipc_token ) {
      if ( alive == 0 ) {
        reset_ipc = true;
        ctrl->ipc_token = ipc_token;
      }
    }
    ipc_ok = ( ipc_token == 0 || ipc_token == ctrl->ipc_token );
    if ( ! ipc_ok )
      ret = false;
  }
  if ( ret ) {
    ps = new ( aligned_malloc( sizeof( KvPubSub ) ) )
               KvPubSub( sr, *ctrl, ipc_name, ipc_token, ctx_name );
    ret = ps->init();
  }
  kv_release_fence();
  ctrl->init_ctx_spin = 0;

  if ( dead != 0 )
    fprintf( stderr, "kv ctrl %s cleared %u dead pids\n", ipc_name, dead );
  if ( reset_ipc )
    fprintf( stderr, "kv ctrl %s ipc token reset, no pids alive\n", ipc_name );
  if ( ret && ps != NULL ) {
    sr.add_route_notify( *ps );
    return ps;
  }
  if ( ! magic_ok )
    fprintf( stderr, "kv ctrl file bad magic (%s)\n", ipc_name );
  if ( ! ipc_ok )
    fprintf( stderr, "kv ctrl ipc token %" PRIx64 " not matched "
                     "(old kv app still alive?), pids alive %u (%s)\n",
             ipc_token, alive, ipc_name );
  fprintf( stderr, "unable to attach to ipc ctrl file (%s)\n", ipc_name );
  return NULL;
}

void
PsCtrlFile::check_dead_pids( uint32_t &dead,  uint32_t &alive ) noexcept
{
  dead  = 0;
  alive = 0;
  for ( uint32_t ctx_id = 0; ctx_id < KVPS_CTRL_CTX_SIZE; ctx_id++ ) {
    PsCtrlCtx & el = this->ctx[ ctx_id ];
    PsGuard     guard( el, 0 );

    if ( el.serial != 0 ) {
      uint32_t pid = el.pid;
      alive++;
  #if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
      if ( ::kill( pid, 0 ) == 0 )
        continue;
      if ( errno == EPERM )
        continue;
      fprintf( stderr, "ctx %u: pid %u = kill errno %d/%s\n",
               ctx_id, pid, errno, strerror( errno ) );
  #else
      HANDLE process = OpenProcess( SYNCHRONIZE, FALSE, pid );
      DWORD  ret     = WaitForSingleObject( process, 0 );
      CloseHandle( process );
      if ( ret == WAIT_TIMEOUT )
        continue;
      fprintf( stderr, "ctx %u: pid %u = wait status %ld\n",
               ctx_id, pid, ret );
  #endif
      el.serial = 0;
      alive--;
      dead++;
    }
  }
}

static uint64_t
get_init_ns( void ) noexcept
{
  uint64_t ns = current_realtime_ns();
  for ( uint64_t d = 1000000; d > 10; d /= 10 ) {
    if ( ( ns % d ) == 0 ) {
      uint64_t r;
      rand::fill_urandom_bytes( &r, sizeof( r ) );
      ns += r % d;
      break;
    }
  }
  return ns;
}

bool
KvPubSub::init( void ) noexcept
{
  bool ret = true;
  uint32_t i;
  /* make sure that instances are added in the order of this->serial,
   * because this connects to any serial < my serial, but serials > my serial
   * connect to me */
  this->serial = this->ctrl.next_serial.add( 1 );
  for ( i = 0; i < 10; i++ ) {
    this->init_ns = get_init_ns();
#ifdef KVPS_USE_UNIX_SOCKET
    char path[ 128 ];
    ::snprintf( path, sizeof( path ), "/tmp/%s", this->ipc_name );
    ::mkdir( path, 0777 );
    ::snprintf( path, sizeof( path ), "/tmp/%s/%lx", this->ipc_name,
                this->init_ns );
    if ( this->listen2( path, DEFAULT_UNIX_LISTEN_OPTS,
                        "kv_pubsub", this->sub_route.route_id ) == 0 )
      break;
#else
    uint16_t port = (uint16_t) ( this->init_ns | 0x8000 );
    if ( this->listen2( "127.0.0.1", port, DEFAULT_TCP_LISTEN_OPTS,
                        "kv_pubsub", this->sub_route.route_id ) == 0 )
      break;
#endif
  }
  if ( i == 10 )
    ret = false;
  if ( ret && ! this->attach_ctx() )
    ret = false;
  if ( ret ) {
    for ( i = 0; i < KVPS_CTRL_CTX_SIZE; i++ ) {
      if ( i == this->ctx_id )
        continue;
      PsCtrlCtx & el = this->ctrl.ctx[ i ];
      PsGuard     guard( el, this->init_ns );
      uint64_t    peer_ns = 0;
      if ( el.serial != 0 ) {
        if ( el.serial < this->serial ) {
          peer_ns = el.time_ns;
          if ( kv_ps_debug )
            printf( "connect to %" PRIx64 " ser=%u my=%u\n", peer_ns, el.serial,
                    this->serial );
        }
      }
      guard.unlock();

      if ( peer_ns != 0 ) {
        int status;
        KvPubSubPeer * c =
          this->poll.get_free_list<KvPubSubPeer,
                                   KvPubSub &>( this->peer_sock_type, *this );
        if ( c != NULL ) {
          c->time_ns = peer_ns;
          c->ctx_id  = i;
#ifdef KVPS_USE_UNIX_SOCKET
          char path[ 128 ];
          ::snprintf( path, sizeof( path ), "/tmp/%s/%lx", this->ipc_name,
                      peer_ns );
          status = EvUnixConnection::connect( *c, path,
                                              DEFAULT_UNIX_CONNECT_OPTS,
                                             "kv_pubsub_peer",
                                             this->sub_route.route_id );
#else
          uint16_t port = (uint16_t) ( peer_ns | 0x8000 );
          status = EvTcpConnection::connect2( *c, "127.0.0.1", port,
                                              DEFAULT_TCP_CONNECT_OPTS,
                                              "kv_pubsub_peer",
                                              this->sub_route.route_id );
#endif
          if ( status == 0 ) {
            this->peer_list.push_tl( c );
            this->peer_set.add( c->fd );
            this->send_hello( *c );
          }
        }
      }
    }
  }
  if ( ret ) {
    this->sub_tab.time_ns = this->init_ns;
    this->sub_tab.sub_upd = this->ctrl.ctx[ this->ctx_id ].sub_upd;
    this->PeerData::set_name( this->ipc_name, ::strlen( this->ipc_name ) );
    return true;
  }
  return false;
}

void
KvPubSub::send_hello( KvPubSubPeer &c ) noexcept
{
  size_t   data_len  = this->sub_tab.vec_size * sizeof( PsSubTab::VecData );
  uint32_t i;

  KvEst e;
  e.ctx_id()
   .time_ns()
   .sub_seqno()
   .data( data_len );

  KvMsg &m = *(new ( c.alloc_temp( e.len() ) ) KvMsg( KV_MSG_HELLO ) );
  m.ctx_id( this->ctx_id )
   .time_ns( this->init_ns )
   .sub_seqno( this->sub_seqno - 1 );

  uint8_t * data = (uint8_t *) m.ptr( KV_FLD_DATA, data_len );
  for ( i = 0; i < this->sub_tab.vec_size; i++ ) {
    ::memcpy( data, this->sub_tab.vec[ i ], sizeof( PsSubTab::VecData ) );
    data = &data[ sizeof( PsSubTab::VecData ) ];
  }
  c.append_iov( (void *) m.msg(), m.len() );

  for ( BloomRoute *rt = this->sub_route.bloom_list.hd( 0 ); rt != NULL;
        rt = rt->next ) {
    if ( this->peer_set.is_member( rt->r ) )
      continue;
    for ( i = 0; i < rt->nblooms; i++ ) {
      BloomRef * ref = rt->bloom[ i ];
      if ( &ref->bloom_db != &this->sub_route.g_bloom_db )
        continue;
      BloomCodec code;
      ref->encode( code );
      size_t len = ::strlen( ref->name ) + 1;

      KvEst e;
      e.name( len )
       .ref_num()
       .data( code.code_sz * 4 );

      KvMsg &m = *(new ( c.alloc_temp( e.len() ) ) KvMsg( KV_MSG_BLOOM ) );
      m.name( ref->name, len )
       .ref_num( ref->ref_num )
       .data( code.ptr, code.code_sz * 4 );

      c.append_iov( (void *) m.msg(), m.len() );
      c.msgs_sent++;
    }
  }
  c.idle_push_write();
}

void
KvPubSub::bcast_msg( KvMsg &m ) noexcept
{
  for ( KvPubSubPeer * c = this->peer_list.hd; c != NULL; c = c->next ) {
    c->append( m.msg(), m.len() );
    c->msgs_sent++;
    c->idle_push_write();
  }
}

void
KvPubSubPeer::process( void ) noexcept
{
  typedef void (KvPubSubPeer::*dispatch_func)( KvMsgIn & );
  static const dispatch_func dispatch[] = kv_dispatch_msg;
  KvMsgIn  msg;
  KvMsgErr err;
  for (;;) {
    err = msg.decode( &this->recv[ this->off ], this->len - this->off );
    if ( err == KV_NOT_ENOUGH )
      break;
    if ( err != KV_MSG_OK ) {
      fprintf( stderr, "kv pub sub peer error %d\n", err );
      this->pushpop( EV_CLOSE, EV_PROCESS );
      return;
    }
    (this->*dispatch[ msg.type ])( msg );
    this->msgs_recv++;
    this->off += msg.len;
  }
  this->pop( EV_PROCESS );
}

void
KvPubSubPeer::process_shutdown( void ) noexcept
{
  if ( kv_ps_debug )
    printf( "shutdown %" PRIx64 "\n", this->time_ns );
  if ( ! this->is_shutdown ) {
    this->is_shutdown = true;
    if ( this->poll.quit )
      this->time_ns = 0;
    this->sock_opts &= ~OPT_VERBOSE; /* may be disconnected */
    KvMsg m( KV_MSG_BYE );
    m.ctx_id( this->me.ctx_id )
     .time_ns( this->me.init_ns );
    this->append( m.msg(), m.len() );
    this->msgs_sent++;
    this->push( EV_WRITE );
  }
  else {
    this->push( EV_CLOSE );
  }
}

KvMsgErr
KvMsgIn::decode( const char *msg,  uint32_t msglen ) noexcept
{
  if ( msglen < 6 )
    return KV_NOT_ENOUGH;
  ::memcpy( &this->len, msg, 4 );
  this->len += 4;
  if ( this->len > msglen )
    return KV_NOT_ENOUGH;
  if ( (uint8_t) msg[ 4 ] != 0xab )
    return KV_BAD_MAGIC;
  this->type = (KvMsgType) msg[ 5 ];
  if ( this->type >= KV_MSG_MAX )
    return KV_BAD_MSG_TYPE;
  this->fld_set = 0;

  uint8_t  ftype;
  uint32_t fsize, vlen;
  size_t   off = 6;

  while ( off < (size_t) this->len ) {
    this->decode_ftype( &msg[ off ], ftype, fsize );

    uint32_t mask      = 1U << ftype;
    uint32_t & fsz     = this->fld_size[ ftype ];
    const char * &fptr = this->fld[ ftype ];

    this->fld_set |= mask;
    /* fixed length field */
    if ( ( (uint8_t) msg[ off ] & 0x40 ) == 0 ) {
      fsz  = fsize;
      fptr = &msg[ off + 1 ];
      off += 1 + fsize;
    }
    /* varialbe length */
    else {
      if ( off + 1 + fsize > msglen )
        return KV_BAD_FLD_LEN;
      if ( ! this->decode_vlen( &msg[ off ], fsize, vlen ) )
        return KV_BAD_FLD_LEN;

      fsz  = vlen;
      fptr = &msg[ off + fsize + 1 ];
      off += 1 + fsize + vlen;
    }
    if ( off > msglen )
      return KV_BAD_FLD_LEN;
  }
  return KV_MSG_OK;
}

bool
KvPubSubPeer::on_msg( EvPublish &pub ) noexcept
{
  if ( pub.pub_type == 'K' )
    return true;
  KvEst e;
  e.subject    ( pub.subject_len )
   .reply      ( pub.reply_len )
   .subj_hash  ()
   .msg_enc    ()
   .pub_status ()
   .data       ( pub.msg_len );

  KvMsg &m = *(new ( this->alloc_temp( e.len() ) ) KvMsg( KV_MSG_FWD ));
  m.subject  ( pub.subject, pub.subject_len )
   .reply    ( pub.reply, pub.reply_len )
   .subj_hash( pub.subj_hash )
   .msg_enc  ( pub.msg_enc );
  if ( pub.pub_status != 0 )
    m.pub_status( pub.pub_status );
  m.data     ( pub.msg, pub.msg_len );
  this->append_iov( (void *) m.msg(), m.len() );
  this->msgs_sent++;
  return this->idle_push_write();
}

void
KvPubSubPeer::fwd_msg( KvMsgIn &msg ) noexcept
{
  uint32_t     subject_len, reply_len, data_len;
  const char * subject   = msg.get_field( KV_FLD_SUBJECT, subject_len ),
             * reply     = msg.get_field( KV_FLD_REPLY, reply_len ),
             * data      = msg.get_field( KV_FLD_DATA, data_len );
  uint32_t     subj_hash = msg.get<uint32_t>( KV_FLD_SUBJ_HASH ),
               msg_enc   = msg.get<uint32_t>( KV_FLD_MSG_ENC );

  if ( msg.is_field_missing() )
    return;
  if ( kv_ps_debug )
    msg.print();
  EvPublish pub( subject, subject_len, reply, reply_len, data, data_len,
                 this->sub_route, *this, subj_hash, msg_enc, 'K' );
  if ( msg.is_set( KV_FLD_PUB_STATUS ) )
    pub.pub_status = msg.get<uint16_t>( KV_FLD_PUB_STATUS );
  this->sub_route.forward_msg( pub );
}

static const struct {
  KvFieldType t;
  uint8_t sz;
  bool p;
  const char *nm;
} print_fld[] = {
  { KV_FLD_CTX_ID,      4, 1, "ctx_id" },
  { KV_FLD_TIME_NS,     8, 1, "time_ns" },
  { KV_FLD_SUB_SEQNO,   8, 1, "sub_seqno" },
  { KV_FLD_SUBJECT,     0, 1, "subject" },
  { KV_FLD_REPLY,       0, 1, "reply" },
  { KV_FLD_SUBJ_HASH,   4, 1, "subj_hash" },
  { KV_FLD_SUB_COUNT,   4, 1, "sub_count" },
  { KV_FLD_HASH_COLL,   1, 1, "hash_coll" },
  { KV_FLD_PATTERN,     0, 1, "pattern" },
  { KV_FLD_PAT_FMT,     1, 1, "pat_fmt" },
  { KV_FLD_MSG_ENC,     4, 1, "msg_enc" },
  { KV_FLD_DATA,        0, 0, "data" },
  { KV_FLD_REF_NUM,     4, 1, "ref_num" },
  { KV_FLD_NAME,        0, 1, "name" },
  { KV_FLD_PUB_STATUS,  2, 1, "pub_status" }
};
static const char * print_msg_name[] = kv_msg_name;

void
KvMsgIn::print( void ) noexcept
{
  uint32_t     var_len;
  const char * var, * c = "";
  printf( "kv_msg_type %s: %d {", print_msg_name[ this->type ], this->type );
  for ( int i = 0; i < KV_FLD_MAX; i++ ) {
    if ( ( this->fld_set & ( 1 << i ) ) != 0 ) {
      if ( print_fld[ i ].t != i )
        printf( "%sunknown:%u", c, i );
      else if ( print_fld[ i ].sz == 4 )
        printf( "%s%s:%u", c, print_fld[ i ].nm, this->get<uint32_t>( i ) );
      else if ( print_fld[ i ].sz == 8 )
        printf( "%s%s:%" PRIu64, c, print_fld[ i ].nm, this->get<uint64_t>( i ) );
      else if ( print_fld[ i ].sz == 1 )
        printf( "%s%s:%u", c, print_fld[ i ].nm, this->get<uint8_t>( i ) );
      else if ( print_fld[ i ].sz == 2 )
        printf( "%s%s:%u", c, print_fld[ i ].nm, this->get<uint16_t>( i ) );
      else {
        var = this->get_field( i, var_len );
        if ( ! print_fld[ i ].p )
          printf( "%s%s:len:%u", c, print_fld[ i ].nm, var_len );
        else
          printf( "%s%s:%.*s", c, print_fld[ i ].nm, var_len, var );
      }
      c = ", ";
    }
  }
  printf( "}\n" );
}

void
KvMsgIn::missing_error( void ) noexcept
{
  const char *mstr = "??";
  for ( int i = 0; i < KV_FLD_MAX; i++ ) {
    if ( ( this->missing & ( 1 << i ) ) != 0 ) {
      mstr = print_fld[ i ].nm;
      break;
    }
  }
  fprintf( stderr, "field %x/%s is missing from %s\n",
           this->missing, mstr, print_msg_name[ this->type ] );
}

void
KvPubSubPeer::hello_msg( KvMsgIn &msg ) noexcept
{
  uint32_t tab_len;
  void   * sub_tab = (void *) msg.get_field( KV_FLD_DATA, tab_len );
  this->ctx_id     = msg.get<uint32_t>( KV_FLD_CTX_ID );
  this->time_ns    = msg.get<uint64_t>( KV_FLD_TIME_NS );
  this->sub_seqno  = msg.get<uint64_t>( KV_FLD_SUB_SEQNO );

  if ( msg.is_field_missing() )
    return;
  /*this->me.peer_set.add( this->fd );*/
  if ( kv_ps_debug )
    msg.print();
  if ( tab_len > 0 ) {
    const intptr_t align_mask = sizeof( void * ) - 1;
    if ( ( ( (intptr_t) sub_tab ) & align_mask ) != 0 ) {
      void * sub_tab2 = (void *) ( (intptr_t) sub_tab & ~align_mask );
      ::memmove( sub_tab2, sub_tab, tab_len );
      sub_tab = sub_tab2;
    }
    this->assert_subs( sub_tab, tab_len );
  }
}

void
KvPubSubPeer::bloom_msg( KvMsgIn &msg ) noexcept
{
  uint32_t     ref_num = msg.get<uint32_t>( KV_FLD_REF_NUM );
  uint32_t     bloom_len, name_len;
  void       * bloom = (void *) msg.get_field( KV_FLD_DATA, bloom_len );
  const char * name  = msg.get_field( KV_FLD_NAME, name_len );

  if ( msg.is_field_missing() )
    return;
  if ( kv_ps_debug )
    msg.print();
  if ( bloom_len > 0 ) {
    const intptr_t align_mask = sizeof( uint32_t ) - 1;
    if ( ( ( (intptr_t) bloom ) & align_mask ) != 0 ) {
      void * bloom2 = (void *) ( (intptr_t) bloom & ~align_mask );
      ::memmove( bloom2, bloom, bloom_len );
      bloom = bloom2;
    }
    BloomRef * ref = this->sub_route.update_bloom_ref( bloom, bloom_len,
                                                       ref_num, name,
                                                       this->bloom_db );
    if ( this->bloom_rt == NULL )
      this->bloom_rt = this->sub_route.create_bloom_route( this->fd, ref, 0 );
    else if ( ! ref->has_link( this->fd ) )
      this->bloom_rt->add_bloom_ref( ref );
  }
}

void
KvPubSubPeer::bloom_del_msg( KvMsgIn &msg ) noexcept
{
  uint32_t ref_num = msg.get<uint32_t>( KV_FLD_REF_NUM );

  if ( msg.is_field_missing() )
    return;
  if ( kv_ps_debug )
    msg.print();

  BloomRef * ref = this->bloom_db[ ref_num ];
  if ( ref != NULL && this->bloom_rt != NULL ) {
    this->bloom_rt->del_bloom_ref( ref );
    if ( ref->nlinks == 0 ) {
      this->sub_route.remove_bloom_ref( ref );
    }
  }
}

void
KvPubSubPeer::bye_msg( KvMsgIn &msg ) noexcept
{
  uint32_t ctx_id  = msg.get<uint32_t>( KV_FLD_CTX_ID );
  uint64_t time_ns = msg.get<uint64_t>( KV_FLD_TIME_NS );

  if ( msg.is_field_missing() )
    return;
  if ( kv_ps_debug )
    msg.print();

  if ( ctx_id == this->ctx_id && time_ns == this->time_ns ) {
    this->drop_bloom_refs();
    this->time_ns = 0;
  }
}

void
KvPubSubPeer::drop_bloom_refs( void ) noexcept
{
  if ( this->bloom_rt != NULL ) {
    BloomRef * ref;
    while ( (ref = this->bloom_rt->del_bloom_ref( NULL )) != NULL ) {
      if ( ref->nlinks == 0 ) {
        this->sub_route.remove_bloom_ref( ref );
      }
    }
    this->sub_route.remove_bloom_route( this->bloom_rt );
    this->bloom_rt = NULL;
  }
}

void
KvPubSubPeer::on_sub_msg( KvMsgIn &msg ) noexcept
{
  this->do_sub_msg( msg, true );
}

void
KvPubSubPeer::on_unsub_msg( KvMsgIn &msg ) noexcept
{
  this->do_sub_msg( msg, false );
}

void
KvPubSubPeer::do_sub_msg( KvMsgIn &msg,  bool is_sub ) noexcept
{
  uint64_t     sub_seqno = msg.get<uint64_t>( KV_FLD_SUB_SEQNO );
  uint32_t     subj_hash = msg.get<uint32_t>( KV_FLD_SUBJ_HASH );
  uint32_t     subject_len, reply_len;
  const char * subject   = msg.get_field( KV_FLD_SUBJECT, subject_len ),
             * reply     = msg.get_field( KV_FLD_REPLY, reply_len );
  uint32_t     sub_count = msg.get<uint32_t>( KV_FLD_SUB_COUNT );
  uint8_t      hash_coll = msg.get<uint8_t>( KV_FLD_HASH_COLL );
  uint32_t     ref_num;

  if ( msg.is_field_missing() )
    return;
  if ( kv_ps_debug )
    msg.print();
  if ( sub_seqno > this->sub_seqno ) {
    this->sub_seqno = sub_seqno;
    if ( msg.is_set( KV_FLD_REF_NUM ) ) {
      ref_num = msg.get<uint32_t>( KV_FLD_REF_NUM );
      BloomRef * ref = this->bloom_db[ ref_num ];
      if ( ref != NULL ) {
        if ( is_sub )
          ref->add( subj_hash );
        else
          ref->del( subj_hash );
      }
    }
    else {
      NotifySub nsub( subject, subject_len, reply, reply_len, subj_hash,
                      hash_coll, 'K', *this );
      nsub.sub_count = sub_count;
      if ( is_sub )
        this->sub_route.add_sub( nsub );
      else
        this->sub_route.del_sub( nsub );
    }
  }
}

void
KvPubSubPeer::on_psub_msg( KvMsgIn &msg ) noexcept
{
  this->do_psub_msg( msg, true );
}

void
KvPubSubPeer::on_punsub_msg( KvMsgIn &msg ) noexcept
{
  this->do_psub_msg( msg, false );
}

void
KvPubSubPeer::do_psub_msg( KvMsgIn &msg,  bool is_sub ) noexcept
{
  uint64_t     sub_seqno = msg.get<uint64_t>( KV_FLD_SUB_SEQNO );
  uint32_t     pref_hash = msg.get<uint32_t>( KV_FLD_SUBJ_HASH );
  uint32_t     pattern_len, reply_len;
  const char * pattern   = msg.get_field( KV_FLD_PATTERN, pattern_len ),
             * reply     = msg.get_field( KV_FLD_REPLY, reply_len );
  uint32_t     sub_count = msg.get<uint32_t>( KV_FLD_SUB_COUNT );
  uint8_t      pat_fmt   = msg.get<uint8_t>( KV_FLD_PAT_FMT );
  uint8_t      hash_coll = msg.get<uint8_t>( KV_FLD_HASH_COLL );
  uint16_t     pref_len;
  uint32_t     ref_num;

  if ( msg.is_field_missing() )
    return;
  if ( kv_ps_debug )
    msg.print();
  if ( sub_seqno > this->sub_seqno ) {
    this->sub_seqno = sub_seqno;
    PatternCvt cvt;
    if ( pat_fmt == RV_PATTERN_FMT )
      cvt.convert_rv( pattern, pattern_len );
    else
      cvt.convert_glob( pattern, pattern_len );
    pref_len = cvt.prefixlen;
    if ( msg.is_set( KV_FLD_REF_NUM ) ) {
      ref_num = msg.get<uint32_t>( KV_FLD_REF_NUM );
      BloomRef * ref = this->bloom_db[ ref_num ];
      if ( ref != NULL ) {
        BloomDetail d;
        if ( d.from_pattern( cvt ) ) {
          if ( d.detail_type == NO_DETAIL ) {
            if ( is_sub )
              ref->add_route( pref_len, pref_hash );
            else
              ref->del_route( pref_len, pref_hash );
          }
          else if ( d.detail_type == SUFFIX_MATCH ) {
            if ( is_sub )
              ref->add_suffix_route( pref_len, pref_hash, d.u.suffix );
            else
              ref->del_suffix_route( pref_len, pref_hash, d.u.suffix );
          }
          else if ( d.detail_type == SHARD_MATCH ) {
            if ( is_sub )
              ref->add_shard_route( pref_len, pref_hash, d.u.shard );
            else
              ref->del_shard_route( pref_len, pref_hash, d.u.shard );
          }
        }
      }
    }
    else {
      NotifyPattern npat( cvt, pattern, pattern_len, reply, reply_len,
                          pref_hash, hash_coll, 'K', *this );
      npat.sub_count = sub_count;
      if ( is_sub )
        this->sub_route.add_pat( npat );
      else
        this->sub_route.del_pat( npat );
    }
  }
}

void
KvPubSubPeer::process_close( void ) noexcept
{
  if ( kv_ps_debug )
    printf( "kv_pubsub: close %u\n", this->ctx_id );
  this->client_stats( this->sub_route.peer_stats );
  this->EvSocket::process_close();
}

void
KvPubSubPeer::release( void ) noexcept
{
  if ( kv_ps_debug )
    printf( "kv_pubsub: release %u %" PRIx64 "\n", this->ctx_id, this->time_ns );
  if ( this->time_ns != 0 )
    fprintf( stderr, "kv_pubsub: peer did not msg bye\n" );
  if ( this->bloom_rt != NULL )
    this->drop_bloom_refs();
  if ( this->time_ns != 0 )
    this->drop_sub_tab();
  if ( this->me.peer_set.is_member( this->fd ) ) {
    this->me.peer_set.remove( this->fd );
    this->me.peer_list.pop( this );
  }
  this->EvConnection::release_buffers();
}

void
KvPubSubPeer::assert_subs( void *data,  uint32_t data_len ) noexcept
{
  PsSubTab sub_tab;
  if ( sub_tab.load( data, data_len ) ) {
    uint32_t count = this->iter_sub_tab( sub_tab, true );
    if ( kv_ps_debug )
      printf( "kv_pubsub: assert %u from sub_tab %" PRIx64 "\n", count, this->time_ns );
  }
  else {
    fprintf( stderr, "kv_pubsub: failed to assert sub_tab %" PRIx64 "\n",
             this->time_ns );
  }
  sub_tab.release_vec();
}

void
KvPubSubPeer::drop_sub_tab( void ) noexcept
{
  PsSubTab sub_tab;
  sub_tab.time_ns = this->time_ns;
  sub_tab.sub_upd = this->ctrl.ctx[ this->ctx_id ].sub_upd;
  if ( sub_tab.sub_upd[ 1 ] != 0 ) {
    if ( sub_tab.recover_lost_subs() ) {
      uint32_t count = this->iter_sub_tab( sub_tab, false );
      sub_tab.unmap_vec_data();
      fprintf( stderr, "kv_pubsub: recovered %u from lost sub_tab %" PRIx64 "\n", count,
               this->time_ns );
    }
    else {
      fprintf( stderr, "kv_pubsub: failed to load lost sub_tab %" PRIx64 "\n",
               this->time_ns );
    }
  }
  sub_tab.release_vec();
}

uint32_t
KvPubSubPeer::iter_sub_tab( PsSubTab &sub_tab,  bool add ) noexcept
{
  RouteLoc loc;
  uint32_t count = 0;
  for ( PsSub *sub = sub_tab.first( loc ); sub != NULL;
        sub = sub_tab.next( loc ) ) {
    if ( sub->type == PS_SUB ) {
      NotifySub nsub( sub->value, sub->len, sub->hash, 0, 'K', *this );
      if ( add )
        this->sub_route.add_sub( nsub );
      else
        this->sub_route.del_sub( nsub );
      count++;
    }
    else if ( sub->type == PS_PATTERN ) {
      PatternCvt cvt;
      bool       b;
      if ( sub->fmt == RV_PATTERN_FMT )
        b = cvt.convert_glob( sub->value, sub->len );
      else
        b = cvt.convert_rv( sub->value, sub->len );
      if ( b ) {
        uint32_t h = kv_crc_c( sub->value, cvt.prefixlen,
                               this->sub_route.prefix_seed( cvt.prefixlen ) );
        NotifyPattern npat( cvt, sub->value, sub->len, h, 0, 'K', *this );
        if ( add )
          this->sub_route.add_pat( npat );
        else
          this->sub_route.del_pat( npat );
        count++;
      }
    }
  }
  return count;
}

bool
PsSubTab::load( void *data,  uint32_t data_len ) noexcept
{
  ArrayCount<VecData *, 64> tmp;
  size_t    i;
  if ( data == NULL || data_len == 0 )
    return false;
  for ( i = 0; data_len > 0; ) {
    tmp[ i ] = (VecData *) data;
    if ( data_len < sizeof( VecData ) ) {
      tmp.clear();
      return false;
    }
    data = &((uint8_t *) data)[ sizeof( VecData ) ];
    data_len -= sizeof( VecData );
  }
  this->preload( &tmp.ptr[ 0 ], tmp.count );
  tmp.clear();
  return true;
}


void *
PsSubTab::get_vec_data( uint32_t id ) noexcept
{
  char path[ 64 ];
  ::snprintf( path, sizeof( path ), "%" PRIx64 ".%u", this->time_ns, id );
  MapFile map( path, sizeof( VecData ) );
  int fl = MAP_FILE_SHM | MAP_FILE_RDWR | MAP_FILE_NOUNMAP;
  if ( ! map.open( fl ) )
    return NULL;
  return map.map;
}

void
PsSubTab::unmap_vec_data( void ) noexcept
{
  for ( uint32_t i = 0; i < this->vec_size; i++ )
    MapFile::unmap( this->vec[ i ], sizeof( VecData ) );
}

bool
PsSubTab::recover_lost_subs( void ) noexcept
{
  ArrayCount<VecData *, 64> tmp;
  uint32_t  id = this->sub_upd[ 0 ];
  size_t    i;
  VecData * start = (VecData *) this->get_vec_data( id ),
          * data;
  if ( start == NULL )
    return false;
  tmp[ start->index ] = start;
  for ( data = start; data->index != 0; ) {
    if ( data->prev_id == data->id )
      goto fail;
    if ( (data = (VecData *) this->get_vec_data( data->prev_id )) == NULL )
      goto fail;
    tmp[ data->index ] = data;
  }
  for ( data = start; data->next_id != data->id; ) {
    if ( (data = (VecData *) this->get_vec_data( data->next_id )) == NULL )
      goto fail;
    tmp[ data->index ] = data;
  }
  for ( i = 0; i < tmp.count; i++ ) {
    if ( tmp.ptr[ i ] == NULL )
      goto fail;
  }
  this->preload( &tmp.ptr[ 0 ], tmp.count );
  tmp.clear();
  return true;
fail:;
  for ( i = 0; i < tmp.count; i++ ) {
    if ( tmp.ptr[ i ] != NULL )
      MapFile::unmap( tmp.ptr[ i ], sizeof( VecData ) );
  }
  tmp.clear();
  return false;
}

void *
PsSubTab::new_vec_data( uint32_t id,  size_t sz ) noexcept
{
  char path[ 64 ];
  ::snprintf( path, sizeof( path ), "%" PRIx64 ".%u", this->time_ns, id );
  MapFile map( path, sz );
  int fl = MAP_FILE_SHM | MAP_FILE_CREATE | MAP_FILE_RDWR | MAP_FILE_NOUNMAP;
  if ( ! map.open( fl ) )
    return NULL;
  this->sub_upd[ 0 ] = id;
  this->sub_upd[ 1 ]++;
  return map.map;
}

void
PsSubTab::free_vec_data( uint32_t id,  void *p,  size_t sz ) noexcept
{
  char path[ 64 ];
  if ( id == this->sub_upd[ 0 ] ) {
    uint32_t prev_id = ((VecData *) p)->next_id,
             next_id = ((VecData *) p)->prev_id;
    if ( prev_id == id )
      this->sub_upd[ 0 ] = next_id;
    else
      this->sub_upd[ 0 ] = prev_id;
  }
  ::snprintf( path, sizeof( path ), "%" PRIx64 ".%u", this->time_ns, id );
  MapFile::unmap( p, sz );
  MapFile::unlink( path, true );
}

bool
KvPubSub::attach_ctx( void ) noexcept
{
  uint32_t i     = this->ctrl.next_ctx.add( 1 ) % KVPS_CTRL_CTX_SIZE,
           start = i;
  bool     second_time = false;

  for (;;) {
    PsCtrlCtx & el = this->ctrl.ctx[ i ];
    PsGuard guard( el, this->init_ns );
    /* keep used entries around for history, unless there are no more spots */
    if ( el.pid == 0 || ( second_time && el.serial == 0 ) ) {
      this->ctx_id = i;
      el.zero();
      el.time_ns = this->init_ns;
      el.serial  = this->serial;
      el.pid     = ::getpid();
      ::memcpy( el.name, this->ctx_name, sizeof( el.name ) );
      guard.mark( this->init_ns );
      return true;
    }
    i = ( i + 1 ) % KVPS_CTRL_CTX_SIZE;
    /* checked all slots */
    if ( i == start ) {
      if ( second_time )
        return false;
      second_time = true;
    }
  }
}

uint64_t
PsGuard::lock( PsCtrlCtx &el,  uint64_t time_ns ) noexcept
{
  const uint64_t bizyid = ZOMBIE64 | time_ns;
  uint64_t val;

  while ( ( (val = el.key.xchg( bizyid )) & ZOMBIE64 ) != 0 )
    kv_sync_pause();
  return val;
}

void
PsGuard::unlock( PsCtrlCtx &el,  uint64_t val ) noexcept
{
  kv_release_fence();
  el.key.xchg( val );
}

void
KvPubSub::detach_ctx( void ) noexcept
{
  PsCtrlCtx & el = this->ctrl.ctx[ this->ctx_id ];
  PsGuard guard( el, this->init_ns );
  el.serial = 0;
  guard.release();
  this->sub_tab.release();
}

void
KvPubSub::process_close( void ) noexcept
{
  this->client_stats( this->sub_route.peer_stats );
  this->EvSocket::process_close();
}

void
KvPubSub::release( void ) noexcept
{
  this->detach_ctx();
#ifdef KVPS_USE_UNIX_SOCKET
  char path[ 128 ];
  ::snprintf( path, sizeof( path ), "/tmp/%s/%lx", this->ipc_name,
              this->init_ns );
  ::unlink( path );
#endif
}

EvSocket *
KvPubSub::accept( void ) noexcept
{
  KvPubSubPeer * c =
    this->poll.get_free_list<KvPubSubPeer, KvPubSub &>( this->peer_sock_type,
                                                        *this );
  if ( c == NULL )
    return NULL;
  if ( this->accept2( *c, "pubsub_peer" ) ) {
    if ( kv_ps_debug )
      printf( "accept from %s\n", this->peer_address.buf );
    this->peer_list.push_tl( c );
    this->peer_set.add( c->fd );
    this->send_hello( *c );
    return c;
  }
  return NULL;
}

KvMsg &
KvPubSub::get_msg_buf( KvEst &e,  int mtype ) noexcept
{
  KvMsg * m = new ( this->msg_buf.make( e.len() ) ) KvMsg( mtype );
  return *m;
}

/* RouteNotify */
void
KvPubSub::on_sub( NotifySub &sub ) noexcept
{
  if ( sub.src_type == 'K' )
    return;
  if ( sub.ref != NULL ) {
    RouteRef & rte = *sub.ref;
    if ( rte.rcnt > 1 ) { /* check if subscribed already */
      for ( uint32_t i = 0; i < rte.rcnt; i++ ) {
        uint32_t r = rte.routes[ i ];
        if ( r != (uint32_t) sub.src.fd && ! this->peer_set.is_member( r ) &&
             r <= this->poll.maxfd ) {
          EvSocket *s;
          if ( (s = this->poll.sock[ r ]) != NULL ) {
            uint8_t v = s->is_subscribed( sub );
            if ( ( v & EV_NOT_SUBSCRIBED ) == 0 )
              goto already_subscribed;
          }
        }
      }
    }
    this->add_sub( sub.subj_hash, sub.subject, sub.subject_len, PS_SUB );
  }
already_subscribed:;
  if ( ! this->peer_list.is_empty() )
    this->do_on_sub( sub, KV_MSG_ON_SUB );
}

void
KvPubSub::on_unsub( NotifySub &sub ) noexcept
{
  if ( sub.src_type == 'K' )
    return;
  if ( sub.ref != NULL ) {
    RouteRef & rte = *sub.ref;
    if ( rte.rcnt > 0 ) { /* check all unsubs */
      for ( uint32_t i = 0; i < rte.rcnt; i++ ) {
        uint32_t r = rte.routes[ i ];
        if ( r != (uint32_t) sub.src.fd && ! this->peer_set.is_member( r ) &&
             r <= this->poll.maxfd ) {
          EvSocket *s;
          if ( (s = this->poll.sock[ r ]) != NULL ) {
            uint8_t v = s->is_subscribed( sub );
            if ( ( v & EV_NOT_SUBSCRIBED ) == 0 )
              goto is_subscribed;
          }
        }
      }
    }
    this->rem_sub( sub.subj_hash, sub.subject, sub.subject_len, PS_SUB );
  }
is_subscribed:;
  if ( ! this->peer_list.is_empty() )
    this->do_on_sub( sub, KV_MSG_ON_UNSUB );
}

void
KvPubSub::do_on_sub( NotifySub &sub,  int mtype ) noexcept
{
  KvEst e;
  e.sub_seqno()
   .subj_hash()
   .subject  ( sub.subject_len )
   .reply    ( sub.reply_len )
   .sub_count()
   .hash_coll()
   .ref_num  ();

  KvMsg &m = this->get_msg_buf( e, mtype );
  m.sub_seqno( this->sub_seqno++ )
   .subj_hash( sub.subj_hash )
   .subject  ( sub.subject, sub.subject_len )
   .reply    ( sub.reply, sub.reply_len )
   .sub_count( sub.sub_count )
   .hash_coll( sub.hash_collision );
  if ( sub.bref != NULL )
    m.ref_num( sub.bref->ref_num );
  this->bcast_msg( m );
}

void
KvPubSub::on_psub( NotifyPattern &pat ) noexcept
{
  if ( pat.src_type == 'K' ) /* came from somewhere else */
    return;
  if ( pat.ref != NULL ) {
    uint32_t h;
    RouteRef & rte = *pat.ref;
    if ( rte.rcnt > 1 ) { /* check if psubscribed already */
      for ( uint32_t i = 0; i < rte.rcnt; i++ ) {
        uint32_t r = rte.routes[ i ];
        if ( r != (uint32_t) pat.src.fd && ! this->peer_set.is_member( r ) &&
             r <= this->poll.maxfd ) {
          EvSocket *s;
          if ( (s = this->poll.sock[ r ]) != NULL ) {
            uint8_t v = s->is_psubscribed( pat );
            if ( ( v & EV_NOT_SUBSCRIBED ) == 0 )
              goto already_subscribed;
          }
        }
      }
    }
    h = kv_crc_c( pat.pattern, pat.pattern_len, 0 );
    this->add_sub( h, pat.pattern, pat.pattern_len, PS_PATTERN, pat.cvt.fmt );
  }
already_subscribed:;
  if ( ! this->peer_list.is_empty() )
    this->do_on_psub( pat, KV_MSG_ON_PSUB );
}

void
KvPubSub::on_punsub( NotifyPattern &pat ) noexcept
{
  if ( pat.src_type == 'K' ) /* came from somewhere else */
    return;
  if ( pat.ref != NULL ) {
    uint32_t h;
    RouteRef & rte = *pat.ref;
    if ( rte.rcnt > 0 ) { /* check all unsubs */
      for ( uint32_t i = 0; i < rte.rcnt; i++ ) {
        uint32_t r = rte.routes[ i ];
        if ( r != (uint32_t) pat.src.fd && ! this->peer_set.is_member( r ) &&
             r <= this->poll.maxfd ) {
          EvSocket *s;
          if ( (s = this->poll.sock[ r ]) != NULL ) {
            uint8_t v = s->is_psubscribed( pat );
            if ( ( v & EV_NOT_SUBSCRIBED ) == 0 )
              goto is_subscribed;
          }
        }
      }
    }
    h = kv_crc_c( pat.pattern, pat.pattern_len, 0 );
    this->rem_sub( h, pat.pattern, pat.pattern_len, PS_PATTERN, pat.cvt.fmt );
  }
is_subscribed:;
  if ( ! this->peer_list.is_empty() )
    this->do_on_psub( pat, KV_MSG_ON_PUNSUB );
}

void
KvPubSub::do_on_psub( NotifyPattern &pat,  int mtype ) noexcept
{
  KvEst e;
  e.sub_seqno()
   .subj_hash()
   .pattern  ( pat.pattern_len )
   .reply    ( pat.reply_len )
   .pat_fmt  ()
   .sub_count()
   .hash_coll()
   .ref_num  ();

  KvMsg &m = this->get_msg_buf( e, mtype );
  m.sub_seqno( this->sub_seqno++ )
   .subj_hash( pat.prefix_hash )
   .pattern  ( pat.pattern, pat.pattern_len )
   .reply    ( pat.reply, pat.reply_len )
   .pat_fmt  ( pat.cvt.fmt )
   .sub_count( pat.sub_count )
   .hash_coll( pat.hash_collision );
  if ( pat.bref != NULL )
    m.ref_num( pat.bref->ref_num );
  this->bcast_msg( m );
}

bool
KvPubSub::add_sub( uint32_t h,  const char *sub,  uint16_t sublen,
                   PsSubType t,  PatternFmt f ) noexcept
{
  RouteLoc loc;
  PsSub * s = this->sub_tab.upsert( h, sub, sublen, loc );
  for (;;) {
    if ( loc.is_new ) {
      s->type    = t;
      s->fmt     = f;
      return true;
    }
    if ( s->type == t && s->fmt == f )
      return false;
    s = this->sub_tab.find_next( h, sub, sublen, loc );
    if ( s == NULL ) {
      s = this->sub_tab.insert( h, sub, sublen );
      loc.is_new = true;
    }
  }
}

bool
KvPubSub::rem_sub( uint32_t h,  const char *sub,  uint16_t sublen,
                   PsSubType t,  PatternFmt f ) noexcept
{
  RouteLoc loc;
  PsSub  * s = this->sub_tab.find( h, sub, sublen, loc );
  for (;;) {
    if ( s == NULL )
      return false;
    if ( s->type == t && s->fmt == f ) {
      this->sub_tab.remove( loc );
      return true;
    }
    s = this->sub_tab.find_next( h, sub, sublen, loc );
  }
}

void
KvPubSub::on_reassert( uint32_t ,  RouteVec<RouteSub> &,
                       RouteVec<RouteSub> & ) noexcept
{
}

void
KvPubSub::on_bloom_ref( BloomRef &ref ) noexcept
{
  BloomCodec code;
  ref.encode( code );
  size_t len = ::strlen( ref.name ) + 1;

  KvEst e;
  e.name( len )
   .ref_num()
   .data( code.code_sz * 4 );

  for ( KvPubSubPeer * c = this->peer_list.hd; c != NULL; c = c->next ) {
    KvMsg &m = *(new ( c->alloc_temp( e.len() ) ) KvMsg( KV_MSG_BLOOM ) );
    m.name( ref.name, len )
     .ref_num( ref.ref_num )
     .data( code.ptr, code.code_sz * 4 );

    c->append_iov( (void *) m.msg(), m.len() );
    this->msgs_sent++;
    c->idle_push_write();
  }
}

void
KvPubSub::on_bloom_deref( BloomRef &ref ) noexcept
{
  KvEst e;
  size_t len = ::strlen( ref.name ) + 1;
  e.name( len )
   .ref_num();

  for ( KvPubSubPeer * c = this->peer_list.hd; c != NULL; c = c->next ) {
    KvMsg &m = *(new ( c->alloc_temp( e.len() ) ) KvMsg( KV_MSG_BLOOM_DEL ) );
    m.name( ref.name, len )
     .ref_num( ref.ref_num );

    c->append_iov( (void *) m.msg(), m.len() );
    this->msgs_sent++;
    c->idle_push_write();
  }
}

