#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stddef.h>
#include <unistd.h>
#include <fcntl.h>
#include <assert.h>
#include <time.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/signal.h>
#include <sys/signalfd.h>

#include <raikv/ev_publish.h>
#include <raikv/kv_pubsub.h>
#include <raikv/cube_route.h>

static bool kv_use_pipes = true;
/* signal other processes that a message available */
static const int kv_msg_signal = SIGUSR2;
static const uint64_t kv_timer_ival_ms  = 250; /* 4 times a second */
                          /* 20000 per sec / ivals per sec = rate / ival
                           *  if msg rate is above 1 per 50 usecs */
static const uint64_t kv_busy_loop_rate = 20000 / ( 1000 / kv_timer_ival_ms );

using namespace rai;
using namespace kv;

static const uint16_t KV_CTX_BYTES = KV_MAX_CTX_ID / 8;
#if __cplusplus > 201103L
  static_assert( KV_MAX_CTX_ID == sizeof( CubeRoute128 ) * 8, "CubeRoute128" );
  static_assert( 5 == sizeof( KvPrefHash ), "KvPrefHash" );
#endif

static const char   sys_mc[]  = "_SYS.MC",
                    sys_ibx[] = "_SYS.";
static const size_t mc_name_size  = sizeof( sys_mc ),
                    ibx_name_size = sizeof( sys_ibx ) + 2;
/* 8 byte inbox size is the limit for msg list with immediate key */

/*static const char b64[] =
 "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";*/

static char
b64( uint8_t n )
{
  if ( n <= 'Z' - 'A' )
    return 'A' + n;
  n -= ( 'Z' - 'A' ) + 1;
  if ( n <= 'z' - 'a' )
    return 'a' + n;
  n -= ( 'z' - 'a' ) + 1;
  if ( n <= '9' - '0' )
    return '0' + n;
  n -= ( '9' - '0' ) + 1;
  return n == 0 ? '+' : '/';
}

static void
make_ibx( char *ibname,  uint16_t ctx_id,  uint16_t ibx_num )
{
  uint32_t n = (uint32_t) ctx_id * KV_CTX_INBOX_COUNT + (uint32_t) ibx_num;
  ::strcpy( ibname, sys_ibx );
  ibname[ ibx_name_size - 3 ] = b64( ( n >> 6 ) & 63 );
  ibname[ ibx_name_size - 2 ] = b64( n & 63 );
  ibname[ ibx_name_size - 1 ] = '\0';
}

KvPubSub *
KvPubSub::create( EvPoll &poll,  uint8_t db_num ) noexcept
{
  KvPubSub * ps;
  void     * p,
           * mqptr,
           * mcptr,
           * ibptr;
  size_t     i, kvsz, mcsz, mqsz, ibsz, n;
  char       ibname[ 12 ];
  sigset_t   mask;
  int        fd;
  uint32_t   dbx_id;

  dbx_id = poll.map->attach_db( poll.ctx_id, db_num );
  if ( dbx_id == KV_NO_DBSTAT_ID )
    return NULL;

  sigemptyset( &mask );
  sigaddset( &mask, kv_msg_signal );

  if ( sigprocmask( SIG_BLOCK, &mask, NULL ) == -1 ) {
    perror("sigprocmask");
    return NULL;
  }
  fd = signalfd( -1, &mask, SFD_NONBLOCK );
  if ( fd == -1 ) {
    perror( "signalfd" );
    return NULL;
  }
  kvsz = align<size_t>( sizeof( KvPubSub ), 64 );
  mcsz = align<size_t>( sizeof( KvMcastKey ) + 8, 64 );
  mqsz = align<size_t>( sizeof( KvMsgQueue ) + 8, 64 );
  ibsz = align<size_t>( sizeof( KvInboxKey ) + 8, 64 );

  n = kvsz + mcsz +
      mqsz * ( MAX_CTX_ID - 1 ) + /* size of send queues (minus myself) */
      ibsz * KV_CTX_INBOX_COUNT;     /* size of recv queues */
  if ( (p = aligned_malloc( n )) == NULL )
    return NULL;
  mcptr = (void *) &((uint8_t *) p)[ kvsz ];
  mqptr = (void *) &((uint8_t *) mcptr)[ mcsz ];
  ibptr = (void *) &((uint8_t *) mqptr)[ mqsz * ( MAX_CTX_ID - 1 ) ];
  ps = new ( p ) KvPubSub( poll, fd, mcptr, sys_mc, mc_name_size, dbx_id );
  /* for each ctx_id create queue */
  for ( i = 0; i < MAX_CTX_ID; i++ ) {
    if ( poll.ctx_id != i ) {
      uint16_t id = poll.ctx_id & ( KV_CTX_INBOX_COUNT - 1 );
      make_ibx( ibname, i, id );
      ps->snd_inbox[ i ] =
        new ( mqptr ) KvMsgQueue( ps->kctx, ibname, ibx_name_size, i );
      mqptr = &((uint8_t *) mqptr)[ mqsz ];
    }
    else {
      ps->snd_inbox[ i ] = NULL;
    }
  }
  for ( i = 0; i < KV_CTX_INBOX_COUNT; i++ ) {
    make_ibx( ibname, poll.ctx_id, i );
    ps->rcv_inbox[ i ] =
      new ( ibptr ) KvInboxKey( ps->kctx, ibname, ibx_name_size );
    ibptr = &((uint8_t *) ibptr)[ ibsz ];
  }
  if ( ! ps->register_mcast() || poll.add_sock( ps ) < 0 ) {
    ::close( fd );
    return NULL;
  }
  ps->idle_push( EV_PROCESS );
  ps->push( EV_WRITE );
  return ps;
}

void
KvPubSub::print_backlog( void ) noexcept
{
  size_t i;
  for ( i = 0; i < MAX_CTX_ID; i++ ) {
    if ( this->snd_inbox[ i ] != NULL ) {
      KvMsgQueue & ibx = *this->snd_inbox[ i ];
      if ( ( ibx.pub_size | ibx.backlog_size ) != 0 ) {
        printf( "[ %lu ] pub_size=%lu pub_cnt=%lu pub_msg=%lu "
                "back_size=%lu back_cnt=%lu sig=%lu high=%lu\n", i,
                ibx.pub_size, ibx.pub_cnt, ibx.pub_msg,
                ibx.backlog_size, ibx.backlog_cnt,
                ibx.signal_cnt, ibx.high_water_size );
      }
    }
  }
}

bool
KvPubSub::timer_expire( uint64_t tid,  uint64_t ) noexcept
{
  if ( this->timer_id != tid )
    return false;
  /*printf( "timer %lu\n", this->timer_cnt );*/
  if ( ( ++this->timer_cnt & 31 ) == 0 )
    this->idle_push( EV_PROCESS );
  if ( ( this->poll.map->ctx[ this->ctx_id ].ctx_flags & KV_NO_SIGUSR ) != 0 ) {
    if ( this->inbox_msg_cnt < kv_busy_loop_rate )
      this->poll.map->ctx[ this->ctx_id ].ctx_flags &= ~KV_NO_SIGUSR;
  }
  else {
    if ( this->test( EV_BUSY_POLL ) ) {
      if ( this->inbox_msg_cnt < kv_busy_loop_rate ) {
        this->pop( EV_BUSY_POLL );
        this->idle_push( EV_READ_LO );
      }
      else
        this->poll.map->ctx[ this->ctx_id ].ctx_flags |= KV_NO_SIGUSR;
    }
  }
  this->sigusr_recv_cnt = 0;
  this->inbox_msg_cnt = 0;
  if ( this->test( EV_BUSY_POLL | EV_READ_LO ) == 0 )
    this->idle_push( EV_READ_LO );
  return true;
}

bool
KvPubSub::is_dead( uint32_t id ) const noexcept
{
  /* check that this route is valid by pinging the pid */
  uint32_t pid = this->kctx.ht.ctx[ id ].ctx_pid;
  if ( pid == 0 || this->kctx.ht.ctx[ id ].ctx_id == KV_NO_CTX_ID )
    return true;
  if ( ::kill( pid, 0 ) == 0 )
    return false;
  return true;
}

bool
KvPubSub::get_set_mcast( CubeRoute128 &result_cr ) noexcept
{
  void    * val;
  KeyStatus status;
  bool      res = false;

  result_cr.zero();
  this->dead_cr.zero();
  this->kctx.set_key( this->mcast.kbuf );
  this->kctx.set_hash( this->mcast.hash1, this->mcast.hash2 );
  /* add my ctx_id to the mcast key, which is the set of all ctx_ids */
  if ( (status = this->kctx.acquire( &this->wrk )) <= KEY_IS_NEW ) {
    bool is_new = ( status == KEY_IS_NEW );
    if ( (status = this->kctx.resize( &val, KV_CTX_BYTES, true )) == KEY_OK ) {
      CubeRoute128 &cr = *(CubeRoute128 *) val;
      if ( is_new ) {
        cr.zero();
      }
      else {
        size_t id;
        if ( cr.first_set( id ) ) {
          do {
            if ( this->ctx_id == id )
              continue;
            if ( this->is_dead( id ) ) {
              uint32_t pid = this->kctx.ht.ctx[ id ].ctx_pid,
                       tid = this->kctx.ht.ctx[ id ].ctx_thrid;
              this->dead_cr.set( id );
              fprintf( stderr, "ctx %lu pid %u tid %u is dead\n", id,
                       pid, tid );
            }
            else {
              result_cr.set( id );
            }
          } while ( cr.next_set( id ) );
        }
      }
      /* always set my cr */
      cr.set( this->ctx_id );
      res = true;
    }
    this->kctx.release();
  }
  if ( ! res ) {
    fprintf( stderr, "Unable to register mcast, kv status %d\n",
             (int) status );
  }
  return res;
}

bool
KvPubSub::register_mcast( void ) noexcept
{
  CubeRoute128 test_cr, result_cr, conn_cr, bad_cr;
  size_t id;
  bool res;

  conn_cr.zero(); /* try to connect to every other instance */
  bad_cr.zero();
  /* timer for timing out connection */
  for ( useconds_t t = 50; ; t *= 10 ) {
    if ( ! this->get_set_mcast( result_cr ) ) { /* the instances */
      res = false;
      break;
    }
    test_cr.copy_from( &conn_cr );
    test_cr.or_bits( bad_cr );
    test_cr.and_bits( result_cr );
    /* if result == ( connected | bad ) */
    if ( result_cr.equals( test_cr ) ) {
      res = true;
      break;
    }
    test_cr.zero();
    if ( result_cr.first_set( id ) ) {
      do {
        if ( ! bad_cr.is_set( id ) ) {
          /* try to make the pipe */
          if ( this->make_rcv_pipe( id ) && this->make_snd_pipe( id ) ) {
            conn_cr.set( id );
            this->pipe_cr.set( id );
          }
          else {
            test_cr.set( id );
          }
        }
      } while ( result_cr.next_set( id ) );
    }
    if ( test_cr.first_set( id ) ) {
      do {
        KvMsgQueue & ibx = *this->snd_inbox[ id ];
        KvMsg msg;
        msg.set_seqno_stamp( 0, this->stamp );
        msg.size       = sizeof( KvMsg );
        msg.src        = this->ctx_id;
        msg.dest_start = id;
        msg.dest_end   = id;
        msg.msg_type   = KV_MSG_HELLO;
        if ( ! this->send_kv_msg( ibx, msg, false ) )
          bad_cr.set( id );
        else {
          uint32_t pid = this->kctx.ht.ctx[ id ].ctx_pid,
                   tid = this->kctx.ht.ctx[ id ].ctx_thrid;
          uint16_t fl  = this->kctx.ht.ctx[ id ].ctx_flags;
          if ( pid > 0 && ( fl & KV_NO_SIGUSR ) == 0 ) {
            if ( ::kill( tid, kv_msg_signal ) != 0 ) {
              if ( errno != EPERM ) {
                ::perror( "kill notify msg" );
                bad_cr.set( id );
              }
              else {
                static int errcnt;
                if ( errcnt++ == 0 )
                  perror( "kill notify msg" );
              }
            }
            else {
              ibx.signal_cnt++;
            }
          }
        }
      } while ( test_cr.next_set( id ) );
    }
    static const useconds_t MAX_PIPE_CONNECT_TIME_USECS =
      (useconds_t) 1000000 * (useconds_t) 10;
    size_t i = 0;
    if ( test_cr.first_set( id ) ) {
      do {
        if ( t >= MAX_PIPE_CONNECT_TIME_USECS ) {
          uint32_t pid = this->kctx.ht.ctx[ id ].ctx_pid,
                   tid = this->kctx.ht.ctx[ id ].ctx_thrid;
          fprintf( stderr, "giving up connecting pipe to ctx %lu pid %u tid %u"
                           " (time %u secs)\n",
                   id, pid, tid, t / 1000000 );
          bad_cr.set( id );
        }
        else {
          if ( i++ == 0 ) /* just usleep once per iter */
            ::usleep( t );
          if ( this->make_snd_pipe( id ) ) {
            test_cr.clear( id );
            conn_cr.set( id );
            this->pipe_cr.set( id );
          }
        }
      } while ( test_cr.next_set( id ) );
    }
  }
  if ( bad_cr.first_set( id ) ) {
    do {
      this->close_rcv_pipe( id );
    } while ( bad_cr.next_set( id ) );
  }
  return res;
}

void
KvPubSub::clear_mcast_dead_routes( void ) noexcept
{
  void    * val;
  uint64_t  sz;
  size_t    i;
  KeyStatus status;
  bool      res = false;

  if ( this->dead_cr.is_empty() )
    return;
  this->kctx.set_key( this->mcast.kbuf );
  this->kctx.set_hash( this->mcast.hash1, this->mcast.hash2 );
  if ( (status = this->kctx.acquire( &this->wrk )) <= KEY_IS_NEW ) {
    bool is_new = ( status == KEY_IS_NEW ); /* shouldn't be new */
    if ( ! is_new ) {
      status = this->kctx.value( &val, sz );
      if ( status == KEY_OK && sz == KV_CTX_BYTES ) {
        CubeRoute128 &cr = *(CubeRoute128 *) val;
        cr.not_bits( this->dead_cr );
        this->mc_cr = cr;
        res = true;
      }
    }
    this->kctx.release();
  }
  if ( ! res ) {
    fprintf( stderr, "Unable to clear mcast dead routes, kv status %d\n",
             (int) status );
  }
  if ( this->dead_cr.first_set( i ) ) {
    do {
      if ( this->snd_inbox[ i ] != NULL ) {
        KvMsgQueue & ibx = *this->snd_inbox[ i ];
        this->kctx.set_key( ibx.kbuf );
        this->kctx.set_hash( ibx.hash1, ibx.hash2 );
        if ( (status = this->kctx.acquire( &this->wrk )) == KEY_OK ) {
          /*fprintf( stderr, "drop kv inbox %lu\n", i );*/
          this->kctx.tombstone();
        }
        this->kctx.release();
      }
    } while ( this->dead_cr.next_set( i ) );
  }
  this->dead_cr.zero();
  this->force_unsubscribe();
}

/* ways to force_unsubscribe:
 * 1. during startup, register mcast fails to a node, dead_cr
 * 2. during idle, is_dead( ctx_id ) == true, dead_cr
 * 3. when backlog has no progress and determines peer is_dead
 */
void
KvPubSub::force_unsubscribe( void ) noexcept
{
  struct TmpSub {
    TmpSub * next;
    uint32_t hash;
    uint16_t len;
    char     value[ 2 ];
  };
  TmpSub * list = NULL, * sub;
  CubeRoute128 cr;
  kv::WorkAllocT< 8192 > wrk;

  uint32_t v;
  uint16_t off;
  KvSubRoute *rt = this->sub_tab.first( v, off );
  if ( rt != NULL ) {
    do {
      cr.copy_from( rt->rt_bits );
      cr.and_bits( this->mc_cr );
      if ( ! cr.is_empty() ) {
        cr.copy_to( rt->rt_bits );
      }
      else {
        sub = (TmpSub *) wrk.alloc(
          align<size_t>( sizeof( TmpSub ) + rt->len, sizeof( void * ) ) );
        sub->next = list;
        list      = sub;
        sub->hash = rt->hash;
        sub->len  = rt->len;
        ::memcpy( sub->value, rt->value, rt->len );
      }
    } while ( (rt = this->sub_tab.next( v, off )) != NULL );
  }

  for ( sub = list; sub != NULL; sub = sub->next ) {
    const char * key  = sub->value;
    size_t       plen = sizeof( SYS_WILD_PREFIX ) - 1;
    if ( ::memcmp( key, SYS_WILD_PREFIX, plen ) == 0 &&
         key[ plen ] >= '0' && key[ plen ] <= '9' ) {
      size_t prefixlen = key[ plen ] - '0';
      if ( key[ plen + 1 ] >= '0' && key[ plen + 1 ] <= '9' ) {
        plen += 1;
        prefixlen = prefixlen * 10 + ( key[ plen ] - '0' );
      }
      plen += 2; /* skip N. in _SYS.WN.prefix */
      this->sub_tab.remove( sub->hash, key, sub->len );
      if ( this->sub_tab.find_by_hash( sub->hash ) == NULL ) {
        /*printf( "force delpat %.*s\n", (int) prefixlen, &key[ plen ] );*/
        this->poll.del_pattern_route( &key[ plen ], prefixlen, sub->hash,
                                      this->fd );
      }
    }
    else {
      this->sub_tab.remove( sub->hash, key, sub->len );
      if ( this->sub_tab.find_by_hash( sub->hash ) == NULL ) {
        /*printf( "force delrte %.*s\n", (int) sub->len, key );*/
        this->poll.del_route( key, sub->len, sub->hash, this->fd );
      }
    }
  }
}

void
KvPubSub::check_peer_health( void ) noexcept
{
  size_t i;
  if ( this->subscr_cr.first_set( i ) ) {
    do {
      if ( this->is_dead( i ) ) {
        this->dead_cr.set( i );
      }
    } while ( this->subscr_cr.next_set( i ) );
  }
  this->subscr_cr.not_bits( this->dead_cr );
  this->clear_mcast_dead_routes();
}

bool
KvPubSub::unregister_mcast( void ) noexcept
{
  void    * val;
  uint64_t  sz;
  KeyStatus status;
  bool      res = false;

  this->kctx.set_key( this->mcast.kbuf );
  this->kctx.set_hash( this->mcast.hash1, this->mcast.hash2 );
  if ( (status = this->kctx.acquire( &this->wrk )) <= KEY_IS_NEW ) {
    bool is_new = ( status == KEY_IS_NEW ); /* shouldn't be new */
    if ( ! is_new ) {
      status = this->kctx.value( &val, sz );
      if ( status == KEY_OK && sz == KV_CTX_BYTES ) {
        CubeRoute128 &cr = *(CubeRoute128 *) val;
        if ( cr.is_set( this->ctx_id ) ) {
          cr.clear( this->ctx_id );
          this->kctx.next_serial( ValueCtr::SERIAL_MASK );
          /*printf( "mcast clear %u\n", this->ctx_id );*/
          size_t id;
          if ( cr.first_set( id ) ) {
            do {
              KvMsg *m = this->create_kvmsg( KV_MSG_BYE, sizeof( KvMsg ) );
              m->dest_start = id;
              m->dest_end   = id;
            } while ( cr.next_set( id ) );
          }
        }
        res = true;
      }
    }
    this->kctx.release();
  }
  if ( ! res ) {
    fprintf( stderr, "Unable to unregister mcast, kv status %d\n",
             (int) status );
  }
  return res;
}

bool
KvPubSub::update_mcast_sub( const char *sub,  size_t len,  int flags ) noexcept
{
  KeyBuf        kbuf;
  KeyFragment * kb = &kbuf;
  void        * val;
  uint64_t      sz,
                hash1,
                hash2;
  KeyStatus     status;
  bool          res = false;

  if ( kbuf.copy( sub, len + 1 ) != len + 1 ) {
    size_t sz = sizeof( KeyFragment ) + len;
    kb = (KeyFragment *) this->snd_wrk.alloc( sz );
    ::memcpy( kb->u.buf, sub, len );
  }
  kb->u.buf[ len ] = '\0';
  this->hs.hash( *kb, hash1, hash2 );
  this->kctx.set_key( *kb );
  this->kctx.set_hash( hash1, hash2 );
  /* check if already set by using find(), lower cost when route is expected
   * to be set */
  if ( ( flags & USE_FIND ) != 0 ) {
    if ( (status = this->kctx.find( &this->wrk )) == KEY_OK ) {
      if ( (status = this->kctx.value( &val, sz )) == KEY_OK &&
           sz == sizeof( CubeRoute128 ) ) {
        CubeRoute128 cr;
        cr.copy_from( val );
        if ( ( flags & ACTIVATE ) != 0 ) {
          if ( cr.is_set( this->ctx_id ) )
            return true;
        }
        else {
          if ( ! cr.is_set( this->ctx_id ) )
            return true;
        }
      }
    }
  }
  /* doesn't exist or skip find, use acquire */
  if ( (status = this->kctx.acquire( &this->wrk )) <= KEY_IS_NEW ) {
    CubeRoute128 *cr;
    /* new sub */
    if ( status == KEY_IS_NEW ) {
      if ( ( flags & ACTIVATE ) != 0 ) {
        status = this->kctx.resize( &val, KV_CTX_BYTES );
        if ( status == KEY_OK ) {
          cr = (CubeRoute128 *) val;
          cr->zero();
          cr->set( this->ctx_id );
          res = true;
        }
      }
      else { /* doesn't exist, don't create it just to clear it */
        res = true;
      }
    }
    else { /* exists, get the value and set / clear the ctx bit */
      status = this->kctx.value( &val, sz );
      if ( status == KEY_OK && sz == KV_CTX_BYTES ) {
        res = true;
        cr = (CubeRoute128 *) val;
        if ( ( flags & ACTIVATE ) != 0 ) {
          cr->set( this->ctx_id );
        }
        else {
          cr->clear( this->ctx_id );
          if ( cr->is_empty() )
            this->kctx.tombstone(); /* is the last subscriber */
        }
      }
    }
    this->kctx.release();
  }
  if ( ! res && ( flags & ACTIVATE ) != 0 ) {
    fprintf( stderr, "Unable to register subject %.*s mcast, kv status %d\n",
             (int) len, sub, (int) status );
  }
  return res;
}

void
KvPubSub::do_sub( uint32_t h,  const char *sub,  size_t sublen,
                  uint32_t /*sub_id*/,  uint32_t rcnt,  char src_type,
                  const char *rep,  size_t rlen ) noexcept
{
  int use_find = USE_FIND;
  if ( rcnt == 1 ) /* first route added */
    use_find = 0;
  else if ( rcnt == 2 ) { /* if first route and subscribed elsewhere */
    if ( this->poll.sub_route.is_member( h, this->fd ) )
      use_find = 0;
  }
  /* subscribe must check the route is set because the hash used for the route
   * is may have collisions:  when another subject is subscribed and has a
   * collision, the route count will be for both subjects */
  this->update_mcast_sub( sub, sublen, use_find | ACTIVATE );

  KvSubMsg *submsg =
    this->create_kvsubmsg( h, sub, sublen, src_type, KV_MSG_SUB, rep, rlen );
  if ( submsg != NULL ) {
  /*printf( "subscribe %x %.*s %u:%c\n", h, (int) len, sub, sub_id, src_type );*/
    this->idle_push( EV_WRITE );
    if ( ! this->sub_notifyq.is_empty() )
      this->forward_sub( *submsg );
  }
}

void
KvPubSub::do_unsub( uint32_t h,  const char *sub,  size_t sublen,
                    uint32_t,  uint32_t rcnt,  char src_type ) noexcept
{
  bool do_unsubscribe = false;
  if ( rcnt == 0 ) /* no more routes left */
    do_unsubscribe = true;
  else if ( rcnt == 1 ) { /* if the only route left is not in my server */
    if ( this->poll.sub_route.is_member( h, this->fd ) )
      do_unsubscribe = true;
  }
  if ( do_unsubscribe )
    this->update_mcast_sub( sub, sublen, DEACTIVATE );

  KvSubMsg *submsg =
    this->create_kvsubmsg( h, sub, sublen, src_type, KV_MSG_UNSUB, NULL, 0 );
  if ( submsg != NULL ) {
  /*printf( "unsubscribe %x %.*s\n", h, (int) len, sub );*/
    this->idle_push( EV_WRITE );
    if ( ! this->sub_notifyq.is_empty() )
      this->forward_sub( *submsg );
  }
}

void
KvPubSub::do_psub( uint32_t h,  const char *pattern,  size_t patlen,
                   const char *prefix,  uint8_t prefix_len,
                   uint32_t,  uint32_t rcnt,  char src_type ) noexcept
{
  int use_find = USE_FIND;
  if ( rcnt == 1 ) /* first route added */
    use_find = 0;
  else if ( rcnt == 2 ) { /* if first route and subscribed elsewhere */
    if ( this->poll.sub_route.is_member( h, this->fd ) )
      use_find = 0;
  }
  /* subscribe must check the route is set because the hash used for the route
   * is may have collisions:  when another subject is subscribed and has a
   * collision, the route count will be for both subjects */
  SysWildSub w( prefix, prefix_len );
  this->update_mcast_sub( w.sub, w.len, use_find | ACTIVATE );

  KvSubMsg *submsg =
    this->create_kvpsubmsg( h, pattern, patlen, prefix, prefix_len, src_type,
                            KV_MSG_PSUB );
  if ( submsg != NULL ) {
    this->idle_push( EV_WRITE );
  /*printf( "psubscribe %x %.*s %s %u:%c rcnt=%u\n",
            h, (int) len, pattern, w.sub, sub_id, src_type, rcnt );*/
    if ( ! this->sub_notifyq.is_empty() )
      this->forward_sub( *submsg );
  }
}

void
KvPubSub::do_punsub( uint32_t h,  const char *pattern,  size_t patlen,
                     const char *prefix,  uint8_t prefix_len,
                     uint32_t /*sub_id*/,  uint32_t rcnt,  char src_type ) noexcept
{
  bool do_unsubscribe = false;
  if ( rcnt == 0 ) /* no more routes left */
    do_unsubscribe = true;
  else if ( rcnt == 1 ) { /* if the only route left is not in my server */
    if ( this->poll.sub_route.is_member( h, this->fd ) )
      do_unsubscribe = true;
  }
  SysWildSub w( prefix, prefix_len );
  if ( do_unsubscribe )
    this->update_mcast_sub( w.sub, w.len, DEACTIVATE );
  KvSubMsg *submsg =
    this->create_kvpsubmsg( h, pattern, patlen, prefix, prefix_len, src_type,
                            KV_MSG_PUNSUB );
  if ( submsg != NULL ) {
    this->idle_push( EV_WRITE );
  /*printf( "punsubscribe %x %.*s %s %u:%c rcnt=%u\n",
            h, (int) len, pattern, w.sub, sub_id, src_type, rcnt );*/
    if ( ! this->sub_notifyq.is_empty() )
      this->forward_sub( *submsg );
  }
}

void
KvPubSub::process( void ) noexcept
{
  if ( ( this->flags & KV_INITIAL_SCAN ) == 0 ) {
    this->flags |= KV_INITIAL_SCAN;
    this->clear_mcast_dead_routes();
    this->update_mcast_route();
    this->scan_ht();
    this->poll.add_timer_millis( fd, kv_timer_ival_ms, this->timer_id, 0 );
  }
  else {
    this->check_peer_health();
  }
  this->pop( EV_PROCESS );
}

void
KvPubSub::scan_ht( void ) noexcept
{
  CubeRoute128  cr, invalid_cr;
  HashTab     * map = this->poll.map;
  KeyFragment * kp;
  KeyCtx        scan_kctx( *map, this->dbx_id, NULL );
  uint64_t      ht_size = map->hdr.ht_size, sz;
  void        * val;
  KeyStatus     status;

  cr.zero();
  invalid_cr.zero(); /* invalid = bits set which should be clear */
  invalid_cr.or_bits( this->mc_cr );
  invalid_cr.set( this->ctx_id );
  invalid_cr.flip();

  for ( uint64_t pos = 0; pos < ht_size; pos++ ) {
    status = scan_kctx.fetch( &this->wrk, pos );
    if ( status == KEY_OK && scan_kctx.entry->test( FL_DROPPED ) == 0 ) {
      /* if key is in the right db num */
      if ( scan_kctx.get_db() == this->kctx.db_num ) {
        /* get the key, check for _SYS. _SYS.WN.prefix */
        status = scan_kctx.get_key( kp );
        if ( status == KEY_OK ) {
          bool    is_sys      = false,
                  is_sys_wild = false;
          uint8_t prefixlen   = 0;
          size_t  plen        = sizeof( SYS_WILD_PREFIX ) - 1;
          const char * key    = (const char *) kp->u.buf;
          /* is it a wildcard? */
          if ( ::memcmp( key, "_SYS.", 5 ) == 0 ) {
            if ( ::memcmp( key, SYS_WILD_PREFIX, plen ) == 0 &&
                 key[ plen ] >= '0' && key[ plen ] <= '9' ) {
              is_sys_wild = true;
              prefixlen = key[ plen ] - '0';
              if ( key[ plen + 1 ] >= '0' && key[ plen + 1 ] <= '9' ) {
                plen += 1;
                prefixlen = prefixlen * 10 + ( key[ plen ] - '0' );
              }
              plen += 2; /* skip N. in _SYS.WN.prefix */
            }
            is_sys = true;
          }
          /* if not an inbox or mcast, absorb the routes */
          if ( ! is_sys || is_sys_wild ) {
            if ( (status = scan_kctx.value( &val, sz )) == KEY_OK &&
                 sz == sizeof( CubeRoute128 ) ) {
              cr.copy_from( val );
              /* if some routes are bad, fix them */
              if ( cr.test_bits( invalid_cr ) ) {
                printf( "fixkey: %.*s\n", kp->keylen, key );
                for (;;) {
                  status = scan_kctx.try_acquire_position( pos );
                  if ( status != KEY_BUSY )
                    break;
                }
                /* copy and del if empty */
                if ( status == KEY_OK ) { /* could be dropped already */
                  status = scan_kctx.resize( &val, KV_CTX_BYTES );
                  /* clear invalid bits, if empty then drop */
                  if ( status == KEY_OK ) {
                    CubeRoute128 &cr2 = *(CubeRoute128 *) val;
                    cr2.not_bits( invalid_cr );
                    cr2.clear( this->ctx_id );
                    cr.copy_from( val );
                    if ( cr.is_empty() ) {
                      printf( "emptykey: %.*s\n", kp->keylen, key );
                      scan_kctx.tombstone();
                    }
                  }
                }
                else {
                  cr.zero();
                }
                scan_kctx.release();
              }
            }
            else {
              cr.zero();
            }
            cr.clear( this->ctx_id );
            if ( ! cr.is_empty() && kp->keylen > 0 ) {
            /*printf( "addkey: %.*s\r\n", kp->keylen - 1, key );*/
              uint32_t hash = kv_crc_c( key, kp->keylen - 1, 0 );
              KvSubRoute * rt;
              rt = this->sub_tab.upsert( hash, key, kp->keylen - 1 );
              cr.copy_to( rt->rt_bits );
              this->subscr_cr.or_bits( cr );
              if ( ! is_sys_wild )
                this->poll.add_route( key, kp->keylen - 1, hash,
                                      this->fd );
              else
                this->poll.add_pattern_route( &key[ plen ], prefixlen,
                                              hash, this->fd );
            }
          }
        }
      }
    }
  }
}

void
KvPubSub::process_shutdown( void ) noexcept
{
  if ( this->unregister_mcast() )
    this->push( EV_WRITE );
  /*else if ( this->test( EV_WRITE ) == 0 )
    this->pushpop( EV_CLOSE, EV_SHUTDOWN );*/
}

static void
make_pipe_name( uint64_t stamp,  uint32_t src,  uint32_t dest,  char *name )
{
  size_t n = uint64_to_string( stamp, name );
  name[ n++ ] = '_';
  name[ n++ ] = 's';
  n += uint64_to_string( src, &name[ n ] );
  name[ n++ ] = '_';
  name[ n++ ] = 'd';
  n += uint64_to_string( dest, &name[ n ] );
  ::strcpy( &name[ n ], ".pipe" );
}

void
KvPubSub::process_close( void ) noexcept
{
#if 0
  printf( "kv process close\n" );
  for ( size_t i = 0; i < MAX_CTX_ID; i++ ) {
    if ( this->rcv_pipe[ i ] != NULL ) {
      char nm[ 64 ];
      make_pipe_name( this->poll.map->hdr.create_stamp, this->ctx_id, i, nm );
      printf( "close rcv pipe %s\n", nm );
      this->rcv_pipe[ i ]->close();
      this->rcv_pipe[ i ] = NULL;
    }
  }
#endif
}
/* this refreshes the routing bits for all ctx_ids in the system */
bool
KvPubSub::update_mcast_route( void ) noexcept
{
  size_t    sz;
  void    * val;
  KeyStatus status;

  if ( this->mc_pos != 0 ) {
    if ( this->kctx.if_value_equals( this->mc_pos, this->mc_value_ctr ) )
      return ! this->mc_cr.is_empty();
  }
  /* push read, in case unsubs are in the inbox */
  this->idle_push( EV_READ_LO );
  /*printf( "mcast changed\n" );*/
  this->kctx.set_key( this->mcast.kbuf );
  this->kctx.set_hash( this->mcast.hash1, this->mcast.hash2 );
  if ( (status = this->kctx.find( &this->wrk )) == KEY_OK ) {
    if ( (status = this->kctx.value( &val, sz )) == KEY_OK &&
         sz == sizeof( CubeRoute128 ) ) {
      this->mc_cr.copy_from( val );
      this->mc_cr.clear( this->ctx_id );
      this->kctx.get_pos_value_ctr( this->mc_pos, this->mc_value_ctr );
      this->mc_cnt = this->mc_cr.branch4( this->ctx_id, 0, MAX_CTX_ID,
                                          this->mc_range.b );
      return ! this->mc_cr.is_empty();
    }
  }
  this->mc_cr.zero();
  this->mc_cnt = 0;
  this->mc_range.w = 0;
  return false;
}

bool
KvPubSub::push_backlog( KvMsgQueue &ibx,  KvBacklogList &list,  size_t cnt,
                        KvMsg **vec,  msg_size_t *siz,
                        uint64_t vec_size ) noexcept
{
  uint8_t    *  p;
  KvMsg      ** vp;
  msg_size_t *  szp;
  KvBacklog  *  back = list.tl;
  size_t        i, j, k = 0;

  if ( back != NULL && back->cnt + cnt <= 64 ) {
    p = (uint8_t *) ibx.tmp.alloc( vec_size );
    if ( p == NULL )
      return false;

    vp  = back->vec;
    szp = back->siz;
    i   = back->cnt;
    j   = i + cnt;
    back->cnt = j;
  }
  else {
    size_t arsz = ( cnt < 64 ? 64 : cnt );
    p = (uint8_t *) ibx.tmp.alloc( sizeof( KvBacklog ) + vec_size +
                          arsz * ( sizeof( msg_size_t ) + sizeof( KvMsg * ) ) );
    if ( p == NULL )
      return false;

    vp   = (KvMsg **) (void *) &p[ sizeof( KvBacklog ) ];
    szp  = (msg_size_t *) (void *) &vp[ arsz ];
    back = new ( p ) KvBacklog( cnt, vp, szp, vec_size );
    p    = (uint8_t *) (void *) &szp[ arsz ];
    i    = 0;
    j    = cnt;
  }

  do {
    msg_size_t n = siz[ k ];
    vp[ i ]  = (KvMsg *) (void *) p;
    szp[ i ] = n;
    ::memcpy( p, vec[ k ], n );
    p = &p[ n ];
    k++;
  } while ( ++i < j );

  ibx.backlog_size += vec_size;
  ibx.backlog_cnt  += cnt;
  if ( back != list.tl ) {
    if ( list.hd == NULL ) {
      this->backlogq.push_tl( &ibx );
      ibx.backlog_progress_ns = this->time_ns;
    }
    list.push_tl( back );
  }
  ibx.need_signal   = true;
  this->push( EV_WRITE_HI );

  return true;
}

bool
KvPubSub::send_pipe_backlog( KvMsgQueue &ibx ) noexcept
{
  if ( ibx.snd_pipe == NULL || ibx.pipe_backlog.hd == NULL )
    return true;
  PipeBuf    & pb = *ibx.snd_pipe;
  KvBacklog  * back;

  for (;;) {
    if ( (back = ibx.pipe_backlog.hd) == NULL )
      return true;

    size_t       cnt      = back->cnt,
                 vec_size = back->vec_size;
    msg_size_t * siz      = back->siz;
    KvMsg     ** vec      = back->vec;

    for (;;) {
      PipeWriteCtx ctx;
      size_t       i,
                   j;
      uint64_t     idx,
                   data_len,
                   size,
                   more;

      if ( ! pb.alloc_space( ctx, siz[ 0 ] ) ) {
        back->cnt      = cnt;
        back->vec_size = vec_size;
        back->vec      = vec;
        back->siz      = siz;
        goto break_loop;
      }
      data_len = ctx.rec_len - PipeBuf::HDR_LEN;
      for ( i = 1; i < cnt; i++ ) {
        more = align<uint64_t>( siz[ i ], PipeBuf::HDR_LEN );
        if ( ctx.required + more > ctx.available ||
             ctx.required + more > PipeBuf::CAPACITY - ctx.record_idx )
          break;
        ctx.required += more;
        data_len     += more;
      }
      ctx.rec_len = data_len + PipeBuf::HDR_LEN;
      idx  = ctx.record_idx + PipeBuf::HDR_LEN;
      size = 0;
      for ( j = 0; j < i; j++ ) {
        ::memcpy( &pb.buf[ idx ], vec[ j ], siz[ j ] );
        idx  += align<uint64_t>( siz[ j ], PipeBuf::HDR_LEN );
        size += siz[ j ];
      }
      pb.set_rec_length( idx, 0 ); /* terminate w/zero */
      /* mem fence here if not total store ordered cpu */
      pb.set_rec_length( ctx.record_idx, data_len, PipeBuf::FLAG2 );
      pb.x.tail_counter = ctx.tail + ctx.rec_len + ctx.padding;

      vec_size         -= size;
      ibx.pub_size     += size;
      ibx.pub_cnt      += 1;
      ibx.pub_msg      += j;
      ibx.backlog_size -= size;
      ibx.backlog_cnt  -= j;
      ibx.backlog_progress_ns = this->time_ns;
      if ( cnt == j ) {
        ibx.pipe_backlog.pop_hd();
        goto break_loop;
      }
      cnt -= j;
      vec  = &vec[ j ];
      siz  = &siz[ j ];
    }
  break_loop:;
    if ( back == ibx.pipe_backlog.hd )
      break;
  }
  ibx.need_signal = true;
  this->push( EV_WRITE_HI );
  return false;
}

bool
KvPubSub::send_kv_backlog( KvMsgQueue &ibx ) noexcept
{
  if ( ibx.kv_backlog.hd == NULL )
    return true;
  KvBacklog * back;
  KeyStatus   status = KEY_OK;

  /* send vector to kv, acquire the inbox key */
  this->kctx.set_key( ibx.kbuf );
  this->kctx.set_hash( ibx.hash1, ibx.hash2 );
  for (;;) {
    if ( (back = ibx.kv_backlog.hd) == NULL )
      break;
    if ( (status = this->kctx.acquire( &this->wrk )) <= KEY_IS_NEW ) {
      if ( status == KEY_IS_NEW || this->kctx.entry->test( FL_IMMEDIATE_VALUE ) )
        ibx.need_signal = true;
      /* append the vector to inbox */
      status = this->kctx.append_vector( back->cnt, back->vec, back->siz,
                                         ibx.high_water_size );
      /* if success status */
      if ( status <= KEY_IS_NEW ) {
        ibx.backlog_size -= back->vec_size;
        ibx.backlog_cnt  -= back->cnt;
        ibx.backlog_progress_ns = this->time_ns;
        ibx.kv_backlog.pop_hd();
      }
    }
    this->kctx.release();
    if ( status > KEY_IS_NEW )
      break;
  }
  /* some other error, corruption */
  if ( status > KEY_IS_NEW && status != KEY_MSG_LIST_FULL ) {
    fprintf( stderr, "KvPubSub: unable to send backlog, status %d+%s/%s\n",
             status, kv_key_status_string( status ),
             kv_key_status_description( status ) );
  }
  /* if all clear, return true */
  if ( ibx.kv_backlog.hd == NULL )
    return true;
  /* still more backlog to send */
  ibx.need_signal = true;
  this->push( EV_WRITE_HI );
  return false;
}

inline bool
KvPubSub::pipe_msg( KvMsgQueue &ibx,  KvMsg &msg ) noexcept
{
  bool backed_up = ( ibx.pipe_backlog.hd != NULL );
  if ( ! backed_up ) {
    if ( ibx.snd_pipe != NULL ) {
      /* write a message to a pipe */
      kv::PipeBuf & pb = *ibx.snd_pipe;
      if ( pb.write( &msg, msg.size ) != 0 )
        ibx.need_signal = true;
      else
        backed_up = true;
    }
    else {
      return this->send_kv_msg( ibx, msg, true );
    }
  }
  /* msg didn't fit, push to backlog */
  if ( backed_up ) {
    KvMsg    * p  = &msg;
    msg_size_t sz = msg.size;
    if ( this->push_backlog( ibx, ibx.pipe_backlog, 1, &p, &sz, sz ) )
      return true;
    /* failure */
    fprintf( stderr, "KvPubSub: unable to pipe msg\n" );
    return false;
  }
  /* ok, sent */
  ibx.pub_size += msg.size;
  ibx.pub_cnt  += 1;
  ibx.pub_msg  += 1;
  return true;
}

bool
KvPubSub::send_kv_msg( KvMsgQueue &ibx,  KvMsg &msg,  bool backitup ) noexcept
{
  KeyStatus status = KEY_OK;
  if ( ibx.kv_backlog.hd == NULL ) {
    /* push message to kv inbox */
    this->kctx.set_key( ibx.kbuf );
    this->kctx.set_hash( ibx.hash1, ibx.hash2 );
    if ( (status = this->kctx.acquire( &this->wrk )) <= KEY_IS_NEW ) {
      if ( status == KEY_IS_NEW || this->kctx.entry->test( FL_IMMEDIATE_VALUE ) )
        ibx.need_signal = true;
      status = this->kctx.append_msg( &msg, msg.size, ibx.high_water_size );
      this->kctx.release();
    }
  }
  else {
    status = KEY_MSG_LIST_FULL;
  }
  /* doesn't fit, push to backlog */
  if ( status == KEY_MSG_LIST_FULL && backitup ) {
    KvMsg    * p  = &msg;
    msg_size_t sz = msg.size;
    if ( this->push_backlog( ibx, ibx.kv_backlog, 1, &p, &sz, sz ) )
      return true;
  }
  /* ok, sent */
  else if ( status == KEY_OK ) {
    ibx.pub_size += msg.size;
    ibx.pub_cnt  += 1;
    ibx.pub_msg  += 1;
    return true;
  }
  /* failure */
  fprintf( stderr, "KvPubSub: unable to send msg, status %d+%s/%s\n", status,
         kv_key_status_string( status ), kv_key_status_description( status ) );
  return false;
}

bool
KvPubSub::pipe_vec( size_t cnt,  KvMsg **vec,  msg_size_t *siz,
                    KvMsgQueue &ibx,  uint64_t vec_size ) noexcept
{
  if ( ibx.snd_pipe == NULL )
    return this->send_kv_vec( cnt, vec, siz, ibx, vec_size );
  bool b = true;

  if ( ibx.pipe_backlog.hd == NULL ) {
    PipeBuf    & pb = *ibx.snd_pipe;
    PipeWriteCtx ctx;
    size_t       i,
                 j;
    uint64_t     idx,
                 data_len,
                 size,
                 more;
    for (;;) {
      /* write smaller than the large msgs by sending an array to the pipe
       * starting with the first and adding more until it doesn't fit */
      if ( ! pb.alloc_space( ctx, siz[ 0 ] ) )
        goto break_loop;
      data_len = ctx.rec_len - PipeBuf::HDR_LEN;
      for ( i = 1; i < cnt; i++ ) {
        more = align<uint64_t>( siz[ i ], PipeBuf::HDR_LEN );
        if ( ctx.required + more > ctx.available ||
             ctx.required + more > PipeBuf::CAPACITY - ctx.record_idx )
          break;
        ctx.required += more;
        data_len     += more;
      }
      /* copy the data into the pipe */
      ctx.rec_len = data_len + PipeBuf::HDR_LEN;
      idx  = ctx.record_idx + PipeBuf::HDR_LEN;
      size = 0;
      for ( j = 0; j < i; j++ ) {
        ::memcpy( &pb.buf[ idx ], vec[ j ], siz[ j ] );
        idx  += align<uint64_t>( siz[ j ], PipeBuf::HDR_LEN );
        size += siz[ j ];
      }
      pb.set_rec_length( idx, 0 ); /* terminate pipe data w/zero */
      /* mem fence here if not total store ordered cpu */
      pb.set_rec_length( ctx.record_idx, data_len );    /* data is valid now */
      pb.x.tail_counter = ctx.tail + ctx.rec_len + ctx.padding;
      /* step through the vector to the next msgs */
      vec_size     -= size;
      ibx.pub_size += size;
      ibx.pub_cnt  += 1;
      ibx.pub_msg  += j;
      cnt -= j;
      if ( cnt == 0 )
        goto break_loop2;
      vec  = &vec[ j ];
      siz  = &siz[ j ];
    }
  }
break_loop:;
  if ( cnt > 0 ) /* if some doesn't fit, backlog it */
    b &= this->push_backlog( ibx, ibx.pipe_backlog, cnt, vec, siz, vec_size );
break_loop2:;
  ibx.need_signal = true;
  return b;
}

bool
KvPubSub::send_kv_vec( size_t cnt,  KvMsg **vec,  msg_size_t *siz,
                       KvMsgQueue &ibx,  uint64_t vec_size ) noexcept
{
  KeyStatus status = KEY_OK;

  if ( ibx.kv_backlog.hd == NULL ) {
    this->kctx.set_key( ibx.kbuf );
    this->kctx.set_hash( ibx.hash1, ibx.hash2 );
    if ( (status = this->kctx.acquire( &this->wrk )) <= KEY_IS_NEW ) {
      if ( status == KEY_IS_NEW ||
           this->kctx.entry->test( FL_IMMEDIATE_VALUE ) )
        ibx.need_signal = true;
      status = this->kctx.append_vector( cnt, vec, siz, ibx.high_water_size );
      this->kctx.release();
    }
  }
  else {
    status = KEY_MSG_LIST_FULL;
  }
  if ( status == KEY_MSG_LIST_FULL ) {
    if ( this->push_backlog( ibx, ibx.kv_backlog, cnt, vec, siz, vec_size ) )
      return true;
  }
  else if ( status == KEY_OK ) {
    ibx.pub_size += vec_size;
    ibx.pub_cnt  += 1;
    ibx.pub_msg  += cnt;
    return true;
  }
  fprintf( stderr, "KvPubSub: unable to send vec, status %d+%s/%s\n", status,
         kv_key_status_string( status ), kv_key_status_description( status ) );
  return false;
}

static inline size_t test_set_range( CubeRoute128 &used,  const uint8_t *b,
                                     size_t cnt ) noexcept
{
  size_t j = 0;
  for ( size_t i = 0; i < cnt; i += 2 ) {
    j += used.test_set( b[ i ] );
  }
  return j;
}

inline size_t
KvPubSub::resolve_routes( CubeRoute128 &used ) noexcept
{
  size_t veccnt = 0;
  /* for each message, determine the subscription destinations */
  for ( KvMsgList * l = this->sendq.hd; l != NULL; l = l->next ) {
    KvMsg & msg   = l->msg;
    uint8_t start = msg.dest_start,
            end   = msg.dest_end;
    /*print_msg( msg );*/
    /* if not sending to a single node */
    if ( start != end ) {
      /* if msg goes to all nodes, calculate the dest range */
      if ( is_kv_bcast( msg.msg_type ) ) {
        /* if sending to all nodes */
        if ( end - start == MAX_CTX_ID ) {
          l->range.w = this->mc_range.w;
          l->cnt = this->mc_cnt;
        }
        else {
          l->cnt = this->mc_cr.branch4( this->ctx_id, start, end, l->range.b );
        }
      }
      /* if msg is cast to a subscription routes (incl wildcard) */
      else {
        KvSubMsg &submsg = (KvSubMsg &) msg;
        CubeRoute128 cr;
        uint8_t pref_cnt = submsg.get_prefix_cnt();
        cr.zero();
        /* or all of the sub matches together, multiple wildcards + exact */
        for ( uint8_t i = 0; i < pref_cnt; i++ ) {
          KvPrefHash & pf = submsg.prefix_hash( i );
          uint32_t h = pf.get_hash();
          KvSubRoute * rt;
          if ( pf.pref == 64 ) {
            rt = this->sub_tab.find( h, submsg.subject(), submsg.sublen );
          }
          else {
            SysWildSub w( submsg.subject(), pf.pref );
            rt = this->sub_tab.find( h, w.sub, w.len );
          }
          if ( rt != NULL )
            cr.or_from( rt->rt_bits );
          /*else  <- route can disappear while in msg queue
            printf( "no route\n" );*/
        }
        /* mask currently connected routes */
        cr.and_bits( this->mc_cr );
        cr.clip( start, end );
        KvRouteCache * p = &this->rte_cache[ cr.fold8() ];
        if ( ! p->cr.equals( cr ) ) {
          p->cr  = cr;
          p->cnt = CubeRoute128::branch4x( this->ctx_id, cr, p->range.b );
        }
        l->cnt = p->cnt;
        l->range.w = p->range.w;
      }
      veccnt += test_set_range( used, l->range.b, l->cnt );
    }
    /* dest is single node */
    else if ( this->mc_cr.is_set( start ) ) {
      l->range.b[ 0 ] = start;
      l->range.b[ 1 ] = start;
      l->cnt = 2;
      veccnt += used.test_set( start );
    }
  }
  return veccnt;
}
#if 0
void
KvPubSub::check_seqno( void ) noexcept
{
  for ( KvMsgList  * l = this->sendq.hd; l != NULL; l = l->next ) {
    KvMsg & msg = l->msg;
    uint64_t seqno = msg.get_seqno();
    if ( seqno != this->last_seqno[ this->ctx_id ] + 1 ) {
      printf( "missing seqno %lu -> %lu\n", this->last_seqno[ this->ctx_id ],
              seqno );
    }
    this->last_seqno[ this->ctx_id ] = seqno;
  }
}
#endif
inline void
KvPubSub::write_send_queue_fast( void ) noexcept
{
  /*this->check_seqno();*/
  /* use pipe_msg() op each queued msg, doesn't vectorize */
  for ( KvMsgList  * l = this->sendq.hd; l != NULL; l = l->next ) {
    KvMsg & msg = l->msg;
    while ( l->off < l->cnt ) {
      msg.dest_start = l->range.b[ l->off ];
      msg.dest_end   = l->range.b[ l->off + 1 ];
      l->off += 2;
      KvMsgQueue & ibx = *this->snd_inbox[ msg.dest_start ];
      this->pipe_msg( ibx, msg );
    }
  }
}

void
KvPubSub::write_send_queue_slow( void ) noexcept
{
  static const size_t VEC_CHUNK_SIZE = 1024;
  size_t       j = 0;
  uint64_t     vec_size = 0;
  msg_size_t   siz[ VEC_CHUNK_SIZE ];
  KvMsg      * vec[ VEC_CHUNK_SIZE ];
  KvMsgList  * hd = this->sendq.hd;
  /*this->check_seqno();*/
  /* send arrays of messages by iterating through the destinations */
  for (;;) {
    while ( hd->off == hd->cnt ) {
      if ( (hd = hd->next) == NULL )
        return;
    }
    size_t dest = hd->range.b[ hd->off ];
    KvMsgQueue & ibx = *this->snd_inbox[ dest ];

    for ( KvMsgList *l = hd; l != NULL; l = l->next ) {
      if ( l->off < l->cnt && l->range.b[ l->off ] == dest ) {
        KvMsg & msg = l->msg;
        msg.dest_start = l->range.b[ l->off ];
        msg.dest_end   = l->range.b[ l->off + 1 ];
        l->off += 2;
        vec[ j ] = &msg;
        siz[ j ] = msg.size;
        vec_size += msg.size;
        if ( ++j == VEC_CHUNK_SIZE ) {
          this->pipe_vec( VEC_CHUNK_SIZE, vec, siz, ibx, vec_size );
          j = 0;
          vec_size = 0;
        }
      }
    }
    if ( j > 0 ) {
      this->pipe_vec( j, vec, siz, ibx, vec_size );
      j = 0;
      vec_size = 0;
    }
  }
}

void
KvPubSub::notify_peers( CubeRoute128 &used ) noexcept
{
  if ( ( this->flags & KV_DO_NOTIFY ) != 0 ) {
    size_t dest;
    /* notify each dest fd through poll() */
    if ( used.first_set( dest ) ) {
      do {
        KvMsgQueue & ibx = *this->snd_inbox[ dest ];
        if ( ibx.need_signal ) {
          ibx.need_signal = false;
          uint32_t pid = this->kctx.ht.ctx[ dest ].ctx_pid,
                   tid = this->kctx.ht.ctx[ dest ].ctx_thrid;
          uint16_t fl  = this->kctx.ht.ctx[ dest ].ctx_flags;
          if ( pid > 0 && ( fl & KV_NO_SIGUSR ) == 0 ) {
            if ( this->poll.quit )
              printf( "quit[ %u ], notify pid:%d tid:%d dest ctx %ld\n",
                      this->ctx_id, pid, tid, dest );
            ::kill( tid, kv_msg_signal );
            ibx.signal_cnt++;
          }
        }
      } while ( used.next_set( dest ) );
    }
  }
}

void
KvPubSub::write( void ) noexcept
{
  CubeRoute128 used;
  int old_state = this->state;

  this->pop2( EV_WRITE, EV_WRITE_HI );
  used.zero();
  /* if there are other contexts recving msgs */
  if ( this->update_mcast_route() ) {
    if ( this->send_cnt > this->route_cnt ) {
      size_t veccnt = this->resolve_routes( used );
      if ( this->send_cnt - this->route_cnt < 100 || veccnt == 0 )
        this->write_send_queue_fast();
      else
        this->write_send_queue_slow();
      this->route_cnt  = this->send_cnt;
      this->route_size = this->send_size;
    }
  }
  /* if there are contexts with a backlog of msgs */
  if ( this->backlogq.hd != NULL ) {
    KvMsgQueue *ibx = this->backlogq.hd;
    int dead = 0;
    this->time_ns = this->poll.current_coarse_ns();
    while ( ibx != NULL ) {
      KvMsgQueue * next = ibx->next;
      static const int IS_CLEARED = 1, IS_DEAD = 2;
      int state = 0;
      if ( ! this->mc_cr.is_set( ibx->ibx_num ) ) {
        state = IS_CLEARED;
      }
      else {
        used.set( ibx->ibx_num );
        if ( this->send_pipe_backlog( *ibx ) &&
             this->send_kv_backlog( *ibx ) ) {
          state = IS_CLEARED;
        }
        else {
          if ( this->check_backlog_warning( *ibx ) ) {
            state = IS_CLEARED | IS_DEAD;
            used.clear( ibx->ibx_num );
          }
        }
      }
      if ( state != 0 ) {
        this->backlogq.pop( ibx );
        ibx->tmp.reset();
        ibx->kv_backlog.init();
        ibx->pipe_backlog.init();
        ibx->backlog_size = 0;
        ibx->backlog_cnt  = 0;
        ibx->backlog_progress_cnt  = 0;
        ibx->backlog_progress_size = 0;

        if ( ( state & IS_DEAD ) != 0 ) {
          this->close_pipe( ibx->ibx_num );
          this->dead_cr.set( ibx->ibx_num );
          dead++;
        }
      }
      ibx = next;
    }
    if ( dead != 0 )
      this->clear_mcast_dead_routes();
  }
  /* if peers need notify signals */
  this->notify_peers( used );

  /* reset sendq, free mem */
  this->sendq.init();
  this->snd_wrk.reset();
  /* alternate write hi with read/write for balance */
  if ( ( this->test( EV_WRITE_HI ) & old_state ) != 0 )
    this->pushpop( EV_WRITE | EV_READ, EV_WRITE_HI );
}

static const uint64_t BACKLOG_WARN_NS   = 5 * (uint64_t) 1000000000,
                      BACKLOG_WARN_CNT  = 1000000,
                      BACKLOG_WARN_SIZE = BACKLOG_WARN_CNT * 1000;
bool
KvPubSub::check_backlog_warning( KvMsgQueue &ibx ) noexcept
{
  bool ret = false;
  if ( this->time_ns > ibx.backlog_progress_ns + BACKLOG_WARN_NS ||
       ibx.backlog_cnt > ibx.backlog_progress_cnt + BACKLOG_WARN_CNT ||
       ibx.backlog_size > ibx.backlog_progress_size + BACKLOG_WARN_SIZE ) {
    const char *s;
    if ( this->is_dead( ibx.ibx_num ) ) {
      s = "*dead*";
      ret = true;
    }
    else
      s = "";
    printf( "[ %u ] %s no progress (time=%.3f): %lu size, %lu cnt, high %d\n",
            ibx.ibx_num, s,
        (double) ( this->time_ns - ibx.backlog_progress_ns ) / 1000000000.0,
                ibx.backlog_size, ibx.backlog_cnt,
                this->test( EV_WRITE_HI ) );
    ibx.backlog_progress_ns = this->time_ns;
    ibx.backlog_progress_cnt = ibx.backlog_cnt;
    ibx.backlog_progress_size = ibx.backlog_size;
  }
  return ret;
}

bool
KvPubSub::get_sub_mcast( const char *sub,  size_t len,
                         CubeRoute128 &cr ) noexcept
{
  KeyBuf        kbuf;
  KeyFragment * kb = &kbuf;
  void        * val;
  uint64_t      sz,
                hash1,
                hash2;
  KeyStatus     status;

  if ( kbuf.copy( sub, len + 1 ) != len + 1 ) {
    size_t sz = sizeof( KeyFragment ) + len;
    kb = (KeyFragment *) this->snd_wrk.alloc( sz );
    ::memcpy( kb->u.buf, sub, len );
  }
  kb->u.buf[ len ] = '\0';
  this->hs.hash( *kb, hash1, hash2 );
  this->rt_kctx.set_key( *kb );
  this->rt_kctx.set_hash( hash1, hash2 );
  /* check if already set by using find(), lower cost when route is expected
   * to be set */
  for ( int cnt = 0; ; ) {
    if ( (status = this->rt_kctx.find( &this->rt_wrk )) == KEY_OK ) {
      if ( (status = this->rt_kctx.value( &val, sz )) == KEY_OK &&
           sz == sizeof( CubeRoute128 ) ) {
        cr.copy_from( val );
        return true;
      }
    }
    if ( status == KEY_NOT_FOUND )
      break;
    if ( ++cnt == 50 ) {
      fprintf( stderr, "error kv_pubsub mc lookup (%.*s): (%s) %s\n",
               (int) len, sub,
               kv_key_status_string( status ),
               kv_key_status_description( status ) );
      break;
    }
  }
  cr.zero();
  return false;
}

void
KvSubNotifyList::on_sub( KvSubMsg & ) noexcept
{
}

void
KvPubSub::forward_sub( KvSubMsg &submsg ) noexcept
{
  for ( KvSubNotifyList * l = this->sub_notifyq.hd; l != NULL; l = l->next )
    l->on_sub( submsg );
}

bool
KvPubSub::route_msg( KvMsg &msg ) noexcept
{
  /*msg.print();*/
  /* if msg destination has more hops */
  if ( msg.dest_start != msg.dest_end ) {
    KvMsgList * l = (KvMsgList *)
                    this->snd_wrk.alloc( sizeof( KvMsgList ) + msg.size );
    l->init_route();
    ::memcpy( &l->msg, &msg, msg.size );
    this->sendq.push_tl( l );
    this->send_size += msg.size;
    this->send_cnt  += 1;

    this->idle_push( EV_WRITE );
  }
  uint64_t seqno = msg.get_seqno();
/*  if ( seqno != this->last_seqno[ msg.src ] + 1 ) {
    printf( "missing seqno %lu -> %lu\n", this->last_seqno[ msg.src ],
            seqno );
  }
  this->last_seqno[ msg.src ] = seqno;*/
  if ( msg.msg_type == KV_MSG_PUBLISH ) {
    /* forward message from publisher to shm */
    KvSubMsg  & submsg = (KvSubMsg &) msg;
    KvFragAsm * frag   = this->frag_asm[ submsg.src ];
    if ( frag == NULL || seqno - frag->frag_count != frag->first_seqno ) {
      EvPublish pub( submsg.subject(), submsg.sublen,
                     submsg.reply(), submsg.replylen,
                     submsg.get_msg_data(), submsg.msg_size,
                     this->fd, submsg.hash, NULL, 0,
                     submsg.msg_enc, submsg.code );
      this->poll.forward_msg( pub, NULL, submsg.get_prefix_cnt(),
                              submsg.prefix_array() );
    }
    else if ( frag->msg_size >= submsg.msg_size ) {
      ::memcpy( frag->buf, submsg.get_msg_data(), submsg.msg_size );
      EvPublish pub( submsg.subject(), submsg.sublen,
                     submsg.reply(), submsg.replylen,
                     frag->buf, frag->msg_size,
                     this->fd, submsg.hash, NULL, 0,
                     submsg.msg_enc, submsg.code );
      this->poll.forward_msg( pub, NULL, submsg.get_prefix_cnt(),
                              submsg.prefix_array() );
    }
    return true;
  }
  switch ( msg.msg_type ) {
    case KV_MSG_FRAGMENT: {
      KvSubMsg  & submsg = (KvSubMsg &) msg;
      size_t      off  = submsg.get_frag_off(),
                  size = off + submsg.msg_size;
      KvFragAsm * frag = this->frag_asm[ submsg.src ];

      if ( frag == NULL || size > frag->buf_size ) {
        this->frag_asm[ submsg.src ] =
          (KvFragAsm *) ::realloc( frag, sizeof( KvFragAsm ) + size );
        bool is_first = ( frag == NULL );
        frag = this->frag_asm[ submsg.src ];
        frag->buf_size = size;
        if ( is_first ) /* initialize with curr seqno */
          goto is_first_seqno;
      }
      /* check if in sequence with a previous frag from src */
      if ( seqno - frag->frag_count != frag->first_seqno ) {
    is_first_seqno:;
        frag->first_seqno = seqno;
        frag->frag_count  = 1;
        frag->msg_size    = size;
      }
      else {
        frag->frag_count++;
      }
      ::memcpy( &frag->buf[ off ], submsg.get_msg_data(), submsg.msg_size );
      break;
    }
    case KV_MSG_SUB: /* update my routing table when sub/unsub occurs */
    case KV_MSG_UNSUB: {
      /*msg.print_sub();*/
      KvSubMsg &submsg = (KvSubMsg &) msg;
      CubeRoute128 cr;

      this->get_sub_mcast( submsg.subject(), submsg.sublen, cr );
      cr.clear( this->ctx_id ); /* remove my subscriber id */
      /* if no more routes to shm exist, then remove */
      if ( cr.is_empty() ) {
        /*printf( "remsub: %.*s\r\n", submsg.sublen, submsg.subject() );*/
        this->sub_tab.remove( submsg.hash, submsg.subject(), submsg.sublen );
        if ( this->sub_tab.find_by_hash( submsg.hash ) == NULL )
          this->poll.del_route( submsg.subject(), submsg.sublen, submsg.hash,
                                this->fd );
      }
      /* adding a route, publishes will be forwarded to shm */
      else {
        KvSubRoute * rt;
        /*printf( "addsub: %.*s\r\n", submsg.sublen, submsg.subject() );*/
        rt = this->sub_tab.upsert( submsg.hash, submsg.subject(),
                                   submsg.sublen );
        cr.copy_to( rt->rt_bits );
        this->subscr_cr.or_bits( cr );
        this->poll.add_route( submsg.subject(), submsg.sublen, submsg.hash,
                              this->fd );
      }
      if ( ! this->sub_notifyq.is_empty() )
        this->forward_sub( submsg );
      break;
    }
    case KV_MSG_PSUB:
    case KV_MSG_PUNSUB: {
      /*msg.print_sub();*/
      KvSubMsg &submsg = (KvSubMsg &) msg;
      if ( submsg.get_prefix_cnt() == 1 ) {
        KvPrefHash &pf = submsg.prefix_hash( 0 );
        CubeRoute128 cr;
        SysWildSub w( submsg.reply(), pf.pref );

        this->get_sub_mcast( w.sub, w.len, cr );
        cr.clear( this->ctx_id );
        /* if no more routes to shm exist, then remove */
        if ( cr.is_empty() ) {
          /*printf( "remwild: %.*s\r\n", (int) w.len, w.sub );*/
          this->sub_tab.remove( submsg.hash, w.sub, w.len );
          if ( this->sub_tab.find_by_hash( submsg.hash ) == NULL )
            this->poll.del_pattern_route( submsg.reply(), pf.pref, submsg.hash,
                                          this->fd );
        }
        /* adding a route, publishes will be forwarded to shm */
        else {
          KvSubRoute * rt;
          /*printf( "addwild: %.*s\r\n", (int) w.len, w.sub );*/
          rt = this->sub_tab.upsert( submsg.hash, w.sub, w.len );
          cr.copy_to( rt->rt_bits );
          this->subscr_cr.or_bits( cr );
          this->poll.add_pattern_route( submsg.reply(), pf.pref, submsg.hash,
                                        this->fd );
        }
      }
      if ( ! this->sub_notifyq.is_empty() )
        this->forward_sub( submsg );
      break;
    }
    case KV_MSG_HELLO:
      /*msg.print();*/
      if ( kv_use_pipes )
        this->open_pipe( msg.src );
      break;

    case KV_MSG_BYE:
      /*msg.print();*/
      this->close_pipe( msg.src );
      return false;

    default:
      break;
  }
  return true;
}

bool
KvPubSub::make_rcv_pipe( size_t src ) noexcept
{
  PipeBuf * pb;
  char rcv_p[ 64 ];
  if ( src >= MAX_CTX_ID ) {
    fprintf( stderr, "bad source\n" );
    return false;
  }
  if ( this->rcv_pipe[ src ] != NULL )
    return true;
  /* recv pipe */
  make_pipe_name( this->poll.map->hdr.create_stamp, this->ctx_id, src, rcv_p );
  pb = PipeBuf::open( rcv_p, true, 0660 );
  this->rcv_pipe[ src ] = pb;
  if ( pb == NULL ) {
    fprintf( stderr, "failed to make recv pipe\n" );
    return false;
  }
  pb->x.owner.x.rcv_ctx_id = this->ctx_id;
  pb->x.owner.x.rcv_pid    = this->poll.map->ctx[ this->ctx_id ].ctx_pid;
  return true;
}

bool
KvPubSub::make_snd_pipe( size_t src ) noexcept
{
  PipeBuf * pb;
  char snd_p[ 64 ];
  if ( src >= MAX_CTX_ID ) {
    fprintf( stderr, "bad source\n" );
    return false;
  }
  if ( this->snd_inbox[ src ]->snd_pipe != NULL )
    return true;
  /* send pipe */
  make_pipe_name( this->poll.map->hdr.create_stamp, src, this->ctx_id, snd_p );
  pb = PipeBuf::open( snd_p, false, 0660 );
  this->snd_inbox[ src ]->snd_pipe = pb;
  if ( pb == NULL )
    return false;
  pb->x.owner.x.snd_ctx_id = this->ctx_id;
  pb->x.owner.x.snd_pid    = this->poll.map->ctx[ this->ctx_id ].ctx_pid;
  return true;
}

void
KvPubSub::open_pipe( size_t src ) noexcept
{
  if ( this->make_snd_pipe( src ) && this->make_rcv_pipe( src ) )
    this->pipe_cr.set( src );
  else {
    static time_t mute;
    static uint64_t cnt;
    time_t n = ::time( 0 );
    cnt++;
    if ( mute != n ) {
      mute = n;
      fprintf( stderr,
               "unable to open pipe (%u, %lu) (times=%lu) is_dead = %s\n",
            this->ctx_id, src, cnt, this->is_dead( src ) ? "true" : "false" );
    }
  }
}

void
KvPubSub::close_pipe( size_t src ) noexcept
{
  char nm[ 64 ];
  this->close_rcv_pipe( src );
  make_pipe_name( this->poll.map->hdr.create_stamp, src, this->ctx_id, nm );
  if ( this->snd_inbox[ src ]->snd_pipe != NULL ) {
    this->snd_inbox[ src ]->snd_pipe->close();
    this->snd_inbox[ src ]->snd_pipe = NULL;
    PipeBuf::unlink( nm );
  }
}

void
KvPubSub::close_rcv_pipe( size_t src ) noexcept
{
  char nm[ 64 ];
  this->pipe_cr.clear( src );
  make_pipe_name( this->poll.map->hdr.create_stamp, this->ctx_id, src, nm );
  if ( this->rcv_pipe[ src ] != NULL ) {
    this->rcv_pipe[ src ]->close();
    this->rcv_pipe[ src ] = NULL;
    PipeBuf::unlink( nm );
  }
}

void
KvPubSub::read( void ) noexcept
{
  bool until_empty = false;
  if ( this->test( EV_READ ) ) {
    struct signalfd_siginfo fdsi;
    while ( ::read( this->fd, &fdsi, sizeof( fdsi ) ) > 0 )
      this->sigusr_recv_cnt++;
    if ( this->sigusr_recv_cnt > kv_busy_loop_rate ) {
      this->poll.map->ctx[ this->ctx_id ].ctx_flags |= KV_NO_SIGUSR;
      this->push( EV_BUSY_POLL );
    }
    /* when signal read() and not busy, then read until empty */
    until_empty = ( this->test( EV_BUSY_POLL ) == 0 );
  }
  this->pop3( EV_READ, EV_READ_LO, EV_READ_HI );
  this->read_inbox( until_empty );
}

static void
read_error( size_t src,  const void *data,  size_t data_len )
{
  fprintf( stderr, "Bad data from %lu\n", src );
  KvHexDump::dump_hex( data, data_len );
}

bool
KvPubSub::busy_poll( void ) noexcept
{
  if ( this->read_inbox( false ) == 0 )
    return false;
  return true; /* progress made */
}

size_t
KvPubSub::read_inbox( bool read_until_empty ) noexcept
{
  /*static size_t flag2_cnt;*/
  /* guard against reentrant calls */
  if ( ( this->flags & KV_READ_INBOX ) != 0 ) {
    this->push( EV_READ_LO );
    return 0;
  }
  this->flags |= KV_READ_INBOX;

  size_t i, n = 0, m = 0;
  bool retry;
  do {
    retry = false;
    for ( i = 0; i < KV_CTX_INBOX_COUNT; i++ )
      n += this->read_inbox2( *this->rcv_inbox[ i ], read_until_empty );

    if ( this->pipe_cr.first_set( i ) ) {
      do {
        PipeBuf   & pb = *this->rcv_pipe[ i ];
        PipeReadCtx ctx;
        while ( pb.read_ctx( ctx ) ) {
          KvMsg  * msg  = (KvMsg *) ctx.data;
          uint64_t left = ctx.data_len, sz;
          /*if ( ( ctx.fl & PipeBuf::FLAG2 ) != 0 )
            flag2_cnt++;*/
          while ( left > 0 ) {
            /*uint64_t seqno = msg->get_seqno();
            assert( seqno == pb.x.owner.x.rcv_seqno + 1 ||
                    pb.x.owner.x.rcv_seqno == 0 );
            pb.x.owner.x.rcv_seqno = seqno;*/
            if ( ! msg->is_valid( left ) ) {
              if ( left == ctx.data_len ) {
                retry = true; /* leave it in the pipe, match with kv */
                goto break_loop;
              }
              else {
                /*assert( 0 );*/
                read_error( i, ctx.data, left );
                pb.consume_ctx( ctx );
              }
              break;
            }
            m++;
            sz = align<uint64_t>( msg->size, PipeBuf::HDR_LEN );
            /*assert( sz > 0 && sz < MAX_PIPE_MSG_SIZE );*/
            if ( ! this->route_msg( *msg ) ) /* route pipe msg and consume it */
              goto break_loop; /* if closes, returns false */
            if ( left <= sz ) {
              pb.consume_ctx( ctx );
              break;
            }
            left -= sz;
            msg = (KvMsg *) &((uint8_t *) (void *) msg)[ sz ];
          }
        }
      break_loop:;
      } while ( this->pipe_cr.next_set( i ) );
    }
  } while ( retry );

  this->inbox_msg_cnt += n;
  this->pipe_msg_cnt  += m;
  this->flags &= ~KV_READ_INBOX;
  return n;
}

size_t
KvPubSub::read_inbox2( KvInboxKey &ibx,  bool read_until_empty ) noexcept
{
  static const size_t VEC_CHUNK_SIZE = 1024;
  void       * data[ VEC_CHUNK_SIZE ];
  msg_size_t   data_sz[ VEC_CHUNK_SIZE ];
  KeyStatus    status;
  size_t       count = 0, total = 0;

  if ( ibx.kbuf != &ibx.ibx_kbuf ) {
    ibx.set_key( ibx.ibx_kbuf );
    ibx.set_hash( ibx.hash1, ibx.hash2 );
    if ( ibx.acquire( &this->ib_wrk ) == KEY_IS_NEW ) {
      uint8_t b = 0;
      ibx.append_msg( &b, 1, 0 );
    }
    ibx.release();
  }
  /*printf( "read_inbox %.*s %lx %lx\n", (int) ibx.ibx_kbuf.keylen,
           (char *) ibx.ibx_kbuf.u.buf, ibx.hash1, ibx.hash2 );*/
  for (;;) {
    bool retry = false;
    if ( ibx.ibx_pos == 0 ) {
      status = ibx.find( &this->ib_wrk );
    }
    else {
      if ( ! read_until_empty ) {
        if ( ibx.if_value_equals( ibx.ibx_pos, ibx.ibx_value_ctr ) )
          break;
      }
      status = ibx.fetch( &this->ib_wrk, ibx.ibx_pos );
    }

    if ( status == KEY_OK ) {
      for (;;) {
        size_t   j;
        uint64_t seqno  = ibx.ibx_seqno,
                 seqno2 = seqno + VEC_CHUNK_SIZE;
        if ( (status = ibx.msg_value( seqno, seqno2, data,
                                      data_sz )) != KEY_OK ) {
          if ( status == KEY_MUTATED )
            retry = true;
          else if ( ibx.ibx_pos == 0 && status == KEY_NOT_FOUND )
            ibx.get_pos_value_ctr( ibx.ibx_pos, ibx.ibx_value_ctr );
          break;
        }
        ibx.ibx_seqno = seqno2;
        j = seqno2 - seqno;
        ibx.read_cnt  += 1;
        ibx.read_msg  += j;
        for ( size_t i = 0; i < j; i++ ) {
          if ( data_sz[ i ] >= sizeof( KvMsg ) ) {
            KvMsg &msg = *(KvMsg *) data[ i ];
            ibx.read_size += data_sz[ i ];
            if ( msg.is_valid( data_sz[ i ] ) ) {
#if 0
              /* check these, make sure messages are in order ?? */
              this->snd_inbox[ msg.src ]->src_session_id = msg.session_id();
              this->snd_inbox[ msg.src ]->src_seqno      = msg.get_seqno();
#endif
              this->route_msg( msg );
            }
          }
        }
        count += j;
        if ( j != VEC_CHUNK_SIZE ) { /* didn't fill entire array, find() again*/
          break;
        }
      }
    }
    else if ( status == KEY_MUTATED )
      retry = true;
    else if ( status == KEY_NOT_FOUND ) {
      if ( ibx.ibx_pos != 0 )
        retry = true;
      ibx.ibx_pos = 0;
    }
    /* remove msgs consumed */
    if ( count > total ) {
      if ( ibx.acquire( &this->ib_wrk ) <= KEY_IS_NEW ) {
        ibx.trim_msg( ibx.ibx_seqno );

        if ( ibx.entry->test( FL_IMMEDIATE_VALUE ) != 0 )
          ibx.get_pos_value_ctr( ibx.ibx_pos, ibx.ibx_value_ctr );
        else
          retry = true;
        ibx.release();
      }
      total = count;
    }
    if ( ! retry || ! read_until_empty )
      break;
  }
  return total;
}

bool
KvPubSub::on_msg( EvPublish &pub ) noexcept
{
  /* no publish to self */
  if ( (uint32_t) this->fd != pub.src_route ) {
    this->create_kvpublish( pub.subj_hash, pub.subject, pub.subject_len,
                            pub.prefix, pub.hash, pub.prefix_cnt,
                            (const char *) pub.reply, pub.reply_len, pub.msg,
                            pub.msg_len, pub.pub_type, pub.msg_enc );
    this->idle_push( EV_WRITE );
  }
  if ( this->backlogq.is_empty() )
    return true;
  /* hash backperssure, could be more specific for the stream destination */
  return false;
}

bool
KvPubSub::hash_to_sub( uint32_t h,  char *key,  size_t &keylen ) noexcept
{
  KvSubRoute * rt;
  if ( (rt = this->sub_tab.find_by_hash( h )) != NULL /*||
     (rt = this->sub_tab.find_by_hash( h | UIntHashTab::SLOT_USED )) != NULL*/){
    ::memcpy( key, rt->value, rt->len );
    keylen = rt->len;
    return true;
  }
  return false;
}

void KvPubSub::release( void ) noexcept {}

KvHexDump::KvHexDump() : boff( 0 ), stream_off( 0 ) {
  this->flush_line();
}

void
KvHexDump::reset( void )
{
  this->boff = 0;
  this->stream_off = 0;
  this->flush_line();
}

void
KvHexDump::flush_line( void )
{
  this->stream_off += this->boff;
  this->boff  = 0;
  this->hex   = 9;
  this->ascii = 61;
  this->init_line();
}

char
KvHexDump::hex_char( uint8_t x )
{
  return x < 10 ? ( '0' + x ) : ( 'a' - 10 + x );
}

void
KvHexDump::init_line( void )
{
  uint64_t j, k = this->stream_off;
  ::memset( this->line, ' ', 79 );
  this->line[ 79 ] = '\0';
  this->line[ 5 ] = hex_char( k & 0xf );
  k >>= 4; j = 4;
  while ( k > 0 ) {
    this->line[ j ] = hex_char( k & 0xf );
    if ( j-- == 0 )
      break;
    k >>= 4;
  }
}

uint32_t
KvHexDump::fill_line( const void *ptr, uint64_t off,  uint64_t len )
{
  while ( off < len && this->boff < 16 ) {
    uint8_t b = ((uint8_t *) ptr)[ off++ ];
    this->line[ this->hex ]   = hex_char( b >> 4 );
    this->line[ this->hex+1 ] = hex_char( b & 0xf );
    this->hex += 3;
    if ( b >= ' ' && b <= 127 )
      line[ this->ascii ] = b;
    this->ascii++;
    if ( ( ++this->boff & 0x3 ) == 0 )
      this->hex++;
  }
  return off;
}

void
KvHexDump::dump_hex( const void *ptr,  uint64_t size )
{
  KvHexDump hex;
  for ( uint64_t off = 0; off < size; ) {
    off = hex.fill_line( ptr, off, size );
    printf( "%s\r\n", hex.line );
    fflush( stdout );
    hex.flush_line();
  }
}

