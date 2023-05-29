#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include <raikv/ev_net.h>
#include <raikv/publish_ctx.h>

using namespace rai;
using namespace kv;
uint32_t rai::kv::kv_pub_debug;

namespace rai {
namespace kv {
struct ForwardBase {
  RoutePublish & sub_route;
  uint32_t       total;
  ForwardBase( RoutePublish &p ) : sub_route( p ), total( 0 ) {}
  void debug_subject( EvPublish &pub, EvSocket *s, const char *w ) {
    printf( "%s(%.*s,%x,%x) %s -> %s.%s(%u)\n", w,
            (int) pub.subject_len, pub.subject, pub.subj_hash,
            kv_crc_c( pub.msg, pub.msg_len, 0 ),
            this->sub_route.service_name,
            s->name[ 0 ] ? s->name : s->peer_address.buf, s->kind, s->fd );
  }
  void debug_total( EvPublish &pub ) {
    if ( this->total == 0 ) {
      printf( "no routes for %.*s\n", (int) pub.subject_len, pub.subject );
    }
  }
};
struct ForwardAll : public ForwardBase {
  ForwardAll( RoutePublish &p ) : ForwardBase( p ) {}
  bool on_msg( uint32_t fd,  EvPublish &pub ) {
    EvSocket * s;
    if ( fd <= this->sub_route.poll.maxfd &&
         (s = this->sub_route.poll.sock[ fd ]) != NULL ) {
      this->total++;
      if ( kv_pub_debug )
        this->debug_subject( pub, s, "fwd_all" );
      return s->on_msg( pub );
    }
    return true;
  }
};
struct ForwardSome : public ForwardBase {
  uint32_t * routes,
             rcnt;
  ForwardSome( RoutePublish &p,  uint32_t *rtes,  uint32_t cnt )
    : ForwardBase( p ), routes( rtes ), rcnt( cnt ) {}
  bool on_msg( uint32_t fd,  EvPublish &pub ) {
    EvSocket * s;
    uint32_t off = bsearch_route( fd, this->routes, this->rcnt );
    if ( off == this->rcnt || this->routes[ off ] != fd ) {
      if ( fd <= this->sub_route.poll.maxfd &&
           (s = this->sub_route.poll.sock[ fd ]) != NULL ) {
        this->total++;
        if ( kv_pub_debug )
          this->debug_subject( pub, s, "fwd_som" );
        return s->on_msg( pub );
      }
    }
    return true;
  }
};
struct ForwardSet : public ForwardBase {
  const BitSpace & fdset;
  ForwardSet( RoutePublish &p,  const BitSpace &b )
    : ForwardBase( p ), fdset( b ) {}
  bool on_msg( uint32_t fd,  EvPublish &pub ) {
    EvSocket * s;
    if ( this->fdset.is_member( fd ) && fd <= this->sub_route.poll.maxfd &&
         (s = this->sub_route.poll.sock[ fd ]) != NULL ) {
      this->total++;
      if ( kv_pub_debug )
        this->debug_subject( pub, s, "fwd_set" );
      return s->on_msg( pub );
    }
    return true;
  }
};
struct ForwardExcept : public ForwardBase {
  const BitSpace & fdexcept;
  ForwardExcept( RoutePublish &p,  const BitSpace &b )
    : ForwardBase( p ), fdexcept( b ) {}
  bool on_msg( uint32_t fd,  EvPublish &pub ) {
    EvSocket * s;
    if ( ! this->fdexcept.is_member( fd ) && fd <= this->sub_route.poll.maxfd &&
         (s = this->sub_route.poll.sock[ fd ]) != NULL ) {
      this->total++;
      if ( kv_pub_debug )
        this->debug_subject( pub, s, "fwd_exc" );
      return s->on_msg( pub );
    }
    return true;
  }
};
struct ForwardNotFd : public ForwardBase {
  uint32_t not_fd;
  ForwardNotFd( RoutePublish &p,  uint32_t fd )
    : ForwardBase( p ), not_fd( fd ) {}
  bool on_msg( uint32_t fd,  EvPublish &pub ) {
    EvSocket * s;
    if ( fd != this->not_fd && fd <= this->sub_route.poll.maxfd &&
         (s = this->sub_route.poll.sock[ fd ]) != NULL ) {
      this->total++;
      if ( kv_pub_debug )
        this->debug_subject( pub, s, "fwd_not" );
      return s->on_msg( pub );
    }
    return true;
  }
};
struct ForwardNotFd2 : public ForwardBase {
  uint32_t not_fd, not_fd2;
  ForwardNotFd2( RoutePublish &p,  uint32_t fd,  uint32_t fd2 )
    : ForwardBase( p ), not_fd( fd ), not_fd2( fd2 ) {}
  bool on_msg( uint32_t fd,  EvPublish &pub ) {
    EvSocket * s;
    if ( fd != this->not_fd && fd != this->not_fd2 &&
         fd <= this->sub_route.poll.maxfd &&
         (s = this->sub_route.poll.sock[ fd ]) != NULL ) {
      this->total++;
      if ( kv_pub_debug )
        this->debug_subject( pub, s, "fwd_nt2" );
      return s->on_msg( pub );
    }
    return true;
  }
};

struct PubFanout64 {
  BitSet64 bi;
  uint32_t start_fd;
  uint8_t  fdidx[ 64 ],
           fdcnt[ 64 ];
  PubFanout64( RoutePublishData *rpd,  uint32_t n,  uint32_t min_fd ) noexcept;
  bool first( uint32_t &k ) { return this->bi.first( k ); }
  bool next( uint32_t &k )  { return this->bi.next( k ); }
};

struct PubFanout512 {
  uint64_t   bits[ 512 / 64 ];
  UIntBitSet fdset;
  uint32_t   start_fd;
  uint16_t   fdidx[ 512 ];
  uint8_t    fdcnt[ 512 ];
  PubFanout512( RoutePublishData *rpd,  uint32_t n,  uint32_t min_fd ) noexcept;
  bool first( uint32_t &k ) { return this->fdset.first( k, 512 ); }
  bool next( uint32_t &k )  { return this->fdset.next( k, 512 ); }
};

struct PubFanoutN {
  void     * bits;
  UIntBitSet fdset;
  uint32_t   start_fd, nfds;
  uint32_t * fdidx;
  uint8_t  * fdcnt;

  PubFanoutN( RoutePublishData *rpd,  uint32_t n,  uint32_t min_fd,  uint32_t range,
              FDSet &spc ) noexcept;
  bool first( uint32_t &k ) { return this->fdset.first( k, this->nfds ); }
  bool next( uint32_t &k )  { return this->fdset.next( k, this->nfds ); }
};
}
}
/* different publishers for different size route matches, one() is the most
 * common, but multi / queue need to be used with multiple routes */
template<class Forward>
static bool
publish_one( EvPublish &pub,  RoutePublishData &rpd,  Forward &fwd ) noexcept
{
  uint32_t * routes = rpd.routes;
  uint32_t   rcount = rpd.rcount;
  uint32_t   hash[ 1 ];
  uint8_t    prefix[ 1 ];
  bool       flow_good = true;

  hash[ 0 ]   = rpd.hash;
  prefix[ 0 ] = rpd.prefix;
  EvPubTmp tmp_hash( pub, hash, prefix, 1 );
  for ( uint32_t i = 0; i < rcount; i++ ) {
    flow_good &= fwd.on_msg( routes[ i ], pub );
  }
  if ( kv_pub_debug )
    fwd.debug_total( pub );
  return flow_good;
}

/* case where max fd < 64, so uint64_t can hold a fdset */
PubFanout64::PubFanout64( RoutePublishData *rpd,  uint32_t n,
                          uint32_t min_fd ) noexcept
{
  this->start_fd = min_fd;
  this->bi.w     = 0;
  for ( uint32_t i = 0; i < n; i++ ) {
    for ( uint32_t j = 0; j < rpd[ i ].rcount; j++ ) {
      uint32_t k = rpd[ i ].routes[ j ] - min_fd;
      uint64_t mask = (uint64_t) 1 << k;
      if ( ( this->bi.w & mask ) == 0 ) {
        this->bi.w |= mask;
        this->fdcnt[ k ] = 1;
        this->fdidx[ k ] = i;
      }
      else {
        this->fdcnt[ k ]++;
      }
    }
  }
}
/* case where max fd < 512, so 64 bytes can hold a fdset */
PubFanout512::PubFanout512( RoutePublishData *rpd,  uint32_t n,
                            uint32_t min_fd ) noexcept
{
  this->start_fd  = min_fd;
  this->fdset.ptr = this->bits;
  this->fdset.zero( 512 );
  for ( uint32_t i = 0; i < n; i++ ) {
    for ( uint32_t j = 0; j < rpd[ i ].rcount; j++ ) {
      uint32_t k = rpd[ i ].routes[ j ] - min_fd;
      if ( ! this->fdset.test_set( k ) ) {
        this->fdcnt[ k ] = 1;
        this->fdidx[ k ] = i;
      }
      else {
        this->fdcnt[ k ]++;
      }
    }
  }
}

FDSet &
FDSetStack::push( void ) noexcept
{
  if ( this->tos >= this->size )
    this->make( this->tos + 1, true );
  if ( this->ptr[ this->tos ] == NULL )
    this->ptr[ this->tos ] = new ( ::malloc( sizeof( FDSet ) ) ) FDSet();
  return *this->ptr[ this->tos++ ];
}

/* case where max fd >= 512, allocates fdset */
PubFanoutN::PubFanoutN( RoutePublishData *rpd,  uint32_t n,  uint32_t min_fd,
                        uint32_t range,  FDSet &spc ) noexcept
{
  size_t fdsz   = align<size_t>( range, 64 );
  size_t nbytes = align<size_t>( fdsz / 64 + fdsz * sizeof( uint32_t ) +
                                 fdsz * sizeof( uint8_t ), sizeof( uint64_t ) );
  uint64_t * m  = spc.make( nbytes / sizeof( uint64_t ) );

  this->start_fd  = min_fd;
  this->bits      = m;
  this->nfds      = fdsz;
  this->fdset.ptr = m;

  m = &m[ fdsz / 64 ];
  this->fdidx = (uint32_t *) (void *) m;
  this->fdcnt = (uint8_t *) (void *) &this->fdidx[ fdsz ];

  this->fdset.zero( fdsz );
  for ( uint32_t i = 0; i < n; i++ ) {
    for ( uint32_t j = 0; j < rpd[ i ].rcount; j++ ) {
      uint32_t k = rpd[ i ].routes[ j ] - min_fd;
      if ( ! this->fdset.test_set( k ) ) {
        this->fdcnt[ k ] = 1;
        this->fdidx[ k ] = i;
      }
      else {
        this->fdcnt[ k ]++;
      }
    }
  }
}

template<class Forward, class Fanout>
static bool
publish_multi( EvPublish &pub,  RoutePublishData *rpd,  uint32_t n,
               Forward &fwd,  Fanout &m ) noexcept
{
  uint8_t  prefix[ MAX_RTE ];
  uint32_t hash[ MAX_RTE ], /* limit is number of rpd elements */
           min_route, i, k, cnt;
  bool     flow_good = true;

  EvPubTmp tmp_hash( pub, hash, prefix );
  for ( bool b = m.first( k ); b; b = m.next( k ) ) {
    min_route = k + m.start_fd;
    cnt = 0;
    for ( i = m.fdidx[ k ]; i < n; i++ ) {
      /* accumulate hashes going to min_route */
      if ( min_route != rpd[ i ].routes[ 0 ] )
        continue;
      if ( --rpd[ i ].rcount != 0 )
        rpd[ i ].routes++;
      hash[ cnt ]   = rpd[ i ].hash;
      prefix[ cnt ] = rpd[ i ].prefix;
      cnt++;
      if ( --m.fdcnt[ k ] == 0 )
        break;
    }
    /* send hashes to min_route */
    pub.prefix_cnt = cnt;
    flow_good &= fwd.on_msg( min_route, pub );
  }
  if ( kv_pub_debug )
    fwd.debug_total( pub );
  return flow_good; /* if no routes left */
}

void
EvSocket::notify_ready( void ) noexcept
{
  BPList & list = this->poll.bp_wait.ptr[ this->fd ];
  while ( ! list.is_empty() ) {
    if ( this->test2( EV_WRITE_POLL, EV_WRITE_HI ) )
      return;
    BPData * data = list.pop_hd();
    data->bp_flags &= ~BP_IN_LIST;
    if ( data->bp_id == this->start_ns )
      data->on_write_ready();
  }
}

bool
EvSocket::wait_empty( void ) noexcept
{
  return this->poll.bp_wait.is_empty( (uint32_t) this->fd );
}

void
BPWait::push( uint32_t fd,  BPData &data ) noexcept
{
  if ( fd >= this->size )
    this->make( fd + 1, true );
  this->ptr[ fd ].push_tl( &data );
  data.bp_flags |= BP_IN_LIST;
}

void
BPWait::pop( uint32_t fd,  BPData &data ) noexcept
{
  this->ptr[ fd ].pop( &data );
  data.bp_flags &= ~BP_IN_LIST;
}

void
BPData::on_write_ready( void ) noexcept
{
}

void
EvSocket::bp_retire( BPData &data ) noexcept
{
  if ( data.bp_in_list() )
    this->poll.bp_wait.pop( data.bp_fd, data );
}

bool
BPData::has_back_pressure( EvPoll &poll, uint32_t fd ) noexcept
{
  EvSocket * s;
  if ( fd > poll.maxfd || (s = poll.sock[ fd ]) == NULL )
    return false;
  if ( s->test2( EV_WRITE_POLL, EV_WRITE_HI ) ) {
    if ( this->bp_in_list() )
      poll.bp_wait.pop( this->bp_fd, *this );
    this->bp_state = s->sock_state;
    this->bp_fd    = fd;
    this->bp_id    = s->start_ns;
    if ( this->bp_notify() )
      poll.bp_wait.push( fd, *this );
    return true;
  }
  return false;
}

static bool
test_back_pressure_one( BPData &bp,  EvPoll &poll,
                        RoutePublishData &rpd ) noexcept
{
  for ( uint32_t j = 0; j < rpd.rcount; j++ ) {
    if ( bp.has_back_pressure( poll, rpd.routes[ j ] ) )
      return true;
  }
  bp.bp_state = 0;
  return false;
}

template <class Fanout>
static bool
test_back_pressure_multi( BPData &bp,  EvPoll &poll,  Fanout &m ) noexcept
{
  uint32_t k;
  for ( bool b = m.first( k ); b; b = m.next( k ) ) {
    if ( bp.has_back_pressure( poll, k + m.start_fd ) )
      return true;
  }
  bp.bp_state = 0;
  return false;
}

RoutePublishContext::RoutePublishContext( RouteDB &db,  EvPublish &p ) noexcept
  : RouteLookup( p.subject, p.subject_len, p.subj_hash, p.shard ),
    pub( p ), rdb( db ), set( db, *this ), save_type( p.publish_type )
{
  this->set.init();
  if ( db.queue_db_size != 0 )
    this->add_queues();
  RouteLookup & look = *this;
  db.get_sub_route( look );
  if ( look.rcount > 0 )
    this->set.add( SUB_RTE, look.rcount, look.subj_hash, look.routes, 0 );
  look.setup_prefix_hash( db.pat_mask() );
  /* the rest of the prefixes are hashed */
  for ( uint32_t k = 0; k < look.prefix_cnt; k++ ) {
    db.get_route( look.keylen[ k ], look.hash[ k ], look );
    if ( look.rcount > 0 )
      this->set.add( look.keylen[ k ], look.rcount, look.hash[ k ], look.routes,
                     look.mask );
  }
  this->set.finish();
}

void
RouteLookup::setup_prefix_hash( uint64_t pat_mask ) noexcept
{
  if ( pat_mask == this->prefix_mask )
    return;
  BitSet64 bi( pat_mask );
  uint32_t len, cnt = 0;
  for ( bool b = bi.first( len ); b; b = bi.next( len ) ) {
    if ( len > this->sublen )
      break;
    this->keylen[ cnt ] = len;
    this->hash[ cnt++ ] = RouteGroup::pre_seed[ len ];
  }
  this->prefix_cnt = cnt;
  this->prefix_mask = pat_mask;

  if ( cnt > 0 ) {
    uint32_t k = ( this->keylen[ 0 ] == 0 ? 1 : 0 );
    if ( cnt > k )
      kv_crc_c_key_array( this->sub, &this->keylen[ k ],
                          &this->hash[ k ], cnt - k );
  }
}

void
RoutePublishContext::make_qroutes( RouteGroup &db ) noexcept
{
  RouteLookup & look = *this;
  RouteRef ref( db.zip, (uint16_t) ( 59 + db.group_num ) );
  look.qroutes = (QueueRef *) ref.route_spc.make( look.rcount * 2 );
  for ( uint32_t i = 0; i < look.rcount; i++ ) {
    look.qroutes[ i ].r = look.routes[ i ];
    look.qroutes[ i ].refcnt = 1;
  }
  look.add_ref( ref );
}

void
RoutePublishContext::add_queues( void ) noexcept
{
  RouteLookup & look = *this;
  bool one = false;

  for ( size_t i = 0; i < this->rdb.queue_db_size; i++ ) {
    QueueDB     & q  = this->rdb.queue_db[ i ];
    RouteGroup  & db = *q.route_group;
    RouteQueueSet qset( db, *this );

    qset.init();
    look.queue_hash = q.queue_hash;
    look.qroutes = NULL;
    db.get_sub_route( look );
    if ( look.rcount > 0 ) {
      if ( look.qroutes == NULL )
        this->make_qroutes( db );
      qset.add( SUB_RTE, look.rcount, look.subj_hash, look.qroutes, 0 );
    }
    look.setup_prefix_hash( db.pat_mask() );
    /* the rest of the prefixes are hashed */
    for ( uint32_t k = 0; k < look.prefix_cnt; k++ ) {
      look.qroutes = NULL;
      db.get_route( look.keylen[ k ], look.hash[ k ], look );
      if ( look.rcount > 0 ) {
        if ( look.qroutes == NULL )
          this->make_qroutes( db );
        qset.add( look.keylen[ k ], look.rcount, look.hash[ k ], look.qroutes,
                  look.mask );
      }
    }
    if ( qset.n != 0 ) {
      RoutePublishSet prune_set( db, *this );
      this->select_queue( q, qset, prune_set );
      this->set.add( prune_set );
      one = true;
    }
  }
  look.qroutes = NULL;
  look.queue_hash = 0;
  if ( one )
    pub.publish_type |= PUB_TYPE_QUEUE;
}

void
RoutePublishContext::select_queue( QueueDB &q,  RouteQueueSet &qset,
                                   RoutePublishSet &prune_set ) noexcept
{
  RouteGroup & db = *q.route_group;
  BitSet64     bi;
  QueueRef   * routes = NULL;
  uint32_t     pref = 0, rcnt = 0,
               i = ( qset.rpd[ 0 ].rcount != 0 ? 1 : 0 );

  bi.w = qset.rpd_mask;
  if ( ( i + kv_popcountl( qset.rpd_mask ) ) == 1 ) {
    if ( qset.rpd[ 0 ].rcount != 0 ) {
      routes = qset.rpd[ 0 ].routes;
      rcnt   = qset.rpd[ 0 ].rcount;
    }
    else {
      bi.first( pref );
      routes = qset.rpd[ pref + 1 ].routes;
      rcnt   = qset.rpd[ pref + 1 ].rcount;
    }
  }
  else {
    RouteRef ref( db.zip, (uint16_t) ( 55 + db.group_num ) );
    if ( qset.rpd[ 0 ].rcount != 0 ) {
      routes = (QueueRef *) ref.route_spc.make( qset.rpd[ 0 ].rcount * 2 );
      rcnt   = 0;
      do {
        routes[ rcnt ] = qset.rpd[ 0 ].routes[ rcnt ];
      } while ( ++rcnt < qset.rpd[ 0 ].rcount );
    }
    if ( qset.rpd_mask != 0 ) {
      for ( bool b = bi.first( pref ); b; b = bi.next( pref ) ) {
        RouteQueueData &data = qset.rpd[ pref + 1 ];
        routes = (QueueRef *) ref.route_spc.make( ( rcnt + data.rcount ) * 2 );
        rcnt   = merge_queue2( routes, rcnt, data.routes, data.rcount );
      }
    }
    this->add_ref( ref );
  }
  uint32_t q_select = 0, r;
  prune_set.init();
  prune_set.n = 1;
  if ( rcnt > 1 ) {
    uint32_t refcnt = 0, refmax = 0;
    for ( i = 0; i < rcnt; i++ ) {
      refmax |= routes[ i ].refcnt;
      refcnt += routes[ i ].refcnt;
    }
    if ( refmax == 1 ) {
      q_select = q.next_route++ % rcnt;
    }
    else {
      uint32_t nextref = q.next_route++ % refcnt;
      refcnt = 0;
      for ( q_select = 0; q_select < rcnt; q_select++ ) {
        refcnt += routes[ q_select ].refcnt;
        if ( refcnt > nextref )
          break;
      }
    }
  }
  r = routes[ q_select ].r;
  prune_set.min_fd = prune_set.max_fd = r;

  if ( qset.rpd[ 0 ].rcount != 0 ) {
    if ( qset.rpd[ 0 ].is_member( r ) ) {
      RoutePublishData & prune_data = prune_set.rpd[ 0 ];
      prune_data.prefix = qset.rpd[ 0 ].prefix;
      prune_data.hash   = qset.rpd[ 0 ].hash;
      prune_data.routes = &prune_set.min_fd;
      prune_data.rcount = 1;
    }
    else {
      qset.rpd[ 0 ].rcount = 0;
    }
  }
  if ( qset.rpd_mask != 0 ) {
    prune_set.rpd_mask = qset.rpd_mask;
    for ( bool b = bi.first( pref ); b; b = bi.next( pref ) ) {
      RouteQueueData &data = qset.rpd[ pref + 1 ];
      if ( data.is_member( r ) ) {
        RoutePublishData & prune_data = prune_set.rpd[ pref + 1 ];
        prune_data.prefix = data.prefix;
        prune_data.hash   = data.hash;
        prune_data.routes = &prune_set.min_fd;
        prune_data.rcount = 1;
      }
      else {
        prune_set.rpd_mask &= ~( (uint64_t) 1 << pref );
      }
    }
  }
}

template<class Forward>
static bool
forward_message( EvPublish &pub,  RoutePublish &sub_route,
                 Forward &fwd,  BPData *data ) noexcept
{
  RoutePublishContext ctx( sub_route, pub );
  RoutePublishSet   & set = ctx.set;
  uint32_t n = set.n;
  bool b;
  if ( n == 0 )
    return true;
  if ( n == 1 ) {
    if ( data != NULL ) {
      b = test_back_pressure_one( *data, sub_route.poll, set.rpd[ 0 ] );
      if ( b && ! data->bp_fwd() )
        return false;
    }
    return publish_one<Forward>( pub, set.rpd[ 0 ], fwd );
  }
  uint32_t min_fd = set.min_fd,
           range  = ( set.max_fd - min_fd ) + 1;
  if ( range < 64 ) {
    PubFanout64 m( set.rpd, n, min_fd );
    if ( data != NULL ) {
      b = test_back_pressure_multi( *data, sub_route.poll, m );
      if ( b && ! data->bp_fwd() )
        return false;
    }
    return publish_multi<Forward, PubFanout64>( pub, set.rpd, n, fwd, m );
  }
  if ( range < 512 ) {
    PubFanout512 m( set.rpd, n, min_fd );
    if ( data != NULL ) {
      b = test_back_pressure_multi( *data, sub_route.poll, m );
      if ( b && ! data->bp_fwd() )
        return false;
    }
    return publish_multi<Forward, PubFanout512>( pub, set.rpd, n, fwd, m );
  }
  else {
    FDFrame frame( sub_route.poll.fd_stk );
    PubFanoutN m( set.rpd, n, min_fd, range, frame.fdset );
    if ( data != NULL ) {
      b = test_back_pressure_multi( *data, sub_route.poll, m );
      if ( b && ! data->bp_fwd() )
        return false;
    }
    return publish_multi<Forward, PubFanoutN>( pub, set.rpd, n, fwd, m );
  }
}

/* match subject against route db and forward msg to fds subscribed, route
 * db contains both exact matches and wildcard prefix matches */
bool
RoutePublish::forward_msg( EvPublish &pub,  BPData *data ) noexcept
{
  ForwardAll fwd( *this );
  return forward_message<ForwardAll>( pub, *this, fwd, data );
}
bool
RoutePublish::forward_with_cnt( EvPublish &pub,  uint32_t &rcnt,
                                BPData *data ) noexcept
{
  ForwardAll fwd( *this );
  bool b = forward_message<ForwardAll>( pub, *this, fwd, data );
  rcnt = fwd.total;
  return b;
}
bool
RoutePublish::forward_except( EvPublish &pub,  const BitSpace &fdexcept,
                              BPData *data ) noexcept
{
  ForwardExcept fwd( *this, fdexcept );
  return forward_message<ForwardExcept>( pub, *this, fwd, data );
}
bool
RoutePublish::forward_except_with_cnt( EvPublish &pub, const BitSpace &fdexcept,
                                       uint32_t &rcnt,  BPData *data ) noexcept
{
  ForwardExcept fwd( *this, fdexcept );
  bool b = forward_message<ForwardExcept>( pub, *this, fwd, data );
  rcnt = fwd.total;
  return b;
}
bool
RoutePublish::forward_set( EvPublish &pub, const BitSpace &fdset,
                           BPData *data ) noexcept
{
  ForwardSet fwd( *this, fdset );
  return forward_message<ForwardSet>( pub, *this, fwd, data );
}
bool
RoutePublish::forward_set_with_cnt( EvPublish &pub, const BitSpace &fdset,
                                    uint32_t &rcnt,  BPData *data ) noexcept
{
  ForwardSet fwd( *this, fdset );
  bool b = forward_message<ForwardSet>( pub, *this, fwd, data );
  rcnt = fwd.total;
  return b;
}
/* publish to some destination */
bool
RoutePublish::forward_some( EvPublish &pub,  uint32_t *routes,
                            uint32_t rcnt,  BPData *data ) noexcept
{
  ForwardSome fwd_some( *this, routes, rcnt );
  return forward_message<ForwardSome>( pub, *this, fwd_some, data );
}
/* publish except to fd */
bool
RoutePublish::forward_not_fd( EvPublish &pub,  uint32_t not_fd,
                              BPData *data ) noexcept
{
  ForwardNotFd fwd_not( *this, not_fd );
  return forward_message<ForwardNotFd>( pub, *this, fwd_not, data );
}
/* publish except to fd */
bool
RoutePublish::forward_not_fd2( EvPublish &pub,  uint32_t not_fd,
                               uint32_t not_fd2,  BPData *data ) noexcept
{
  ForwardNotFd2 fwd_not2( *this, not_fd, not_fd2 );
  return forward_message<ForwardNotFd2>( pub, *this, fwd_not2, data );
}
#if 0
/* publish to destinations, no route matching */
bool
RoutePublish::forward_all( EvPublish &pub,  uint32_t *routes,
                           uint32_t rcnt ) noexcept
{
  bool    flow_good = true;
  uint8_t prefix    = SUB_RTE;

  pub.hash       = &pub.subj_hash;
  pub.prefix     = &prefix;
  pub.prefix_cnt = 1;

  for ( uint32_t i = 0; i < rcnt; i++ ) {
    EvSocket * s;
    uint32_t   fd = routes[ i ];
    if ( fd <= this->poll.maxfd && (s = this->poll.sock[ fd ]) != NULL ) {
      flow_good &= s->on_msg( pub );
    }
  }
  return flow_good;
}
#endif
bool
RoutePublish::forward_set_no_route( EvPublish &pub,
                                    const BitSpace &fdset ) noexcept
{
  uint32_t cnt       = 0;
  bool     flow_good = true;
  uint8_t  prefix    = SUB_RTE;
  EvPubTmp tmp_hash( pub, &pub.subj_hash, &prefix, 1 );

  for ( size_t i = 0; i < fdset.size; i++ ) {
    uint32_t fd   = (uint32_t) ( i * fdset.WORD_BITS );
    uint64_t word = fdset.ptr[ i ];
    for ( ; word != 0; fd++ ) {
      if ( ( word & 1 ) != 0 ) {
        EvSocket * s;
        if ( fd > this->poll.maxfd )
          break;
        if ( (s = this->poll.sock[ fd ]) != NULL ) {
          flow_good &= s->on_msg( pub );
          if ( kv_pub_debug ) {
            printf( "fwd_set %u\n", fd );
            cnt++;
          }
        }
      }
      word >>= 1;
    }
  }
  if ( kv_pub_debug && cnt == 0 )
    printf( "fwd_set empty\n" );

  return flow_good;
}

bool
RoutePublish::forward_set_no_route_not_fd( EvPublish &pub,  const BitSpace &fdset,
                                           uint32_t not_fd ) noexcept
{
  uint32_t cnt       = 0;
  bool     flow_good = true;
  uint8_t  prefix    = SUB_RTE;
  EvPubTmp tmp_hash( pub, &pub.subj_hash, &prefix, 1 );

  for ( size_t i = 0; i < fdset.size; i++ ) {
    uint32_t fd   = (uint32_t) ( i * fdset.WORD_BITS );
    uint64_t word = fdset.ptr[ i ];
    for ( ; word != 0; fd++ ) {
      if ( ( word & 1 ) != 0 && fd != not_fd ) {
        EvSocket * s;
        if ( fd > this->poll.maxfd )
          break;
        if ( (s = this->poll.sock[ fd ]) != NULL ) {
          flow_good &= s->on_msg( pub );
          if ( kv_pub_debug ) {
            printf( "fwd_not_%u %u\n", not_fd, fd );
            cnt++;
          }
        }
      }
      word >>= 1;
    }
  }
  if ( kv_pub_debug && cnt == 0 )
    printf( "fwd_not_%u empty\n", not_fd );
  return flow_good;
}

/* publish to one destination */
bool
RoutePublish::forward_to( EvPublish &pub,  uint32_t fd,  BPData *data ) noexcept
{
  RouteLookup look( pub.subject, pub.subject_len, pub.subj_hash, 0 );
  uint32_t    n = 0;
  uint32_t    phash[ MAX_RTE ];
  uint8_t     prefix[ MAX_RTE ];

  if ( this->is_sub_member( pub.subj_hash, fd ) ) {
    phash[ 0 ]  = pub.subj_hash;
    prefix[ 0 ] = SUB_RTE;
    n++;
  }
  look.setup_prefix_hash( this->pat_mask() );
  /* the rest of the prefixes are hashed */
  for ( uint32_t k = 0; k < look.prefix_cnt; k++ ) {
    if ( this->is_member( look.keylen[ k ], look.hash[ k ], fd ) ) {
      phash[ n ]  = look.hash[ k ];
      prefix[ n ] = (uint8_t) look.keylen[ k ];
      n++;
    }
  }
  EvPubTmp tmp_hash( pub, phash, prefix, n );

  EvSocket * s;
  bool       b = true;
  if ( fd <= this->poll.maxfd && (s = this->poll.sock[ fd ]) != NULL ) {
    if ( data != NULL ) {
      if ( data->has_back_pressure( this->poll, fd ) && ! data->bp_fwd() )
        return false;
    }
    b = s->on_msg( pub );
    if ( kv_pub_debug ) {
      printf( "fwd_to_%u ok\n", fd );
    }
  }
  else if ( kv_pub_debug ) {
    printf( "fwd_to_%u empty\n", fd );
  }
  return b;
}

/* convert a hash into a subject string, this may have collisions */
bool
RoutePublish::hash_to_sub( uint32_t r,  uint32_t h,  char *key,
                           size_t &keylen ) noexcept
{
  EvSocket *s;
  bool b = false;
  if ( r <= this->poll.maxfd && (s = this->poll.sock[ r ]) != NULL )
    b = s->hash_to_sub( h, key, keylen );
  return b;
}
#if 0
void
RoutePublish::resolve_collisions( NotifySub &sub,  RouteRef &rte ) noexcept
{
  for ( uint32_t i = 0; i < rte.rcnt; i++ ) {
    uint32_t r = rte.routes[ i ];
    if ( r != sub.src_fd && r <= this->poll.maxfd ) {
      EvSocket *s;
      if ( (s = this->poll.sock[ r ]) != NULL ) {
        uint8_t v = s->is_subscribed( sub );
        if ( ( v & EV_COLLISION ) != 0  )
          sub.hash_collision = 1;
        if ( ( v & EV_NOT_SUBSCRIBED ) != 0 )
          sub.sub_count--;
      }
    }
  }
}

void
RoutePublish::resolve_pcollisions( NotifyPattern &pat,  RouteRef &rte ) noexcept
{
  for ( uint32_t i = 0; i < rte.rcnt; i++ ) {
    uint32_t r = rte.routes[ i ];
    if ( r != pat.src_fd && r <= this->poll.maxfd ) {
      EvSocket *s;
      if ( (s = this->poll.sock[ r ]) != NULL ) {
        uint8_t v = s->is_psubscribed( pat );
        if ( ( v & EV_COLLISION ) != 0  )
          pat.hash_collision = 1;
        if ( ( v & EV_NOT_SUBSCRIBED ) != 0 )
          pat.sub_count--;
      }
    }
  }
}
#endif
#if 0
/* modify keyspace route */
void
RedisKeyspaceNotify::update_keyspace_route( uint32_t &val,  uint16_t bit,
                                            int add,  uint32_t fd ) noexcept
{
  RouteRef rte( this->sub_route, this->sub_route.zip.route_spc[ 0 ] );
  uint32_t rcnt = 0, xcnt;
  uint16_t & key_flags = this->sub_route.key_flags;

  /* if bit is set, then val has routes */
  if ( ( key_flags & bit ) != 0 )
    rcnt = rte.decompress( val, add > 0 );
  /* if unsub or sub */
  if ( add < 0 )
    xcnt = rte.remove( fd );
  else
    xcnt = rte.insert( fd );
  /* if route changed */
  if ( xcnt != rcnt ) {
    /* if route deleted */
    if ( xcnt == 0 ) {
      val = 0;
      key_flags &= ~bit;
    }
    /* otherwise added */
    else {
      key_flags |= bit;
      val = rte.compress();
    }
    rte.deref();
  }
}
/* track number of subscribes to keyspace subjects to enable them */
void
RedisKeyspaceNotify::update_keyspace_count( const char *sub,  size_t len,
                                            int add,  uint32_t fd ) noexcept
{
  /* keyspace subjects are special, since subscribing to them can create
   * some overhead */
  static const char kspc[] = "__keyspace@",
                    kevt[] = "__keyevent@",
                    lblk[] = "__listblkd@",
                    zblk[] = "__zsetblkd@",
                    sblk[] = "__strmblkd@",
                    moni[] = "__monitor_@";
  if ( ::memcmp( kspc, sub, len ) == 0 ) /* len <= 11, could match multiple */
    this->update_keyspace_route( this->keyspace, EKF_KEYSPACE_FWD, add, fd );
  if ( ::memcmp( kevt, sub, len ) == 0 )
    this->update_keyspace_route( this->keyevent, EKF_KEYEVENT_FWD, add, fd );
  if ( ::memcmp( lblk, sub, len ) == 0 )
    this->update_keyspace_route( this->listblkd, EKF_LISTBLKD_NOT, add, fd );
  if ( ::memcmp( zblk, sub, len ) == 0 )
    this->update_keyspace_route( this->zsetblkd, EKF_ZSETBLKD_NOT, add, fd );
  if ( ::memcmp( sblk, sub, len ) == 0 )
    this->update_keyspace_route( this->strmblkd, EKF_STRMBLKD_NOT, add, fd );
  if ( ::memcmp( moni, sub, len ) == 0 )
    this->update_keyspace_route( this->monitor , EKF_MONITOR     , add, fd );

  /*printf( "%.*s %d key_flags %x\n", (int) len, sub, add, this->key_flags );*/
}
/* client subscribe, notify to kv pubsub */
void
RedisKeyspaceNotify::on_sub( NotifySub &sub ) noexcept
{
  if ( sub.subject_len > 11 )
    this->update_keyspace_count( sub.subject, 11, 1, sub.src_fd );
}

void
RedisKeyspaceNotify::on_unsub( NotifySub &sub ) noexcept
{
  if ( sub.subject_len > 11 )
    this->update_keyspace_count( sub.subject, 11, -1, sub.src_fd );
}
/* client pattern subscribe, notify to kv pubsub */
void
RedisKeyspaceNotify::on_psub( NotifyPattern &pat ) noexcept
{
  size_t pre_len = ( pat.cvt.prefixlen < 11 ? pat.cvt.prefixlen : 11 );
  this->update_keyspace_count( pat.pattern, pre_len, 1, pat.src_fd );
}

void
RedisKeyspaceNotify::on_punsub( NotifyPattern &pat ) noexcept
{
  size_t pre_len = ( pat.cvt.prefixlen < 11 ? pat.cvt.prefixlen : 11 );
  this->update_keyspace_count( pat.pattern, pre_len, -1, pat.src_fd );
}

void
RedisKeyspaceNotify::on_reassert( uint32_t ,  RouteVec<RouteSub> &,
                                  RouteVec<RouteSub> & ) noexcept
{
}
#endif
void
RoutePublish::notify_reassert( uint32_t fd,  SubRouteDB &sub_db,
                               SubRouteDB &pat_db ) noexcept
{
  for ( RouteNotify *p = this->notify_list.hd; p != NULL; p = p->next ) {
    p->on_reassert( fd, sub_db, pat_db );
  }
}
/* defined for vtable to avoid pure virtuals */
void
RouteNotify::on_sub( NotifySub &sub ) noexcept
{
  printf( "on_sub( sub=%.*s, rep=%.*s, h=0x%x, fd=%u, cnt=%u, col=%u, %c )\n",
          sub.subject_len, sub.subject, sub.reply_len, sub.reply,
          sub.subj_hash, sub.src.fd, sub.sub_count, sub.hash_collision,
          sub.src_type );
}
void
RouteNotify::on_resub( NotifySub & ) noexcept
{
}
void
RouteNotify::on_unsub( NotifySub &sub ) noexcept
{
  printf( "on_unsub( sub=%.*s, h=0x%x, fd=%u, cnt=%u, col=%u, %c )\n",
          sub.subject_len, sub.subject,
          sub.subj_hash, sub.src.fd, sub.sub_count, sub.hash_collision,
          sub.src_type );
}
void
RouteNotify::on_psub( NotifyPattern &pat ) noexcept
{
  printf( "on_psub( sub=%.*s, rep=%.*s, h=0x%x, fd=%u, cnt=%u, col=%u, %c )\n",
          pat.pattern_len, pat.pattern, pat.reply_len, pat.reply,
          pat.prefix_hash, pat.src.fd, pat.sub_count, pat.hash_collision,
          pat.src_type );
}
void
RouteNotify::on_repsub( NotifyPattern & ) noexcept
{
}
void
RouteNotify::on_punsub( NotifyPattern &pat ) noexcept
{
  printf( "on_punsub( sub=%.*s, h=0x%x, fd=%u, cnt=%u, col=%u, %c )\n",
          pat.pattern_len, pat.pattern,
          pat.prefix_hash, pat.src.fd, pat.sub_count,
          pat.hash_collision, pat.src_type );
}
void
RouteNotify::on_reassert( uint32_t,  SubRouteDB &,  SubRouteDB & ) noexcept
{
}
void
RouteNotify::on_bloom_ref( BloomRef & ) noexcept
{
}
void
RouteNotify::on_bloom_deref( BloomRef & ) noexcept
{
}
void
RouteNotify::on_sub_q( NotifyQueue &sub ) noexcept
{
  printf( "on_sub_q( sub=%.*s, h=0x%x, que=%.*s, qh=0x%x, fd=%u, cnt=%u, col=%u, %c )\n",
          sub.subject_len, sub.subject, sub.subj_hash, sub.queue_len, sub.queue,
          sub.queue_hash, sub.src.fd, sub.sub_count, sub.hash_collision,
          sub.src_type );
}
void
RouteNotify::on_resub_q( NotifyQueue & ) noexcept
{
}
void
RouteNotify::on_unsub_q( NotifyQueue &sub ) noexcept
{
  printf( "on_unsub_q( sub=%.*s, h=0x%x, qh=0x%x, fd=%u, cnt=%u, col=%u, %c )\n",
          sub.subject_len, sub.subject, sub.subj_hash,
          sub.queue_hash, sub.src.fd, sub.sub_count, sub.hash_collision,
          sub.src_type );
}

void
RouteNotify::on_psub_q( NotifyPatternQueue &pat ) noexcept
{
  printf( "on_psub_q( sub=%.*s, rep=%.*s, h=0x%x, fd=%u, cnt=%u, col=%u, %c )\n",
          pat.pattern_len, pat.pattern, pat.reply_len, pat.reply,
          pat.prefix_hash, pat.src.fd, pat.sub_count, pat.hash_collision,
          pat.src_type );
}
void
RouteNotify::on_repsub_q( NotifyPatternQueue & ) noexcept
{
}
void
RouteNotify::on_punsub_q( NotifyPatternQueue &pat ) noexcept
{
  printf( "on_punsub_q( sub=%.*s, h=0x%x, fd=%u, cnt=%u, col=%u, %c )\n",
          pat.pattern_len, pat.pattern,
          pat.prefix_hash, pat.src.fd, pat.sub_count,
          pat.hash_collision, pat.src_type );
}

