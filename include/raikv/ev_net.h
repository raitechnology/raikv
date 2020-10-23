#ifndef __rai_raikv__ev_net_h__
#define __rai_raikv__ev_net_h__

#include <raikv/shm_ht.h>
#include <raikv/prio_queue.h>
#include <raikv/dlinklist.h>
#include <raikv/stream_buf.h>
#include <raikv/route_db.h>

namespace rai {
namespace kv {

struct EvSocket;           /* base class for anything with a fd */
struct EvPrefetchQueue;    /* queue for prefetching key memory */
struct EvPublish;          /* data for publishing, key + msg */
struct EvPoll;             /* manages events with epoll() */
struct KvPubSub;           /* manages pubsub through kv shm */
struct EvTimerQueue;       /* timerfd with heap queue of events */
struct EvTimerEvent;       /* a timer event signal */
struct EvKeyCtx;           /* a key operand, an expr may have multiple keys */

enum EvState {
  EV_READ_HI    = 0, /* listen port accept */
  EV_CLOSE      = 1, /* if close set, do that before write/read */
  EV_WRITE_POLL = 2, /* when in write hi and send blocked, EGAIN */
  EV_WRITE_HI   = 3, /* when send buf full at send_highwater or read pressure */
  EV_READ       = 4, /* use read to fill until no more data or recv_highwater */
  EV_PROCESS    = 5, /* process read buffers */
  EV_PREFETCH   = 6, /* process key prefetch */
  EV_WRITE      = 7, /* write at low priority, suboptimal send of small buf */
  EV_SHUTDOWN   = 8, /* showdown after writes */
  EV_READ_LO    = 9, /* read at low prio, back pressure from full write buf */
  EV_BUSY_POLL  = 10 /* busy poll, loop and keep checking for new data */
};

enum EvSockOpts {
  OPT_REUSEADDR   = 1,  /* set SO_RESUSEADDR true */
  OPT_REUSEPORT   = 2,  /* set SO_REUSEPORT true, multiple services, same port*/
  OPT_TCP_NODELAY = 4,  /* set TCP_NODELAY true */
  OPT_AF_INET     = 8,  /* use ip4 stack */
  OPT_AF_INET6    = 16, /* use ip6 stack */
  OPT_KEEPALIVE   = 32, /* set kSO_KEEPALIVE true */
  OPT_LINGER      = 64, /* set SO_LINGER true, 10 secs */
  OPT_READ_HI     = 128,/* set EV_READ_HI when read event occurs on poll */
  OPT_NO_POLL     = 256,/* do not add fd to poll */
  OPT_NO_CLOSE    = 512,/* do not close fd */

  /* opts inherited from listener, set in EvTcpListen::set_sock_opts()  */
  ALL_TCP_ACCEPT_OPTS = OPT_TCP_NODELAY | OPT_KEEPALIVE | OPT_LINGER,

  DEFAULT_TCP_LISTEN_OPTS  = 128|64|32|16|8|4|2|1,
  DEFAULT_TCP_CONNECT_OPTS = 64|32|16|8|4,
  DEFAULT_UDP_LISTEN_OPTS  = 16|8|2|1,
  DEFAULT_UDP_CONNECT_OPTS = 16|8
};

enum EvSockFlags {
  IN_NO_LIST     = 0, /* no list */
  IN_NO_QUEUE    = 0, /* no queue */
  IN_ACTIVE_LIST = 1, /* in the active list */
  IN_FREE_LIST   = 2, /* in a free list */
  IN_EVENT_QUEUE = 4,
  IN_WRITE_QUEUE = 8
};

struct EvSocket : public PeerData /* fd and address of peer */ {
  EvPoll      & poll;       /* the parent container */
  uint64_t      prio_cnt;   /* timeslice each socket for a slot to run */
  uint32_t      state;      /* bit mask of states, the queues the sock is in */
  uint16_t      sock_opts;  /* sock opt bits above */
  const uint8_t sock_type;  /* listen or connection */
  uint8_t       sock_flags; /* in active list or free list */
  uint64_t      bytes_recv, /* stat counters for bytes and msgs */
                bytes_sent,
                msgs_recv,
                msgs_sent;
  uint8_t       pad[ 8 ];   /* pad out to align 256 bytes */

  EvSocket( EvPoll &p,  const uint8_t t )
    : poll( p ), prio_cnt( 0 ), state( 0 ),  sock_opts( 0 ), sock_type( t ),
      sock_flags( 0 ), bytes_recv( 0 ), bytes_sent( 0 ), msgs_recv( 0 ),
      msgs_sent( 0 ) {}

  /* if socket mem is free */
  bool test_opts( EvSockOpts o ) const {
    return ( this->sock_opts & o ) != 0;
  }
  bool test_flags( EvSockFlags f,  uint16_t test ) const {
    if ( f == IN_NO_LIST )
      return ( this->sock_flags & test ) == 0;
    return ( this->sock_flags & f ) != 0;
  }
  bool in_list( EvSockFlags l ) const {
    return this->test_flags( l, IN_ACTIVE_LIST | IN_FREE_LIST );
  }
  void set_list( EvSockFlags l ) {
    this->sock_flags &= ~( IN_ACTIVE_LIST | IN_FREE_LIST );
    this->sock_flags |= l;
  }

  /* if in event queue  */
  bool in_queue( EvSockFlags q ) const {
    return this->test_flags( q, IN_EVENT_QUEUE | IN_WRITE_QUEUE );
  }
  void set_queue( EvSockFlags q ) {
    this->sock_flags &= ~( IN_EVENT_QUEUE | IN_WRITE_QUEUE );
    this->sock_flags |= q;
  }
  static const char * state_string( EvState state ) noexcept;

  /* priority queue states */
  EvState get_dispatch_state( void ) const {
    return (EvState) ( __builtin_ffs( this->state ) - 1 );
  }
  /* priority queue test, ordered by first bit set (EV_WRITE > EV_READ).
   * a sock with EV_READ bit set will have a higher priority than one with
   * EV_WRITE */
  int test( int s ) const { return this->state & ( 1U << s ); }
  void push( int s )      { this->state |= ( 1U << s ); }
  void pop( int s )       { this->state &= ~( 1U << s ); }
  void pop2( int s, int t ) {
    this->state &= ~( ( 1U << s ) | ( 1U << t ) ); }
  void pop3( int s, int t, int u ) {
    this->state &= ~( ( 1U << s ) | ( 1U << t ) | ( 1U << u ) ); }
  void popall( void )     { this->state = 0; }
  void pushpop( int s, int t ) {
    this->state = ( this->state | ( 1U << s ) ) & ~( 1U << t ); }
  void idle_push( EvState s ) noexcept;

  /* The dispatch functions that Poll uses to process EvState state */
  /* the sock type name */
  virtual const char *type_string( void ) noexcept;
  /* doesn't do anything yet, maybe add a debug state*/
  virtual void dbg( const char *where ) noexcept;
  /* send protocol data: EV_WRITE, EV_WRITE_HI, EV_WRITE_POLL */
  virtual void write( void ) noexcept = 0;
  /* read protocol data: EV_READ, EV_READ_LO, EV_READ_HI */
  virtual void read( void ) noexcept = 0;
  /* a process state is set by the protocol, after reads: EV_PROCESS */
  virtual void process( void ) noexcept = 0;
  /* for busy wait looping, called repeatedly if no other events, EV_BUSY_POLL*/
  virtual bool busy_poll( void ) noexcept; /* do read */
  /* after close and remove from poll set, then release is called, free bufs */
  virtual void release( void ) noexcept = 0;
  /* a timer fired: { fd, timer_id, event_id }, return true if should rearm */
  virtual bool timer_expire( uint64_t timer_id, uint64_t event_id ) noexcept;
  /* find a subject for hash, no collision resolution */
  virtual bool hash_to_sub( uint32_t h, char *k, size_t &klen ) noexcept;
  /* deliver published message */
  virtual bool on_msg( EvPublish &pub ) noexcept;
  /* prefetch a key while processing other keys with continue: EV_PREFETCH */
  virtual void key_prefetch( EvKeyCtx &ctx ) noexcept;
  /* after key prefetch, continue operation */
  virtual int  key_continue( EvKeyCtx &ctx ) noexcept;
  /* shutdown, prepare for close: EV_SHUTDOWN */
  virtual void process_shutdown( void ) noexcept;
  /* complete close, called after release, use to log or printf: EV_CLOSE */
  virtual void process_close( void ) noexcept;

  /* PeerData */
  /* sprint socket info to buf */
  virtual int client_list( char *buf,  size_t buflen ) noexcept;
  /* put socket into close mode, return true if it is killable */
  virtual bool client_kill( void ) noexcept;
  /* match socket by args, return true if matched */
  virtual bool match( PeerMatchArgs &ka ) noexcept;
  /* accumulate i/o stats */
  virtual void client_stats( PeerStats &ps ) noexcept;
  /* fetch stats from closed sockets in poll */
  virtual void retired_stats( PeerStats &ps ) noexcept;
  /* return true if peer data + args + string classes match */
  static bool client_match( PeerData &pd,  PeerMatchArgs *ka,  ... ) noexcept;
};

#if __cplusplus >= 201103L
  /* 64b align */
  static_assert( 256 == sizeof( EvSocket ), "socket size" );
#endif

static inline void *aligned_malloc( size_t sz ) {
#ifdef _ISOC11_SOURCE
  return ::aligned_alloc( sizeof( kv::BufAlign64 ), sz ); /* >= RH7 */
#else
  return ::memalign( sizeof( kv::BufAlign64 ), sz ); /* RH5, RH6.. */
#endif
}

/* route_db.h has RoutePublish which contains the function for publishing -
 *   bool publish( pub, rcount, pref_cnt, ph )
 *   publishers may not need to see EvPoll, only RoutePublish, that is why it
 *   is a sepearate structure */
struct EvPoll : public RoutePublish {
  static bool is_event_greater( EvSocket *s1,  EvSocket *s2 ) {
    int x1 = __builtin_ffs( s1->state ),
        x2 = __builtin_ffs( s2->state );
    /* prio_cnt is incremented forever, it makes queue fair */
    return x1 > x2 || ( x1 == x2 && s1->prio_cnt > s2->prio_cnt );
  }
  static bool is_active_older( EvSocket *s1,  EvSocket *s2 ) {
    /* active_ns is set when epoll says sock is read ready */
    return s1->PeerData::active_ns < s2->PeerData::active_ns;
  }
  /* order by event priority */
  kv::PrioQueue<EvSocket *, EvPoll::is_event_greater> ev_queue;
  /* order by last active time */
  kv::PrioQueue<EvSocket *, EvPoll::is_active_older>  ev_write;

  void push_event_queue( EvSocket *s ) {
    if ( s->in_queue( IN_NO_QUEUE ) ) {
      s->set_queue( IN_EVENT_QUEUE );
      this->ev_queue.push( s );
    }
  }
  /* sock is blocked on a write, add to epoll set and write queue, if
   * client is idle for a long time and sock is in write queue, it may
   * need to be closed to free buffer space */
  void push_write_queue( EvSocket *s ) {
    if ( s->in_queue( IN_NO_QUEUE ) ) {
      s->set_queue( IN_WRITE_QUEUE );
      this->ev_write.push( s );
    }
  }
  void remove_write_queue( EvSocket *s ) {
    if ( s->in_queue( IN_WRITE_QUEUE ) ) {
      s->set_queue( IN_NO_QUEUE );
      this->ev_write.remove( s );
    }
  }

  EvSocket          ** sock,            /* sock array indexed by fd */
                    ** wr_poll;         /* write queue waiting for epoll */
  struct epoll_event * ev;              /* event array used by epoll() */
  kv::HashTab        * map;             /* the data store */
  EvPrefetchQueue    * prefetch_queue;  /* ordering keys */
  KvPubSub           * pubsub;          /* cross process pubsub */
  EvTimerQueue       * timer_queue;     /* timer events */
  uint64_t             prio_tick,       /* priority queue ticker */
                       wr_timeout_ns,   /* timeout for writes in EV_WRITE_POLL */
                       so_keepalive_ns, /* keep alive ping timeout */
                       next_id;         /* unique id for connection */
  uint32_t             ctx_id,          /* this thread context */
                       dbx_id,          /* the db context */
                       fdcnt,           /* num fds in poll set */
                       wr_count,        /* num fds in wr_poll[] */
                       maxfd,           /* current maximum fd number */
                       nfds;            /* max epoll() fds, array sz this->ev */
  int                  efd,             /* epoll fd */
                       null_fd,         /* /dev/null fd for null sockets */
                       quit;            /* when > 0, wants to exit */
  static const size_t  ALLOC_INCR    = 64, /* alloc size of poll socket ar */
                       PREFETCH_SIZE = 8;  /* pipe size of number of pref */
  size_t               prefetch_pending;   /* count of elems in prefetch queue*/
                   /*, prefetch_cnt[ PREFETCH_SIZE + 1 ]*/
  RouteDB              sub_route;       /* subscriptions */
  RoutePublishQueue    pub_queue;       /* temp routing queue: */
  PeerStats            peer_stats;      /* accumulator after sock closes */
     /* this causes a message matching multiple wildcards to be sent once */

  /* socket lists, active and free lists, multiple socks are allocated at a
   * time to speed up accept and connection setup */
  kv::DLinkList<EvSocket> active_list;    /* active socks in poll */
  kv::DLinkList<EvSocket> free_list[ 256 ];     /* socks for accept */
  const char            * sock_type_str[ 256 ]; /* name of sock_type */

  /*bool single_thread; (if kv single threaded) */
  /* alloc ALLOC_INCR(64) elems of the above list elems at a time, aligned 64 */
  template<class T>
  T *get_free_list( const uint8_t sock_type ) {
    T *c = (T *) this->free_list[ sock_type ].hd;
    if ( c == NULL ) {
      size_t sz  = kv::align<size_t>( sizeof( T ), 64 );
      void * m   = aligned_malloc( sz * EvPoll::ALLOC_INCR );
      char * end = &((char *) m)[ sz * EvPoll::ALLOC_INCR ];
      if ( m == NULL )
        return NULL;
      while ( (char *) m < end ) {
        end = &end[ -sz ];
        c = new ( end ) T( *this, sock_type );
        this->push_free_list( c );
      }
    }
    this->pop_free_list( c );
    return c;
  }
  template<class T, class S>
  T *get_free_list2( const uint8_t sock_type,  S &stats ) {
    T *c = (T *) this->free_list[ sock_type ].hd;
    if ( c == NULL ) {
      size_t sz  = kv::align<size_t>( sizeof( T ), 64 );
      void * m   = aligned_malloc( sz * EvPoll::ALLOC_INCR );
      char * end = &((char *) m)[ sz * EvPoll::ALLOC_INCR ];
      if ( m == NULL )
        return NULL;
      while ( (char *) m < end ) {
        end = &end[ -sz ];
        c = new ( end ) T( *this, sock_type, stats );
        this->push_free_list( c );
      }
    }
    this->pop_free_list( c );
    return c;
  }
  void push_free_list( EvSocket *s ) {
    if ( ! s->in_list( IN_FREE_LIST ) ) {
      s->set_list( IN_FREE_LIST );
      this->free_list[ s->sock_type ].push_hd( s );
    }
  }
  void pop_free_list( EvSocket *s ) {
    if ( s->in_list( IN_FREE_LIST ) ) {
      s->set_list( IN_NO_LIST );
      this->free_list[ s->sock_type ].pop( s );
    }
  }
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  /* 16 seconds */
  static const uint64_t DEFAULT_NS_TIMEOUT = (uint64_t) 16 * 1000 * 1000 * 1000;

  EvPoll()
    : sock( 0 ), wr_poll( 0 ), ev( 0 ), map( 0 ), prefetch_queue( 0 ),
      pubsub( 0 ), timer_queue( 0 ), prio_tick( 0 ),
      wr_timeout_ns( DEFAULT_NS_TIMEOUT ), so_keepalive_ns( DEFAULT_NS_TIMEOUT ),
      next_id( 0 ), ctx_id( kv::MAX_CTX_ID ), dbx_id( kv::MAX_STAT_ID ),
      fdcnt( 0 ), wr_count( 0 ), maxfd( 0 ), nfds( 0 ), efd( -1 ),
      null_fd( -1 ), quit( 0 ), prefetch_pending( 0 ),
      sub_route( *this ) /*, single_thread( false )*/ {
    ::memset( this->sock_type_str, 0, sizeof( this->sock_type_str ) );
  }
  /* return false if duplicate type */
  uint8_t register_type( const char *s ) noexcept;
  /* initialize epoll */
  int init( int numfds,  bool prefetch/*,  bool single*/ ) noexcept;
  /* initialize kv */
  int init_shm( EvShm &shm ) noexcept;    /* open shm pubsub */
  /* add a pattern route for hash */
  void add_pattern_route( const char *sub,  size_t prefix_len,  uint32_t hash,
                          uint32_t fd ) noexcept;
  /* remove a pattern route for hash */
  void del_pattern_route( const char *sub,  size_t prefix_len,  uint32_t hash,
                          uint32_t fd ) noexcept;
  /* add a subscription route for hash */
  void add_route( const char *sub,  size_t sub_len,  uint32_t hash,
                  uint32_t fd ) noexcept;
  /* remove a subscription route for hash */
  void del_route( const char *sub,  size_t sub_len,  uint32_t hash,
                  uint32_t fd ) noexcept;
  int wait( int ms ) noexcept;            /* call epoll() with ms timeout */

  void idle_close( EvSocket *s,  uint64_t ns ) noexcept;

  enum { /* dispatch return bits */
    DISPATCH_IDLE  = 0, /* no events dispatched */
    POLL_NEEDED    = 1, /* a timer is about to expire, timer_queue fd */
    DISPATCH_BUSY  = 2, /* some event occured */
    BUSY_POLL      = 4, /* something is busy looping, like kv pubsub */
    WRITE_PRESSURE = 8  /* something in a write hi state, retry write again */
  };
  int dispatch( void ) noexcept;          /* process any sock in the queues */
  void drain_prefetch( void ) noexcept;   /* process prefetches */

  /* thse publish are used by RoutePublish::forward_msg() */
  bool publish_one( EvPublish &pub,  uint32_t *rcount_total,
                    RoutePublishData &rpd ) noexcept; /* publish to one dest */
  template<uint8_t N>
  bool publish_multi( EvPublish &pub,  uint32_t *rcount_total,
                      RoutePublishData *rpd ) noexcept; /* a couple of dest */
  /* publish to more than multi destinations, uses a heap to sort dests */
  bool publish_queue( EvPublish &pub,  uint32_t *rcount_total ) noexcept;
  uint64_t current_coarse_ns( void ) const noexcept; /* current time from kv*/
  int get_null_fd( void ) noexcept;         /* return dup /dev/null fd */
  int add_sock( EvSocket *s ) noexcept;     /* add to poll set */
  void remove_sock( EvSocket *s ) noexcept; /* remove from poll set */
  bool timer_expire( EvTimerEvent &ev ) noexcept; /* process timer event fired */
  void process_quit( void ) noexcept;       /* quit state close socks */
};

struct EvListen : public EvSocket {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  uint64_t      accept_cnt, /* how many accept() calls */
                timer_id;   /* a unique id for each socket accepted */
  const uint8_t accept_sock_type; /* what kind of sock is accepted */

  EvListen( EvPoll &p,  const char *lname,  const char *name );

  virtual bool accept( void ) noexcept = 0;
  virtual void write( void ) noexcept;
  virtual void read( void ) noexcept; /* do accept */
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
  /* PeerData */
  virtual int client_list( char *buf,  size_t buflen ) noexcept;
  virtual bool match( PeerMatchArgs &ka ) noexcept;
  virtual void client_stats( PeerStats &ps ) noexcept;
};

struct EvConnection : public EvSocket, public StreamBuf {
  static const size_t RCV_BUFSIZE = 16 * 1024;
  char   * recv;           /* initially recv_buf, but may realloc */
  uint32_t off,            /* offset of recv_buf consumed */
           len,            /* length of data in recv_buf */
           recv_size,      /* recv buf size */
           recv_highwater, /* recv_highwater: switch to low priority read */
           send_highwater, /* send_highwater: switch to high priority write */
           pad;
  char     recv_buf[ RCV_BUFSIZE ] __attribute__((__aligned__( 64 )));

  EvConnection( EvPoll &p, const uint8_t t ) : EvSocket( p, t ) {
    this->recv           = this->recv_buf;
    this->off            = 0;
    this->len            = 0;
    this->recv_size      = sizeof( this->recv_buf );
    this->recv_highwater = RCV_BUFSIZE - RCV_BUFSIZE / 32;
    this->send_highwater = StreamBuf::SND_BUFSIZE - StreamBuf::SND_BUFSIZE / 32;
    this->pad            = 0xaa99bb88U;
  }
  void release_buffers( void ) { /* release all buffs */
    this->clear_buffers();
    this->StreamBuf::release();
  }
  void clear_buffers( void ) {   /* clear any allocations and counters */
    this->StreamBuf::reset();
    this->off = this->len = 0;
    if ( this->recv != this->recv_buf ) {
      ::free( this->recv );
      this->recv = this->recv_buf;
      this->recv_size = sizeof( this->recv_buf );
      this->recv_highwater = this->recv_size - this->recv_size / 8;
      this->send_highwater = this->recv_size * 2;
    }
  }
  void adjust_recv( void ) {     /* data is read at this->recv[ this->len ] */
    if ( this->off > 0 ) {
      this->len -= this->off;
      if ( this->len > 0 )
        ::memmove( this->recv, &this->recv[ this->off ], this->len );
      else if ( this->recv != this->recv_buf ) {
        ::free( this->recv );
        this->recv = this->recv_buf;
        this->recv_size = sizeof( this->recv_buf );
      }
      this->off = 0;
    }
  }
  bool resize_recv_buf( void ) noexcept;   /* need more buffer space */

  /* read/write to socket */
  virtual void read( void ) noexcept;      /* fill recv buf */
  virtual void write( void ) noexcept;     /* flush stream buffer */

  bool push_write( void ) {
    size_t buflen = this->StreamBuf::pending();
    if ( buflen > 0 ) {
      this->push( EV_WRITE );
      if ( buflen > this->send_highwater )
        this->pushpop( EV_WRITE_HI, EV_WRITE );
      return true;
    }
    return false;
  }
  void close_alloc_error( void ) noexcept; /* if stream buf alloc failed */
  /* PeerData */
  virtual int client_list( char *buf,  size_t buflen ) noexcept;
  virtual bool match( PeerMatchArgs &ka ) noexcept;
};

struct EvUdp : public EvSocket, public StreamBuf {
  struct    mmsghdr * in_mhdr,
                    * out_mhdr;
  uint32_t  in_moff,   /* offset from 0 -> in_nmsgs */
            in_nmsgs,  /* number of msgs recvd */
            in_size,   /* array size of in_mhdr[] */
            in_nsize,  /* new array size, ajusted based on activity */
            out_nmsgs;

  EvUdp( EvPoll &p, const uint8_t t ) : EvSocket( p, t ),
    in_mhdr( 0 ), out_mhdr( 0 ), in_moff( 0 ), in_nmsgs( 0 ), in_size( 0 ),
    in_nsize( 1 ), out_nmsgs( 0 ) {}
  void zero( void ) {
    this->in_mhdr = this->out_mhdr = NULL;
    this->in_moff = this->in_nmsgs = 0;
    this->out_nmsgs = this->in_size = 0;
  }
  bool alloc_mmsg( void ) noexcept;
  ssize_t discard_pkt( void ) noexcept;
  int listen( const char *ip,  int port,  int opts,  const char *k ) noexcept;
  int connect( const char *ip,  int port,  int opts ) noexcept;

  void release_buffers( void ) { /* release all buffs */
    this->clear_buffers();
    this->StreamBuf::release();
  }
  void clear_buffers( void ) {   /* clear any allocations and counters */
    this->zero();
    this->StreamBuf::reset();
  }
  /* read/write packets */
  virtual void read( void ) noexcept;  /* fill recv msgs */
  virtual void write( void ) noexcept; /* flush send msgs */

  bool push_write( void ) {
    size_t buflen = this->StreamBuf::pending();
    if ( buflen > 0 ) {
      this->push( EV_WRITE );
      /*if ( buflen > this->send_highwater )
        this->pushpop( EV_WRITE_HI, EV_WRITE );*/
      return true;
    }
    return false;
  }
  /* PeerData */
  virtual int client_list( char *buf,  size_t buflen ) noexcept;
  virtual bool match( PeerMatchArgs &ka ) noexcept;
};

}
}
#endif
