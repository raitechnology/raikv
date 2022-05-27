#ifndef __rai_raikv__ev_net_h__
#define __rai_raikv__ev_net_h__

#include <raikv/shm_ht.h>
#include <raikv/stream_buf.h>
#include <raikv/route_db.h>
/*#define EV_NET_DBG*/
#include <raikv/ev_dbg.h>

extern "C" {
struct epoll_event;
struct mmsghdr;
}

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
struct NotifySub;          /* notify a subject subscription */
struct NotifyPattern;      /* notify a pattern subscription */

extern uint32_t kv_pub_debug, kv_ps_debug;

enum EvState {       /* state bits: */
  EV_READ_HI    = 0, /*   1, listen port accept */
  EV_CLOSE      = 1, /*   2, if close set, do that before write/read */
  EV_WRITE_POLL = 2, /*   4, when in write hi and send blocked, EGAIN */
  EV_WRITE_HI   = 3, /*   8, when send buf full at send_highwater or rd press */
  EV_READ       = 4, /*  10, use read to fill until recv_highwater */
  EV_PROCESS    = 5, /*  20, process read buffers */
  EV_PREFETCH   = 6, /*  40, process key prefetch */
  EV_WRITE      = 7, /*  80, write at low priority, suboptimal send of sm buf */
  EV_SHUTDOWN   = 8, /* 100, showdown after writes */
  EV_READ_LO    = 9, /* 200, read at low prio, back pressure, full write buf */
  EV_BUSY_POLL  = 10 /* 400, busy poll, loop and keep checking for new data */
};

enum EvSockOpts {
  OPT_REUSEADDR   = 1,  /* set SO_RESUSEADDR true */
  OPT_REUSEPORT   = 2,  /* set SO_REUSEPORT true, multiple services, same port*/
  OPT_TCP_NODELAY = 4,  /* set TCP_NODELAY true */
  OPT_AF_INET     = 8,  /* use ip4 stack */
  OPT_AF_INET6    = 16, /* use ip6 stack */
  OPT_KEEPALIVE   = 32, /* set SO_KEEPALIVE true */
  OPT_LINGER      = 64, /* set SO_LINGER true, 10 secs */
  OPT_READ_HI     = 128,/* set EV_READ_HI when read event occurs on poll */
  OPT_NO_POLL     = 256,/* do not add fd to poll */
  OPT_NO_CLOSE    = 512,/* do not close fd */
  OPT_VERBOSE     = 1024,/* print errors stderr */
  OPT_CONNECT_NB  = 2048, /* non-block connect */

  /* opts inherited from listener, set in EvTcpListen::set_sock_opts()  */
  ALL_TCP_ACCEPT_OPTS = OPT_TCP_NODELAY | OPT_KEEPALIVE | OPT_LINGER,

  DEFAULT_TCP_LISTEN_OPTS   = 1024|128|64|32|16|8|4|2|1,
  DEFAULT_TCP_CONNECT_OPTS  = 1024|64|32|16|8|4,
  DEFAULT_UDP_LISTEN_OPTS   = 1024|16|8|2|1,
  DEFAULT_UDP_CONNECT_OPTS  = 1024|16|8,
  DEFAULT_UNIX_LISTEN_OPTS  = 1024|1,
  DEFAULT_UNIX_CONNECT_OPTS = 1024,
  DEFAULT_UNIX_BIND_OPTS    = 1024|1
};

enum EvSockFlags {
  IN_NO_LIST     = 0, /* no list */
  IN_NO_QUEUE    = 0, /* no queue */
  IN_ACTIVE_LIST = 1, /* in the active list, attached to an fd */
  IN_FREE_LIST   = 2, /* in a free list for reuse, not active socket */
  IN_EVENT_QUEUE = 4, /* in event queue for dispatch of an event */
  IN_WRITE_QUEUE = 8, /* in write queue, stuck in epoll for write */
  IN_EPOLL_READ  = 16,/* in epoll set waiting for read (normal) */
  IN_EPOLL_WRITE = 32,/* in epoll set waiting for write (if blocked) */
  IN_SOCK_MEM    = 64 /* alloced from sock mem */
};

enum EvSockErr {
  EV_ERR_NONE          = 0,
  EV_ERR_CLOSE         = 1, /* fd failed to close */
  EV_ERR_WRITE_TIMEOUT = 2, /* no progress writing */
  EV_ERR_BAD_WRITE     = 3, /* write failed  */
  EV_ERR_WRITE_RESET   = 4, /* write connection reset */
  EV_ERR_BAD_READ      = 5, /* read failed  */
  EV_ERR_READ_RESET    = 6, /* read connection reset */
  EV_ERR_WRITE_POLL    = 7, /* epoll failed to mod write */
  EV_ERR_READ_POLL     = 8, /* epoll failed to mod read */
  EV_ERR_ALLOC         = 9, /* alloc sock data */
  EV_ERR_GETADDRINFO   = 10, /* resolver failed */
  EV_ERR_BIND          = 11, /* bind failed */
  EV_ERR_CONNECT       = 12, /* connect failed */
  EV_ERR_BAD_FD        = 13, /* fd is not valid */
  EV_ERR_SOCKET        = 14, /* failed to create socket */
  EV_ERR_LAST          = 15 /* extend errors after LAST */
};
bool ev_would_block( int err ) noexcept;

enum EvSubState {
  EV_SUBSCRIBED     = 1,
  EV_NOT_SUBSCRIBED = 2,
  EV_COLLISION      = 4
};

enum EvSockBase {
  EV_OTHER_BASE      = 0,
  EV_LISTEN_BASE     = 1,
  EV_CONNECTION_BASE = 2,
  EV_DGRAM_BASE      = 3
};

struct EvSocket : public PeerData /* fd and address of peer */EV_DBG_INHERIT {
  EvPoll      & poll;       /* the parent container */
  uint64_t      prio_cnt;   /* timeslice each socket for a slot to run */
  uint32_t      sock_state; /* bit mask of states, the queues the sock is in */
  uint16_t      sock_opts;  /* sock opt bits above */
  const uint8_t sock_type;  /* listen or connection */
  uint8_t       sock_flags; /* in active list or free list */
  uint16_t      sock_err,   /* error condition */
                sock_errno;
  const uint8_t sock_base;
  uint8_t       sock_pad[ 3 ];
  uint64_t      bytes_recv, /* stat counters for bytes and msgs */
                bytes_sent,
                msgs_recv,
                msgs_sent;

  EvSocket( EvPoll &p,  const uint8_t t,  const uint8_t b = EV_OTHER_BASE )
    : poll( p ), prio_cnt( 0 ), sock_state( 0 ),  sock_opts( 0 ),
      sock_type( t ), sock_flags( 0 ), sock_base( b ) { this->init_stats(); }
  void init_stats( void ) {
    this->sock_err   = 0;
    this->sock_errno = 0;
    this->bytes_recv = 0;
    this->bytes_sent = 0;
    this->msgs_recv  = 0;
    this->msgs_sent  = 0;
  }
  int set_sock_err( uint16_t serr,  uint16_t err ) noexcept;
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
  bool in_sock_mem( void ) const {
    return ( this->sock_flags & IN_SOCK_MEM ) != 0;
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
  void set_poll( EvSockFlags p ) {
    this->sock_flags &= ~( IN_EPOLL_READ | IN_EPOLL_WRITE );
    this->sock_flags |= p;
  }
  bool in_poll( EvSockFlags p ) const {
    return this->test_flags( p, IN_EPOLL_READ | IN_EPOLL_WRITE );
  }
  static const char * state_string( EvState state ) noexcept;
  /* err strings defined as EvSockErr */
  static const char * err_string( EvSockErr err ) noexcept;

  /* priority queue states */
  EvState get_dispatch_state( void ) const {
    return (EvState) ( kv_ffsw( this->sock_state ) - 1 );
  }
  /* priority queue test, ordered by first bit set (EV_WRITE > EV_READ).
   * a sock with EV_READ bit set will have a higher priority than one with
   * EV_WRITE */
  int test( int s ) const { return this->sock_state & ( 1U << s ); }
  void push( int s )      { this->sock_state |= ( 1U << s ); }
  void pop( int s )       { this->sock_state &= ~( 1U << s ); }
  void pop2( int s, int t ) {
    this->sock_state &= ~( ( 1U << s ) | ( 1U << t ) ); }
  void pop3( int s, int t, int u ) {
    this->sock_state &= ~( ( 1U << s ) | ( 1U << t ) | ( 1U << u ) ); }
  void popall( void )     { this->sock_state = 0; }
  void pushpop( int s, int t ) {
    this->sock_state = ( this->sock_state | ( 1U << s ) ) & ~( 1U << t ); }
  void idle_push( EvState s ) noexcept;
  void close_error( uint16_t serr,  uint16_t err ) noexcept;
  /* convert sock_err to string, or null if unknown code */
  virtual const char *sock_error_string( void ) noexcept;
  /* describe sock and error */
  virtual size_t print_sock_error( char *out = NULL,
                                   size_t outlen = 0 ) noexcept;
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
  /* deliver published message */
  virtual bool on_msg( EvPublish &pub ) noexcept;
  /* find a subject for hash, no collision resolution */
  virtual bool hash_to_sub( uint32_t h, char *k, size_t &klen ) noexcept;
  /* test is subject is subscribed, no = 0, yes = 1, collision = 2 */
  virtual uint8_t is_subscribed( const NotifySub &sub ) noexcept;
  /* test is pattern is subscribed, no = 0, prefix sub = 1, pattern sub = 2 */
  virtual uint8_t is_psubscribed( const NotifyPattern &pat ) noexcept;
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
  static_assert( sizeof( EvSocket ) % 256 == 0, "socket size" );
#endif

struct EvPoll {
  static bool is_event_greater( EvSocket *s1,  EvSocket *s2 ) {
    int x1 = kv_ffsw( s1->sock_state ),
        x2 = kv_ffsw( s2->sock_state );
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

  EvSocket           ** sock;            /* sock array indexed by fd */
  struct epoll_event  * ev;              /* event array used by epoll() */
  TimerQueue            timer;           /* timer events */
  EvPrefetchQueue     * prefetch_queue;  /* ordering keys */
  uint64_t              prio_tick,       /* priority queue ticker */
                        wr_timeout_ns,   /* timeout writes in EV_WRITE_POLL */
                        conn_timeout_ns, /* timeout writes in EV_WRITE_POLL */
                        so_keepalive_ns, /* keep alive ping timeout */
                        next_id,         /* unique id for connection */
                        now_ns,          /* updated by current_coarse_ns() */
                        init_ns;         /* when map or poll was created */
  uint32_t              fdcnt,           /* num fds in poll set */
                        wr_count,        /* num fds with write set */
                        maxfd,           /* current maximum fd number */
                        nfds;            /* max epoll() fds, array sz ev[] */
  int                   efd,             /* epoll fd */
                        null_fd,         /* /dev/null fd for null sockets */
                        quit;            /* when > 0, wants to exit */
  static const size_t   ALLOC_INCR    = 16, /* alloc size of poll socket ar */
                        PREFETCH_SIZE = 8;  /* pipe size of number of pref */
  uint32_t              prefetch_pending; /* count of elems in prefetch queue */
  RoutePDB              sub_route;       /* subscriptions */
  RoutePublishQueue     pub_queue;       /* temp routing queue: */
  PeerStats             peer_stats;      /* accumulator after sock closes */
  BloomDB               g_bloom_db;
     /* this causes a message matching multiple wildcards to be sent once */

  /* socket lists, active and free lists, multiple socks are allocated at a
   * time to speed up accept and connection setup */
  kv::DLinkList<EvSocket> active_list;    /* active socks in poll */
  kv::DLinkList<EvSocket> free_list[ 256 ];     /* socks for accept */
  const char            * sock_type_str[ 256 ]; /* name of sock_type */
  void                  * sock_mem;
  size_t                  sock_mem_left;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  /* 16 seconds */
  static const uint64_t DEFAULT_NS_TIMEOUT = (uint64_t) 16 * 1000 * 1000 * 1000,
                        DEFAULT_NS_CONNECT_TIMEOUT =  1000 * 1000 * 1000;

  EvPoll()
    : sock( 0 ), ev( 0 ), prefetch_queue( 0 ), prio_tick( 0 ),
      wr_timeout_ns( DEFAULT_NS_TIMEOUT ),
      conn_timeout_ns( DEFAULT_NS_CONNECT_TIMEOUT ),
      so_keepalive_ns( DEFAULT_NS_TIMEOUT ),
      next_id( 0 ), now_ns( 0 ), init_ns( 0 ), fdcnt( 0 ), wr_count( 0 ),
      maxfd( 0 ), nfds( 0 ), efd( -1 ), null_fd( -1 ), quit( 0 ),
      prefetch_pending( 0 ), sub_route( *this ), sock_mem( 0 ),
      sock_mem_left( 0 ) {
    ::memset( this->sock_type_str, 0, sizeof( this->sock_type_str ) );
  }
  /* alloc ALLOC_INCR(64) elems of the above list elems at a time, aligned 64 */
  template<class T, class... Ts>
  T *get_free_list( const uint8_t sock_type,  Ts... args ) {
    T *c = (T *) this->free_list[ sock_type ].hd;
    if ( c != NULL )
      this->pop_free_list( c );
    void * p = ( c == NULL ? this->alloc_sock( sizeof( T ) ) : (void *) c );
    if ( p == NULL ) return NULL;
    c = new ( p ) T( *this, sock_type, args... );
    c->sock_flags |= IN_SOCK_MEM;
    return c;
  }
  void *alloc_sock( size_t sz ) noexcept;

  void push_free_list( EvSocket *s ) {
    if ( s->sock_type != 0 && ! s->in_list( IN_FREE_LIST ) &&
         s->in_sock_mem() ) {
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
  /* return false if duplicate type */
  uint8_t register_type( const char *s ) noexcept;
  /* initialize epoll */
  int init( int numfds,  bool prefetch ) noexcept;
  /* initialize kv */
  void add_write_poll( EvSocket *s ) noexcept;
  void remove_write_poll( EvSocket *s ) noexcept;
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
  uint64_t current_coarse_ns( void ) noexcept; /* current time from kv */
  uint64_t create_ns( void ) const noexcept; /* create time from kv */
  int get_null_fd( void ) noexcept;         /* return dup /dev/null fd */
  int add_sock( EvSocket *s ) noexcept;     /* add to poll set */
  void remove_sock( EvSocket *s ) noexcept; /* remove from poll set */
  bool timer_expire( EvTimerEvent &ev ) noexcept; /* process timer event fired */
  void process_quit( void ) noexcept;       /* quit state close socks */
};

struct EvConnection;
struct EvConnectionNotify {
  /* notifcation of ready, after authentication, etc */
  virtual void on_connect( EvSocket &conn ) noexcept;
  /* notification of connection close or loss */
  virtual void on_shutdown( EvSocket &conn,  const char *err,
                            size_t elen ) noexcept;
};

struct EvListen : public EvSocket {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  uint64_t      accept_cnt, /* how many accept() calls */
                timer_id;   /* a unique id for each socket accepted */
  EvConnectionNotify * notify;   /* notify after accept */
  const uint8_t accept_sock_type; /* what kind of sock is accepted */

  EvListen( EvPoll &p,  const char *lname,  const char *name );

  virtual EvSocket *accept( void ) noexcept = 0;
  virtual void write( void ) noexcept;
  virtual void read( void ) noexcept; /* do accept */
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
  /* PeerData */
  virtual int client_list( char *buf,  size_t buflen ) noexcept;
  virtual bool match( PeerMatchArgs &ka ) noexcept;
  virtual void client_stats( PeerStats &ps ) noexcept;
  virtual void reset_read_poll( void ) noexcept;
};

struct EvConnection : public EvSocket, public StreamBuf {
  static const size_t RCV_BUFSIZE = 16 * 1024;
  char   * recv;           /* initially recv_buf, but may realloc */
  uint32_t off,            /* offset of recv_buf consumed */
           len,            /* length of data in recv_buf */
           recv_size,      /* recv buf size */
           recv_highwater, /* recv_highwater: switch to low priority read */
           send_highwater; /* send_highwater: switch to high priority write */
  EvConnectionNotify * notify;
#ifndef _MSC_VER
  char     recv_buf[ RCV_BUFSIZE ] __attribute__((__aligned__( 64 )));
#else
  __declspec(align(64)) char recv_buf[ RCV_BUFSIZE ];
#endif
  EvConnection( EvPoll &p, const uint8_t t, EvConnectionNotify *n = NULL )
      : EvSocket( p, t, EV_CONNECTION_BASE ) {
    this->off    = 0;
    this->len    = 0;
    this->notify = n;
    this->reset_recv();
  }
  void reset_recv( void ) {
    this->recv           = this->recv_buf;
    this->recv_size      = sizeof( this->recv_buf );
    this->recv_highwater = RCV_BUFSIZE - RCV_BUFSIZE / 32;
    this->send_highwater = StreamBuf::SND_BUFSIZE - StreamBuf::SND_BUFSIZE / 32;
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
      this->reset_recv();
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
  bool idle_push_write( void ) {
    size_t buflen = this->StreamBuf::pending();
    bool flow_good = ( buflen <= this->send_highwater );
    this->idle_push( flow_good ? EV_WRITE : EV_WRITE_HI );
    return flow_good;
  }
  /* PeerData */
  virtual int client_list( char *buf,  size_t buflen ) noexcept;
  virtual bool match( PeerMatchArgs &ka ) noexcept;
};

struct EvDgram : public EvSocket, public StreamBuf {
  struct    mmsghdr * in_mhdr,
                    * out_mhdr;
  uint32_t  in_moff,   /* offset from 0 -> in_nmsgs */
            in_nmsgs,  /* number of msgs recvd */
            in_size,   /* array size of in_mhdr[] */
            in_nsize,  /* new array size, ajusted based on activity */
            out_nmsgs;

  EvDgram( EvPoll &p, const uint8_t t,  const uint8_t b ) : EvSocket( p, t, b ),
    in_mhdr( 0 ), out_mhdr( 0 ), in_moff( 0 ), in_nmsgs( 0 ), in_size( 0 ),
    in_nsize( 1 ), out_nmsgs( 0 ) {}
  void zero( void ) {
    this->in_mhdr = this->out_mhdr = NULL;
    this->in_moff = this->in_nmsgs = 0;
    this->out_nmsgs = this->in_size = 0;
  }
  bool alloc_mmsg( void ) noexcept;
  int discard_pkt( void ) noexcept;

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
};

struct EvUdp : public EvDgram {
  EvUdp( EvPoll &p, const uint8_t t ) : EvDgram( p, t, EV_DGRAM_BASE ) {}

  int listen2( const char *ip,  int port,  int opts,  const char *k ) noexcept;
  int connect( const char *ip,  int port,  int opts,  const char *k ) noexcept;

  /* PeerData */
  virtual int client_list( char *buf,  size_t buflen ) noexcept;
  virtual bool match( PeerMatchArgs &ka ) noexcept;
};

}
}
#endif
