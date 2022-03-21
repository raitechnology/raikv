#ifndef __rai_raikv__mainloop_h__
#define __rai_raikv__mainloop_h__

#include <raikv/ev_net.h>
/*#include <raikv/kv_pubsub.h>*/

namespace rai {
namespace kv {

struct MainLoopVars {
  SignalHandler sighndl;      /* installed for signalling exit (int, hup) */
  size_t        thr_exit,     /* wait for my turn to exit */
                thr_start,    /* wait for my turn to start */
                thr_error;    /* if failed to start */
  const char  * map_name;
  int           maxfd,        /* max fd count */
                timeout,      /* keep alive timeout */
                num_threads,  /* thread count */
                tcp_opts,     /* sock options for tcp */
                udp_opts;     /* sock options for udp */
  bool          use_reuseport,/* true to set SO_REUSEPORT on sock */
                use_ipv4,     /* true to only bind to ipv4 address */
                use_sigusr,   /* true to use sig usr to signal messages */
                busy_poll,    /* true to busy poll kv msgs */
                use_prefetch, /* prefetch keys in batches */
                all,          /* start all ports with default */
                no_threads,   /* don't want threading options */
                no_reuseport, /* don't want so_reuseport */
                no_map,       /* don't want shm */
                no_default;   /* don't want discard default -X */
  uint8_t       db_num;       /* which db to attach */

  const char * desc[ 16 ]; /* extra help arg descriptions */
  int n; /* cnt of desc */

  MainLoopVars() { ::memset( this, 0, sizeof( *this ) ); }

  void add_desc( const char *s ) {
    this->desc[ this->n++ ] = s;
  }
  static const char *get_arg( int argc, const char *argv[], int b,
                      const char *f, const char *def, const char *env = 0 ) {
    for ( int i = 1; i < argc - b; i++ )
      if ( ::strcmp( f, argv[ i ] ) == 0 ) /* -m map -p port */
        return argv[ i + b ];
    const char *var = ( env != NULL ? ::getenv( env ) : NULL );
    return ( var == NULL ? def : var ); /* default value or env var */
  }
  static bool streq( const char *s,  const char *t ) {
    return ::strcmp( s, t ) == 0;
  }
  static bool bool_arg( int argc, const char *argv[], int b,
                        const char *f, const char *def,  const char *env = 0 ) {
    const char *s = get_arg( argc, argv, b, f, def, env );
    return ( s != nullptr && b == 0 ) ||
           ( s != nullptr && b == 1 && ( streq( s, "1" ) || streq( s, "true" ) ) );
  }
  static int int_arg( int argc, const char *argv[], int b,
                      const char *f, const char *def,  const char *env = 0 ) {
    const char *s = get_arg( argc, argv, b, f, def, env );
    return ( s == nullptr ? 0 : atoi( s ) );
  }
  void print_help( void ) const {
    if ( ! this->no_map )
      printf(
        "  -m map   = kv shm map name       (" KV_DEFAULT_SHM ") (" KV_MAP_NAME_ENV ")\n" );
    for ( int i = 0; i < n; i++ )
      printf( "%s\n", this->desc[ i ] );
    if ( ! this->no_map )
      printf( "  -D dbnum = default db num          (0) (" KV_DB_NUM_ENV ")\n" );
    printf( "  -x maxfd = max fds                 (10000) (" KV_MAXFD_ENV ")\n" );
    printf( "  -k secs  = keep alive timeout      (16) (" KV_KEEPALIVE_ENV ")\n" );
    if ( ! this->no_map )
      printf( "  -f prefe = prefetch keys:          (1) 0 = no, 1 = yes (" KV_PREFETCH_ENV ")\n" );
    if ( ! this->no_reuseport )
      printf( "  -P       = set SO_REUSEPORT for clustering multiple instances (" KV_REUSEPORT_ENV ")\n" );
    if ( ! this->no_threads )
      printf( "  -t nthr  = spawn N threads         (1) (implies -P) (" KV_NUM_THREADS_ENV ")\n" );
    printf( "  -4       = use only ipv4 listeners (" KV_IPV4_ONLY_ENV ")\n" );
    if ( ! this->no_map )
      printf( "  -s       = do not use signal USR1 publish notification (" KV_USE_SIGUSR_ENV ")\n" );
    printf( "  -b       = busy poll               (" KV_BUSYPOLL_ENV ")\n" );
    if ( ! this->no_default )
      printf( "  -X       = do not listen to default ports, only using cmd line\n" );
  }
  bool parse_args( int argc, const char *argv[] ) {
    /* check help */
    if ( get_arg( argc, argv, 0, "-h", 0 ) != nullptr ||
         get_arg( argc, argv, 0, "-help", 0 ) != nullptr ||
         get_arg( argc, argv, 0, "--help", 0 ) != nullptr ) {
      this->print_help();
      return false;
    }
    if ( ! this->no_map ) {
      this->map_name = get_arg( argc, argv, 1, "-m", KV_DEFAULT_SHM,
                                                     KV_MAP_NAME_ENV );
      this->db_num = (uint8_t) int_arg( argc, argv, 1, "-D", "0", KV_DB_NUM_ENV );
    }
    else {
      this->map_name = "none";
    }
    this->maxfd       = int_arg(  argc, argv, 1, "-x", "10000", KV_MAXFD_ENV );
    this->timeout     = int_arg(  argc, argv, 1, "-k", "16", KV_KEEPALIVE_ENV );
    if ( ! this->no_map )
      this->use_prefetch = bool_arg( argc, argv, 1, "-f", "1", KV_PREFETCH_ENV );
    this->busy_poll   = bool_arg( argc, argv, 0, "-b", 0, KV_BUSYPOLL_ENV );
    if ( ! this->no_reuseport )
      this->use_reuseport = bool_arg( argc, argv, 0, "-P", 0, KV_REUSEPORT_ENV );
    if ( ! this->no_threads )
      this->num_threads = int_arg(  argc, argv, 1, "-t", "1", KV_NUM_THREADS_ENV);
    this->use_ipv4 = bool_arg( argc, argv, 0, "-4", 0, KV_IPV4_ONLY_ENV );
    if ( ! this->no_map )
      this->use_sigusr  = bool_arg( argc, argv, 0, "-s", 0, KV_USE_SIGUSR_ENV );
    if ( ! this->no_default )
      this->all = ! bool_arg( argc, argv, 0, "-X", 0 );

    this->tcp_opts = DEFAULT_TCP_LISTEN_OPTS;
    this->udp_opts = DEFAULT_UDP_LISTEN_OPTS;
    if ( this->num_threads > 1 ) /* each thread has a port */
      this->use_reuseport = true;
    if ( ! this->use_reuseport ) {
      this->tcp_opts &= ~OPT_REUSEPORT;
      this->udp_opts &= ~OPT_REUSEPORT;
    }
    if ( this->use_ipv4 ) {
      this->tcp_opts &= ~OPT_AF_INET6;
      this->udp_opts &= ~OPT_AF_INET6;
    }
    return true;
  }
  int parse_port( int argc, const char *argv[], const char *f,
                  const char *def ) {
    return int_arg( argc, argv, 1, f, this->all ? def : 0 );
  }
};

/* call out to init listeners:
 *
 * struct Loop : public MainLoop<Args> {
 *   bool init( void ) noexcept;
 *
 *   static bool initialize( void *me ) noexcept {
 *     return ((Loop *) me)->init();
 *   }
 * };
 *
 * Runner<Args, Loop> runner( args, shm, Loop::initialize );
 */
typedef bool (*initialize_func_t)( void * );

template<class MAIN_LOOP_ARGS>
struct MainLoop {
  EvPoll            poll;
  EvShm             shm;
  MAIN_LOOP_ARGS  & r;
  size_t            thr_num;
  bool              running,
                    done;
  initialize_func_t initialize;

  template <class Sock>
  void Alloc( Sock * &s ) {
    void * p = aligned_malloc( sizeof( Sock ) );
    s = new ( p ) Sock( this->poll );
  }
  template <class Sock>
  bool Listen( const char *addr,  int pt,  Sock* &l,  int opts ) {
    if ( pt != 0 ) {
      Alloc<Sock>( l );
      if ( l->listen( addr, pt, opts ) != 0 ) {
        fprintf( stderr, "unable to open listen socket on %d\n", pt );
        return false; /* bad port or network error */
      }
    }
    return true;
  }
  /* the allocs are 64 byte aligned for poll */
  void * operator new( size_t, void *ptr ) { return ptr; }
  MainLoop( EvShm &m,  MAIN_LOOP_ARGS &args,  size_t num,
            initialize_func_t ini )
    : shm( m ), r( args ) {
    uint8_t * b = (uint8_t *) (void *) &this->thr_num;
    ::memset( b, 0, (uint8_t *) (void *) &this[ 1 ] -  b );
    this->initialize = ini;
    this->thr_num    = num;
  }
  /* initialize poll event */
  bool poll_init( void ) {
    if ( this->r.num_threads > 1 ) {
      if ( this->shm.attach( this->r.db_num ) != 0 )
        return false;
    }
    /* set timeouts */
    this->poll.wr_timeout_ns   = (uint64_t) this->r.timeout * 1000000000;
    this->poll.so_keepalive_ns = (uint64_t) this->r.timeout * 1000000000;

    if ( this->poll.init( this->r.maxfd, this->r.use_prefetch ) != 0 ||
         this->poll.init_shm( this->shm ) != 0 ) {
      fprintf( stderr, "unable to init poll\n" );
      return false;
    }
#if 0
    if ( this->poll.pubsub != NULL ) {
      if ( this->r.busy_poll )
        this->poll.pubsub->idle_push( EV_BUSY_POLL );
      if ( ! this->r.use_sigusr )
        this->poll.pubsub->flags &= ~KV_DO_NOTIFY;
    }
#endif
    return true;
  }
#ifdef _MSC_VER
  void idle( void ) { Sleep( 1 ); }
#else
  void idle( void ) { usleep( 1 ); }
#endif
  /* mainloop runner */
  void run( void ) {
    int idle_cnt = 0;
    while ( this->r.thr_start < this->thr_num ) /* wait for my turn */
      idle();
    if ( this->r.thr_error == 0 && this->poll_init() &&
         this->initialize( this ) ) {
      this->r.thr_start++;
      this->running = true;
      for (;;) {
        if ( this->poll.quit >= 5 ) {
          this->r.thr_exit++;
          break;
        }
        int state = this->poll.dispatch(); /* 0 if idle, 1, 2, 3 if busy */
        if ( state == EvPoll::DISPATCH_IDLE )
          idle_cnt++;
        else
          idle_cnt = 0;
        this->poll.wait( idle_cnt > 255 ? 100 : 0 );
        if ( this->r.sighndl.signaled && ! poll.quit ) {
          if ( this->r.thr_exit >= this->thr_num ) /* wait for my turn */
            this->poll.quit++;
        }
      }
    }
    else {
      this->r.thr_error++;
      this->r.thr_exit++;
      this->r.thr_start++;
    }
    this->done = true;
  }
  /* detach from shared memory */
  void detach( void ) {
    this->shm.detach();
  }
};

template <class MAIN_LOOP_ARGS, class MAIN_LOOP>
struct Runner {
  static const size_t MAX_THREADS = KV_MAX_CTX_ID;

  MAIN_LOOP * children[ MAX_THREADS ];
#ifndef _MSC_VER
  pthread_t   tid[ MAX_THREADS ];
#endif
  size_t      num_thr;

  /* pthread spawner */
  static void * thread_runner( void *loop ) {
    ((MAIN_LOOP *) loop)->run();
    return nullptr;
  }

  Runner( MAIN_LOOP_ARGS &r,  EvShm &shm,  bool (*ini)( void * ) ) {
    this->num_thr = ( r.num_threads <= 1 ? 1 : r.num_threads );

    const size_t size = kv::align<size_t>( sizeof( MAIN_LOOP ), 64 );
    char * buf = (char *) aligned_malloc( size * this->num_thr );
    size_t i, off = 0;

    for ( i = 0; i < this->num_thr && i < MAX_THREADS; i++ ) {
      this->children[ i ] = new ( &buf[ off ] ) MAIN_LOOP( shm, r, i, ini );
      off += size;
    } 
    if ( this->num_thr == 1 ) {
      r.sighndl.install(); /* catch sig int */
      this->children[ 0 ]->run();
    }
#ifndef _MSC_VER
    else {
      shm.detach(); /* each child will attach */
      signal( SIGUSR2, SIG_IGN );
      r.sighndl.install(); /* catch sig int */

      for ( i = 0; i < this->num_thr && i < MAX_THREADS; i++ )
        pthread_create( &this->tid[ i ], nullptr, thread_runner,
                        this->children[ i ] );
      while ( r.thr_start < this->num_thr )
        usleep( 1 );
      if ( r.thr_error > 0 )
        printf( "%lu errors\n", r.thr_error );
      printf( "%lu started\n", r.thr_start );
      for ( i = 0; i < this->num_thr && i < MAX_THREADS; i++ )
        pthread_join( this->tid[ i ], nullptr );
    }
    shm.detach();
#endif
    printf( "\nbye\n" );
  }
};

}
}

#endif
