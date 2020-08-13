#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <math.h>
#include <ctype.h>
#include <errno.h>

#include <raikv/shm_ht.h>

using namespace rai;
using namespace kv;

struct Server {
  HashTab      & map;
  HashTabStats & hts;
  uint64_t       stats_ival,
                 check_ival,
                 stats_counter;

  Server( HashTab &m,  uint64_t i,  uint64_t x )
    : map( m ), hts( *HashTabStats::create( m ) ), stats_ival( i ),
      check_ival( x ), stats_counter( 0 ) {}

  void loop( void );
  void print_stats( void );
  void print_ops( void );
  void check_thread_ctx( void ); /* check for broken locks */
};

static SignalHandler sighndl;

void
Server::loop( void )
{
  char     junk[ 8 ];
  ssize_t  nbytes = 0;
  uint64_t curr   = 0,
           last   = 0,
           ival   = 0,
           check  = 0,
           stats  = 0;

  sighndl.install();
  fcntl( 0, F_SETFL, fcntl( 0, F_GETFL, 0 ) | O_NONBLOCK );
  this->check_thread_ctx();
  this->map.update_load();
  last = current_monotonic_coarse_ns();

  for (;;) {
    bool do_print_stats = false;

    usleep( 50 * 1000 /* 50 ms */);
    if ( sighndl.signaled )
      return;
    curr   = current_monotonic_coarse_ns();
    ival   = curr - last;
    last   = curr;
    stats += ival;
    check += ival;

    /* if a keyboard <cr> on cmd line or stats interval expires */
    if ( (nbytes = read( 0, junk, sizeof( junk ) )) > 0 ||
         stats >= this->stats_ival || this->stats_counter == 0 ) {
      stats %= this->stats_ival;
      if ( nbytes > 0 ) {
        this->stats_counter = 0; /* print header again */
        nbytes = 0;
      }
      do_print_stats = true;
    }

    if ( check >= this->check_ival ) {
      check %= this->check_ival;
      this->check_thread_ctx();
      this->map.update_load();
    }
    else {
      this->map.hdr.current_stamp = current_realtime_coarse_ns();
    }
    if ( do_print_stats )
      this->print_stats();
  }
}

void
Server::print_stats( void )
{
  bool b = this->hts.fetch();
  if ( b || ( this->stats_counter == 0 && this->hts.ival > 0 ) ) {
    /* print hdr if stats counter == 0 */
    if ( this->stats_counter == 0 ) {
      fputs( print_map_geom( &this->map, MAX_CTX_ID ), stdout );
      for ( uint32_t db = 0; db < DB_COUNT; db++ ) {
        if ( this->map.hdr.test_db_opened( db ) ) {
          printf( "db[ %u ].entry_cnt:%s %lu\n", db,
                  ( ( db < 10 ) ? "   " : ( ( db < 100 ) ? "  " : " " ) ),
                  this->hts.db_stats[ db ].last.add -
                  this->hts.db_stats[ db ].last.drop );
        }
      }
    }
    /* print interval ops */
    this->print_ops();
    fflush( stdout );
  }
}

static char *
mstring( double f,  char *buf,  int64_t k )
{
  return mem_to_string( (int64_t) ceil( f ), buf, k );
}

void
Server::print_ops( void )
{
  HashCounters & ops  = this->hts.hops,
               & tot  = this->hts.htot;
  MemCounters  & chg  = this->hts.mops;
  double         ival = this->hts.ival;
  char buf[ 16 ], buf2[ 16 ], buf3[ 16 ], buf4[ 16 ], buf5[ 16 ],
       buf6[ 16 ], buf7[ 16 ], buf8[ 16 ], buf9[ 16 ];

  if ( ( this->stats_counter++ % 16 ) == 0 )
    printf( "   op/s   1/ops chns    get    put   spin ht va  "
            "entry    GC  drop   hits   miss\n" );

  double op, ns, ch;
  if ( ops.rd + ops.wr == 0 ) {
    op = 0;
    ns = 0;
    ch = 0;
  }
  else {
    op = (double) ( ops.rd + ops.wr ) / ival;
    ns = ival / (double) ( ops.rd + ops.wr ) * 1000000000.0;
    if ( ns > 99999.9 ) {
      if ( op < 1.0 )
        ns = 0;
      else
        ns = 99999.9;
    }
    ch = 1.0 + ( (double) ops.chains / (double) ( ops.rd + ops.wr ) );
  }
  printf( "%7s %7.1f %4.1f %6s %6s %6s %2u %2u %6s %5s %5s %6s %6s\n",
         mstring( op, buf, 1000 ), ns, ch,
         mstring( (double) ops.rd / ival, buf2, 1000 ),
         mstring( (double) ops.wr / ival, buf3, 1000 ),
         mstring( (double) ops.spins / ival, buf4, 1000 ),
         (uint32_t) ( this->map.hdr.ht_load * 100.0 + 0.5 ),
         (uint32_t) ( this->map.hdr.value_load * 100.0 + 0.5 ),
         mstring( tot.add - tot.drop, buf5, 1000 ),
         mstring( (double) chg.move_msgs / ival, buf6, 1000 ),
         mstring( (double) ops.drop / ival, buf7, 1000 ),
         mstring( (double) ops.hit / ival, buf8, 1000 ),
         mstring( (double) ops.miss / ival, buf9, 1000 ) );
}

void
Server::check_thread_ctx( void )
{
  uint32_t hash_entry_size = this->map.hdr.hash_entry_size;
  for ( uint32_t ctx_id = 1; ctx_id < MAX_CTX_ID; ctx_id++ ) {
    uint32_t pid = this->map.ctx[ ctx_id ].ctx_pid;
    if ( pid == 0 || this->map.ctx[ ctx_id ].ctx_id == KV_NO_CTX_ID )
      continue;
    if ( ::kill( pid, 0 ) == 0 )
      continue;
    if ( errno == EPERM )
      continue;
    printf( "ctx %u: pid %u = kill errno %d/%s\n",
            ctx_id, pid, errno, strerror( errno ) );

    uint64_t used, recovered = 0;
    if ( (used = this->map.ctx[ ctx_id ].mcs_used) != 0 ) {
      for ( uint32_t id = 0; id < 64; id++ ) {
        if ( ( used & ( (uint64_t) 1 << id ) ) == 0 )
          continue;
        uint64_t mcs_id = ( ctx_id << ThrCtxEntry::MCS_SHIFT ) | id;
        ThrMCSLock &mcs = this->map.ctx[ ctx_id ].get_mcs_lock( mcs_id );
        MCSStatus status;
        printf(
        "ctx %u: pid %u, mcs %u, val 0x%lx, lock 0x%lx, next 0x%lx, link %lu\n",
                 ctx_id, pid, id, mcs.val.load(), mcs.lock.load(),
                 mcs.next.load(), mcs.lock_id );
        if ( mcs.lock_id != 0 ) {
          HashEntry *el = this->map.get_entry( mcs.lock_id - 1,
                                         this->map.hdr.hash_entry_size );
          ThrCtxOwner  closure( this->map.ctx );
          status = mcs.recover_lock( el->hash, ZOMBIE64, mcs_id, closure );
          if ( status == MCS_OK ) {
            ValueCtr &ctr = el->value_ctr( hash_entry_size );
            if ( ctr.seal == 0 )
              ctr.seal = 1; /* these are lost with the context thread */
            status = mcs.recover_unlock( el->hash, ZOMBIE64, mcs_id, closure );
            if ( status == MCS_OK ) {
              printf( "mcs_id %u:%u recovered\n", ctx_id, id );
              recovered |= ( (uint64_t) 1 << id );
            }
          }
          if ( status != MCS_OK ) {
            printf( "mcs_id %u:%u status %s\n", ctx_id, id,
                    status == MCS_WAIT ? "MCS_WAIT" : "MCS_INACTIVE" );
          }
        }
      }
      this->map.ctx[ ctx_id ].mcs_used &= ~recovered;
    }
    if ( used != recovered ) {
      printf( "ctx %u still has locks\n", ctx_id );
    }
    else {
      this->map.detach_ctx( ctx_id );
    }
  }
}

static const char *
get_arg( int argc, char *argv[], int b, const char *f, const char *def )
{
  for ( int i = 1; i < argc - b; i++ )
    if ( ::strcmp( f, argv[ i ] ) == 0 )
      return argv[ i + b ];
  return def; /* default value */
}

int
main( int argc, char *argv[] )
{
  HashTabGeom   geom;
  HashTab     * map        = NULL;
  double        ratio      = 0.5;
  uint64_t      stats_ival = NANOS,
                check_ival = NANOS;
  uint64_t      mbsize     = 1024 * 1024 * 1024; /* 1G */
  uint32_t      entsize    = 64,                 /* 64b */
                valsize    = 1024 * 1024;        /* 1MB */
  uint8_t       arity      = 2;                  /* cuckoo 2+4 */
  uint16_t      buckets    = 4;

  const char * mn = get_arg( argc, argv, 1, "-m", KV_DEFAULT_SHM ),
             * mb = get_arg( argc, argv, 1, "-s", "1024" ),
             * pc = get_arg( argc, argv, 1, "-k", "0.25" ),
             * cu = get_arg( argc, argv, 1, "-c", "2+4" ),
             * mo = get_arg( argc, argv, 1, "-o", "ug+rw" ),
             * vz = get_arg( argc, argv, 1, "-v", "2048" ),
             * ez = get_arg( argc, argv, 1, "-e", "64" ),
             * at = get_arg( argc, argv, 0, "-a", 0 ),
             * rm = get_arg( argc, argv, 0, "-r", 0 ),
             * iv = get_arg( argc, argv, 1, "-i", "1" ),
             * ix = get_arg( argc, argv, 1, "-x", "1" ),
             * he = get_arg( argc, argv, 0, "-h", 0 );

  if ( he != NULL ) {
  cmd_error:;
    fprintf( stderr, "raikv version: %s\n", kv_stringify( KV_VER ) );
    fprintf( stderr,
  "%s [-m map] [-s MB] [-k ratio] [-c cuckoo a+b] "
     "[-v value-sz] [-e entry-sz] [-a] [-r]\n"
  "  -m map         = name of map file (default: " KV_DEFAULT_SHM ")\n"
  "  -s MB          = size of HT (MB * 1024 * 1024, default: 1024)\n"
  "  -k ratio       = entry to segment memory ratio (float 0 -> 1, def: 0.5)\n"
  "                  (1 = all ht, 0 = all msg -- must have some ht)\n"
  "  -c cuckoo a+b  = cuckoo hash arity and buckets (default: 2+4)\n"
  "  -o mode        = create map using mode (default: ug+rw)\n"
  "  -v value-sz    = max value size or min seg size (in KB, default: 1024)\n"
  "  -e entry-sz    = hash entry size (multiple of 64, default: 64)\n"
  "  -a             = attach to map, don't create (default: create)\n"
  "  -r             = remove map and then exit\n"
  "  -i secs        = stats interval (default: 1)\n"
  "  -x secs        = check interval (default: 1)\n",
             argv[ 0 ] );
    return 1;
  }

  mbsize = (uint64_t) ( strtod( mb, 0 ) * (double) ( 1024 * 1024 ) );
  if ( mbsize == 0 )
    goto cmd_error;
  ratio = strtod( pc, 0 );
  if ( ratio < 0.0 || ratio > 1.0 )
    goto cmd_error;
  if ( isdigit( cu[ 0 ] ) &&
       cu[ 1 ] == '+' &&
       isdigit( cu[ 2 ] ) ) {
    arity   = cu[ 0 ] - '0';
    buckets = atoi( &cu[ 2 ] );
  }
  else {
    goto cmd_error;
  }
  valsize = (uint32_t) atoi( vz ) * (uint32_t) 1024;
  if ( valsize == 0 && ratio < 1.0 )
    goto cmd_error;
  entsize = (uint32_t) atoi( ez );
  if ( entsize == 0 )
    goto cmd_error;
  stats_ival = (uint64_t) ( strtod( iv, 0 ) * NANOSF );
  check_ival = (uint64_t) ( strtod( ix, 0 ) * NANOSF );

  if ( at == NULL && rm == NULL ) {
    int mode, x;
    geom.map_size         = mbsize;
    geom.max_value_size   = ratio < 0.999 ? valsize : 0;
    geom.hash_entry_size  = align<uint32_t>( entsize, 64 );
    geom.hash_value_ratio = ratio;
    geom.cuckoo_buckets   = buckets;
    geom.cuckoo_arity     = arity;
    mode = atoi( mo );
    if ( mode == 0 ) {
      x = mode = 0;
      if ( ::strchr( mo, 'r' ) != NULL ) x = 4;
      if ( ::strchr( mo, 'w' ) != NULL ) x |= 2;
      if ( ::strchr( mo, 'u' ) != NULL ) mode |= x << 6;
      if ( ::strchr( mo, 'g' ) != NULL ) mode |= x << 3;
      if ( ::strchr( mo, 'o' ) != NULL ) mode |= x;
    }
    if ( mode == 0 ) {
      fprintf( stderr, "Invalide create map mode: %s (0%o)\n", mo, mode );
      goto cmd_error;
    }
    printf( "Creating map %s, mode %s (0%o)\n", mn, mo, mode );
    map = HashTab::create_map( mn, 0, geom, mode );
  }
  else if ( at != NULL ) {
    printf( "Attaching map %s\n", mn );
    map = HashTab::attach_map( mn, 0, geom );
  }
  else {
    if ( HashTab::remove_map( mn, 0 ) == 0 ) {
      printf( "removed %s\n", mn );
      return 0;
    }
    printf( "failed to remove %s\n", mn );
    /* return 1 below */
  }
  if ( map == NULL )
    return 1;
  //print_map_geom( map, MAX_CTX_ID );

  Server svr( *map, stats_ival, check_ival );

  svr.loop();

  printf( "bye\n" );
  fflush( stdout );
  delete map;
  return 0;
}

