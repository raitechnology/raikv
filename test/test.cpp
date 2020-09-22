#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <math.h>
#include <unistd.h>
#include <pthread.h>

#include <raikv/shm_ht.h>
#include <raikv/key_buf.h>
#include <raikv/zipf.h>

using namespace rai;
using namespace kv;

SignalHandler sighndl;

struct Results {
  uint64_t     total_ops,
               evict,
               hit,
               miss,
               cuckacq,
               cuckfet,
               cuckmov,
               cuckret,
               cuckmax;
  double       ival,      /* seconds */
               ops_rate,  /* ops / second */
               ns_per,    /* nanos per operation */
               collision, /* avg number of hash entries examined */
               rd_rate,   /* rd ops / second */
               wr_rate,   /* wr ops / second */
               spin_rate; /* busy wait spins / second */

  Results() {
    ::memset( this, 0, sizeof( *this ) );
  }
  Results & operator+=( const Results &x ) {
    this->total_ops += x.total_ops;
    this->evict     += x.evict;
    this->hit       += x.hit;
    this->miss      += x.miss;
    this->cuckacq   += x.cuckacq; /* num acquires by cuckoo path search */
    this->cuckfet   += x.cuckfet; /* num fetches by cuckoo path search */
    this->cuckmov   += x.cuckmov; /* num elems moved by cuckoo path search */
    this->cuckret   += x.cuckret; /* num retries by cuckoo path search */
    this->cuckmax   += x.cuckmax; /* ht full by cuckoo path */
    this->ival      += x.ival;
    this->ops_rate  += x.ops_rate;
    this->ns_per    += x.ns_per;
    this->collision += x.collision;
    this->rd_rate   += x.rd_rate;
    this->wr_rate   += x.wr_rate;
    this->spin_rate += x.spin_rate;
    return *this;
  }
  Results & operator/=( double n ) {
    /*this->total_ops  = (uint64_t) ( (double) this->total_ops / n );*/
    this->ival      /= n;
    //this->ops_rate  /= n;
    this->ns_per    /= n;
    this->collision /= n;
    //this->rd_rate   /= n;
    //this->wr_rate   /= n;
    //this->spin_rate /= n;
    return *this;
  }

  void print_hdr( void ) {
    printf( "%5s%12s%12s%6s%6s%12s%12s%14s\n",
            "ival", "tot", "op/s", "ns", "coll", "rd/s", "wr/s", "spin/s" );
  }
  void print_hdr2( void ) {
    printf( "     %12s%12s%12s%12s%12s%12s\n",
            "hit", "mis", "evi", "acq", "fet", "mov" );
  }
  /* cols matches up with hdr */
  void print_stats( void ) {
    printf( "%5.1f", this->ival );
    printf( "%12lu", this->total_ops );
    printf( "%12.0f", this->ops_rate );
    printf( "%6.1f", this->ns_per );
    printf( "%6.2f", this->collision );
    printf( "%12.0f", this->rd_rate );
    printf( "%12.0f", this->wr_rate );
    printf( "%14.0f\n", this->spin_rate );
  }
  void print_stats2( void ) {
    printf( "     %12lu", this->hit );
    printf( "%12lu", this->miss );
    printf( "%12lu", this->evict );
    printf( "%12lu", this->cuckacq );
    printf( "%12lu", this->cuckfet );
    printf( "%12lu\n", this->cuckmov );
  }
  /* space separated */
  void print_line( void ) {
    printf( "%.1f ", this->ival );
    printf( "%lu ", this->total_ops );
    printf( "%.1f ", this->ops_rate );
    printf( "%.1f ", this->ns_per );
    printf( "%.2f ", this->collision );
    printf( "%.1f ", this->rd_rate );
    printf( "%.1f ", this->wr_rate );
    printf( "%.1f ", this->spin_rate );
    printf( "%lu ", this->evict );
    printf( "%lu ", this->hit );
    printf( "%lu ", this->miss );
    printf( "%lu ", this->cuckacq );
    printf( "%lu ", this->cuckfet );
    printf( "%lu ", this->cuckmov );
    printf( "%lu ", this->cuckret );
    printf( "%lu\n", this->cuckmax );
  }
};

struct Test {
  HashTab         & map;
  HashDeltaCounters stats;
  HashCounters      ops,
                    tot;
  WorkAlloc8k       wrk;
  HashSeed          hs;
  uint64_t          test_count,
                    found,
                    not_found,
                    acquire_set,
                    acquire_fail;
  rand::xoroshiro128plus rand, start_rand;
  double            load_pct,
                    ratio_pct,
                    num_secs,
                    ival;
  uint32_t          dbx_id,
                    ctx_id,
                    prefetch;
  uint8_t           db_num;
  bool              use_find,
                    use_ratio,
                    use_rand,
                    use_zipf,
                    do_fill,
                    evict,
                    quiet;
  int               thr_num,
                    num_threads;
  const char      * generator;
  Results           results;
  void            * data;
  uint64_t          datasize;

  void * operator new( size_t, void *ptr ) { return ptr; }
  Test( HashTab &m,  int nthr ) : map( m ),
      test_count( 0 ), found( 0 ), not_found( 0 ),
      acquire_set( 0 ), acquire_fail( 0 ), num_threads( nthr ) {
    this->rand.init();
    this->start_rand = this->rand;
    this->stats.zero();
  }

  void init_hash_seed( void ) {
    this->map.hdr.get_hash_seed(
      this->map.hdr.stat_link[ this->dbx_id ].db_num, this->hs );
  }
  void sum_stats( void ) noexcept;
  void print_hdr( void ) noexcept;
  void print_stats( void ) noexcept;
  void print_hdr2( void ) noexcept;
  void print_stats2( void ) noexcept;
  void test_one( void ) noexcept;
  void test_rand( void ) noexcept;
  void test_incr( void ) noexcept;
  void test_int( void ) noexcept;
  void run( void ) noexcept;
  Test * copy( void ) noexcept {
    void * p = ::aligned_alloc( 64, sizeof( Test ) ); /* this is necessary */
    Test * t = new ( p ) Test( this->map, this->num_threads );
    t->test_count = this->test_count;
    t->load_pct   = this->load_pct;
    t->ratio_pct  = this->ratio_pct;
    t->num_secs   = this->num_secs;
    t->prefetch   = this->prefetch;
    t->db_num     = this->db_num;
    t->generator  = this->generator;
    t->use_find   = this->use_find;
    t->use_ratio  = this->use_ratio;
    t->use_rand   = this->use_rand;
    t->use_zipf   = this->use_zipf;
    t->do_fill    = this->do_fill;
    t->evict      = this->evict;
    t->quiet      = this->quiet;
    t->data       = this->data;
    t->datasize   = this->datasize;
    return t;
  }
};

void
Test::print_hdr( void ) noexcept
{
  if ( this->quiet )
    return;
  this->results.print_hdr();
}
void
Test::print_stats( void ) noexcept
{
  if ( this->quiet )
    return;
  this->results.print_stats();
}
void
Test::print_hdr2( void ) noexcept
{
  if ( this->quiet )
    return;
  this->results.print_hdr2();
}
void
Test::print_stats2( void ) noexcept
{
  if ( this->quiet )
    return;
  this->results.print_stats2();
}


void
Test::sum_stats( void ) noexcept
{
  Results & r = this->results;

  r.total_ops = this->ops.rd + this->ops.wr;
  r.evict     = this->ops.htevict;
  r.hit       = this->ops.hit;
  r.miss      = this->ops.miss;
  r.cuckacq   = this->ops.cuckacq;
  r.cuckfet   = this->ops.cuckfet;
  r.cuckmov   = this->ops.cuckmov;
  r.cuckret   = this->ops.cuckret;
  r.cuckmax   = this->ops.cuckmax;
  r.ival      = this->ival;
  r.ops_rate  = (double) ( this->ops.rd + this->ops.wr ) / this->ival;
  r.ns_per    = this->ival / (double) ( this->ops.rd +
                                       this->ops.wr ) * 1000000000.0;
  r.collision = 1.0 + ( (double) this->ops.chains /
                        (double) ( this->ops.rd + this->ops.wr ) );
  r.rd_rate   = (double) this->ops.rd / this->ival;
  r.wr_rate   = (double) this->ops.wr / this->ival;
  r.spin_rate = (double) this->ops.spins / this->ival;
}

/* test latency to hit one entry over and over again, no hashing */
void
Test::test_one( void ) noexcept
{
  KeyBuf kb;
  double start, mono, last;
  uint64_t i, h1, h2, total;
  void *p;

  kb.zero();
  kb.keylen = sizeof( uint64_t );

  sighndl.install();
  this->print_hdr();

  start = mono = last = current_monotonic_time_s();
  this->map.sum_ht_thr_delta( this->stats, this->ops, this->tot, this->ctx_id );

  if ( this->num_secs > 0 )
    total = (uint64_t) ( 10 * 1000000.0 * this->num_secs ); /* estimate */
  else
    total = 10 * 1000000;
  while ( ! sighndl.signaled ) {
    KeyCtx kctx( this->map, this->dbx_id, &kb );
    kb.set( (uint64_t) 0 ); /* use value 0 */
    this->hs.get( h1, h2 );
    kb.hash( h1, h2 );
    kctx.set_hash( h1, h2 );
    if ( this->use_find ) {
      for ( i = 0; i < total; i++ )
        kctx.find( &this->wrk );
    }
    else {
      for ( i = 0; i < total; i++ ) {
        if ( kctx.acquire( &this->wrk ) <= KEY_IS_NEW ) {
          if ( kctx.resize( &p, this->datasize ) == KEY_OK )
            ::memcpy( p, this->data, this->datasize );
          kctx.release();
        }
      }
    }
    mono = current_monotonic_time_s();
    if ( this->num_secs <= 0 || mono - start >= this->num_secs ) {
      this->ival = mono - last; last = mono;
      if ( this->map.sum_ht_thr_delta( this->stats, this->ops, this->tot,
                                       this->ctx_id ) > 0 ) {
        this->sum_stats();
        this->print_stats();
      }
      if ( this->num_secs > 0 )
        return;
    }
  }
}

/* test latency of a key = integer between 0 -> total_count */
void
Test::test_int( void ) noexcept
{
  ZipfianGen<99,100,rand::xoroshiro128plus> zipf( this->test_count, this->rand);
  KeyBuf   kb, ukb;
  double   start, mono, last;
  void   * p;
  uint64_t i, k, total, r;
  uint32_t j;
  uint64_t do_bits = 0, /* find to insert ratio */
           next    = 0;
  uint8_t  find_b  = (uint8_t) ( 256.0 * this->ratio_pct / 100.0 ),
           do_cnt  = 0,
           which   = ( this->use_find ? 1 : 0 ); /* which == 1 for find */
  bool     done    = false;
  uint8_t & ldper  = this->map.hdr.load_percent,
          & critld = this->map.hdr.critical_load;

  sighndl.install();

  start = mono = last = current_monotonic_time_s();
  this->map.sum_ht_thr_delta( this->stats, this->ops, this->tot, this->ctx_id );

  const uint32_t  stride = ( this->prefetch > 1 ? this->prefetch : 1 );
  KeyCtx        * kar;
  KeyBufAligned * kbar;
  /* allocate */
  kar  = KeyCtx::new_array( this->map, this->dbx_id, NULL, stride );
  kbar = KeyBufAligned::new_array( NULL, stride );

  i = 0;
  if ( this->num_secs > 0 )
    total = (uint64_t) ( 1000000.0 * this->num_secs ); /* estimate */
  else
    total = 10 * 1000000;
  while ( ! sighndl.signaled ) {
    /* reinitialze */
    kar  = KeyCtx::new_array( this->map, this->dbx_id, kar, stride );
    kbar = KeyBufAligned::new_array( kbar, stride );

    /* loop for a million or more */
    for ( k = 0; k < total; k += stride ) {
      uint16_t evict_acquire =
        ( ( this->evict && ldper >= critld ) ? KEYCTX_EVICT_ACQUIRE : 0 );
      if ( this->use_ratio ) {
        if ( do_cnt == 0 ) {
          while ( do_cnt < 64 ) {
            if ( ( do_cnt++ & 7 ) == 0 )
              next = this->rand.next();
            do_bits <<= 1;
            if ( ( next & 0xff ) < find_b )
              do_bits |= 1;
            next >>= 8;
          }
        }
        which = ( do_bits & 1 );
        do_bits >>= 1;
        do_cnt -= 1;
      }
      /* use random key */
      if ( this->use_rand ) {
        if ( ! this->use_zipf ) {
          for ( j = 0; j < stride; j++ ) {
            r = this->rand.next() % this->test_count;
            kbar[ j ].set( r );
          }
        }
        else {
          for ( j = 0; j < stride; j++ ) {
            r = zipf.next();
            kbar[ j ].set( r );
          }
        }
      }
      /* use integer key */
      else {
        for ( j = 0; j < stride; j++ )
          kbar[ j ].set( i + j );
      }
      /* hash and prefetch */
      for ( j = 0; j < stride; j++ ) {
        kar[ j ].set_key_hash( kbar[ j ] );
        kar[ j ].prefetch( which );
        kar[ j ].set( evict_acquire );
      }
      /* get the key value */
      if ( which ) {
        for ( j = 0; j < stride; j++ ) {
          if ( kar[ j ].find( &this->wrk ) == KEY_OK )
            this->found++;
          else
            this->not_found++;
        }
      }
      /* set the key value */
      else {
        for ( j = 0; j < stride; j++ ) {
          bool success = false;
          kbar[ j ].get( r );
          if ( kar[ j ].acquire( &this->wrk ) <= KEY_IS_NEW ) {
            if ( kar[ j ].resize( &p, this->datasize ) == KEY_OK ) {
              ::memcpy( p, this->data, this->datasize );
              success = true;
            }
            kar[ j ].release();
          }
          if ( success )
            this->acquire_set++;
          else
            this->acquire_fail++;
        }
      }
      if ( (i += stride) >= this->test_count ) {
        i = 0;
        /* fill hash table */
        if ( this->do_fill ) {
          done = true;
          break;
        }
      }
    }
    /* check if times up */
    mono = current_monotonic_time_s();
    if ( this->num_secs <= 0 || mono - start >= this->num_secs || done ) {
      this->ival = mono - last; last = mono;
      if ( this->map.sum_ht_thr_delta( this->stats, this->ops, this->tot,
                                       this->ctx_id ) > 0 ) {
        this->sum_stats();
        this->print_hdr();
        this->print_stats();
        this->print_hdr2();
        this->print_stats2();
      }
      if ( this->num_secs > 0 || done )
        return;
    }
  }
}

void
Test::run( void ) noexcept
{
  this->ctx_id = this->map.attach_ctx( ::getpid() );
  if ( this->ctx_id == MAX_CTX_ID ) {
    fprintf( stderr, "no more ctx available\n" );
    return;
  }
  this->dbx_id = this->map.attach_db( this->ctx_id, this->db_num );
  this->init_hash_seed();

  if ( ! this->quiet ) {
    const char *s = print_map_geom( &this->map, this->ctx_id );
    fputs( s, stdout );
    printf( "generator:   %s\n", this->generator );
    printf( "num threads: %d\n", this->num_threads );
    printf( "elem count:  %lu\n", this->test_count );
    printf( "prefetch:    %u\n", this->prefetch );
    printf( "use find:    %s\n", this->use_find ? "yes" : "no" );
    printf( "use ratio:   %s\n", this->use_ratio ? "yes" : "no" );
    printf( "do fill:     %s\n", this->do_fill ? "yes" : "no" );
    printf( "num secs:    %.1f\n", this->num_secs );
  }
  if ( ::strcmp( this->generator, "one" ) == 0 )
    this->test_one();
  else
    this->test_int();

  this->map.detach_ctx( this->ctx_id );
}

static void *
run_test( void *p )
{
  ((Test *) p)->run();
  return NULL;
}

static const char *
get_arg( int argc, char *argv[], int b, const char *f, const char *def )
{
  for ( int i = 1; i < argc - b; i++ )
    if ( ::strcmp( f, argv[ i ] ) == 0 )
      return argv[ i + b ];
  return def; /* default value */
}

static uint64_t
tomem( const char *s )
{
  static const struct {
    const char *s;
    uint64_t val;
  } suffix[] = {
  { "B", 1 }, { "K", 1000 }, { "M", 1000 * 1000 }, { "G", 1000 * 1000 * 1000 },
  { "KB", 1024 }, { "MB", 1024 * 1024 }, { "GB", 1024 * 1024 * 1024 }
  };
  for ( size_t i = 0; i < sizeof( suffix ) / sizeof( suffix[ 0 ] ); i++ )
    if ( ::strcasecmp( suffix[ i ].s, s ) == 0 )
      return suffix[ i ].val;
  return 0;
}

int
main( int argc, char *argv[] )
{
  HashTabGeom   geom;
  HashTab     * map;

  const char * mn = get_arg( argc, argv, 1, "-m", KV_DEFAULT_SHM ),
             * cr = get_arg( argc, argv, 1, "-c", NULL ),
             * th = get_arg( argc, argv, 1, "-t", NULL ),
             * ge = get_arg( argc, argv, 1, "-x", "int" ),
             * pc = get_arg( argc, argv, 1, "-p", "50" ),
             * ra = get_arg( argc, argv, 1, "-r", "90" ),
             * fe = get_arg( argc, argv, 1, "-f", "1" ),
             * op = get_arg( argc, argv, 1, "-o", "ins" ),
             * nn = get_arg( argc, argv, 1, "-n", NULL ),
             * db = get_arg( argc, argv, 1, "-d", "0" ),
             * sz = get_arg( argc, argv, 1, "-z", "0" ),
             * ev = get_arg( argc, argv, 0, "-e", NULL ),
             * qu = get_arg( argc, argv, 0, "-q", NULL ),
             * he = get_arg( argc, argv, 0, "-h", 0 );

  if ( he != NULL ) {
  cmd_error:;
    fprintf( stderr, "raikv version: %s\n", kv_stringify( KV_VER ) );
    fprintf( stderr,
  "%s [-m map] [-c size] [-t num-thr] [-x gen] [-p pct] [-r ratio] "
     "[-f prefetch] [-o oper] [-n secs] [-d db-num] [-e] [-q]\n"
  "  -m map      = name of map file (default: " KV_DEFAULT_SHM ")\n"
  "  -c size     = size of map file to create in MB\n"
  "  -t num-thr  = num threads to run simul (def: 0)\n"
  "  -x gen      = key generator kind: one, int, rand, zipf, fill (def: int)\n"
  "  -p pct      = percent coverage of total hash entries (def: 50%%)\n"
  "  -r ratio    = ratio of find to insert (def: 90%%)\n"
  "  -f prefetch = number of prefetches to perform (def: 1)\n"
  "  -o oper     = find, insert, ratio (def: ins)\n"
  "  -n secs     = num seconds (def: continue forever)\n"
  "  -d db-num   = database number to use (def: 0)\n"
  "  -z data-sz  = size of data field (def: 0)\n"
  "  -e          = evict elems when ht full\n"
  "  -q          = be quiet, only results\n", argv[ 0 ] );
    return 1;
  }

  double   szf = strtod( sz, 0 );
  size_t   szl = ::strlen( sz );
  uint64_t m;

  while ( szl > 0 && isalpha( sz[ szl - 1 ] ) )
    szl--;
  if ( sz[ szl ] != '\0' && (m = tomem( &sz[ szl ] )) != 0 )
    szf *= (double) m;
  if ( cr != NULL ) {
    geom.map_size         = strtod( cr, 0 ) * 1024 * 1024;
    geom.hash_entry_size  = 64;
    geom.cuckoo_buckets   = 4;
    geom.cuckoo_arity     = 2;

    if ( szf == 0 ) {
      geom.max_value_size   = 0; /* all ht, no value */
      geom.hash_value_ratio = 1;
    }
    else {
      geom.max_value_size   = (uint64_t) szf + 128;
      geom.hash_value_ratio = 1.0 / /* hash ratio of / 64 bytes */
                    ( ( ( (uint64_t) szf | 127 ) + 129 ) / 64.0 );
    }
    map = HashTab::create_map( mn, 0, geom, 0666 );
  }
  else {
    map = HashTab::attach_map( mn, 0, geom );
  }
  if ( map == NULL )
    return 1;

  int nthr = ( th != NULL ? atoi( th ) : 1 );
  if ( nthr < 1 ) nthr = 1;
  double load_pct = strtod( pc, 0 );
  /* allow load > 100, for find misses and insert more than ht can hold */
  if ( load_pct == 0 /*|| load_pct > 100.0*/ )
    goto cmd_error;
  double ratio_pct = strtod( ra, 0 );
  if ( ratio_pct == 0 || ratio_pct > 100.0 )
    goto cmd_error;
  int prefetch = atoi( fe );
  if ( prefetch == 0 )
    goto cmd_error;
  uint8_t db_num = (uint8_t) atoi( db );

  Test test( *map, nthr );

  test.load_pct   = load_pct;
  test.ratio_pct  = ratio_pct;
  test.prefetch   = prefetch;
  test.db_num     = db_num;
  test.generator  = ge;
  test.datasize   = (uint64_t) szf;
  test.data       = ( test.datasize > 0 ? ::malloc( test.datasize ) : NULL );
  if ( test.data != NULL ) {
    for ( size_t i = 0; i < test.datasize; i++ )
      ((uint8_t *) test.data)[ i ] = (uint8_t) i;
  }
  else {
    static char hello[ 6 ] = "hello";
    test.datasize = 6;
    test.data     = (void *) hello;
  }
  test.test_count = (uint64_t)
    ( ( (double) map->hdr.ht_size * test.load_pct ) / 100.0 );
  test.use_find   = ::strncmp( op, "find", 4 ) == 0;  /* insert default */
  test.use_ratio  = ::strncmp( op, "ratio", 5 ) == 0; /* ratio find/insert */
  test.use_zipf   = ::strncmp( ge, "zipf", 4 ) == 0;  /* use zipf sequence */
  test.use_rand   = test.use_zipf ||
                    ::strncmp( ge, "rand", 4 ) == 0;  /* use rand sequence */
  test.do_fill    = ::strncmp( ge, "fill", 4 ) == 0;  /* insert load_pct int */
  test.num_secs   = ( nn == NULL ? 0 : strtod( nn, 0 ) );
  test.evict      = ( ev != NULL );
  test.quiet      = ( qu != NULL || nthr > 1 );

  /* if one thread */
  if ( nthr <= 1 ) {
    test.run();
    if ( test.quiet ) {
      printf( "1 %.1f ", (double) geom.map_size / 1024.0 / 1024.0 );
      test.results.print_line();
    }
  }
  /* if multiple threads */
  else {
    int i = 0;
    pthread_t tid[ 256 ];
    Test * tid_test[ 256 ];

    if ( nthr > 256 )
      nthr = 256;
    for ( i = 0; i < nthr; i++ ) {
      tid_test[ i ] = test.copy();
      pthread_create( &tid[ i ], NULL, run_test, tid_test[ i ] );
    }

    Results total;
    for ( i = 0; i < nthr; i++ ) {
      pthread_join( tid[ i ], NULL );
      total += tid_test[ i ]->results;
    }
    total /= (double) nthr;
    /*total.print_hdr();*/
    printf( "%d %.1f ", nthr, (double) geom.map_size / 1024.0 / 1024.0 );
    total.print_line();
  }
  if ( ! test.quiet )
    printf( "bye\n" );

  return 0;
}
