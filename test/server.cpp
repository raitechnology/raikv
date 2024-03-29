#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
#include <unistd.h>
#include <fcntl.h>
#else
#include <raikv/win.h>
#endif
#include <math.h>
#include <ctype.h>

#include <raikv/shm_ht.h>
#include <raikv/monitor.h>

using namespace rai;
using namespace kv;

static const char *
get_arg( int argc, char *argv[], int b, const char *f, const char *def,
         const char *env = 0 )
{
  for ( int i = 1; i < argc - b; i++ )
    if ( ::strcmp( f, argv[ i ] ) == 0 )
      return argv[ i + b ];
  const char *var = ( env != NULL ? ::getenv( env ) : NULL );
  return ( var == NULL ? def : var ); /* default value or env var */
}

int
main( int argc, char *argv[] )
{
  HashTabGeom   geom;
  HashTab     * map        = NULL;
  double        ratio      = 0.5;
  uint64_t      stats_ival = NANOS,
                check_ival = NANOS / 10;
  uint64_t      mbsize     = 1024 * 1024 * 1024; /* 1G */
  uint32_t      entsize    = 64,                 /* 64b */
                valsize    = 1024 * 1024;        /* 1MB */
  uint8_t       arity      = 2;                  /* cuckoo 2+4 */
  uint16_t      buckets    = 4;

  const char * mn = get_arg( argc, argv, 1, "-m",
                             KV_DEFAULT_SHM, KV_MAP_NAME_ENV ),
             * mb = get_arg( argc, argv, 1, "-s", "2048",  KV_MAP_SIZE_ENV ),
             * pc = get_arg( argc, argv, 1, "-k", "0.25",  KV_HT_RATIO_ENV ),
             * cu = get_arg( argc, argv, 1, "-c", "2+4",   KV_CUCKOO_ENV ),
             * mo = get_arg( argc, argv, 1, "-o", "ug+rw", KV_MAP_MODE_ENV ),
             * vz = get_arg( argc, argv, 1, "-v", "2048",  KV_VALUE_SIZE_ENV ),
             * ez = get_arg( argc, argv, 1, "-e", "64",    KV_ENTRY_SIZE_ENV ),
             * at = get_arg( argc, argv, 0, "-a", 0 ),
             * rm = get_arg( argc, argv, 0, "-r", 0 ),
             * iv = get_arg( argc, argv, 1, "-i", "1" ),
             * ix = get_arg( argc, argv, 1, "-x", "0.1" ),
             * he = get_arg( argc, argv, 0, "-h", 0 );

  if ( he != NULL ) {
  cmd_error:;
    fprintf( stderr, "raikv version: %s\n", kv_stringify( KV_VER ) );
    fprintf( stderr,
  "%s\n"
  "  -m map        = name of map file (" KV_DEFAULT_SHM ") (" KV_MAP_NAME_ENV ")\n"
  "  -s MB         = size of HT in MB (2048) (" KV_MAP_SIZE_ENV ")\n"
  "  -k ratio      = entry to value ratio (float 0 -> 1, 0.25) (" KV_HT_RATIO_ENV ")\n"
  "                 (1 = all ht, 0 = all msg -- must have some ht)\n"
  "  -c cuckoo a+b = cuckoo hash arity and buckets (2+4) (" KV_CUCKOO_ENV ")\n"
  "  -o mode       = create map using mode (ug+rw) (" KV_MAP_MODE_ENV ")\n"
  "  -v value-sz   = max value size in KB (2048) (" KV_VALUE_SIZE_ENV ")\n"
  "  -e entry-sz   = hash entry size (mult of 64, 64) (" KV_ENTRY_SIZE_ENV ")\n"
  "  -a            = attach to map, don't create (create)\n"
  "  -r            = remove map and then exit\n"
  "  -i secs       = stats interval (1)\n"
  "  -x secs       = check interval (0.1)\n",
             argv[ 0 ] );
    return 1;
  }

  mbsize = (uint64_t) ( strtod( mb, 0 ) * (double) ( 1024 * 1024 ) );
  if ( mbsize == 0 )
    goto cmd_error;
  ratio = strtod( pc, 0 );
  if ( ratio < 0.0 || ratio > 1.0 )
    goto cmd_error;
  /* look for arity+buckets */
  if ( isdigit( cu[ 0 ] ) && cu[ 1 ] == '+' && isdigit( cu[ 2 ] ) ) {
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
    geom.hash_value_ratio = (float) ratio;
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

  SignalHandler sighndl;
  Monitor svr( *map, stats_ival, check_ival );
  sighndl.install();

#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
  bool tty = false;
  if ( isatty( 0 ) ) {
    fcntl( 0, F_SETFL, fcntl( 0, F_GETFL, 0 ) | O_NONBLOCK );
    tty = true;
  }
  for (;;) {
    ssize_t nbytes;
    char    junk[ 8 ];
    if ( tty && (nbytes = read( 0, junk, sizeof( junk ) )) > 0 )
      svr.stats_counter = 0; /* print header again */
    svr.interval_update();
    usleep( 1000 /* 1 ms */);
    if ( sighndl.signaled )
      break;
  }
#else
  for (;;) {
    Sleep( 1 );

    svr.interval_update();
    if ( sighndl.signaled )
      break;
  }
#endif
  printf( "bye\n" );
  fflush( stdout );
  delete map;
  return 0;
}
