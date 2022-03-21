#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <string.h>
#include <ctype.h>
#ifndef _MSC_VER
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#else
#include <raikv/win.h>
#endif
#include <raikv/shm_ht.h>
#include <raikv/radix_sort.h>

static kv_hash_tab_t * ht;
static uint8_t db_num;
extern void print_map_geom_c( kv_hash_tab_t *map, uint32_t ctx_id );

#define MAX_THR 64
#define MAX_TOKEN_SIZE 256
#define CTX_COUNT 24

typedef struct {
  uint64_t hash, hash2;
  kv_key_frag_t *frag;
} xh_t;

typedef struct {
  char str[ 256 * 1024 ];
  char buf[ 64 * 1024 ];
  xh_t xh[ 16 * 1024 ];
  kv_key_ctx_t    * ctx[ CTX_COUNT ];
  kv_work_alloc_t * wrk;

  kv_atom_uint64_t ready,
                   consumed;
  size_t   count,
           dup_count;

  uint32_t ctx_id,
           dbx_id,
           frag_count;

  kv_atom_uint8_t  done,
                   running;
  uint16_t         id;
  uint64_t         status_cnt[ KEY_MAX_STATUS ];
} thr_data_t;

thr_data_t *thr[ MAX_THR ];
int testing = 0;
#if 0
static int isutf( int c ) {
  return (uint8_t) c > 127 && ( c & 0xC0 ) != 0;
}
#endif
#if 0
int
cmp_frag_hash( const void *p1,  const void *p2 )
{
  int64_t n = ((xh_t *) p1)->hash - ((xh_t *) p2)->hash;
  return n == 0 ? 0 : ( n < 0 ? -1 : 1 );
}
#endif
uint32_t
next_pos( void )
{
  static kv_atom_uint32_t val;
  return kv_sync_add32( &val, 1 );
}

void
process_key_frags( thr_data_t *t )
{
  size_t i, j, k, fail;
  uint32_t dup[ CTX_COUNT ], src[ CTX_COUNT ];

  for ( i = 0; i < t->frag_count; i++ )
    kv_hash_key_frag( ht, t->xh[ i ].frag, &t->xh[ i ].hash, &t->xh[ i ].hash2 );

  if ( testing == 2 ) {
    t->frag_count = 0;
    return;
  }

  /*qsort( t->xh, i, sizeof( t->xh[ 0 ] ), cmp_frag_hash );*/
  /* kv_ht_sort_t is generic version of xh_t */
  kv_ht_radix_sort( (kv_ht_sort_t *) (void *) t->xh, (uint32_t) i, ht );

  if ( testing == 1 ) {
    t->frag_count = 0;
    return;
  }

  for ( k = 1; k < i; k++ ) {
    if ( t->xh[ k - 1 ].hash == t->xh[ k ].hash &&
         t->xh[ k - 1 ].hash2 == t->xh[ k ].hash2 ) {
      t->xh[ k - 1 ].hash = 0;
      t->dup_count++;
    }
  }
  j = 0;
  for ( i = 0; ; ) {
    for ( k = j; k < CTX_COUNT; k++ )
      dup[ k ] = 1;
    while ( j < CTX_COUNT ) {
      if ( i == t->frag_count )
        break;
      if ( t->xh[ i ].hash != 0 ) {
        kv_set_key( t->ctx[ j ], t->xh[ i ].frag );
        kv_set_hash( t->ctx[ j ], t->xh[ i ].hash, t->xh[ i ].hash2 );
        kv_key_prefetch( t->ctx[ j ], 0 );
        src[ j ] = (uint32_t) i;
        j++;
      }
      else {
        dup[ j ]++;
      }
      i++;
    }
    for (;;) {
      fail = 0;
      for ( k = 0; k < j; k++ ) {
        kv_key_status_t status = kv_try_acquire( t->ctx[ k ], t->wrk );
        t->status_cnt[ status ]++;
        if ( status <= KEY_IS_NEW ) {
          typedef struct {
            uint32_t count;
            /*uint32_t pos;*/
            uint16_t id;
          } kv_data_t;
          kv_data_t *d;
          if ( kv_resize( t->ctx[ k ], &d, sizeof( kv_data_t ) ) == KEY_OK ) {
            if ( status == KEY_IS_NEW ) {
              d->count = 0;
              /*d->pos   = next_pos();*/
              d->id    = t->id;
            }
            d->count += dup[ k ];
          }
          kv_release( t->ctx[ k ] );
        }
        else if ( status != KEY_HT_FULL ) {
          if ( k > fail ) {
            kv_set_key( t->ctx[ fail ], t->xh[ src[ k ] ].frag );
            kv_set_hash( t->ctx[ fail ], t->xh[ src[ k ] ].hash,
                         t->xh[ src[ k ] ].hash2 );
            src[ fail ] = src[ k ];
          }
          fail++;
        }
      }
      j = fail;
      if ( j < CTX_COUNT )
        break;
      kv_sync_pause();
    }
    if ( j == 0 && i == t->frag_count )
      break;
  }
  t->frag_count = 0;
}

void
process_data( thr_data_t *t )
{
  kv_key_frag_t * frag;
  char          * in,
                * out,
                * end,
                * w,
                * ens;
  int64_t         i;
  size_t          n;
  char          * p;
  uint32_t        j;

  t->ctx_id = kv_attach_ctx( ht, t->id );
  t->dbx_id = kv_attach_db( ht, t->ctx_id, db_num );
  for ( j = 0; j < CTX_COUNT; j++ )
    t->ctx[ j ] = kv_create_key_ctx( ht, t->dbx_id );
  t->wrk = kv_create_ctx_alloc( 8 * 1024, NULL, NULL, 0 );
  memset( t->status_cnt, 0, sizeof( t->status_cnt ) );

  for (;;) {
    while ( t->ready == t->consumed && ! t->done )
      kv_sync_pause();
    if ( t->ready == t->consumed && t->done )
      break;

    n   = t->ready - t->consumed;
    i   = 0;
    w   = t->str;
    in  = t->buf;
    out = t->buf;
    end = &t->buf[ sizeof( t->buf ) ];
    ens = &t->str[ n ];
    t->frag_count = 0;
    for ( p = t->str; ; p++ ) {
      if ( p < ens && ! ( *p == ' ' || *p == '\n' || *p == '\t' ) )
        i++;
#if 0
      if ( p < ens && ( isalpha( *p ) || isdigit( *p ) || isutf( *p ) ) )
        i++;
#endif
      else {
        if ( i > 0 ) {
          if ( i < MAX_TOKEN_SIZE ) {
            if ( t->frag_count == sizeof( t->xh ) / sizeof( t->xh[ 0 ] ) )
              process_key_frags( t );
            frag = kv_make_key_frag( (uint16_t)( i + 1 ), end - out, in, &out );
            if ( frag == NULL ) {
              process_key_frags( t );
              in   = t->buf;
              out  = t->buf;
              frag = kv_make_key_frag( (uint16_t)( i + 1 ), end - out, in,
                                       &out );
            }
            w = p - i;
            kv_set_key_frag_string( frag, w, (uint16_t) i );
            t->xh[ t->frag_count++ ].frag = frag;
            t->count++;
            in = out;
          }
          i = 0;
        }
        if ( p == ens )
          break;
      }
    }
    t->consumed = t->ready;
    if ( t->frag_count > 0 )
      process_key_frags( t );
  }
  kv_detach_ctx( ht, t->ctx_id );
  t->running = 0;
}

void
create_thr_data( uint16_t num_thr )
{
  uint16_t i;
  for ( i = 0; i < num_thr; i++ ) {
    thr[ i ] = (thr_data_t *) malloc( sizeof( thr_data_t ) );
    thr[ i ]->count    = 0;
    thr[ i ]->ready    = 0;
    thr[ i ]->consumed = 0;
    thr[ i ]->done     = 0;
    thr[ i ]->running  = 1;
    thr[ i ]->id       = i;
  }
}

#ifndef _MSC_VER
void *
pthr_process( void *p )
{
  process_data( (thr_data_t *) p );
  return NULL;
}
#else
DWORD WINAPI
win_process( void *p )
{
  process_data( (thr_data_t *) p );
  return 0;
}
#endif

void
start_threads( uint32_t num_thr )
{
  uint32_t i;
#ifndef _MSC_VER
  pthread_t thrid;
  for ( i = 0; i < num_thr; i++ )
    pthread_create( &thrid, NULL, pthr_process, thr[ i ] );
#else
  for ( i = 0; i < num_thr; i++ )
    CreateThread( NULL, 0, win_process, thr[ i ], 0, NULL );
#endif
}

void
process_input_data( const char *fn,  uint32_t num_thr )
{
  ssize_t x = 0, n = 0;
  uint32_t i;
#ifndef _MSC_VER
  int fd = ( fn == NULL ? 0 : open( fn, O_RDONLY ) );
  if ( fd < 0 ) {
    perror( fn );
    return;
  }
#else
  HANDLE fd;
  DWORD  nbytes;
  if ( fn == NULL )
    fd = GetStdHandle( STD_INPUT_HANDLE );
  else
    fd = CreateFileA( fn, GENERIC_READ, 0, NULL, OPEN_EXISTING,
                      FILE_ATTRIBUTE_NORMAL, NULL );
  if ( fd == INVALID_HANDLE_VALUE) {
    fprintf(stderr, "%s: Error %u", fn, GetLastError() );
    return;
  }
#endif
  create_thr_data( num_thr );
  start_threads( num_thr );

  for ( i = 0; i < num_thr; i++ ) {
#ifndef _MSC_VER
    n = read( fd, thr[ i ]->str, sizeof( thr[ i ]->str ) );
#else
    n = 0;
    if ( ReadFile( fd, thr[ i ]->str, sizeof( thr[ i ]->str ), &nbytes, NULL ) )
      n = nbytes;
#endif
    if ( n <= 0 )
      break;
    kv_sync_add64( &thr[ i ]->ready, n );
  }

  while ( n > 0 ) {
    for ( i = 0; i < num_thr; i++ ) {
      if ( thr[ i ]->ready == thr[ i ]->consumed ) {
#ifndef _MSC_VER
        n = read( fd, thr[ i ]->str, sizeof( thr[ i ]->str ) );
#else
        n = 0;
        if ( ReadFile( fd, thr[ i ]->str, sizeof( thr[ i ]->str ), &nbytes,
                       NULL ) )
          n = nbytes;
#endif
        if ( n <= 0 )
          break;
        kv_sync_add64( &thr[ i ]->ready, n );
      }
    }
    kv_sync_pause();
  }

  do {
    x = 0;
    for ( i = 0; i < num_thr; i++ ) {
      x += thr[ i ]->running;
      if ( thr[ i ]->running )
        kv_sync_xchg8( &thr[ i ]->done, 1 );
    }
    kv_sync_pause();
  } while ( x > 0 );

  for ( i = 0; i < num_thr; i++ ) {
    int j;
    printf( "[%d] %" PRIu64 " words, %" PRIu64 " bytes, %" PRIu64 " dup -- ",
            i, thr[ i ]->count, thr[ i ]->consumed, thr[ i ]->dup_count );
    for ( j = 0; j < KEY_MAX_STATUS; j++ ) {
      if ( thr[ i ]->status_cnt[ j ] != 0 ) {
        const char *s = kv_key_status_string( (kv_key_status_t) j );
        printf( "%s:%" PRIu64 " ", &s[ 4 ], thr[ i ]->status_cnt[ j ] );
      }
    }
    printf( "\n" );
  }
#ifndef _MSC_VER
  if ( fn != NULL )
    close( fd );
#else
  if ( fn != NULL )
    CloseHandle( fd );
#endif
}

static const char *
get_arg( int argc, char *argv[], int b, const char *f, const char *def )
{
  int i;
  for ( i = 1; i < argc - b; i++ )
    if ( strcmp( f, argv[ i ] ) == 0 )
      return argv[ i + b ];
  return def; /* default value */
}

int
main( int argc,  char *argv[] )
{
  kv_geom_t geom;
  uint32_t num_thr = 8;

  /* [sysv2m:shm.test] [4] [0] [0] */
  const char * mn = get_arg( argc, argv, 1, "-m", KV_DEFAULT_SHM ),
             * nt = get_arg( argc, argv, 1, "-t", "8" ),
             * db = get_arg( argc, argv, 1, "-d", "0" ),
             * te = get_arg( argc, argv, 1, "-x", "0" ),
             * fn = get_arg( argc, argv, 1, "-f", NULL ),
             * he = get_arg( argc, argv, 0, "-h", 0 );

  if ( he != NULL ) {
  cmd_error:;
    fprintf( stderr, "raikv version: %s\n", kv_stringify( KV_VER ) );
    fprintf( stderr,
  "%s [-m map] [-t num-thr] [-d db-num] [-f file] [-x testing]\n"
  "  -m map      = name of map file (prefix w/ file:, sysv:, posix:)\n"
  "  -t num-thr  = number of worker threads to utilize\n"
  "  -d db-num   = database number to use\n"
  "  -f filename = filename to read\n"
  "  -x testing  = debug testing\n", argv[ 0 ] );
    return 1;
  }
  testing = atoi( te );
  db_num = (uint8_t) atoi( db );
  if ( (num_thr = atoi( nt )) == 0 )
    goto cmd_error;

  ht = kv_attach_map( mn, 0, &geom );
  if ( ht == NULL )
    perror( mn );
  fputs( kv_print_map_geom( ht, KV_MAX_CTX_ID, NULL, 0 ), stdout );
  process_input_data( fn, num_thr );
  kv_close_map( ht );
  return 0;
}

