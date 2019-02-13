#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/syscall.h>

#include <raikv/shm_ht.h>
#include <raikv/util.h>

using namespace rai;
using namespace kv;

static const int SHM_CL_SZ = 32;
static void *shm_closed[ SHM_CL_SZ ];
static unsigned int shm_closed_idx;

/* prevent multiple munmap() via HashTab::close_map() called multiple times */
static bool is_closed( void *p ) {
  for ( int i = 0; i < SHM_CL_SZ; i++ )
    if ( shm_closed[ i ] == p )
      return true;
  return false;
}
static void add_closed( void *p ) {
  if ( ! is_closed( p ) ) {
    shm_closed[ shm_closed_idx++ ] = p;
    shm_closed_idx %= SHM_CL_SZ;
  }
}
static void remove_closed( void *p ) {
  for ( int i = 0; i < SHM_CL_SZ; i++ ) {
    if ( shm_closed[ i ] == p )
      shm_closed[ i ] = NULL;
  }
}

/* the constructor initializes the memory, only used in one thread context */
HashTab::HashTab( const char *map_name,  const HashTabGeom &geom )
{
  this->initialize( map_name, geom );
}
                                          /* 01234567890123456 */
const char HashTab::shared_mem_sig[ 16 ]  = "rai.kv.tab 0.1";
static const int SHM_TYPE_IDX = 14;
static const char ALLOC_TYPE = 'a', /* types that go in the SHM_TYPE_IDX pos */
                  FILE_TYPE  = 'f', /* also g, h for huge 2m, huge 1g */
                  POSIX_TYPE = 'p', /* also q, r */
                  SYSV_TYPE  = 'v'; /* also w, x */
/* zero the shm ht context, this does not determine whether some other
   thread is using it */
void
HashTab::initialize( const char *map_name,  const HashTabGeom &geom )
{
  uint64_t tab_size,  /* mem used by ht[] */
           data_size, /* mem used by segment[] */
           sz,        /* used for pseudo prime calculation */
           data_area, /* memory size minus the headers */
           seg_off,   /* init seg[] boundaries */
           seg_size,  /* size of each segment, must be large enough for max */
           max_sz,    /* maximum entry size */
           mask,
           fraction,
           max_idx,
           el_cnt;
  uint32_t i, j,
           nsegs;     /* number of seg[] entries */
  uint16_t szlog2;
  uint8_t  shift;
#if 0
  printf( "FileHdr %lu == %lu\n", sizeof( FileHdr ), HT_FILE_HDR_SIZE );
  printf( "ThrCtx %lu == %lu\n", sizeof( ThrCtx ), HT_THR_CTX_SIZE );
  printf( "HashHdr %lu == %lu\n", sizeof( HashHdr ), HT_HDR_SIZE );
  printf( "ThrCtx %lu * %lu == %lu\n", sizeof( ThrCtx ), MAX_CTX_ID, HT_CTX_SIZE );
#endif
  assert( sizeof( FileHdr ) == HT_FILE_HDR_SIZE );
  assert( sizeof( ThrCtx ) == HT_THR_CTX_SIZE );
  assert( sizeof( HashHdr ) == HT_HDR_SIZE );
  assert( sizeof( ThrCtx ) * MAX_CTX_ID == HT_CTX_SIZE );

  ::memset( (void *) &this->hdr, 0, HT_HDR_SIZE );
  ::memset( (void *) &this->ctx[ 0 ], 0, HT_CTX_SIZE );

  /* sig is set later, it indicates the mapping type (malloc, posix, sysv) */
  ::strcpy( this->hdr.name, map_name ); /* length checked before calling */
  this->hdr.last_entry_count = 0;
  this->hdr.hash_value_ratio = geom.hash_value_ratio;
  this->hdr.critical_load    = 90;
  this->hdr.create_stamp     = current_realtime_ns();
  this->hdr.current_stamp    = this->hdr.create_stamp;
  this->hdr.map_size         = geom.map_size;
  this->hdr.hash_entry_size  = geom.hash_entry_size;
  this->hdr.max_value_size   = geom.max_value_size;
  this->hdr.cuckoo_buckets   = geom.cuckoo_buckets;
  this->hdr.cuckoo_arity     = geom.cuckoo_arity;

  data_area = geom.map_size - ( HT_HDR_SIZE + HT_CTX_SIZE );
  el_cnt    = (uint64_t) ( geom.hash_value_ratio * (double) data_area ) /
                           (uint64_t) geom.hash_entry_size;
  sz = el_cnt;
  //printf( "stash %lu\n", el_cnt - sz );
  for ( szlog2 = 1; ( (uint64_t) 1 << szlog2 ) < sz; szlog2++ )
    ;
  assert( sz > 0 );
  tab_size  = sz * (uint64_t) geom.hash_entry_size;
  data_size = data_area - tab_size;
  /* check for overflow of the ( hash & mask ) bits for the mod calculation
   * 30 bits works up to 16 * 10^9, or 1<<34 ( 30 + 34 = 64 ), that would be
   * a ht mem size of 1tb ( 16gb * 64 or 1<<34 * 1<<6 == 1<<40 ) */
  for ( shift = 30; shift > 1; shift-- ) {
    mask     = ( (uint64_t) 1 << szlog2 ) - 1;
    fraction = (uint64_t) ( ( (double) sz / (double) mask ) * 
                              (double) ( (uint64_t) 1 << shift ) );
    max_idx  = ( mask * fraction ) >> shift;
    if ( max_idx > sz / 2 ) {
      /* example of case where fraction is too large:
       *  sz == 7, mask == 7, fraction = 0x40000000,
       *  max_idx == ( 7 * 0x40000000 ) >> 30 == 7, and
       *  max_idx == ( 7 * 0x3fffffff ) >> 30 == 6 */
      if ( max_idx == sz )
        fraction--;
      break;
    }
  }

  assert( max_idx > sz / 2 );
  assert( data_size < data_area );
  assert( tab_size <= data_area );

  this->hdr.ht_size         = sz;      /* number of entries */
  this->hdr.log2_ht_size    = szlog2;
  this->hdr.ht_mod_shift    = shift;
  this->hdr.ht_mod_mask     = mask;
  this->hdr.ht_mod_fraction = fraction;
  this->hdr.seg_align_shift = 6;       /* 1 << 6 = 64 */

  if ( geom.max_value_size == 0 )
    nsegs = 0;
  else {
    max_sz = (uint64_t) geom.max_value_size + 63 /* pad */ +
                        sizeof( MsgHdr ) + 16; /* 16 = key size */
    max_sz = align<uint64_t>( max_sz, this->hdr.seg_align() );

    /* make sure that the bits can be stored */
    while ( ( max_sz >> this->hdr.seg_align_shift ) >
            ( (uint64_t) 1 << ValuePtr::VALUE_SIZE_BITS ) )
      this->hdr.seg_align_shift++;

    /* make each segment enough to hold at least 8 items */
    nsegs = HashHdr::SHM_MAX_SEG_COUNT;  /* max number of segs */
    while ( nsegs > 0 && data_size / nsegs < max_sz )
      nsegs--;
    while ( nsegs > 16 && data_size / nsegs < max_sz * 16 )
      nsegs--;
    assert( nsegs > 0 ); /* at least 1 segs */
  }
  this->hdr.nsegs = nsegs;

  if ( nsegs > 0 ) {
    /* calculate the segment offsets */
    seg_off  = HT_HDR_SIZE + HT_CTX_SIZE + tab_size;
    while ( ( seg_off >> this->hdr.seg_align_shift ) > ( (uint64_t) 1 << 32 ) )
      this->hdr.seg_align_shift++;
    /* calc segment size */
    seg_size = ( data_size / nsegs ) & ~( this->hdr.seg_align() - 1 ); /*floor*/
    while ( ( seg_size >> this->hdr.seg_align_shift ) > ( (uint64_t) 1 << 32 ) )
      this->hdr.seg_align_shift++;
    seg_size = ( data_size / nsegs ) & ~( this->hdr.seg_align() - 1 ); /*floor*/

    this->hdr.seg_start_val = ( seg_off >> this->hdr.seg_align_shift );
    this->hdr.seg_size_val  = ( seg_size >> this->hdr.seg_align_shift );
    this->hdr.seg[ 0 ].init( seg_off, seg_size );

    for ( i = 1; i < nsegs; i++ ) {
      seg_off += seg_size;
      this->hdr.seg[ i ].init( seg_off, seg_size );
    }
    /* initialize the message headers */
    for ( i = 0; i < nsegs; i++ ) {
      MsgHdr * msg_hdr = (MsgHdr *) this->seg_data( i, 0 );
      msg_hdr->size     = seg_size;
      msg_hdr->msg_size = 0;
      msg_hdr->release();
    }
  }
  else {
    seg_size = 0;
    this->hdr.seg_start_val = 0;
    this->hdr.seg_size_val  = 0;
  }
  /* init rand elems */
  uint64_t buf[ MAX_CTX_ID * 2 + DB_COUNT * 2 ];
  rand::fill_urandom_bytes( buf, sizeof( buf ) );
  i = 0;
  for ( j = 0; j < MAX_CTX_ID; j++ ) {
    rand::xoroshiro128plus &rng = this->ctx[ j ].rng;
    rng.init( (void *) &buf[ i ], sizeof( uint64_t ) * 2 );
    i += 2;
    this->ctx[ j ].seg_num = rng.next() % nsegs;
  }
  for ( j = 0; j < DB_COUNT; j++ ) {
    this->hdr.seed[ j ].hash1 = buf[ i ];
    this->hdr.seed[ j ].hash2 = buf[ i + 1 ];
    i += 2;
  }

  if ( nsegs > 0 ) {
    /* check that ht doesn't overflow into seg data */
    assert( (uint8_t *) (void *) this->get_entry( this->hdr.ht_size ) <=
            (uint8_t *) this->seg_data( 0, 0 ) );
    /* check that seg data doesn't overflow map size */
    assert( (uint8_t *) this->seg_data( nsegs, 0 ) <=
            &((uint8_t *) (void *) this)[ geom.map_size ] );
  }
  /* check hash_entry_size is valid */
  assert( geom.hash_entry_size % 64 == 0 );

  this->hdr.max_immed_value_size = geom.hash_entry_size -
    ( sizeof( HashEntry ) + sizeof( ValueCtr ) );
  if ( nsegs > 0 ) {
    assert( this->hdr.seg_size() > sizeof( MsgHdr ) );
    this->hdr.max_segment_value_size = this->hdr.seg_size() -
      ( sizeof( MsgHdr ) + sizeof( ValueCtr ) ); /* msg hdr */
  }
  /* zero the ht[] array */
  sz = (uint64_t) geom.hash_entry_size * this->hdr.ht_size;
  ::memset( this->get_entry( 0 ), 0, sz );
}

HashTab *
HashTab::alloc_map( HashTabGeom &geom )
{
  void * p = ::malloc( geom.map_size );
  if ( p == NULL )
    return NULL;
  HashTab *ht = new ( p ) HashTab( "malloc()", geom );
  ::memcpy( ht->hdr.sig, HashTab::shared_mem_sig, sizeof( ht->hdr.sig ) );
  ht->hdr.sig[ SHM_TYPE_IDX ] = ALLOC_TYPE;
  return ht;
}

static uint8_t
parse_map_name( const char *&fn )
{
  uint8_t facility = 0, i = 0;

  /* one of file:  file2m:  file1g:
   *        sysv:  sysv2m:  sysv1g:
   *        posix: posix2m: posix1g: */
  if ( fn != NULL ) {
    if ( ::strncmp( fn, "file", 4 ) == 0 ) {
      facility = KV_FILE_MMAP;
      i = 4;
    }
    else if ( ::strncmp( fn, "sysv", 4 ) == 0 ) {
      facility = KV_SYSV_SHM;
      i = 4;
    }
    else if ( ::strncmp( fn, "posix", 5 ) == 0 ) {
      facility = KV_POSIX_SHM;
      i = 5;
    }
    if ( i > 0 ) {
      if ( ::strncmp( &fn[ i ], ":", 1 ) == 0 ) {
        fn = &fn[ i + 1 ];
        return facility;
      }
      if ( ::strncmp( &fn[ i ], "1g:", 3 ) == 0 ) {
        fn = &fn[ i + 3 ];
        return facility | KV_HUGE_1GB;
      }
      if ( ::strncmp( &fn[ i ], "2m:", 3 ) == 0 ) {
        fn = &fn[ i + 3 ];
        return facility | KV_HUGE_2MB;
      }
    }
  }
  fprintf( stderr, "Default to file mmap for map name \"%s\"\n", fn );
  return KV_FILE_MMAP;
}
/* 2m uses traditional huge pages (2003)
 * 1g uses gigabyte size huges pages (2.6.18)
 * https://lwn.net/Articles/374424/ */
static const uint64_t PGSZ_2M = 2 * 1024 * 1024,
                      PGSZ_1G = 1024 * 1024 * 1024;
static int do_file_open( const char *f,  int fl,  mode_t m ) {
  return open( f, fl, m );
}

/* XXX this needs synchronization so that clients don't attach before
   server initializes */
HashTab *
HashTab::create_map( const char *map_name,  uint8_t facility,
                     HashTabGeom &geom )
{
  const char * fn         = map_name;
  uint64_t     page_align = (uint64_t) ::sysconf( _SC_PAGESIZE ),
               map_size;
  int          oflags, fd;
  void       * p;
  
  if ( facility == 0 && (facility = parse_map_name( fn )) == 0 )
    return NULL;
  if ( ::strlen( map_name ) + 1 > MAX_FILE_HDR_NAME_LEN ) {
    fprintf( stderr, "map name \"%s\" too large\n", map_name );
    return NULL;
  }

  if ( ( facility & KV_HUGE_2MB ) != 0 )
    page_align = align<uint64_t>( page_align, PGSZ_2M );

  if ( ( facility & KV_HUGE_1GB ) != 0 )
    page_align = align<uint64_t>( page_align, PGSZ_1G );

  /* need a pid file or a lck file fo exclusive access as server */
  map_size = align<uint64_t>( geom.map_size, page_align );
  assert( map_size >= geom.map_size );

  /* create with 0666 */
  int create_flags = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;
  int (*do_open)( const char *, int, mode_t ) = NULL;
  int (*do_unlink)( const char * ) = NULL;

  if ( ( facility & KV_SYSV_SHM ) == 0 ) {
    if ( ( facility & KV_FILE_MMAP ) != 0 ) {
      do_open   = ::do_file_open;
      do_unlink = ::unlink;
    }
    else {
      do_open   = ::shm_open;
      do_unlink = ::shm_unlink;
    }

    /* try exclusive open first, this may be important for NUMA mapping */
    oflags = O_RDWR;
    fd     = do_open( fn, oflags | O_CREAT | O_EXCL, create_flags );
    if ( fd < 0 ) {
      fd = do_open( fn, oflags, create_flags );
      if ( fd < 0 ) {
        ::perror( "shm_open" );
        return NULL;
      }
      ::close( fd );
      if ( do_unlink( fn ) < 0 ) {
        ::perror( "shm_unlink" );
        return NULL;
      }
      fd = do_open( fn, oflags | O_CREAT | O_EXCL, create_flags );
      if ( fd < 0 ) {
        perror( "shm_open" );
        return NULL;
      }
    }
  }
  else {
#ifndef SHM_HUGE_SHIFT
#define SHM_HUGE_SHIFT 26
#endif
    /* create with 0666 */
    int flags = SHM_R | SHM_W |
                ( (SHM_R|SHM_W) >> 3 ) | ( (SHM_R|SHM_W) >> 6 );
    if ( ( facility & KV_HUGE_2MB ) != 0 )
      flags |= ( SHM_HUGETLB | (21 << SHM_HUGE_SHIFT) );
    else if ( ( facility & KV_HUGE_1GB ) != 0 )
      flags |= ( SHM_HUGETLB | (30 << SHM_HUGE_SHIFT) );
    key_t key = (key_t) kv_crc_c( fn, fn == NULL ? 0 : ::strlen( fn ) + 1, 0 );
    fd = ::shmget( key, map_size, flags | IPC_CREAT | IPC_EXCL );
    /* try removing the existing and create again */
    if ( fd < 0 ) {
      fd = ::shmget( key, 0, flags );
      if ( fd < 0 ) {
        ::perror( "shmget" );
        return NULL;
      }
      ::shmctl( fd, IPC_RMID, NULL );
      fd = ::shmget( key, map_size, flags | IPC_CREAT | IPC_EXCL );
      if ( fd < 0 ) {
        ::perror( "shmget" );
        return NULL;
      }
    }
  }

  if ( ( facility & KV_SYSV_SHM ) == 0 ) {
    if ( ::ftruncate( fd, map_size ) == -1 ) {
      ::perror( "ftruncate" );
      ::close( fd );
      do_unlink( fn );
      return NULL;
    }
#ifndef MAP_HUGE_SHIFT
#define MAP_HUGE_SHIFT 26
#endif
#ifndef MAP_HUGETLB
#define MAP_HUGETLB 0x40000
#endif
    int flags = MAP_SHARED | MAP_POPULATE;
    if ( ( facility & KV_HUGE_2MB ) != 0 )
      flags |= ( MAP_HUGETLB | (21 << MAP_HUGE_SHIFT) );
    else if ( ( facility & KV_HUGE_1GB ) != 0 )
      flags |= ( MAP_HUGETLB | (30 << MAP_HUGE_SHIFT) );
    p = ::mmap( 0, map_size, PROT_READ | PROT_WRITE, flags, fd, 0 );
    ::close( fd );
    if ( p == MAP_FAILED ) {
      ::perror( "mmap" );
      do_unlink( fn );
      return NULL;
    }
  }
  else {
    p = ::shmat( fd, NULL, 0 );
    if ( p == (void *) -1 ) {
      ::perror( "shmat" );
      ::shmctl( fd, IPC_RMID, NULL );
      return NULL;
    }
  }

  if ( ::mlock( p, map_size ) != 0 )
    perror( "warning: mlock()" );
  HashTab *ht = new ( p ) HashTab( map_name, geom );
  ::memcpy( ht->hdr.sig, HashTab::shared_mem_sig, sizeof( ht->hdr.sig ) );
  char huge = 0;
  if ( ( facility & KV_HUGE_2MB ) != 0 )
    huge = 1;
  else if ( ( facility & KV_HUGE_1GB ) != 0 )
    huge = 2;
  if ( ( facility & KV_SYSV_SHM ) == 0 ) {
    if ( ( facility & KV_FILE_MMAP ) != 0 )
      ht->hdr.sig[ SHM_TYPE_IDX ] = FILE_TYPE + huge; /* file based */
    else
      ht->hdr.sig[ SHM_TYPE_IDX ] = POSIX_TYPE + huge; /* posix based */
  }
  else {
    ht->hdr.sig[ SHM_TYPE_IDX ] = SYSV_TYPE + huge; /* sysv based */
  }
  remove_closed( p );
  return ht;
}

/* XXX this needs synchronization so that clients don't attach before
   server initializes */
HashTab *
HashTab::attach_map( const char *map_name,  uint8_t facility,
                     HashTabGeom &geom )
{
  const char * fn = map_name;
  HashHdr      hdr;
  uint64_t     page_align = (uint64_t) ::sysconf( _SC_PAGESIZE ),
               map_size;
  void       * p;
  int          oflags, fd;
  
  if ( facility == 0 && (facility = parse_map_name( fn )) == 0 )
    return NULL;

  if ( ( facility & KV_HUGE_2MB ) != 0 )
    page_align = align<uint64_t>( page_align, PGSZ_2M );

  if ( ( facility & KV_HUGE_1GB ) != 0 )
    page_align = align<uint64_t>( page_align, PGSZ_1G );

  if ( ( facility & ( KV_FILE_MMAP | KV_SYSV_SHM ) ) == 0 ) {
    oflags = O_RDWR;
    fd     = ::shm_open( fn, oflags, S_IREAD | S_IWRITE );
    if ( fd < 0 ) {
      ::perror( "shm_open" );
      return NULL;
    }
  }
  else if ( ( facility & KV_FILE_MMAP ) != 0 ) {
    oflags = O_RDWR;
    fd     = ::open( fn, oflags, S_IREAD | S_IWRITE );
    if ( fd < 0 ) {
      ::perror( "open" );
      return NULL;
    }
  }
  else {
#ifndef SHM_HUGE_SHIFT
#define SHM_HUGE_SHIFT 26
#endif
    int flags = SHM_R | SHM_W;
    if ( ( facility & KV_HUGE_2MB ) != 0 )
      flags |= ( SHM_HUGETLB | (21 << SHM_HUGE_SHIFT) );
    else if ( ( facility & KV_HUGE_1GB ) != 0 )
      flags |= ( SHM_HUGETLB | (30 << SHM_HUGE_SHIFT) );
    key_t key = (key_t) kv_crc_c( fn, fn == NULL ? 0 : ::strlen( fn ) + 1, 0 );
    fd = ::shmget( key, 0, flags );
    if ( fd < 0 ) {
      ::perror( "shmget" );
      return NULL;
    }
  }

  if ( ( facility & KV_SYSV_SHM ) == 0 ) {
    if ( ::read( fd, &hdr, sizeof( hdr ) ) != sizeof( hdr ) ) {
       perror( "read" );
      ::close( fd );
      return NULL;
    }
    if ( ::memcmp( hdr.sig, HashTab::shared_mem_sig, SHM_TYPE_IDX ) != 0 ) {
      fprintf( stderr, "shm sig doesn't match: [%s][%s]",
               HashTab::shared_mem_sig, hdr.sig );
      ::close( fd );
      return NULL;
    }
    map_size = align<uint64_t>( hdr.map_size, page_align );
#ifndef MAP_HUGE_SHIFT
#define MAP_HUGE_SHIFT 26
#endif
#ifndef MAP_HUGETLB
#define MAP_HUGETLB 0x40000
#endif
    int flags = MAP_SHARED | MAP_POPULATE;
    if ( ( facility & KV_HUGE_2MB ) != 0 )
      flags |= ( MAP_HUGETLB | (21 << MAP_HUGE_SHIFT) );
    else if ( ( facility & KV_HUGE_1GB ) != 0 )
      flags |= ( MAP_HUGETLB | (30 << MAP_HUGE_SHIFT) );
    p = ::mmap( 0, map_size, PROT_READ | PROT_WRITE, flags, fd, 0 );
    ::close( fd );
    if ( p == MAP_FAILED ) {
      ::perror( "mmap" );
      return NULL;
    }
  }
  else {
    p = ::shmat( fd, NULL, 0 );
    if ( p == (void *) -1 ) {
      ::perror( "shmat" );
      ::shmctl( fd, IPC_RMID, NULL );
      return NULL;
    }
    ::memcpy( (void *) &hdr, p, sizeof( hdr ) );
    map_size = align<uint64_t>( hdr.map_size, page_align );
    if ( ::memcmp( hdr.sig, HashTab::shared_mem_sig, SHM_TYPE_IDX ) != 0 ) {
      fprintf( stderr, "shm sig doesn't match: [%s][%s]",
               HashTab::shared_mem_sig, hdr.sig );
      ::shmdt( p );
      return NULL;
    }
  }
  geom.map_size         = hdr.map_size;
  geom.max_value_size   = hdr.max_value_size;
  geom.hash_entry_size  = hdr.hash_entry_size;
  geom.hash_value_ratio = hdr.hash_value_ratio;
  geom.cuckoo_buckets   = hdr.cuckoo_buckets;
  geom.cuckoo_arity     = hdr.cuckoo_arity;

  if ( ::mlock( p, map_size ) != 0 )
    perror( "warning: mlock()" );
  remove_closed( p );
  return (HashTab *) p;
}

void
HashTab::operator delete( void *ptr )
{
  if ( ptr != NULL ) {
    if ( is_closed( ptr ) )
      return;
    if ( ((char *) ptr)[ SHM_TYPE_IDX ] != 'a' )
      ((HashTab *) ptr)->close_map();
    else
      ::free( ptr );
  }
}

int
HashTab::close_map( void )
{
  uint64_t page_align = (uint64_t) ::sysconf( _SC_PAGESIZE ),
           map_size;
  void   * p = (void *) this;
  char     huge, type = 0;

  if ( p == NULL )
    return -2;
  huge = this->hdr.sig[ SHM_TYPE_IDX ];
  switch ( huge ) {
    case FILE_TYPE: case FILE_TYPE+1: case FILE_TYPE+2:
      type = FILE_TYPE;
      break;
    case POSIX_TYPE: case POSIX_TYPE+1: case POSIX_TYPE+2:
      type = POSIX_TYPE;
      break;
    case SYSV_TYPE: case SYSV_TYPE+1: case SYSV_TYPE+2:
      type = SYSV_TYPE;
      break;
    default:
      perror( "no type" );
      return -1;
  }
  if ( huge == type + 1 )
    page_align = align<uint64_t>( page_align, PGSZ_2M );
  else if ( huge == type + 2 )
    page_align = align<uint64_t>( page_align, PGSZ_1G );
  map_size = align<uint64_t>( this->hdr.map_size, page_align );

  if ( ::munlock( p, map_size ) != 0 )
    perror( "warning: munlock()" );
  int status = 0;
  if ( type == FILE_TYPE || type == POSIX_TYPE ) {
    if ( ::munmap( p, map_size ) != 0 ) {
      perror( "warning: munmap()" );
      status = -1;
    }
  }
  else if ( ::shmdt( p ) != 0 ) {
    perror( "warning: shmdt()" );
    status = -1;
  }
  add_closed( p );
  return status;
}

/* find a new thread entry, key should be non-zero */
uint32_t
HashTab::attach_ctx( uint32_t key,  uint8_t db_num1,  uint8_t db_num2 )
{
  ThrCtx * el;
  uint32_t i     = this->hdr.next_ctx.add( 1 ) % MAX_CTX_ID,
           start = i,
           val;
  const uint32_t bizyid = ZOMBIE32 | key;
  bool second_time      = false;

  if ( ( key & ZOMBIE32 ) != 0 )
    return KV_NO_CTX_ID;

  for (;;) {
    if ( i != 0 ) { /* reserve ctx[ 0 ] for global accum */
      el = &this->ctx[ i ];
      while ( ( (val = el->key.xchg( bizyid )) & ZOMBIE32 ) != 0 )
        kv_sync_pause();
      /* keep used entries around for history, unless there are no more spots */
      if ( el->ctx_pid == 0 || ( second_time && el->ctx_id >= MAX_CTX_ID ) ) {
        el->zero();
        el->ctx_id    = el - this->ctx;
        el->ctx_pid   = ::getpid();
        el->ctx_thrid = ::syscall( SYS_gettid );
        el->db_num1   = db_num1;
        el->db_num2   = db_num2;
        if ( ++el->ctx_seqno == 0 )
          el->ctx_seqno = 1;
        this->hdr.ctx_used.add( 1 );
        el->key.xchg( key );
        return el->ctx_id;
      }
      el->key.xchg( val ); /* unlock */
    }
    i = ( i + 1 ) % MAX_CTX_ID;
    /* checked all slots */
    if ( i == start ) {
      if ( second_time )
        return KV_NO_CTX_ID;
      second_time = true;
    }
  }
}

void
HashTab::retire_ht_thr_stats( uint32_t ctx_id )
{
  ThrCtx       & el      = this->ctx[ ctx_id ],
               & base    = this->ctx[ 0 ];
  uint8_t        db_num1 = el.db_num1;
  HashCounters & db_stat = this->hdr.stat[ db_num1 ];
  const uint32_t bizyid  = ZOMBIE32 | ctx_id;
  uint32_t       val, val2;

  if ( ctx_id >= MAX_CTX_ID || ctx_id == 0 )
    return;

  this->hdr.ht_spin_lock( db_num1 );
  while ( ( (val = base.key.xchg( bizyid )) & ZOMBIE32 ) != 0 )
    kv_sync_pause();
  while ( ( (val2 = el.key.xchg( bizyid )) & ZOMBIE32 ) != 0 )
    kv_sync_pause();
  base.stat1 += el.stat1;
  db_stat += el.stat1;
  if ( ++el.ctx_seqno == 0 )
    el.ctx_seqno = 1;
  el.stat1.zero();
  el.key.xchg( val2 );
  base.key.xchg( val );
  this->hdr.ht_spin_unlock( db_num1 );
}

/* detach thread */
void
HashTab::detach_ctx( uint32_t ctx_id )
{
  ThrCtx       & el       = this->ctx[ ctx_id ],
               & base     = this->ctx[ 0 ];
  uint8_t        db_num1  = el.db_num1,
                 db_num2  = el.db_num2;
  HashCounters & db_stat1 = this->hdr.stat[ db_num1 ],
               & db_stat2 = this->hdr.stat[ db_num2 ];
  const uint32_t bizyid   = ZOMBIE32 | ctx_id;

  if ( ctx_id >= MAX_CTX_ID || ctx_id == 0 )
    return;

  if ( db_num1 != db_num2 ) {
    this->hdr.ht_spin_lock( db_num2 );
    db_stat2 += el.stat2;
    this->hdr.ht_spin_unlock( db_num2 );
  }

  this->hdr.ht_spin_lock( db_num1 );
  while ( ( base.key.xchg( bizyid ) & ZOMBIE32 ) != 0 )
    kv_sync_pause();
  while ( ( el.key.xchg( bizyid ) & ZOMBIE32 ) != 0 )
    kv_sync_pause();
  db_stat1 += el.stat1;
  if ( db_num1 == db_num2 )
    db_stat1 += el.stat2;
  base.stat1 += el.stat1;
  base.stat1 += el.stat2;
  if ( ++el.ctx_seqno == 0 )
    el.ctx_seqno = 1;
  //el.stat.zero();
  el.ctx_id = KV_NO_CTX_ID;
  this->hdr.ctx_used.sub( 1 );
  el.key.xchg( 0 );
  base.key.xchg( 0 );
  this->hdr.ht_spin_unlock( db_num1 );
}

extern "C" {
kv_hash_tab_t *
kv_alloc_map( kv_geom_t *geom )
{
  return (kv_hash_tab_t *) (void *)
         rai::kv::HashTab::alloc_map( *geom );
}

kv_hash_tab_t *
kv_create_map( const char *map_name,  uint8_t facility,  kv_geom_t *geom )
{
  return (kv_hash_tab_t *) (void *)
         rai::kv::HashTab::create_map( map_name, facility, *geom );
}

kv_hash_tab_t *
kv_attach_map( const char *map_name,  uint8_t facility,  kv_geom_t *geom )
{
  return (kv_hash_tab_t *) (void *)
         rai::kv::HashTab::attach_map( map_name, facility, *geom );
}

void
kv_close_map( kv_hash_tab_t *ht )
{
  reinterpret_cast<HashTab *>( ht )->close_map();
}

float
kv_update_load( kv_hash_tab_t *ht )
{
  reinterpret_cast<HashTab *>( ht )->update_load();
  return reinterpret_cast<HashTab *>( ht )->hdr.current_load();
}

uint32_t
kv_attach_ctx( kv_hash_tab_t *ht,  uint32_t key,  uint8_t db_num1,
               uint8_t db_num2 )
{
  return reinterpret_cast<HashTab *>( ht )->attach_ctx( key, db_num1, db_num2 );
}

void
kv_detach_ctx( kv_hash_tab_t *ht,  uint32_t ctx_id )
{
  reinterpret_cast<HashTab *>( ht )->detach_ctx( ctx_id );
}

uint64_t
kv_map_get_size( kv_hash_tab_t *ht )
{
  return reinterpret_cast<HashTab *>( ht )->hdr.ht_size;
}
}

