#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <assert.h>
#ifdef _MSC_VER
#include <raikv/win.h>
#else
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#endif

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
                                          /* 0123456789012345 */
const char HashTab::shared_mem_sig[ KV_SIG_SIZE /* 16 */ ]  = "rai 0.1 xxxxxxx";
static const int SHM_TYPE_IDX  = 8;
static const int SHM_TYPE_SIZE = 8;
static const char * shm_type[ 4 ][ 3 ] = {
  { "allc+4k", "allc+2m", "allc+1g" },
  { "file+4k", "file+2m", "file+1g" },
  { "posx+4k", "posx+2m", "posx+1g" },
  { "sysv+4k", "sysv+2m", "sysv+1g" }
};
static const uint8_t/*ALLOC_TYPE = 0, types that go in the SHM_TYPE_IDX pos: */
                     FILE_TYPE  = 1, /* 1g, 2m, 4k */
                     POSIX_TYPE = 2,
                     SYSV_TYPE  = 3,
                     P2M        = 1, /* shm_type[ SYSV_TYPE ][ P2M ] */
                     P1G        = 2,
                     P4K        = 0;

/* the constructor initializes the memory, only used in one thread context */
HashTab::HashTab( const char *map_name,  const HashTabGeom &geom ) noexcept
{
  this->initialize( map_name, geom );
}
/* zero the shm ht context, this does not determine whether some other
   thread is using it */
void
HashTab::initialize( const char *map_name,  const HashTabGeom &geom ) noexcept
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

  assert( sizeof( FileHdr ) == HT_FILE_HDR_SIZE );
  assert( sizeof( ThrCtx ) == HT_THR_CTX_SIZE );
  assert( sizeof( HashHdr ) == HT_HDR_SIZE );
  assert( sizeof( ThrCtx ) * MAX_CTX_ID == HT_CTX_SIZE );
  assert( sizeof( HashCounters ) * MAX_STAT_ID == HT_STATS_SIZE );

  ::memset( (void *) &this->hdr, 0, HT_HDR_SIZE );
  ::memcpy( this->hdr.sig, HashTab::shared_mem_sig, KV_SIG_SIZE );
  ::memset( (void *) &this->ctx[ 0 ], 0, HT_CTX_SIZE );
  ::memset( (void *) &this->stats[ 0 ], 0, HT_STATS_SIZE );

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

  data_area = geom.map_size - ( HT_HDR_SIZE + HT_CTX_SIZE + HT_STATS_SIZE );
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
  this->hdr.log2_ht_size    = (uint8_t) szlog2;
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
    seg_off  = HT_HDR_SIZE + HT_CTX_SIZE + HT_STATS_SIZE + tab_size;
    while ( ( seg_off >> this->hdr.seg_align_shift ) > ( (uint64_t) 1 << 32 ) )
      this->hdr.seg_align_shift++;
    /* calc segment size */
    seg_size = ( data_size / nsegs ) & ~( this->hdr.seg_align() - 1 ); /*floor*/
    while ( ( seg_size >> this->hdr.seg_align_shift ) > ( (uint64_t) 1 << 32 ) )
      this->hdr.seg_align_shift++;
    seg_size = ( data_size / nsegs ) & ~( this->hdr.seg_align() - 1 ); /*floor*/

    this->hdr.seg_start_val = (uint32_t) ( seg_off >> this->hdr.seg_align_shift );
    this->hdr.seg_size_val  = (uint32_t) ( seg_size >> this->hdr.seg_align_shift );
    this->hdr.seg[ 0 ].init( seg_off, seg_size );

    for ( i = 1; i < nsegs; i++ ) {
      seg_off += seg_size;
      this->hdr.seg[ i ].init( seg_off, seg_size );
    }
    /* initialize the message headers */
    for ( i = 0; i < nsegs; i++ ) {
      MsgHdr * msg_hdr = (MsgHdr *) this->seg_data( i, 0 );
      msg_hdr->size     = (uint32_t) seg_size;
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
    if ( nsegs > 0 )
      this->ctx[ j ].seg_num = (uint16_t) ( rng.next() % nsegs );
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
HashTab::alloc_map( HashTabGeom &geom ) noexcept
{
  void * p = ::malloc( geom.map_size );
  if ( p == NULL )
    return NULL;
  HashTab *ht = new ( p ) HashTab( "malloc()", geom );
  ::memcpy( &ht->hdr.sig[ SHM_TYPE_IDX ], shm_type[ 0 ][ 0 ], SHM_TYPE_SIZE );
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
/*static const uint64_t PGSZ_2M = 2 * 1024 * 1024,
                      PGSZ_1G = 1024 * 1024 * 1024;*/
static void
show_perror( const char *what,  const char *map_name )
{
  char buf[ 1024 ];
  size_t i = ::strlen( what );
  ::strcpy( buf, what );
  buf[ i++ ] = ':'; buf[ i++ ] = ' ';
  while ( i < sizeof( buf ) )
    if ( (buf[ i++ ] = *map_name++) == '\0' )
      break;
  buf[ 1023 ] = '\0';
  ::perror( buf );
}

#ifndef MAP_HUGETLB
/* flag set for page size */
#define MAP_HUGETLB 0x40000
#endif
#ifndef MAP_HUGE_SHIFT
/* mmap page size shift */
#define MAP_HUGE_SHIFT 26
#endif
static const int MAP_PAGE_2M = MAP_HUGETLB | ( 21 << MAP_HUGE_SHIFT ),
                 MAP_PAGE_1G = MAP_HUGETLB | ( 30 << MAP_HUGE_SHIFT );
#ifndef SHM_HUGE_SHIFT
/* sysv shm page size shift */
#define SHM_HUGE_SHIFT 26
#endif
#ifndef SHM_HUGETLB
/* flag set for page size */
#define SHM_HUGETLB 04000
#endif
static const int SHM_PAGE_2M = SHM_HUGETLB | ( 21 << SHM_HUGE_SHIFT ),
                 SHM_PAGE_1G = SHM_HUGETLB | ( 30 << SHM_HUGE_SHIFT );

/* XXX this needs synchronization so that clients don't attach before
   server initializes */
HashTab *
HashTab::create_map( const char *map_name,  uint8_t facility,
                     HashTabGeom &geom,  int map_mode /*0666*/) noexcept
{
  const char * fn = map_name;
  void       * p;
  HashTab    * ht;
  uint64_t     page_align,
               map_size;

#ifndef _MSC_VER
  page_align = (uint64_t) ::sysconf( _SC_PAGESIZE );
#else
  SYSTEM_INFO info;
  GetSystemInfo( &info );
  page_align = info.dwPageSize;
#endif

  map_size = align<uint64_t>( geom.map_size, page_align );
  assert( map_size >= geom.map_size );

  if ( facility == 0 && (facility = parse_map_name( fn )) == 0 )
    return NULL;

  if ( ::strlen( map_name ) + 1 > MAX_FILE_HDR_NAME_LEN ) {
    fprintf( stderr, "map name \"%s\" too large\n", map_name );
    return NULL;
  }

#ifndef _MSC_VER
  int    j,
         flags[ 3 ], /* flags for normal, 2m pages, 1g pages */
         oflags,
         fd,
         mode_flags,
         excl,
         huge;
  bool   is_file_mmap;

  for ( j = 0; j < 3; j++ )
    flags[ j ] = 0;
  
  /* need a pid file or a lck file fo exclusive access as server */
  /* if normal files */
  switch ( facility & ( KV_FILE_MMAP | KV_POSIX_SHM | KV_SYSV_SHM ) ) {
    default:
      fprintf( stderr, "create: bad facility 0x%x\n", facility );
      return NULL;

    case KV_FILE_MMAP:
    case KV_POSIX_SHM:
      is_file_mmap = ( facility & KV_FILE_MMAP ) != 0;
      /* create with 0666 */
      mode_flags  = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;
      mode_flags &= map_mode;
      excl        = O_CREAT | O_EXCL;
      /* try exclusive open first */
      oflags = O_RDWR;
      if ( is_file_mmap )
        fd = ::open( fn, oflags | excl, mode_flags );
      else
        fd = ::shm_open( fn, oflags | excl, mode_flags );
      if ( fd < 0 ) {
        if ( is_file_mmap )
          fd = ::open( fn, oflags, mode_flags );
        else
          fd = ::shm_open( fn, oflags, mode_flags );
        if ( fd < 0 ) {
          show_perror( "open", map_name );
          return NULL;
        }
        ::close( fd );
        if ( is_file_mmap )
          ::unlink( fn );
        else
          ::shm_unlink( fn );
        if ( is_file_mmap )
          fd = ::open( fn, oflags | excl, mode_flags );
        else
          fd = ::shm_open( fn, oflags | excl, mode_flags );
        if ( fd < 0 ) {
          show_perror( "open", map_name );
          return NULL;
        }
      }
      p = MAP_FAILED;
      if ( ::ftruncate( fd, map_size ) == -1 ) {
        show_perror( "ftruncate", map_name );
      }
      else {
        flags[ 0 ] = MAP_SHARED | MAP_POPULATE;
        if ( ( facility & KV_HUGE_2MB ) != 0 )
          flags[ 0 ] |= MAP_PAGE_2M;
        else if ( ( facility & KV_HUGE_1GB ) != 0 )
          flags[ 0 ] |= MAP_PAGE_1G;
        else {
          flags[ 1 ] = flags[ 0 ] | MAP_PAGE_2M;
          flags[ 2 ] = flags[ 0 ] | MAP_PAGE_1G;
        }
        /* try 1g, 2m, then normal */
        for ( j = 2; j >= 0; j-- ) {
          if ( flags[ j ] != 0 ) {
            p = ::mmap( 0, map_size, PROT_READ | PROT_WRITE, flags[ j ], fd, 0);
            if ( p != MAP_FAILED )
              break;
          }
        }
        if ( p == MAP_FAILED )
          show_perror( "mmap", map_name );
      }
      ::close( fd );
      if ( p == MAP_FAILED ) {
        if ( is_file_mmap )
          ::unlink( fn );
        else
          ::shm_unlink( fn );
        return NULL;
      }
      if ( ( flags[ j ] & MAP_PAGE_2M ) == MAP_PAGE_2M )
        huge = P2M;
      else if ( ( flags[ j ] & MAP_PAGE_1G ) == MAP_PAGE_1G )
        huge = P1G;
      else
        huge = P4K;
      break;

    case KV_SYSV_SHM: {
      key_t key;
      excl = IPC_CREAT | IPC_EXCL;
      /* create with 0666 */
      flags[ 0 ]  = SHM_R | SHM_W;
      flags[ 0 ] |= ( flags[ 0 ] >> 3 ) | ( flags[ 0 ] >> 6 );
      flags[ 0 ] &= map_mode;
      /* if not specified, try 1g, then 2m, finally 4k page sizes */
      if ( ( facility & KV_HUGE_2MB ) != 0 )
        flags[ 0 ] |= SHM_PAGE_2M;
      else if ( ( facility & KV_HUGE_1GB ) != 0 )
        flags[ 0 ] |= SHM_PAGE_1G;
      else {
        flags[ 1 ] = flags[ 0 ] | SHM_PAGE_2M;
        flags[ 2 ] = flags[ 0 ] | SHM_PAGE_1G;
      }
      key = (key_t) kv_crc_c( fn, fn == NULL ? 0 : ::strlen( fn ) + 1, 0);
      fd = -1;
      /* try 1g, 2m, then normal */
      for ( int tries = 0; ; tries++ ) {
        for ( j = 2; j >= 0; j-- ) {
          if ( flags[ j ] != 0 ) {         /* create shm id exclusive */
            fd = ::shmget( key, map_size, flags[ j ] | excl );
            if ( fd >= 0 )
              break;
          }
        }
        if ( fd < 0 ) {
          show_perror( "shmget", map_name );
          if ( tries > 0 )
            return NULL;
          fprintf( stderr, "Trying to remove: %s\n", map_name );
        }
        if ( fd < 0 ) {
          for ( j = 2; j >= 0; j-- ) {
            if ( flags[ j ] != 0 ) {        /* try getting without exclusive */
              fd = ::shmget( key, 0, flags[ j ] );
              if ( fd >= 0 )
                break;
            }
          }
          if ( fd >= 0 ) {
            ::shmctl( fd, IPC_RMID, NULL ); /* remove it first, then create it */
            fd = -1;
            for ( j = 2; j >= 0; j-- ) {
              if ( flags[ j ] != 0 ) {
                fd = ::shmget( key, map_size, flags[ j ] | excl );
                if ( fd >= 0 )
                  break;
              }
            }
          }
        }
        if ( fd >= 0 )
          break;
      }
      p = ::shmat( fd, NULL, 0 );
      if ( p == (void *) -1 ) {
        show_perror( "shmat", map_name );
        ::shmctl( fd, IPC_RMID, NULL );
        return NULL;
      }
      if ( ( flags[ j ] & SHM_PAGE_2M ) == SHM_PAGE_2M )
        huge = P2M;
      else if ( ( flags[ j ] & SHM_PAGE_1G ) == SHM_PAGE_1G )
        huge = P1G;
      else
        huge = P4K;
      break;
    }
  }
  ht = new ( p ) HashTab( map_name, geom );

  /* try to lock memory */
  if ( ::mlock( p, map_size ) != 0 )
    show_perror( "warning mlock", map_name );

  switch ( facility & ( KV_FILE_MMAP | KV_POSIX_SHM | KV_SYSV_SHM ) ) {
    case KV_SYSV_SHM:
      ::memcpy( &ht->hdr.sig[ SHM_TYPE_IDX ],
                shm_type[ SYSV_TYPE ][ huge ], SHM_TYPE_SIZE ); /* sysv based */
      break;
    case KV_FILE_MMAP:
      ::memcpy( &ht->hdr.sig[ SHM_TYPE_IDX ],
                shm_type[ FILE_TYPE ][ huge ], SHM_TYPE_SIZE ); /* sysv based */
      break;
    case KV_POSIX_SHM:
      ::memcpy( &ht->hdr.sig[ SHM_TYPE_IDX ],
                shm_type[ POSIX_TYPE ][ huge ], SHM_TYPE_SIZE ); /* sysv based */
      break;
  }
#else
  DWORD  szh = (DWORD) ( map_size >> ( sizeof( DWORD) * 8 ) ),
         szl = (DWORD) ( map_size & ~(DWORD) 0 );
  char   gbuf[ MAX_FILE_HDR_NAME_LEN + sizeof( "Local\\" ) ];
  ::snprintf( gbuf, sizeof( gbuf ), "Local\\%s", fn );
  HANDLE fh  = CreateFileMappingA( INVALID_HANDLE_VALUE, NULL, PAGE_READWRITE,
                                   szh, szl, gbuf );
  if ( fh == NULL ) {
    fprintf( stderr, "Can't create file mapping %s (%d)\n", fn,
             GetLastError() );
    return NULL;
  }
  p = MapViewOfFile( fh, FILE_MAP_ALL_ACCESS, 0, 0, map_size );
  if ( p == NULL ) {
    fprintf( stderr, "Can't map view of file %s (%d)\n", fn,
             GetLastError() );
    CloseHandle( fh );
    return NULL;
  }
  ht = new ( p ) HashTab( map_name, geom );
  ::memcpy( &ht->hdr.sig[ SHM_TYPE_IDX ], "WINDOWS", SHM_TYPE_SIZE );
#endif
  remove_closed( p );
  return ht;
}

/* XXX this needs synchronization so that clients don't attach before
   server initializes */
HashTab *
HashTab::attach_map( const char *map_name,  uint8_t facility,
                     HashTabGeom &geom ) noexcept
{
  const char * fn = map_name;
  void       * p;
  uint64_t     page_align,
               map_size;
  HashHdr      hdr;

#ifndef _MSC_VER
  page_align = (uint64_t) ::sysconf( _SC_PAGESIZE );
#else
  SYSTEM_INFO info;
  GetSystemInfo( &info );
  page_align = info.dwPageSize;
#endif

  if ( facility == 0 && (facility = parse_map_name( fn )) == 0 )
    return NULL;

#ifndef _MSC_VER
  int     fd, j, flags[ 3 ];
  key_t   key;
  bool    is_file_mmap;
  
  for ( j = 0; j < 3; j++ )
    flags[ j ] = 0;
  switch ( facility & ( KV_FILE_MMAP | KV_POSIX_SHM | KV_SYSV_SHM ) ) {
    default:
      fprintf( stderr, "attach: bad facility 0x%x\n", facility );
      return NULL;

    case KV_SYSV_SHM:
      /* create with 0666 */
      flags[ 0 ]  = SHM_R | SHM_W;
      /* if not specified, try 1g, then 2m, finally 4k page sizes */
      if ( ( facility & KV_HUGE_2MB ) != 0 )
        flags[ 0 ] |= SHM_PAGE_2M;
      else if ( ( facility & KV_HUGE_1GB ) != 0 )
        flags[ 0 ] |= SHM_PAGE_1G;
      else {
        flags[ 1 ] = flags[ 0 ] | SHM_PAGE_2M;
        flags[ 2 ] = flags[ 0 ] | SHM_PAGE_1G;
      }
      key = (key_t) kv_crc_c( fn, fn == NULL ? 0 : ::strlen( fn ) + 1, 0 );
      fd = -1;
      for ( int fail_count = 0;; fail_count++ ) {
        /* try 1g, 2m, then normal */
        for ( j = 2; j >= 0; j-- ) {
          if ( flags[ j ] != 0 ) {         /* create shm id exclusive */
            fd = ::shmget( key, 0, flags[ j ] );
            if ( fd >= 0 )
              break;
          }
        }
        if ( fd < 0 ) {
          if ( fail_count == 30 ) {
            show_perror( "shmget", map_name );
            return NULL;
          }
        }
        else {
          p = ::shmat( fd, NULL, 0 );
          if ( p != (void *) -1 )
            goto open_success;

          if ( fail_count == 30 ) {
            show_perror( "shmat", map_name );
            ::close( fd );
            return NULL;
          }
          ::close( fd );
          fd = -1;
        }
        ::usleep( 10000 ); /* 300 ms */
      }
    open_success:;
      for (;;) {
        ::memcpy( (void *) &hdr, p, sizeof( hdr ) );
        if ( hdr.sig[ SHM_TYPE_IDX ] != 'x' && hdr.sig[ SHM_TYPE_IDX ] != '\0' )
          break;
        ::usleep( 1 ); /* wait for initialize to finish */
      }
      map_size = align<uint64_t>( hdr.map_size, page_align );
      if ( ::memcmp( hdr.sig, HashTab::shared_mem_sig, SHM_TYPE_IDX ) != 0 ) {
        fprintf( stderr, "shm sig doesn't match: [%s][%s]",
                 HashTab::shared_mem_sig, hdr.sig );
        ::shmdt( p );
        ::close( fd );
        return NULL;
      }
      break;

    case KV_FILE_MMAP:
    case KV_POSIX_SHM:
      is_file_mmap = ( facility & KV_FILE_MMAP ) != 0;
      for ( int fail_count = 0; ; fail_count++ ) {
        if ( is_file_mmap )
          fd = ::open( fn, O_RDWR, S_IREAD | S_IWRITE );
        else
          fd = ::shm_open( fn, O_RDWR, S_IREAD | S_IWRITE );
        if ( fd >= 0 )
          goto open_success2;
        if ( fail_count == 30 ) {
          show_perror( "open", map_name );
          return NULL;
        }
        usleep( 10000 ); /* 300 ms */
      }
    open_success2:;
      for (;;) {
        if ( ::pread( fd, &hdr, sizeof( hdr ), 0 ) != sizeof( hdr ) ) {
          show_perror( "read", map_name );
          ::close( fd );
          return NULL;
        }
        if ( hdr.sig[ SHM_TYPE_IDX ] != 'x' && hdr.sig[ SHM_TYPE_IDX ] != '\0' )
          break;
        ::usleep( 1 ); /* wait for initialize to finish */
      }
      if ( ::memcmp( hdr.sig, HashTab::shared_mem_sig, SHM_TYPE_IDX ) != 0 ) {
        fprintf( stderr, "shm sig doesn't match: [%s][%s]",
                 HashTab::shared_mem_sig, hdr.sig );
        ::close( fd );
        return NULL;
      }
      map_size = align<uint64_t>( hdr.map_size, page_align );

      flags[ 0 ] = MAP_SHARED | MAP_POPULATE;
      if ( ( facility & KV_HUGE_2MB ) != 0 )
        flags[ 0 ] |= MAP_PAGE_2M;
      else if ( ( facility & KV_HUGE_1GB ) != 0 )
        flags[ 0 ] |= MAP_PAGE_1G;
      else {
        flags[ 1 ] = flags[ 0 ] | MAP_PAGE_2M;
        flags[ 2 ] = flags[ 0 ] | MAP_PAGE_1G;
      }
      /* try 1g, 2m, then normal */
      p = MAP_FAILED;
      for ( j = 2; j >= 0; j-- ) {
        if ( flags[ j ] != 0 ) {
          p = ::mmap( 0, map_size, PROT_READ | PROT_WRITE, flags[ j ], fd, 0);
          if ( p != MAP_FAILED )
            break;
        }
      }
      if ( p == MAP_FAILED )
        show_perror( "mmap", map_name );
      ::close( fd );
      if ( p == MAP_FAILED )
        return NULL;
      break;
  }
  ::mlock( p, map_size ); /* ignore the warning, the create() will show it */

#else
  char gbuf[ MAX_FILE_HDR_NAME_LEN + sizeof( "Local\\" ) ];
  ::snprintf( gbuf, sizeof( gbuf ), "Local\\%s", fn );
  HANDLE fh  = OpenFileMappingA( FILE_MAP_ALL_ACCESS, FALSE, gbuf );
  if ( fh == NULL ) {
    fprintf( stderr, "Can't open file mapping %s (%d)\n", fn,
             GetLastError() );
    return NULL;
  }
  uint64_t hdr_align = align<uint64_t>( sizeof( hdr ), page_align );
  p = MapViewOfFile( fh, FILE_MAP_ALL_ACCESS, 0, 0, hdr_align );
  if ( p == NULL ) {
    fprintf( stderr, "Can't map view of file %s (%d)\n", fn,
             GetLastError() );
    CloseHandle( fh );
    return NULL;
  }
  ::memcpy( &hdr, p, sizeof( hdr ) );
  map_size = align<uint64_t>( hdr.map_size, page_align );
  UnmapViewOfFile( p );

  p = MapViewOfFile( fh, FILE_MAP_ALL_ACCESS, 0, 0, map_size );
#endif

  geom.map_size         = hdr.map_size;
  geom.max_value_size   = hdr.max_value_size;
  geom.hash_entry_size  = hdr.hash_entry_size;
  geom.hash_value_ratio = hdr.hash_value_ratio;
  geom.cuckoo_buckets   = hdr.cuckoo_buckets;
  geom.cuckoo_arity     = hdr.cuckoo_arity;

  /*if ( ::mlock( p, map_size ) != 0 )*/
    /*show_perror( "warning: mlock()", map_name )*/;
  remove_closed( p );
  return (HashTab *) p;
}

int
HashTab::remove_map( const char *map_name,  uint8_t facility ) noexcept
{
  const char * fn = map_name;
  
  if ( facility == 0 && (facility = parse_map_name( fn )) == 0 )
    return -1;

#ifndef _MSC_VER
  int          fd, j, flags[ 3 ];
  key_t        key;
  bool         is_file_mmap;
  for ( j = 0; j < 3; j++ )
    flags[ j ] = 0;
  switch ( facility & ( KV_FILE_MMAP | KV_POSIX_SHM | KV_SYSV_SHM ) ) {
    default:
      fprintf( stderr, "remove: bad facility 0x%x\n", facility );
      return -1;

    case KV_SYSV_SHM:
      /* create with 0666 */
      flags[ 0 ]  = SHM_R | SHM_W;
      /* if not specified, try 1g, then 2m, finally 4k page sizes */
      if ( ( facility & KV_HUGE_2MB ) != 0 )
        flags[ 0 ] |= SHM_PAGE_2M;
      else if ( ( facility & KV_HUGE_1GB ) != 0 )
        flags[ 0 ] |= SHM_PAGE_1G;
      else {
        flags[ 1 ] = flags[ 0 ] | SHM_PAGE_2M;
        flags[ 2 ] = flags[ 0 ] | SHM_PAGE_1G;
      }
      key = (key_t) kv_crc_c( fn, fn == NULL ? 0 : ::strlen( fn ) + 1, 0 );
      fd = -1;
      /* try 1g, 2m, then normal */
      for ( j = 2; j >= 0; j-- ) {
        if ( flags[ j ] != 0 ) {         /* create shm id exclusive */
          fd = ::shmget( key, 0, flags[ j ] );
          if ( fd >= 0 )
            break;
        }
      }
      if ( fd < 0 ) {
        show_perror( "shmget", map_name );
        return -1;
      }
      if ( ::shmctl( fd, IPC_RMID, NULL ) != 0 ) {
        show_perror( "shmctl ipc_rmid", map_name );
        return -1;
      }
      return 0;

    case KV_FILE_MMAP:
    case KV_POSIX_SHM:
      is_file_mmap = ( facility & KV_FILE_MMAP ) != 0;
      if ( is_file_mmap ) {
        if ( ::unlink( fn ) != 0 ) {
          show_perror( "unlink", map_name );
        }
      }
      else {
        if ( ::shm_unlink( fn ) != 0 )
          show_perror( "shm_unlink", map_name );
      }
      return 0;
  }
#endif
  return 0;
}

void
HashTab::operator delete( void *ptr ) noexcept
{
  if ( ptr != NULL ) {
    if ( is_closed( ptr ) )
      return;
    if ( ((char *) ptr)[ SHM_TYPE_IDX ] != 'a' ) /* alloc */
      ((HashTab *) ptr)->close_map();
    else
      ::free( ptr );
  }
}

int
HashTab::close_map( void ) noexcept
{
  void * p = (void *) this;
  int    status = 0;
  if ( p == NULL )
    return -2;

#ifndef _MSC_VER
  uint64_t     page_align,
               map_size;
  const char * s = &this->hdr.sig[ SHM_TYPE_IDX ];

  page_align = (uint64_t) ::sysconf( _SC_PAGESIZE );
  map_size = align<uint64_t>( this->hdr.map_size, page_align );

  if ( ::munlock( p, map_size ) != 0 )
    ::perror( "warning: munlock()" );
  if ( s[ 0 ] == 'p' || s[ 0 ] == 'f' ) { /* posix or file */
    if ( ::munmap( p, map_size ) != 0 ) {
      ::perror( "warning: munmap()" );
      status = -1;
    }
  }
  else if ( s[ 0 ] == 's' ) { /* sysv */
    if ( ::shmdt( p ) != 0 ) {
      ::perror( "warning: shmdt()" );
      status = -1;
    }
  }
  else if ( s[ 0 ] != 'a' ) {
    fprintf( stderr, "bad close_map\n" );
  }
#else
  UnmapViewOfFile( p );
#endif
  add_closed( p );
  return status;
}

/* find a new thread entry, key should be non-zero */
uint32_t
HashTab::attach_ctx( uint64_t key ) noexcept
{
  const uint64_t bizyid = ZOMBIE64 | key;
  uint32_t i     = this->hdr.next_ctx.add( 1 ) % MAX_CTX_ID,
           start = i;
  uint64_t val;
  bool     second_time = false;

  if ( ( key & ZOMBIE64 ) != 0 )
    return KV_NO_CTX_ID;

  for (;;) {
    ThrCtx & el = this->ctx[ i ];
    while ( ( (val = el.key.xchg( bizyid )) & ZOMBIE64 ) != 0 )
      kv_sync_pause();
    /* keep used entries around for history, unless there are no more spots */
    if ( el.ctx_pid == 0 || ( second_time && el.ctx_id >= MAX_CTX_ID ) ) {
      el.zero();
      el.ctx_id     = i;
      el.ctx_pid    = ::getpid();
      el.ctx_thrid  = ::getthrid();
      el.ctx_flags  = 0;
      el.db_stat_hd = MAX_STAT_ID;
      el.db_stat_tl = MAX_STAT_ID;
      if ( ++el.ctx_seqno == 0 )
        el.ctx_seqno = 1;
      this->hdr.ctx_used.add( 1 );
      el.key.xchg( key );
      return el.ctx_id;
    }
    el.key.xchg( val ); /* unlock */
    i = ( i + 1 ) % MAX_CTX_ID;
    /* checked all slots */
    if ( i == start ) {
      if ( second_time )
        return KV_NO_CTX_ID;
      second_time = true;
    }
  }
}

uint32_t
HashTab::attach_db( uint32_t ctx_id,  uint8_t db ) noexcept
{
  ThrCtx      & el = this->ctx[ ctx_id ];
  ThrStatLink * link;
  uint32_t      i,
                j = el.db_stat_hd;

  for (;;) {
    if ( j == MAX_STAT_ID ) /* db not found */
      break;
    link = &this->hdr.stat_link[ j ];
    if ( link->db_num == db )
      return j;
    j = link->next;
  }
  this->hdr.set_db_opened( db );
  j = ctx_id;
  i = 0;
  for (;;) {
    link = &this->hdr.stat_link[ j ];
    while ( link->busy.xchg( 1 ) != 0 )
      kv_sync_pause();
    if ( link->used == 0 ) /* if found a free link */
      break;
    link->busy.xchg( 0 );
    if ( ++i == MAX_STAT_ID * 3 )
      return KV_NO_DBSTAT_ID;
    j += MAX_CTX_ID;       /* step by ctx */
    if ( j >= MAX_STAT_ID )
      j = ( j + 1 ) % MAX_STAT_ID;
  }
  link->used.xchg( 1 );
  link->ctx_id = ctx_id;
  link->db_num = db;
  link->next   = MAX_STAT_ID;
  link->back   = el.db_stat_tl;
  if ( el.db_stat_tl == MAX_STAT_ID )
    el.db_stat_hd = j;
  else {
    ThrStatLink & tl = this->hdr.stat_link[ el.db_stat_tl ];
    tl.next = j;
  }
  el.db_stat_tl = j;
  link->busy.xchg( 0 );
  return j;
}

void
HashTab::detach_db( uint32_t ctx_id,  uint8_t db ) noexcept
{
  ThrCtx      & el   = this->ctx[ ctx_id ];
  ThrStatLink * link = NULL;
  uint32_t      j    = el.db_stat_hd;

  for (;;) {
     if ( j == MAX_STAT_ID ) /* db not found */
       return;
     link = &this->hdr.stat_link[ j ];
     if ( link->db_num == db )
       break;
     j = link->next;
  }
  if ( link->back == MAX_STAT_ID )
    el.db_stat_hd = link->next;
  else {
    ThrStatLink & back = this->hdr.stat_link[ link->back ];
    back.next = link->next;
  }
  if ( link->next == MAX_STAT_ID )
    el.db_stat_tl = link->back;
  else {
    ThrStatLink & next = this->hdr.stat_link[ link->next ];
    next.back = link->back;
  }
  link->next = MAX_STAT_ID;
  link->back = MAX_STAT_ID;

  HashCounters & dbst = this->hdr.db_stat[ db ];
  HashCounters & stat = this->stats[ j ],
                 tmp  = stat;
  this->hdr.ht_spin_lock( db );
  stat.zero();
  dbst += tmp;
  this->hdr.ht_spin_unlock( db );

  link->used.xchg( 0 );
}

/* detach thread */
void
HashTab::detach_ctx( uint32_t ctx_id ) noexcept
{
  ThrCtx       & el     = this->ctx[ ctx_id ];
  const uint64_t bizyid = ZOMBIE64 | ctx_id;

  if ( ctx_id >= MAX_CTX_ID )
    return;

  while ( el.db_stat_hd != MAX_STAT_ID )
    this->detach_db( ctx_id, this->hdr.stat_link[ el.db_stat_hd ].db_num );
  while ( ( el.key.xchg( bizyid ) & ZOMBIE64 ) != 0 )
    kv_sync_pause();
  if ( ++el.ctx_seqno == 0 )
    el.ctx_seqno = 1;
  //el.stat.zero();
  el.ctx_id = KV_NO_CTX_ID;
  this->hdr.ctx_used.sub( 1 );
  el.key.xchg( 0 );
}

int
EvShm::open( const char *map_name,  uint8_t db_num ) noexcept
{
  if ( map_name == NULL || ::strcmp( map_name, "none" ) == 0 )
    return 0;
  HashTabGeom geom;
  this->map = HashTab::attach_map( map_name, 0, geom );
  if ( this->map != NULL )
    return this->attach( db_num );
  return -1;
}

int
EvShm::create( const char *map_name,  kv_geom_t *geom,  int map_mode,
               uint8_t db_num ) noexcept
{
  kv_geom_t default_geom;
  if ( geom == NULL ) {
    ::memset( &default_geom, 0, sizeof( default_geom ) );
    geom = &default_geom;
  }
  if ( geom->map_size == 0 )
    geom->map_size = (uint64_t) 1024*1024*1024;
  if ( geom->hash_value_ratio <= 0.0 || geom->hash_value_ratio > 1.0 )
    geom->hash_value_ratio = 0.25;
  if ( geom->hash_value_ratio < 1.0 ) {
    uint64_t value_space = (uint64_t) ( (double) geom->map_size *
                                        ( 1.0 - geom->hash_value_ratio ) );
    if ( geom->max_value_size == 0 || geom->max_value_size > value_space / 3 ) {
      const uint64_t sz = 256 * 1024 * 1024;
      uint64_t d  = 8; /* 8 = 512MB/64MB, 8 = 1G/128MB, 16 = 2G/256G */
      geom->max_value_size = (uint32_t) ( value_space / d );
      while ( d < 128 && geom->max_value_size / 2 > sz ) {
        geom->max_value_size /= 2;
        d *= 2;
      }
    }
  }
  geom->hash_entry_size = 64;
  geom->cuckoo_buckets  = 2;
  geom->cuckoo_arity    = 4;
  if ( map_mode == 0 )
    map_mode = 0660;
  this->map = HashTab::create_map( map_name, 0, *geom, map_mode );
  if ( this->map != NULL )
    return this->attach( db_num );
  return -1;
}

void
EvShm::print( void ) noexcept
{
  if ( this->map != NULL ) {
    fputs( print_map_geom( this->map, this->ctx_id ), stdout );
    HashTabStats * hts = HashTabStats::create( *this->map );
    hts->fetch();
    for ( uint32_t db = 0; db < DB_COUNT; db++ ) {
      if ( this->map->hdr.test_db_opened( db ) ) {
        printf( "db[ %u ].entry_cnt:%s %" PRIu64 "\n", db,
                ( ( db < 10 ) ? "   " : ( ( db < 100 ) ? "  " : " " ) ),
                hts->db_stats[ db ].last.add -
                hts->db_stats[ db ].last.drop );
      }
    }
    ::free( hts );
    fflush( stdout );
  }
}

EvShm::~EvShm() noexcept
{
  /* should explicity close instead */
  /*if ( this->map != NULL )
    this->close();*/
}

int
EvShm::attach( uint8_t db_num ) noexcept
{
  /* centos don't have gettid() */
  if ( this->map == NULL )
    return -1;
  uint64_t k;
  k = ::getthrid();
  this->ctx_id = this->map->attach_ctx( k );
  if ( this->ctx_id != MAX_CTX_ID ) {
    this->dbx_id = this->map->attach_db( this->ctx_id, db_num );
    return 0;
  }
  return -1;
}

void
EvShm::detach( void ) noexcept
{
  if ( this->map != NULL ) {
    if ( this->ctx_id != MAX_CTX_ID ) {
      this->map->detach_ctx( this->ctx_id );
      this->ctx_id = MAX_CTX_ID;
    }
  }
}

void
EvShm::close( void ) noexcept
{
  if ( this->map != NULL ) {
    this->detach();
    delete this->map;
    this->map = NULL;
  }
}

extern "C" {
kv_hash_tab_t *
kv_alloc_map( kv_geom_t *geom )
{
  return (kv_hash_tab_t *) (void *)
         rai::kv::HashTab::alloc_map( *geom );
}

kv_hash_tab_t *
kv_create_map( const char *map_name,  uint8_t facility,  kv_geom_t *geom,
               int map_mode )
{
  return (kv_hash_tab_t *) (void *)
         rai::kv::HashTab::create_map( map_name, facility, *geom, map_mode );
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
kv_attach_ctx( kv_hash_tab_t *ht,  uint64_t key )
{
  return reinterpret_cast<HashTab *>( ht )->attach_ctx( key );
}

void
kv_detach_ctx( kv_hash_tab_t *ht,  uint32_t ctx_id )
{
  reinterpret_cast<HashTab *>( ht )->detach_ctx( ctx_id );
}

uint32_t
kv_attach_db( kv_hash_tab_t *ht,  uint32_t ctx_id,  uint8_t db )
{
  return reinterpret_cast<HashTab *>( ht )->attach_db( ctx_id, db );
}

void
kv_detach_db( kv_hash_tab_t *ht,  uint32_t ctx_id,  uint8_t db )
{
  reinterpret_cast<HashTab *>( ht )->detach_db( ctx_id, db );
}

uint64_t
kv_map_get_size( kv_hash_tab_t *ht )
{
  return reinterpret_cast<HashTab *>( ht )->hdr.ht_size;
}
}

