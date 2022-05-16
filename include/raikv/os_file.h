#ifndef __rai_raikv__os_file_h__
#define __rai_raikv__os_file_h__

#ifdef _MSC_VER
#pragma warning( disable : 4291 4996 )
#include <windows.h>
#include <io.h>
#include <direct.h>
#include <fcntl.h>
#include <sys/stat.h>
#else
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <fcntl.h>
#endif

namespace rai {
namespace kv {

enum MapFileFlags {
  MAP_FILE_RDONLY  = 1,
  MAP_FILE_RDWR    = 2,
  MAP_FILE_PRIVATE = 4,
  MAP_FILE_LOCK    = 8,
  MAP_FILE_SECURE  = 16,
  MAP_FILE_NOUNMAP = 32,
  MAP_FILE_CREATE  = 64,
  MAP_FILE_SHM     = 128
};

static inline bool map_readonly( int how ) {
  return ( how & MAP_FILE_RDONLY ) != 0;
}
static inline bool map_readwrite( int how ) {
  return ( how & MAP_FILE_RDWR ) != 0;
}
static inline bool map_private( int how ) {
  return ( how & MAP_FILE_PRIVATE ) != 0;
}
static inline bool map_lock( int how ) {
  return ( how & MAP_FILE_LOCK ) != 0;
}
static inline bool map_secure( int how ) {
  return ( how & MAP_FILE_SECURE ) != 0;
}
static inline bool map_nounmap( int how ) {
  return ( how & MAP_FILE_NOUNMAP ) != 0;
}
static inline bool map_create( int how ) {
  return ( how & MAP_FILE_CREATE ) != 0;
}
static inline bool map_shm( int how ) {
  return ( how & MAP_FILE_SHM ) != 0;
}

#ifdef _MSC_VER

struct MapFile {
  const char * path;
  void       * map;
  size_t       map_size;
  HANDLE       h,
               maph;
  bool         no_unmap;

  MapFile( const char *p = NULL,  size_t sz = 0 )
    : path( p ), map( NULL ), map_size( sz ), h( INVALID_HANDLE_VALUE ),
      maph( NULL ), no_unmap( false ) {}
  ~MapFile() {
    this->close();
  }
  void close( void ) {
    if ( this->map != NULL ) {
      if ( ! this->no_unmap ) {
        UnmapViewOfFile( this->map );
        this->map = NULL;
      }
    }
    if ( this->h != INVALID_HANDLE_VALUE ) {
      CloseHandle( this->h );
      this->h = INVALID_HANDLE_VALUE;
    }
    if ( this->maph != NULL ) {
      CloseHandle( this->maph );
      this->maph = NULL;
    }
  }
  static void unmap( void *p,  size_t len ) {
    UnmapViewOfFile( p );
  }
  bool open( int how = ( MAP_FILE_RDONLY | MAP_FILE_PRIVATE ) ) {
    DWORD  szh     = 0,
           szl     = 0;
    DWORD  page    = PAGE_READONLY,
           mapping = FILE_MAP_READ;
    size_t map_sz  = 0;

    this->no_unmap = map_nounmap( how );
    if ( this->path != NULL ) {
      DWORD access = GENERIC_READ,
            mode   = FILE_SHARE_READ;
      LARGE_INTEGER st;
      if ( ! map_readonly( how ) ) {
        access |= GENERIC_WRITE;
        mode   |= FILE_SHARE_WRITE;
      }
      if ( map_private( how ) )
        mode = 0; /* no sharing */
      this->h = CreateFileA( this->path, access, mode, NULL,
                             OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL );
      if ( this->h == INVALID_HANDLE_VALUE ) {
        fprintf( stderr, "err open %s: %u\n", this->path, GetLastError() );
        return false;
      }
      GetFileSizeEx( this->h, &st );
      this->map_size = st.QuadPart;
    }
    else {
      this->map_size = align_page( this->map_size );
      szh = (DWORD) ( this->map_size >> 32 );
      szl = (DWORD) this->map_size;
    }
    if ( ! map_readonly( how ) ) {
      page    = PAGE_READWRITE;
      mapping = FILE_MAP_ALL_ACCESS;
      map_sz  = this->map_size;
    }
    this->maph = CreateFileMappingA( this->h, NULL, page, szh, szl, NULL );
    if ( this->maph == NULL ) {
      fprintf( stderr, "err map %s: %u\n", this->path ? this->path : "anon",
               GetLastError() );
      return false;
    }
    this->map = MapViewOfFile( this->maph, mapping, 0, 0, map_sz );
    if ( this->map == NULL ) {
      fprintf( stderr, "err view %s: %u\n", this->path ? this->path : "anon",
               GetLastError() );
      return false;
    }
    if ( map_lock( how ) )
      return mem_lock( this->map, this->map_size, map_secure( how ) );
    return true;
  }
  static bool mem_lock( void *map,  size_t map_size,  bool secure ) {
    bool res = true;
    for ( int i = 0; i < 2; i++ ) {
      if ( ! VirtualLock( map, map_size ) ) {
        res = false;
        if ( i == 0 ) {
          HANDLE h = GetCurrentProcess();
          size_t min_size, max_size;
          if ( GetProcessWorkingSetSize( h, &min_size, &max_size ) ) {
            if ( min_size + map_size + (size_t) 0x9000 > max_size )
              max_size = min_size + map_size + (size_t) 0x9000;
            SetProcessWorkingSetSizeEx( h, min_size + map_size, max_size,
                                        QUOTA_LIMITS_HARDWS_MIN_ENABLE |
                                          QUOTA_LIMITS_HARDWS_MAX_DISABLE );
          }
        }
      }
      else {
        res = true;
        break;
      }
    }
    if ( ! res )
      fprintf( stderr, "err virtual lock: %u\n", GetLastError() );
    return res;
  }
  static size_t page_size( void ) {
    SYSTEM_INFO info;
    GetSystemInfo( &info );
    return (size_t) info.dwPageSize;
  }
  static size_t align_page( size_t len ) {
    size_t page_align = page_size();
    return ( len + ( page_align - 1 ) ) & ~( page_align - 1 );
  }
};

#ifndef kv_ssizet_defined
#define kv_ssizet_defined
typedef ptrdiff_t ssize_t;
#endif
#ifndef kv_iovec_defined
#define kv_iovec_defined
struct iovec {
  void * iov_base;
  size_t iov_len;
};
#endif

#define STDIN_FILENO _fileno( stdin )
#define STDOUT_FILENO _fileno( stdout )
typedef struct _stat64 os_stat;

static inline int
os_open( const char *path, int oflag, int mode ) noexcept
{
  return _open( path, oflag | _O_BINARY, mode );
}
static inline ssize_t
os_write( int fd, const void *buf, size_t count ) noexcept
{
  return _write( fd, buf, (uint32_t) count );
}
static inline ssize_t
os_writev( int fd, const struct iovec *iov, size_t iovcnt ) noexcept
{
  ssize_t sum = 0;
  for ( size_t i = 0; i < iovcnt; i++ ) {
    int n = _write( fd, iov[ i ].iov_base, (uint32_t) iov[ i ].iov_len );
    if ( n > 0 )
      sum += n;
    if ( n < (int) iov[ i ].iov_len )
      break;
  }
  if ( sum > 0 )
    return sum;
  return -1;
}
static inline ssize_t
os_read( int fd, void *buf, size_t nbyte ) noexcept
{
  return (ssize_t) _read( fd, buf, (uint32_t) nbyte );
}
static inline int
os_fstat( int fd, os_stat *statbuf ) noexcept
{
  return _fstat64( fd, statbuf );
}
static inline int
os_fstat( const char *path, os_stat *statbuf ) noexcept
{
  return _stat64( path, statbuf );
}
static inline int
os_close( int fd ) noexcept
{
  return _close( fd );
}
static inline int
os_unlink( const char *fn ) noexcept
{
  return ::remove( fn );
}
#define F_OK 0
#define W_OK 02
#define R_OK 04
static inline int
os_access( const char *fn,  int mode ) noexcept
{
  return _access( fn, mode );
}
static inline int
os_mkdir( const char *dn,  int mode ) noexcept
{
  return _mkdir( dn );
}
static inline int
os_rmdir( const char *dn ) noexcept
{
  return _rmdir( dn );
}
static inline int
os_rename( const char *oldname,  const char *newname ) noexcept
{
  return rename( oldname, newname );
}
static inline int
os_dup( int fd ) noexcept
{
  return _dup( fd );
}

#else

struct MapFile {
  static const int ugo_mode = 0666;
  const char * path;
  void       * map;
  size_t       map_size;
  int          fd;
  bool         no_unmap,
               is_new;

  MapFile( const char *p = NULL,  size_t sz = 0 )
    : path( p ), map( NULL ), map_size( sz ), fd( -1 ), no_unmap( false ),
      is_new( false ) {}
  ~MapFile() {
    this->close();
  }
  void close( void ) {
    if ( this->map != NULL ) {
      if ( ! this->no_unmap ) {
        ::munmap( this->map, this->map_size );
        this->map = NULL;
      }
    }
    if ( this->fd != -1 ) {
      ::close( this->fd );
      this->fd = -1;
    }
  }
  static void unmap( void *p,  size_t len ) {
    ::munmap( p, align_page( len ) );
  }
  static int unlink( const char *path,  bool is_shm ) {
    if ( ! is_shm )
      return ::unlink( path );
    return ::shm_unlink( path );
  }
  bool open( int how = ( MAP_FILE_RDONLY | MAP_FILE_PRIVATE ) ) {
    struct stat st;
    int  prot      = PROT_READ,
         type      = MAP_SHARED;
    bool is_shm    = map_shm( how );
    this->no_unmap = map_nounmap( how );

    if ( this->path != NULL ) {
      int flags = O_RDONLY;
      if ( ! map_readonly( how ) )
        flags = O_RDWR;
      if ( map_create( how ) ) {
        int create_flags = flags | O_CREAT | O_EXCL;
        if ( ! is_shm )
          this->fd = ::open( this->path, create_flags, ugo_mode );
        else
          this->fd = ::shm_open( this->path, create_flags, ugo_mode );
        if ( this->fd >= 0 && this->map_size > 0 ) {
          if ( ::ftruncate( this->fd, this->map_size ) == -1 ) {
            ::close( this->fd );
            return false;
          }
          this->is_new = true;
        }
        else if ( this->fd < 0 ) {
          if ( ! is_shm )
            this->fd = ::open( this->path, flags, ugo_mode );
          else
            this->fd = ::shm_open( this->path, flags, ugo_mode );
          if ( this->fd < 0 )
            return false;
        }
      }
      else {
        if ( ! is_shm )
          this->fd = ::open( this->path, flags, ugo_mode );
        else
          this->fd = ::shm_open( this->path, flags, ugo_mode );
        if ( this->fd < 0 )
          return false;
      }
      if ( ::fstat( this->fd, &st ) != 0 )
        return false;
      this->map_size = st.st_size;
    }
    else {
      this->map_size = align_page( this->map_size );
    }
    if ( ! map_readonly( how ) )
      prot |= PROT_WRITE;
    if ( this->path == NULL )
      type = MAP_ANONYMOUS;
    if ( map_private( how ) )
      type = ( type & ~MAP_SHARED ) | MAP_PRIVATE;
    this->map = ::mmap( 0, this->map_size, prot, type, this->fd, 0 );
    if ( this->map == MAP_FAILED ) {
      this->map = NULL;
      return false;
    }
    if ( map_readonly( how ) )
      ::madvise( this->map, this->map_size, MADV_SEQUENTIAL );
    if ( map_lock( how ) )
      return mem_lock( this->map, this->map_size, map_secure( how ) );
    return true;
  }
  static bool mem_lock( void *map,  size_t map_size,  bool secure ) {
    bool res = true;
    if ( secure ) {
      if ( ::madvise( map, map_size, MADV_DONTDUMP ) != 0 ) {
        perror( "madvise" );
        res = false;
      }
    }
#ifdef MLOCK_ONFAULT
    if ( ::mlock2( map, map_size, MLOCK_ONFAULT ) != 0 ) {
      perror( "mlock2" );
      res = false;
    }
#else
    if ( ::mlock( map, map_size ) != 0 ) {
      perror( "mlock" );
      res = false;
    }
#endif
    return res;
  }
  static size_t page_size( void ) {
    return (size_t) ::sysconf( _SC_PAGESIZE );
  }
  static size_t align_page( size_t len ) {
    size_t page_align = page_size();
    return ( len + ( page_align - 1 ) ) & ~( page_align - 1 );
  }
};

typedef struct stat os_stat;

static inline int
os_open( const char *path, int oflag, int mode ) noexcept
{
  return ::open( path, oflag, mode );
}
static inline ssize_t
os_write( int fd, const void *buf, size_t count ) noexcept
{
  return ::write( fd, buf, count );
}
static inline ssize_t
os_writev( int fd, const struct iovec *iov, size_t iovcnt ) noexcept
{
  return ::writev( fd, iov, iovcnt );
}
static inline ssize_t
os_read( int fd, void *buf, size_t nbyte ) noexcept
{
  return ::read( fd, buf, nbyte );
}
static inline int
os_fstat( int fd, os_stat *statbuf ) noexcept
{
  return ::fstat( fd, statbuf );
}
static inline int
os_fstat( const char *path, os_stat *statbuf ) noexcept
{
  return ::stat( path, statbuf );
}
static inline int
os_close( int fd ) noexcept
{
  return ::close( fd );
}
static inline int
os_unlink( const char *fn ) noexcept
{
  return ::unlink( fn );
}
static inline int
os_access( const char *fn,  int mode ) noexcept
{
  return ::access( fn, mode );
}
static inline int
os_mkdir( const char *dn,  int mode ) noexcept
{
  return ::mkdir( dn, mode );
}
static inline int
os_rmdir( const char *dn ) noexcept
{
  return ::rmdir( dn );
}
static inline int
os_rename( const char *oldname,  const char *newname ) noexcept
{
  return ::rename( oldname, newname );
}
static inline int
os_dup( int fd ) noexcept
{
  return ::dup( fd );
}

#endif

}
}

#endif
