#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/ipc.h>
#include <sys/shm.h>

#include <raikv/pipe_buf.h>

using namespace rai;
using namespace kv;

PipeBuf *
PipeBuf::open( const char *name,  bool do_create ) noexcept
{
  int       mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH,
            flag = O_RDWR,
            excl = O_CREAT | O_EXCL,
            fd   = -1;
  void    * p    = MAP_FAILED;
  PipeBuf * pb   = NULL;

  if ( do_create )
    flag |= excl;
  fd = ::shm_open( name, flag, mode );
  if ( fd < 0 ) {
    if ( do_create ) {
      ::shm_unlink( name ); /* clean create, not open existing */
      fd = ::shm_open( name, flag, mode );
    }
  }
  if ( fd < 0 ) {
    if ( do_create )
      ::perror( name );
    return NULL;
  }
  if ( do_create ) {
    if ( ::ftruncate( fd, sizeof( PipeBuf ) ) != 0 ) {
      ::perror( name );
      goto done;
    }
  }
  p = ::mmap( 0, sizeof( PipeBuf ), PROT_READ | PROT_WRITE,
              MAP_SHARED | MAP_POPULATE, fd, 0 );
  if ( p == MAP_FAILED ) {
    ::perror( name );
    goto done;
  }
  ::close( fd );
  fd = -1;

  pb = (PipeBuf *) p;
  if ( do_create )
    pb->init();
  return pb;

done:;
  if ( p != MAP_FAILED )
    ::munmap( p, sizeof( PipeBuf ) );
  if ( fd >= 0 )
    ::close( fd );
  return NULL;
}

void
PipeBuf::init( void ) noexcept
{
  ::memset( this, 0, sizeof( PipeBuf ) );
}

void
PipeBuf::close( void ) noexcept
{
  void * p = (void *) this;
  ::munmap( p, sizeof( PipeBuf ) );
}

int
PipeBuf::unlink( const char *name ) noexcept
{
  return ::shm_unlink( name );
}

