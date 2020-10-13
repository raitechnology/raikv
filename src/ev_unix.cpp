#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <raikv/ev_unix.h>

using namespace rai;
using namespace kv;

int
EvUnixListen::listen( const char *path,  const char *k ) noexcept
{
  static int on = 1;
  int sock;
  struct sockaddr_un sunaddr;
  struct stat statbuf;

  sock = ::socket( PF_LOCAL, SOCK_STREAM, 0 );
  if ( sock < 0 ) {
    perror( "error: socket" );
    return -1;
  }
  ::memset( &sunaddr, 0, sizeof( sunaddr ) );
  sunaddr.sun_family = AF_LOCAL;
  if ( ::stat( path, &statbuf ) == 0 &&
       statbuf.st_size == 0 ) { /* make sure it's empty */
    ::unlink( path );
  }
  ::strncpy( sunaddr.sun_path, path, sizeof( sunaddr.sun_path ) - 1 );

  if ( ::setsockopt( sock, SOL_SOCKET, SO_REUSEADDR, &on, sizeof( on ) ) != 0 )
    perror( "warning: SO_REUSEADDR" );
  if ( ::bind( sock, (struct sockaddr *) &sunaddr, sizeof( sunaddr ) ) != 0 ) {
    perror( "error: bind" );
    goto fail;
  }
  if ( ::listen( sock, 128 ) != 0 ) {
    perror( "error: listen" );
    goto fail;
  }
  this->PeerData::init_peer( sock, (struct sockaddr *) &sunaddr, k );
  ::fcntl( sock, F_SETFL, O_NONBLOCK | ::fcntl( sock, F_GETFL ) );
  if ( this->poll.add_sock( this ) < 0 )
    goto fail;
  return 0;
fail:;
  ::close( sock );
  this->fd = -1;
  return -1;
}

int
EvUnixConnection::connect( EvConnection &conn,  const char *path ) noexcept
{
  int sock;
  struct sockaddr_un sunaddr;

  sock = ::socket( PF_LOCAL, SOCK_STREAM, 0 );
  if ( sock < 0 ) {
    perror( "error: socket" );
    return -1;
  }
  ::memset( &sunaddr, 0, sizeof( sunaddr ) );
  sunaddr.sun_family = AF_LOCAL;
  ::strncpy( sunaddr.sun_path, path, sizeof( sunaddr.sun_path ) - 1 );
  if ( ::connect( sock, (struct sockaddr *) &sunaddr,
                  sizeof( sunaddr ) ) != 0 ) {
    perror( "error: connect" );
    goto fail;
  }
  conn.PeerData::init_peer( sock, (struct sockaddr *) &sunaddr, "unix_client");
  ::fcntl( sock, F_SETFL, O_NONBLOCK | ::fcntl( sock, F_GETFL ) );
  if ( conn.poll.add_sock( &conn ) < 0 ) {
  fail:;
    conn.fd = -1;
    ::close( sock );
    return -1;
  }
  return 0;
}

