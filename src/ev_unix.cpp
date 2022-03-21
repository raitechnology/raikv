#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#ifndef _MSC_VER
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/un.h>
#endif
#include <raikv/ev_unix.h>

using namespace rai;
using namespace kv;

int
EvUnixListen::listen( const char *path,  int opts ) noexcept
{
  return this->listen2( path, opts, "unix_listen" );
}

int
EvUnixListen::listen2( const char *path,  int opts,  const char *k ) noexcept
{
#ifdef _MSC_VER
  return -1;
#else
  static int on = 1;
  struct sockaddr_un sunaddr;
  struct stat statbuf;
  int sock, status = 0;

  this->sock_opts = opts;
  sock = ::socket( PF_LOCAL, SOCK_STREAM, 0 );
  if ( sock < 0 )
    return this->set_sock_err( EV_ERR_SOCKET, errno );

  ::memset( &sunaddr, 0, sizeof( sunaddr ) );
  sunaddr.sun_family = AF_LOCAL;
  if ( ::stat( path, &statbuf ) == 0 &&
       statbuf.st_size == 0 ) { /* make sure it's empty */
    ::unlink( path );
  }
  ::strncpy( sunaddr.sun_path, path, sizeof( sunaddr.sun_path ) - 1 );

  if ( ::setsockopt( sock, SOL_SOCKET, SO_REUSEADDR, &on, sizeof( on ) ) != 0 )
    if ( ( opts & OPT_VERBOSE ) != 0 )
      perror( "warning: SO_REUSEADDR" );
  if ( ::bind( sock, (struct sockaddr *) &sunaddr, sizeof( sunaddr ) ) != 0 ) {
    if ( ( opts & OPT_VERBOSE ) != 0 )
      perror( "error: bind" );
    goto fail;
  }
  if ( ::listen( sock, 128 ) != 0 ) {
    if ( ( opts & OPT_VERBOSE ) != 0 )
      perror( "error: listen" );
    goto fail;
  }
  ::fcntl( sock, F_SETFL, O_NONBLOCK | ::fcntl( sock, F_GETFL ) );
  this->PeerData::init_peer( sock, (struct sockaddr *) &sunaddr, k );
  if ( (status = this->poll.add_sock( this )) < 0 )
    goto fail;
  return 0;
fail:;
  ::close( sock );
  this->fd = -1;
  return status;
#endif
}

bool
EvUnixListen::accept2( EvConnection &c,  const char *k ) noexcept
{
#ifndef _MSC_VER
  struct sockaddr_un sunaddr;
  socklen_t addrlen = sizeof( sunaddr );
  int sock = ::accept( this->fd, (struct sockaddr *) &sunaddr, &addrlen );
  if ( sock < 0 ) {
    if ( errno != EINTR ) {
      if ( errno != EAGAIN )
        perror( "accept" );
      this->pop3( EV_READ, EV_READ_LO, EV_READ_HI );
    }
    goto fail;
  }
  ::fcntl( sock, F_SETFL, O_NONBLOCK | ::fcntl( sock, F_GETFL ) );
  this->PeerData::init_peer( sock, (struct sockaddr *) &sunaddr, k );
  if ( this->poll.add_sock( &c ) < 0 ) {
    ::close( sock );
    goto fail;
  }
  return true;
fail:;
#endif
  this->poll.push_free_list( &c );
  return false;
}

int
EvUnixConnection::connect( EvConnection &conn,  const char *path,
                           int opts,  const char *k ) noexcept
{
#ifdef _MSC_VER
  return -1;
#else
  struct sockaddr_un sunaddr;
  int sock, status = 0;

  conn.sock_opts = opts;
  sock = ::socket( PF_LOCAL, SOCK_STREAM, 0 );
  if ( sock < 0 )
    return conn.set_sock_err( EV_ERR_SOCKET, errno );

  ::memset( &sunaddr, 0, sizeof( sunaddr ) );
  sunaddr.sun_family = AF_LOCAL;
  ::strncpy( sunaddr.sun_path, path, sizeof( sunaddr.sun_path ) - 1 );
  if ( ::connect( sock, (struct sockaddr *) &sunaddr,
                  sizeof( sunaddr ) ) != 0 ) {
    status = conn.set_sock_err( EV_ERR_CONNECT, errno );
    goto fail;
  }
  ::fcntl( sock, F_SETFL, O_NONBLOCK | ::fcntl( sock, F_GETFL ) );
  conn.PeerData::init_peer( sock, (struct sockaddr *) &sunaddr, k );
  if ( (status = conn.poll.add_sock( &conn )) < 0 ) {
  fail:;
    conn.fd = -1;
    ::close( sock );
  }
  return status;
#endif
}

