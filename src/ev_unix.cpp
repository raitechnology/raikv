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
  return this->listen2( path, opts, "unix_listen", -1 );
}

int
EvUnixListen::listen2( const char *path,  int opts,  const char *k,
                       uint32_t rte_id ) noexcept
{
#ifdef _MSC_VER
  return -1;
#else
  struct sockaddr_un sunaddr;
  int sock, status = 0;

  this->sock_opts = opts;
  sock = ::socket( PF_LOCAL, SOCK_STREAM, 0 );
  if ( sock < 0 )
    return this->set_sock_err( EV_ERR_SOCKET, errno );

  ::memset( &sunaddr, 0, sizeof( sunaddr ) );
  sunaddr.sun_family = AF_LOCAL;
  ::strncpy( sunaddr.sun_path, path, sizeof( sunaddr.sun_path ) - 1 );

  if ( (opts & OPT_REUSEADDR) != 0 ) {
    struct stat statbuf;
    if ( ::stat( sunaddr.sun_path, &statbuf ) == 0 &&
         ( statbuf.st_mode & S_IFSOCK ) != 0 &&
         statbuf.st_size == 0 ) { /* make sure it's empty */
      if ( ::unlink( sunaddr.sun_path ) != 0 ) {
        perror( sunaddr.sun_path );
        goto fail;
      }
    }
  }
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
  this->PeerData::init_peer( this->poll.get_next_id(), sock, rte_id,
                             (struct sockaddr *) &sunaddr, k );
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
EvUnixListen::accept2( EvConnection &conn,  const char *k ) noexcept
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
  conn.PeerData::init_peer( this->poll.get_next_id(), sock, this->route_id,
                            (struct sockaddr *) &sunaddr, k );
  if ( this->poll.add_sock( &conn ) < 0 ) {
    ::close( sock );
    goto fail;
  }
  return true;
fail:;
#endif
  this->poll.push_free_list( &conn );
  return false;
}

int
EvUnixConnection::connect( EvConnection &conn,  const char *path,
                           int opts,  const char *k,  uint32_t rte_id ) noexcept
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
  conn.PeerData::init_peer( conn.poll.get_next_id(), sock, rte_id,
                            (struct sockaddr *) &sunaddr, k );
  if ( (status = conn.poll.add_sock( &conn )) < 0 ) {
  fail:;
    conn.fd = -1;
    ::close( sock );
  }
  return status;
#endif
}

int
EvUnixDgram::bind( const char *path,  int opts,  const char *k,
                   uint32_t rte_id ) noexcept
{
#ifdef _MSC_VER
  return -1;
#else
  struct sockaddr_un sunaddr;
  int sock, status = 0;

  this->sock_opts = opts;
  sock = ::socket( PF_LOCAL, SOCK_DGRAM, 0 );
  if ( sock < 0 )
    return this->set_sock_err( EV_ERR_SOCKET, errno );

  ::memset( &sunaddr, 0, sizeof( sunaddr ) );
  sunaddr.sun_family = AF_LOCAL;
  ::strncpy( sunaddr.sun_path, path, sizeof( sunaddr.sun_path ) - 1 );

  if ( (opts & OPT_REUSEADDR) != 0 ) {
    struct stat statbuf;
    if ( ::stat( sunaddr.sun_path, &statbuf ) == 0 &&
         ( statbuf.st_mode & S_IFSOCK ) != 0 &&
         statbuf.st_size == 0 ) { /* make sure it's empty */
      if ( ::unlink( sunaddr.sun_path ) != 0 ) {
        perror( sunaddr.sun_path );
        goto fail;
      }
    }
  }
  if ( ::bind( sock, (struct sockaddr *) &sunaddr,
               sizeof( sunaddr ) ) != 0 ) {
    status = this->set_sock_err( EV_ERR_BIND, errno );
    goto fail;
  }
  ::fcntl( sock, F_SETFL, O_NONBLOCK | ::fcntl( sock, F_GETFL ) );
  this->PeerData::init_peer( this->poll.get_next_id(), sock, rte_id,
                             (struct sockaddr *) &sunaddr, k );
  if ( (status = this->poll.add_sock( this )) < 0 ) {
  fail:;
    this->fd = -1;
    ::close( sock );
  }
  return status;
#endif
}

