#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <arpa/nameser.h>
#include <netdb.h>
#include <raikv/ev_cares.h>
#include <ares_dns.h>

using namespace rai;
using namespace kv;

CaresAddrInfo::~CaresAddrInfo()
{
  if ( this->addr_list != NULL )
    this->free_addr_list();

  if ( this->event_id != 0 )
    this->poll.timer.remove_timer_cb( *this, this->timer_id, this->event_id );

  for ( size_t i = 0; i < this->set.count; i++ ) {
    EvCaresAsync * c;
    if ( (c = this->set.ptr[ i ]) != NULL ) {
      c->popall();
      c->idle_push( EV_CLOSE );
      this->set.ptr[ i ] = NULL;
    }
  }
  if ( this->channel != NULL )
    ares_destroy( this->channel );
}

void
CaresAddrInfo::free_addr_list( void ) noexcept
{
  while ( this->addr_list != NULL ) {
    struct addrinfo * next = this->addr_list->ai_next;
    ::free( this->addr_list );
    this->addr_list = next;
  }
  this->tail = NULL;
}
/* version 1.16.0 has getaddrinfo */
#if ARES_VERSION < 0x011000
static void
cares_host_cb( void *arg,  int status,  int timeouts,
               struct hostent *hostent ) noexcept
{
  CaresAddrInfo &info = *(CaresAddrInfo *) arg;

  if ( status != 0 || timeouts != 0 ) {
    info.status   = status;
    info.timeouts = timeouts;
  }
  if ( --info.host_count == 0 )
    info.done = true;

  if ( hostent == NULL || hostent->h_addr_list == NULL ||
       hostent->h_length == 0 )
    return;

  for ( size_t count = 0; hostent->h_addr_list[ count ] != NULL; count++ ) {
    size_t len = 0;
    if ( hostent->h_addrtype == AF_INET && hostent->h_length == 4 )
      len = sizeof( struct addrinfo ) + sizeof( struct sockaddr_in );
    else if ( hostent->h_addrtype == AF_INET6 && hostent->h_length == 16 )
      len = sizeof( struct addrinfo ) + sizeof( struct sockaddr_in6 );

    if ( len == 0 )
      continue;

    struct addrinfo * ai = (struct addrinfo *) ::malloc( len );
    ::memset( ai, 0, len );
    ai->ai_flags    = info.flags;
    ai->ai_family   = hostent->h_addrtype;
    ai->ai_socktype = info.socktype;
    ai->ai_protocol = info.protocol;
    ai->ai_addrlen  = len - sizeof( struct addrinfo );
    ai->ai_addr     = (struct sockaddr *) &ai[ 1 ];
    if ( info.tail != NULL )
      info.tail->ai_next = ai;
    else
      info.addr_list = ai;
    info.tail = ai;

    if ( hostent->h_addrtype == AF_INET && hostent->h_length == 4 ) {
      struct sockaddr_in * sa = (struct sockaddr_in *) ai->ai_addr;
      sa->sin_family = AF_INET;
      sa->sin_port   = htons( info.port );
      ::memcpy( &sa->sin_addr, hostent->h_addr_list[ count ], 4 );
    }
    else if ( hostent->h_addrtype == AF_INET6 && hostent->h_length == 16 ) {
      struct sockaddr_in6 * sa = (struct sockaddr_in6 *) ai->ai_addr;
      sa->sin6_family = AF_INET6;
      sa->sin6_port   = htons( info.port );
      ::memcpy( &sa->sin6_addr, hostent->h_addr_list[ count ], 16 );
    }
  }
}
#else
static void
cares_addr_cb( void *arg, int status, int timeouts,
               struct ares_addrinfo *result ) noexcept
{
  CaresAddrInfo &info = *(CaresAddrInfo *) arg;

  if ( status != 0 || timeouts != 0 ) {
    info.status   = status;
    info.timeouts = timeouts;
  }
  if ( --info.host_count == 0 )
    info.done = true;
  if ( result == NULL )
    return;

  struct ares_addrinfo_node *n = result->nodes;
  for ( ; n != NULL; n = n->ai_next ) {
    size_t len = 0;
    if ( n->ai_family == AF_INET || n->ai_family == AF_INET6 )
      len = sizeof( struct addrinfo ) + n->ai_addrlen;
    if ( len == 0 )
      continue;

    struct addrinfo * ai = (struct addrinfo *) ::malloc( len );
    ::memset( ai, 0, len );
    ai->ai_flags    = n->ai_flags;
    ai->ai_family   = n->ai_family;
    ai->ai_socktype = n->ai_socktype;
    ai->ai_protocol = n->ai_protocol;
    ai->ai_addrlen  = n->ai_addrlen;
    ai->ai_addr     = (struct sockaddr *) &ai[ 1 ];
    ::memcpy( ai->ai_addr, n->ai_addr, n->ai_addrlen );

    if ( info.tail != NULL )
      info.tail->ai_next = ai;
    else
      info.addr_list = ai;
    info.tail = ai;
  }
  ares_freeaddrinfo( result );
}
#endif

int
CaresAddrInfo::get_address( const char *ip,  int port,  int opts ) noexcept
{
  if ( this->channel == NULL ) {
    ares_options opt;
    ::memset( &opt, 0, sizeof( opt ) );
    this->status = ares_init_options( &this->channel, &opt, ARES_OPT_FLAGS );
    if ( status != 0 ) {
      this->done = true;
      return this->status;
    }
  }
  this->done = false;
  this->port = port;

  if ( ( opts & OPT_UDP ) == 0 ) {
    this->socktype = SOCK_STREAM;
    this->protocol = IPPROTO_TCP;
  }
  else {
    this->socktype = SOCK_DGRAM;
    this->protocol = IPPROTO_UDP;
  }
  if ( ( opts & OPT_LISTEN ) != 0 )
    this->flags = AI_PASSIVE;
  else
    this->flags = 0;
  this->host_count = 1;
#if ARES_VERSION < 0x011000
  switch ( opts & ( OPT_AF_INET6 | OPT_AF_INET ) ) {
    case OPT_AF_INET:
      ares_gethostbyname( this->channel, ip, AF_INET, cares_host_cb, this );
      break;
    case OPT_AF_INET6:
      ares_gethostbyname( this->channel, ip, AF_INET6, cares_host_cb, this );
      break;
    default:
    case OPT_AF_INET | OPT_AF_INET6:
      ares_gethostbyname( this->channel, ip, AF_INET, cares_host_cb, this );
      /*this->host_count = 2;
      ares_gethostbyname( this->channel, ip, AF_INET, cares_host_cb, this );
      ares_gethostbyname( this->channel, ip, AF_INET6, cares_host_cb, this );*/
      break;
  }
#else
  struct ares_addrinfo_hints hints;
  ::memset( &hints, 0, sizeof( hints ) );
  hints.ai_socktype = this->socktype;
  hints.ai_protocol = this->protocol;
  hints.ai_flags    = this->flags;
  if ( ( opts & OPT_NO_DNS ) != 0 )
    hints.ai_flags |= AI_NUMERICHOST;
  switch ( opts & ( OPT_AF_INET6 | OPT_AF_INET ) ) {
    case OPT_AF_INET:
      hints.ai_family = AF_INET;
      break;
    case OPT_AF_INET6:
      hints.ai_family = AF_INET6;
      break;
    default:
    case OPT_AF_INET | OPT_AF_INET6:
      hints.ai_family = AF_UNSPEC;
      break;
  }
  char svc[ 16 ];
  ::snprintf( svc, sizeof( svc ), "%d", port );
  ares_getaddrinfo( this->channel, ip, svc, &hints, cares_addr_cb, this );
#endif
  this->do_poll();
  return 0;
}

void
CaresAddrInfo::do_poll( void ) noexcept
{
  if ( this->done )
    return;

  int    nfds;
  fd_set readers, writers;
  size_t i;
  EvCaresAsync * c;

  FD_ZERO( &readers );
  FD_ZERO( &writers );
  nfds = ares_fds( channel, &readers, &writers );
  this->poll_count++;
  /* add fds if polling them */
  for ( int fd = 0; fd < nfds; fd++ ) {
    if ( FD_ISSET( fd, &readers ) ) {
      for ( i = 0; i < this->set.count; i++ ) {
        if ( (c = this->set.ptr[ i ]) != NULL ) {
          if ( c->fd == fd ) {
            c->poll_count = this->poll_count;
            break;
          }
        }
      }
      if ( i == this->set.count ) {
        c = this->poll.get_free_list<EvCaresAsync, CaresAddrInfo &>(
              this->sock_type, *this );
        this->set[ i ] = c;
        c->PeerData::init_peer( fd, -1, NULL, "c-ares" );
        c->poll_count = this->poll_count;
        this->poll.add_sock( c );
      }
      ares_process_fd( this->channel, fd, ARES_SOCKET_BAD );
    }
    if ( FD_ISSET( fd, &writers ) )
      ares_process_fd( this->channel, ARES_SOCKET_BAD, fd );
  }
  /* remove fds not used */
  for ( i = 0; i < this->set.count; i++ ) {
    if ( (c = this->set.ptr[ i ]) != NULL ) {
      if ( c->poll_count != this->poll_count ) {
        c->popall();
        c->idle_push( EV_CLOSE );
        this->set.ptr[ i ] = NULL;
      }
    }
  }
  /* no work left to do */
  if ( nfds == 0 ) {
    if ( this->event_id != 0 ) {
      this->poll.timer.remove_timer_cb( *this, this->timer_id, this->event_id );
      this->event_id = 0;
    }
    this->done = true;
    return;
  }
  /* add timeout */
  struct timeval tval, *tv;
  tval.tv_sec = tval.tv_usec = 0;
  tv = ares_timeout( this->channel, NULL, &tval );
  uint64_t timeout_us = 0,
           cur_time   = 0;
  if ( tv != NULL ) {
    cur_time   = kv_current_monotonic_time_us();
    timeout_us = cur_time + tv->tv_sec * 1000000 + tv->tv_usec;
  }
  if ( this->event_id != 0 && timeout_us < this->event_id ) {
    this->poll.timer.remove_timer_cb( *this, this->timer_id, this->event_id );
    this->event_id = 0;
  }
  if ( this->event_id == 0 && timeout_us != 0 ) {
    this->timer_id++;
    this->event_id = timeout_us;
    this->poll.timer.add_timer_millis( *this, ( timeout_us - cur_time ) / 1000,
                                       this->timer_id, this->event_id );
  }
}

bool
CaresAddrInfo::timer_cb( uint64_t timer_id,  uint64_t event_id ) noexcept
{
  if ( timer_id == this->timer_id && this->event_id == event_id ) {
    this->event_id = 0;
    this->do_poll();
  }
  return false;
}

void
EvCaresAsync::write( void ) noexcept
{
}

void
EvCaresAsync::read( void ) noexcept
{
  ares_process_fd( this->info.channel, this->fd, ARES_SOCKET_BAD );
  this->pop3( EV_READ, EV_READ_LO, EV_READ_HI );
  this->info.do_poll();
}

void
EvCaresAsync::process( void ) noexcept
{
}

void
EvCaresAsync::release( void ) noexcept
{
}
