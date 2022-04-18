#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <raikv/win.h>

static wp_fd_map_t fdmap[ MAX_FD_MAP_SIZE ];

uint32_t
getpid( void )
{
  return GetCurrentProcessId();
}

uint32_t
getthrid( void )
{
  return GetCurrentThreadId();
}

int
pidexists( uint32_t pid )
{
  HANDLE process = OpenProcess( SYNCHRONIZE, FALSE, pid );
  DWORD  ret     = WaitForSingleObject( process, 0 );
  CloseHandle( process );
  if ( ret == WAIT_TIMEOUT )
    return 1;
  return 0;
}

static bool ws_init_done;
int
ws_global_init( void )
{
  WSADATA wsa_data;
  int     r = WSAStartup( MAKEWORD( 2, 2 ), &wsa_data );
  if ( r != 0 )
    return_set_error( -1, r );
  ws_init_done = true;
  return 0;
}

static int
wp_register( wp_fd_type_t t,  SOCKET sock )
{
  for ( size_t i = 0; i < MAX_FD_MAP_SIZE; i++ ) {
    if ( fdmap[ i ].type == WP_FD_NONE ) {
      memset( &fdmap[ i ], 0, sizeof( fdmap[ i ] ) );
      fdmap[ i ].type = t;
      if ( t == WP_FD_SOCKET )
        fdmap[ i ].sock.socket = sock;
      return (int) i;
    }
  }
  return_set_error( -1, ERROR_NO_MORE_FILES );
}

int
wp_make_null_fd( void )
{
  return wp_register( WP_FD_USED, 0 );
}

int
wp_register_fd( SOCKET sock )
{
  return wp_register( WP_FD_SOCKET, sock );
}

int
wp_unregister_fd( int fd )
{
  if ( fd < 0 || (size_t) fd >= MAX_FD_MAP_SIZE ||
       fdmap[ fd ].type == WP_FD_NONE )
    return_set_error( -1, ERROR_INVALID_PARAMETER );
  if ( fdmap[ fd ].type == WP_FD_SOCKET )
    closesocket( fdmap[ fd ].sock.socket );
  fdmap[ fd ].type = WP_FD_NONE;
  return 0;
}

int
wp_close_fd( int fd )
{
  return wp_unregister_fd( fd );
}

int
wp_shutdown_fd( int fd, int how )
{
  if ( fd < 0 || (size_t) fd >= MAX_FD_MAP_SIZE ||
       fdmap[ fd ].type == WP_FD_NONE )
    return_set_error( -1, ERROR_INVALID_PARAMETER );
  if ( fdmap[ fd ].type == WP_FD_SOCKET )
    return shutdown( fdmap[ fd ].sock.socket, how );
  return 0;
}

int
wp_get_socket( int fd,  SOCKET *s )
{
  if ( fd < 0 || (size_t) fd >= MAX_FD_MAP_SIZE ||
       fdmap[ fd ].type != WP_FD_SOCKET )
    return_set_error( -1, ERROR_INVALID_HANDLE );
  *s = fdmap[ fd ].sock.socket;
  return 0;
}

ssize_t
wp_read( int fd,  void *buf,  size_t buflen )
{
  SOCKET s;
  int    n;
  if ( wp_get_socket( fd, &s ) < 0 )
    return -1;
  n = recv( s, (char *) buf, (int) buflen, 0 );
  if ( n == SOCKET_ERROR )
    return_map_error( -1 );
  return n;
}

#define MAX_IOV 64
static size_t
copy_iov( struct iovec *buf,  size_t nbufs,  WSABUF *wbuf )
{
  size_t i;
  for ( i = 0; i < nbufs && i < MAX_IOV; i++ ) {
    wbuf[ i ].buf = (CHAR *) buf[ i ].iov_base;
    wbuf[ i ].len = (ULONG) buf[ i ].iov_len;
  }
  return i;
}

ssize_t
wp_send( int fd,  struct iovec *buf,  size_t nbufs )
{
  WSABUF wbuf[ MAX_IOV ];
  size_t cnt = copy_iov( buf, nbufs, wbuf );
  DWORD  len = 0;
  SOCKET s;
  int    n;
  if ( wp_get_socket( fd, &s ) < 0 )
    return -1;
  n = WSASend( s, wbuf, (DWORD) cnt, &len, 0, 0, 0 );
  if ( n == SOCKET_ERROR )
    return_map_error( -1 );
  return len;
}

ssize_t
wp_recvmsg( int fd,  struct msghdr *msg )
{
  WSABUF wbuf[ MAX_IOV ];
  size_t cnt   = copy_iov( msg->msg_iov, msg->msg_iovlen, wbuf );
  DWORD  len   = 0,
         flags = 0;
  INT    nml   = (INT) msg->msg_namelen;
  SOCKET s;
  int    n;
  if ( wp_get_socket( fd, &s ) < 0 )
    return -1;
  n = WSARecvFrom( s, wbuf, (DWORD) cnt, &len, &flags,
                   (struct sockaddr *) msg->msg_name, &nml, 0, 0 );
  if ( n == SOCKET_ERROR )
    return_map_error( -1 );
  msg->msg_namelen = nml;
  return len;
}

ssize_t
wp_sendmsg( int fd,  struct msghdr *msg )
{
  WSABUF wbuf[ MAX_IOV ];
  size_t cnt = copy_iov( msg->msg_iov, msg->msg_iovlen, wbuf );
  DWORD  len = 0;
  SOCKET s;
  int    n;
  if ( wp_get_socket( fd, &s ) < 0 )
    return -1;
  n = WSASendTo( s, wbuf, (DWORD) cnt, &len, 0,
            (struct sockaddr *) msg->msg_name, (DWORD) msg->msg_namelen, 0, 0 );
  if ( n == SOCKET_ERROR )
    return_map_error( -1 );
  return len;
}

int
epoll_create( int size )
{
  if ( size <= 0 )
    return_set_error( -1, ERROR_INVALID_PARAMETER );

  if ( ! ws_init_done && ws_global_init() != 0 )
    return -1;
  return wp_register( WP_FD_EPOLL, 0 );
}

int
epoll_create1( int flags )
{
  if ( flags != 0 )
    return_set_error( -1, ERROR_INVALID_PARAMETER );

  if ( ! ws_init_done && ws_global_init() != 0 )
    return -1;
  return wp_register( WP_FD_EPOLL, 0 );
}

int
epoll_close( int fd )
{
  return wp_unregister_fd( fd );
}

int
epoll_ctl( int fd,  int op,  int s,  struct epoll_event* event )
{
  wp_port_state_t * port;
  wp_sock_state_t * sock;
  uint32_t          s_off  = (uint32_t) s / 64;
  uint64_t          s_mask = (uint64_t) 1 << ( (uint32_t) s % 64 );

  if ( fd < 0 || (size_t) fd >= MAX_FD_MAP_SIZE ||
       fdmap[ fd ].type != WP_FD_EPOLL )
    return_set_error( -1, ERROR_INVALID_HANDLE );

  if ( s < 0 || (size_t) s >= MAX_FD_MAP_SIZE ||
       fdmap[ s ].type != WP_FD_SOCKET )
    return_set_error( -1, ERROR_INVALID_HANDLE );

  port = &fdmap[ fd ].port;
  sock = &fdmap[ s ].sock;

  if ( op == EPOLL_CTL_ADD || op == EPOLL_CTL_MOD ) {
    port->poll_set_fds[ s_off ] |= s_mask;
    sock->user_data   = event->data;
    sock->user_events = event->events;
  }
  else if ( op == EPOLL_CTL_DEL )
    port->poll_set_fds[ s_off ] &= ~s_mask;

  return 0;
}

int
wp_epoll_wait( int fd,  struct epoll_event* events,  int maxevents,
               uint64_t us_timeout )
{
  fd_set rd_set, wr_set, ex_set;
  struct timeval tv;
  wp_port_state_t * port;
  wp_sock_state_t * sock;
  uint32_t          i, j,
                    rd_count = 0,
                    wr_count = 0,
                    rd[ MAX_FD_MAP_SIZE ],
                    wr[ MAX_FD_MAP_SIZE ];
  int               n;

  if ( fd < 0 || (size_t) fd >= MAX_FD_MAP_SIZE ||
       fdmap[ fd ].type != WP_FD_EPOLL )
    return_set_error( -1, ERROR_INVALID_HANDLE );

  FD_ZERO( &rd_set );
  FD_ZERO( &wr_set );
  FD_ZERO( &ex_set );
  port = &fdmap[ fd ].port;
  for ( i = 0; i < MAX_FD_MAP_SIZE / 64; i++ ) {
    if ( port->poll_set_fds[ i ] != 0 ) {
      for ( j = 0; j < 64; j++ ) {
        uint64_t s_mask = (uint64_t) 1 << j;
        if ( ( port->poll_set_fds[ i ] & s_mask ) != 0 ) {
          uint32_t s = i * 64 + j;
          sock = &fdmap[ s ].sock;
          if ( ( sock->user_events & EPOLLIN ) != 0 ) {
            /* if ET is unset or not triggered */
            if ( ( sock->user_events & EPOLLET ) == 0 ||
                 ( ( sock->user_events & EPOLLET ) != 0 &&
                   ( sock->user_events & EPOLLET_TRIGGERED ) == 0 ) ) {
              FD_SET( sock->socket, &rd_set );
              rd[ rd_count++ ] = s;
            }
          }
          if ( ( sock->user_events & EPOLLOUT ) != 0 ) {
            FD_SET( sock->socket, &wr_set );
            FD_SET( sock->socket, &ex_set );
            sock->user_events &= ~EPOLLOUT_TRIGGERED;
            wr[ wr_count++ ] = s;
          }
        }
      }
    }
  }
  if ( rd_count + wr_count == 0 ) {
    Sleep( (DWORD) ( us_timeout / 1000 ) );
    return 0;
  }
  tv.tv_sec  = 0;
  tv.tv_usec = (long) us_timeout;
  n = select( 0, &rd_set, &wr_set, NULL, &tv );
  if ( n == 0 || maxevents <= 0 )
    return 0;
  if ( n == SOCKET_ERROR )
    return_map_error( -1 );
  n = 0;
  for ( i = 0; i < rd_count; i++ ) {
    sock = &fdmap[ rd[ i ] ].sock;
    if ( FD_ISSET( sock->socket, &rd_set ) ) {
      events[ n ].events = EPOLLIN;
      events[ n ].data   = sock->user_data;
      if ( ( sock->user_events & EPOLLET ) != 0 )
        sock->user_events |= EPOLLET_TRIGGERED;
      if ( ( sock->user_events & EPOLLOUT ) != 0 ) {
        if ( FD_ISSET( sock->socket, &wr_set ) )
          events[ n ].events |= EPOLLOUT;
        sock->user_events |= EPOLLOUT_TRIGGERED;
      }
      if ( ++n == maxevents )
        return n;
    }
  }
  for ( i = 0; i < wr_count; i++ ) {
    sock = &fdmap[ wr[ i ] ].sock;
    if ( ( sock->user_events & EPOLLOUT_TRIGGERED ) == 0 ) {
      events[ n ].events = 0;
      if ( FD_ISSET( sock->socket, &wr_set ) ) {
        events[ n ].events = EPOLLOUT;
        events[ n ].data   = sock->user_data;
      }
      if ( FD_ISSET( sock->socket, &ex_set ) ) {
        events[ n ].events |= EPOLLIN;
        events[ n ].data   = sock->user_data;
      }
      if ( events[ n ].events != 0 ) {
        if ( ++n == maxevents )
          return n;
      }
    }
    else {
      sock->user_events &= ~EPOLLOUT_TRIGGERED;
    }
  }
  return n;
}


#include <errno.h>

#define ERR__ERRNO_MAPPINGS(X)               \
  X(ERROR_ACCESS_DENIED, EACCES)             \
  X(ERROR_ALREADY_EXISTS, EEXIST)            \
  X(ERROR_BAD_COMMAND, EACCES)               \
  X(ERROR_BAD_EXE_FORMAT, ENOEXEC)           \
  X(ERROR_BAD_LENGTH, EACCES)                \
  X(ERROR_BAD_NETPATH, ENOENT)               \
  X(ERROR_BAD_NET_NAME, ENOENT)              \
  X(ERROR_BAD_NET_RESP, ENETDOWN)            \
  X(ERROR_BAD_PATHNAME, ENOENT)              \
  X(ERROR_BROKEN_PIPE, EPIPE)                \
  X(ERROR_CANNOT_MAKE, EACCES)               \
  X(ERROR_COMMITMENT_LIMIT, ENOMEM)          \
  X(ERROR_CONNECTION_ABORTED, ECONNABORTED)  \
  X(ERROR_CONNECTION_ACTIVE, EISCONN)        \
  X(ERROR_CONNECTION_REFUSED, ECONNREFUSED)  \
  X(ERROR_CRC, EACCES)                       \
  X(ERROR_DIR_NOT_EMPTY, ENOTEMPTY)          \
  X(ERROR_DISK_FULL, ENOSPC)                 \
  X(ERROR_DUP_NAME, EADDRINUSE)              \
  X(ERROR_FILENAME_EXCED_RANGE, ENOENT)      \
  X(ERROR_FILE_NOT_FOUND, ENOENT)            \
  X(ERROR_GEN_FAILURE, EACCES)               \
  X(ERROR_GRACEFUL_DISCONNECT, EPIPE)        \
  X(ERROR_HOST_DOWN, EHOSTUNREACH)           \
  X(ERROR_HOST_UNREACHABLE, EHOSTUNREACH)    \
  X(ERROR_INSUFFICIENT_BUFFER, EFAULT)       \
  X(ERROR_INVALID_ADDRESS, EADDRNOTAVAIL)    \
  X(ERROR_INVALID_FUNCTION, EINVAL)          \
  X(ERROR_INVALID_HANDLE, EBADF)             \
  X(ERROR_INVALID_NETNAME, EADDRNOTAVAIL)    \
  X(ERROR_INVALID_PARAMETER, EINVAL)         \
  X(ERROR_INVALID_USER_BUFFER, EMSGSIZE)     \
  X(ERROR_IO_PENDING, EINPROGRESS)           \
  X(ERROR_LOCK_VIOLATION, EACCES)            \
  X(ERROR_MORE_DATA, EMSGSIZE)               \
  X(ERROR_NETNAME_DELETED, ECONNABORTED)     \
  X(ERROR_NETWORK_ACCESS_DENIED, EACCES)     \
  X(ERROR_NETWORK_BUSY, ENETDOWN)            \
  X(ERROR_NETWORK_UNREACHABLE, ENETUNREACH)  \
  X(ERROR_NOACCESS, EFAULT)                  \
  X(ERROR_NONPAGED_SYSTEM_RESOURCES, ENOMEM) \
  X(ERROR_NOT_ENOUGH_MEMORY, ENOMEM)         \
  X(ERROR_NOT_ENOUGH_QUOTA, ENOMEM)          \
  X(ERROR_NOT_FOUND, ENOENT)                 \
  X(ERROR_NOT_LOCKED, EACCES)                \
  X(ERROR_NOT_READY, EACCES)                 \
  X(ERROR_NOT_SAME_DEVICE, EXDEV)            \
  X(ERROR_NOT_SUPPORTED, ENOTSUP)            \
  X(ERROR_NO_MORE_FILES, ENOENT)             \
  X(ERROR_NO_SYSTEM_RESOURCES, ENOMEM)       \
  X(ERROR_OPERATION_ABORTED, EINTR)          \
  X(ERROR_OUT_OF_PAPER, EACCES)              \
  X(ERROR_PAGED_SYSTEM_RESOURCES, ENOMEM)    \
  X(ERROR_PAGEFILE_QUOTA, ENOMEM)            \
  X(ERROR_PATH_NOT_FOUND, ENOENT)            \
  X(ERROR_PIPE_NOT_CONNECTED, EPIPE)         \
  X(ERROR_PORT_UNREACHABLE, ECONNRESET)      \
  X(ERROR_PROTOCOL_UNREACHABLE, ENETUNREACH) \
  X(ERROR_REM_NOT_LIST, ECONNREFUSED)        \
  X(ERROR_REQUEST_ABORTED, EINTR)            \
  X(ERROR_REQ_NOT_ACCEP, EWOULDBLOCK)        \
  X(ERROR_SECTOR_NOT_FOUND, EACCES)          \
  X(ERROR_SEM_TIMEOUT, ETIMEDOUT)            \
  X(ERROR_SHARING_VIOLATION, EACCES)         \
  X(ERROR_TOO_MANY_NAMES, ENOMEM)            \
  X(ERROR_TOO_MANY_OPEN_FILES, EMFILE)       \
  X(ERROR_UNEXP_NET_ERR, ECONNABORTED)       \
  X(ERROR_WAIT_NO_CHILDREN, ECHILD)          \
  X(ERROR_WORKING_SET_QUOTA, ENOMEM)         \
  X(ERROR_WRITE_PROTECT, EACCES)             \
  X(ERROR_WRONG_DISK, EACCES)                \
  X(WSAEACCES, EACCES)                       \
  X(WSAEADDRINUSE, EADDRINUSE)               \
  X(WSAEADDRNOTAVAIL, EADDRNOTAVAIL)         \
  X(WSAEAFNOSUPPORT, EAFNOSUPPORT)           \
  X(WSAECONNABORTED, ECONNABORTED)           \
  X(WSAECONNREFUSED, ECONNREFUSED)           \
  X(WSAECONNRESET, ECONNRESET)               \
  X(WSAEDISCON, EPIPE)                       \
  X(WSAEFAULT, EFAULT)                       \
  X(WSAEHOSTDOWN, EHOSTUNREACH)              \
  X(WSAEHOSTUNREACH, EHOSTUNREACH)           \
  X(WSAEINPROGRESS, EINPROGRESS)             \
  X(WSAEINTR, EINTR)                         \
  X(WSAEINVAL, EINVAL)                       \
  X(WSAEISCONN, EISCONN)                     \
  X(WSAEMSGSIZE, EMSGSIZE)                   \
  X(WSAENETDOWN, ENETDOWN)                   \
  X(WSAENETRESET, EHOSTUNREACH)              \
  X(WSAENETUNREACH, ENETUNREACH)             \
  X(WSAENOBUFS, ENOMEM)                      \
  X(WSAENOTCONN, ENOTCONN)                   \
  X(WSAENOTSOCK, ENOTSOCK)                   \
  X(WSAEOPNOTSUPP, EOPNOTSUPP)               \
  X(WSAEPROCLIM, ENOMEM)                     \
  X(WSAESHUTDOWN, EPIPE)                     \
  X(WSAETIMEDOUT, ETIMEDOUT)                 \
  X(WSAEWOULDBLOCK, EWOULDBLOCK)             \
  X(WSANOTINITIALISED, ENETDOWN)             \
  X(WSASYSNOTREADY, ENETDOWN)                \
  X(WSAVERNOTSUPPORTED, ENOSYS)

static errno_t
err__map_win_error_to_errno( DWORD error )
{
  switch ( error ) {
#define X( error_sym, errno_sym )                                              \
  case error_sym: return errno_sym;
    ERR__ERRNO_MAPPINGS( X )
#undef X
  }
  return EINVAL;
}

void
err_map_win_error( void )
{
  DWORD e = GetLastError();
  errno = err__map_win_error_to_errno( e );
}

void
err_set_win_error( DWORD error )
{
  SetLastError( error );
  errno = err__map_win_error_to_errno( error );
}

int
err_check_handle( HANDLE handle )
{
  DWORD flags;

  /* GetHandleInformation() succeeds when passed INVALID_HANDLE_VALUE, so check
   * for this condition explicitly. */
  if ( handle == INVALID_HANDLE_VALUE )
    return_set_error( -1, ERROR_INVALID_HANDLE );

  if ( !GetHandleInformation( handle, &flags ) )
    return_map_error( -1 );

  return 0;
}
