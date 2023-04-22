#ifndef __rai_raikv__win_h__
#define __rai_raikv__win_h__

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

enum EPOLL_EVENTS {
  EPOLLIN        = 0x001,
#define EPOLLIN EPOLLIN
  EPOLLPRI       = 0x002,
#define EPOLLPRI EPOLLPRI
  EPOLLOUT       = 0x004,
#define EPOLLOUT EPOLLOUT
  EPOLLRDNORM    = 0x040,
#define EPOLLRDNORM EPOLLRDNORM
  EPOLLRDBAND    = 0x080,
#define EPOLLRDBAND EPOLLRDBAND
  EPOLLWRNORM    = 0x100,
#define EPOLLWRNORM EPOLLWRNORM
  EPOLLWRBAND    = 0x200,
#define EPOLLWRBAND EPOLLWRBAND
  EPOLLMSG       = 0x400,
#define EPOLLMSG EPOLLMSG
  EPOLLERR       = 0x008,
#define EPOLLERR EPOLLERR
  EPOLLHUP       = 0x010,
#define EPOLLHUP EPOLLHUP
  EPOLLRDHUP     = 0x2000,
#define EPOLLRDHUP EPOLLRDHUP
  EPOLLEXCLUSIVE = 1u << 28,
#define EPOLLEXCLUSIVE EPOLLEXCLUSIVE
  EPOLLWAKEUP    = 1u << 29,
#define EPOLLWAKEUP EPOLLWAKEUP
  EPOLLONESHOT   = 1u << 30,
#define EPOLLONESHOT EPOLLONESHOT
  EPOLLET        = 1u << 31
#define EPOLLET EPOLLET
};

#define EPOLL_CTL_ADD 1
#define EPOLL_CTL_MOD 2
#define EPOLL_CTL_DEL 3
/* edge trigger causes read to suspend until blocked */
#define EPOLLOUT_TRIGGERED ( 1u << 26 )
#define EPOLLET_TRIGGERED ( 1u << 27 )

#undef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN

#undef _WIN32_WINNT
#define _WIN32_WINNT 0x0600

#include <winsock2.h>
#include <ws2tcpip.h>
#include <ws2spi.h>
#include <mstcpip.h>
#include <windows.h>

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
struct msghdr {
  void         * msg_name;       /* optional address */
  size_t         msg_namelen;    /* size of address */
  struct iovec * msg_iov;        /* scatter/gather array */
  size_t         msg_iovlen;     /* # elements in msg_iov */
  void         * msg_control;    /* ancillary data, see below */
  size_t         msg_controllen; /* ancillary data buffer len */
  int            msg_flags;      /* flags on received message */
};
struct mmsghdr {
  struct msghdr msg_hdr;
  uint32_t      msg_len;
};

int epoll_create( int size );
int epoll_create1( int flags );
int epoll_close( int fd );
struct epoll_event;
int epoll_ctl( int fd,  int op,  int sock,  struct epoll_event* event );
int wp_epoll_wait( int fd,  struct epoll_event* events,  int maxevents,
                   uint64_t us_timeout );
int wp_make_null_fd( void );
int wp_close_fd( int fd );
int wp_shutdown_fd( int fd, int how );
ssize_t wp_read( int fd,  void *buf,  size_t buflen );
ssize_t wp_send( int fd,  struct iovec *buf,  size_t nbufs );
ssize_t wp_recvmsg( int fd,  struct msghdr *msg );
ssize_t wp_sendmsg( int fd,  struct msghdr *msg );
uint32_t getpid( void );
uint32_t getthrid( void );
int pidexists( uint32_t pid );
#define strcasecmp _stricmp
#define strncasecmp _strnicmp

#define return_map_error(value) \
  do {                          \
    err_map_win_error();        \
    return (value);             \
  } while (0)

#define return_set_error(value, error) \
  do {                                 \
    err_set_win_error(error);          \
    return (value);                    \
  } while (0)

void err_map_win_error( void );
void err_set_win_error( DWORD error );
int err_check_handle( HANDLE handle );

typedef enum {
  WP_FD_NONE = 0,
  WP_FD_USED,
  WP_FD_EPOLL,
  WP_FD_SOCKET,
  WP_FD_TTY
} wp_fd_type_t;

#define MAX_FD_MAP_SIZE ( 8 * 64 ) /* 512 */

typedef struct wp_port_state {
  uint64_t poll_set_fds[ MAX_FD_MAP_SIZE / 64 ];
  uint64_t us_wait_accum; /* wait is in usecs, this accumulates */
} wp_port_state_t;

typedef union epoll_data {
  void* ptr;
  int fd;
  uint32_t u32;
  uint64_t u64;
} epoll_data_t;

struct epoll_event {
  uint32_t events;   /* Epoll events and flags */
  epoll_data_t data; /* User data variable */
};

typedef struct wp_epoll_state {
  epoll_data_t      user_data;   /* fd */
  uint32_t          user_events; /* EPOLLIN, EPOLLOUT */
} wp_epoll_state_t;

typedef struct wp_sock_state {
  epoll_data_t      user_data;   /* fd */
  uint32_t          user_events; /* EPOLLIN, EPOLLOUT */
  wp_port_state_t * poll_group;  /* which wp_port_state_t group */
  SOCKET            socket;      /* windows handle */
  WSAEVENT          event;       /* WaitForMultipleEvents */
} wp_sock_state_t;

typedef ssize_t (* tty_reader_f )( void *p,  void *buf,  size_t buflen );

typedef struct wp_tty_state {
  epoll_data_t      user_data;   /* fd */
  uint32_t          user_events; /* EPOLLIN, EPOLLOUT */
  wp_port_state_t * poll_group;  /* which wp_port_state_t group */
  HANDLE            handle;      /* windows console handle */
  void            * closure;
  tty_reader_f      tty_reader;
} wp_tty_state_t;

typedef struct wp_fd_map {
  wp_fd_type_t type;
  union {
    wp_port_state_t  port; /* POLL */
    wp_epoll_state_t state;/* common state */
    wp_sock_state_t  sock; /* SOCKET */
    wp_tty_state_t   tty;  /* HANDLE */
  };
} wp_fd_map_t;

int ws_global_init( void );
int wp_get_socket( int fd,  SOCKET *s );
int wp_register_fd( SOCKET sock );
int wp_register_listen_fd( SOCKET sock );
int wp_register_tty_fd( HANDLE h,  void *cl,  tty_reader_f rdr );
int wp_unregister_fd( int fd );

#ifdef __cplusplus
}
#endif

#endif
