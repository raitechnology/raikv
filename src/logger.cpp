#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#ifndef _MSC_VER
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#else
#include <windows.h>
#include <io.h>
#endif
#include <raikv/logger.h>
#include <raikv/util.h>
#include <raikv/array_space.h>
#include <raikv/ev_net.h>

using namespace rai;
using namespace kv;

struct LogTimeIndex {
  uint64_t time_ns;
  size_t   offset;  /* offset of buffer when data was read */
};

struct LogOutput {
  ArraySpace<char, 1024>       out;   /* output buffer */
  ArraySpace<LogTimeIndex, 32> stamp; /* time stamp of buffer offsets */
  size_t off,      /* out read offset */
         len,      /* out write offset */
         time_off, /* stamp read offset */
         time_len; /* stamp write offset */
  LogOutput() : off( 0 ), len( 0 ), time_off( 0 ), time_len( 0 ) {}
  /* new data appended to out */
  void append( uint64_t cur_ns,  const char *data,  size_t datalen ) noexcept;
  /* read a line from out, return stamp */
  uint64_t getline( char *line,  size_t &datalen ) noexcept;
};

struct LoggerContext;
struct LogOutput;

struct EvLogger : public EvConnection {
  LoggerContext & log;
  LogOutput     & out;

  void * operator new( size_t, void *ptr ) { return ptr; }
  EvLogger( EvPoll &p,  LoggerContext &c,  LogOutput &o ) :
    EvConnection( p, p.register_type( "logger" ) ), log( c ), out( o ) {}
  bool start( int fd,  const char *name ) noexcept;
  virtual void process( void ) noexcept final;
  virtual void release( void ) noexcept final {}
};

static const int STDOUT_FD = 1,
                 STDERR_FD = 2;

struct LoggerContext : public Logger {
  LogOutput  out,       /* stdout buffer */
             err;       /* stderr buffer */
#ifndef _MSC_VER
  int        pout[ 2 ], /* pipe fds, out[ 0 ] = logger input, out[ 1 ] = stdout*/
             perr[ 2 ]; /* pipe fds, err[ 0 ] = logger input, err[ 1 ] = stderr*/
#else
  HANDLE     pout[ 2 ],
             perr[ 2 ];
  OVERLAPPED out_ov,
             err_ov;
  HANDLE     event[ 2 ];
  char       out_buf[ 4 * 1024 ],
             err_buf[ 4 * 1024 ];
#endif
  int        quit;
  EvLogger * ev_out,
           * ev_err;

  void * operator new( size_t, void *ptr ) { return ptr; }
  LoggerContext() : quit( 0 ) {
#ifndef _MSC_VER
    this->pout[ 0 ] = -1;
    this->pout[ 1 ] = -1;
    this->perr[ 0 ] = -1;
    this->perr[ 1 ] = -1;
#else
    this->pout[ 0 ] = INVALID_HANDLE_VALUE;
    this->pout[ 1 ] = INVALID_HANDLE_VALUE;
    this->perr[ 0 ] = INVALID_HANDLE_VALUE;
    this->perr[ 1 ] = INVALID_HANDLE_VALUE;
    ::memset( &this->out_ov, 0, sizeof( this->out_ov ) );
    ::memset( &this->err_ov, 0, sizeof( this->err_ov ) );
    this->event[ 0 ]    = CreateEvent( NULL, TRUE, FALSE, NULL );
    this->event[ 1 ]    = CreateEvent( NULL, TRUE, FALSE, NULL );
    this->out_ov.hEvent = this->event[ 0 ];
    this->err_ov.hEvent = this->event[ 1 ];
#endif
  }
#ifdef _MSC_VER
  void read( int fd ) noexcept;
  int wait( void ) noexcept;
  int result( int fd, char *&buf ) noexcept;
#endif
  /* poll fds and eat pipes */
  bool run( void ) noexcept;
};
#ifdef _MSC_VER
static bool create_named_pipe( HANDLE *p ) noexcept;
static bool dup_pipe_to_fd( HANDLE p,  int fd ) noexcept;
#endif

void
LogOutput::append( uint64_t cur_ns, const char *data, size_t datalen ) noexcept
{
  LogTimeIndex * stp = this->stamp.make( this->time_len + 1 );
  char         * buf = this->out.make( this->len + datalen );

  stp[ this->time_len ].offset  = this->len;
  stp[ this->time_len ].time_ns = cur_ns;
  this->time_len++;

  ::memcpy( &buf[ this->len ], data, datalen );
  this->len += datalen;
}

uint64_t
LogOutput::getline( char *line,  size_t &linelen ) noexcept
{
  if ( linelen == 0 )
    return 0;
  if ( this->len + this->time_len == 0 )
    return 0;

  const char * cur_ptr = &this->out.ptr[ this->off ],
             * end_ptr = &this->out.ptr[ this->len ],
             * p;
  size_t       size    = (size_t) ( end_ptr - cur_ptr ),
               maxlen;
  uint64_t     line_time_ns = 0;

  if ( (p = (const char *) ::memchr( cur_ptr, '\n', size )) != NULL ) {
    end_ptr = p + 1;
    size    = (size_t) ( end_ptr - cur_ptr );
    maxlen  = linelen - 1;

    if ( size > maxlen )
      size = maxlen;
    ::memcpy( line, cur_ptr, size );
    line[ size ] = '\0';
    linelen = size;

    LogTimeIndex * stp = &this->stamp.ptr[ this->time_off ],
                 * end = &this->stamp.ptr[ this->time_len ];

    while ( &stp[ 1 ] < end && stp[ 1 ].offset >= this->off ) {
      stp++;
      this->time_off++;
    }
    line_time_ns = stp->time_ns;
    this->off += size;

    if ( this->off == this->len ) {
      this->off = 0;
      this->len = 0;
      this->time_off = 0;
      this->time_len = 0;
      if ( this->out.size > 16 * 1024 ) {
        this->out.reset();
        this->stamp.reset();
      }
    }
  }

  return line_time_ns;
}

uint64_t
Logger::read_stdout( char *line, size_t &linelen ) noexcept
{
  LoggerContext & log = (LoggerContext &) *this;
  return log.out.getline( line, linelen );
}

uint64_t
Logger::read_stderr( char *line, size_t &linelen ) noexcept
{
  LoggerContext & log = (LoggerContext &) *this;
  return log.err.getline( line, linelen );
}

bool
Logger::avail( void ) noexcept
{
  LoggerContext & log = (LoggerContext &) *this;
  if ( log.ev_out == NULL )
    log.run();
  return ( log.out.off != log.out.len || log.err.off != log.err.len );
}

bool
LoggerContext::run( void ) noexcept
{
  bool b   = false;
#ifndef _MSC_VER
  char buf[ 16 * 1024 ];
  int  out = this->pout[ 0 ],
       err = this->perr[ 0 ];

  struct pollfd fds[ 2 ] = {
    { out, POLLIN, POLLIN },
    { err, POLLIN, POLLIN }
  };
  int x = ::poll( fds, 2, 0 );
  uint64_t cur_ns = 0;
  if ( x != 0 )
    cur_ns = kv_current_realtime_ns();
  while ( x > 0 ) {
    int n;
    x = 0;
    if ( ( fds[ 0 ].revents & POLLIN ) != 0 ) {
      if ( (n = ::read( out, buf, sizeof( buf ) )) > 0 ) {
        x += n;
        b = true;
        this->out.append( cur_ns, buf, n );
      }
    }
    if ( ( fds[ 1 ].revents & POLLIN ) != 0 ) {
      if ( (n = ::read( err, buf, sizeof( buf ) )) > 0 ) {
        x += n;
        b = true;
        this->err.append( cur_ns, buf, n );
      }
    }
  }
#else
  fflush( stdout ); /* no linebuffering on windows */
  fflush( stderr );
  uint64_t cur_ns = 0;
  for (;;) {
    int n, fd = this->wait();
    char * buf;
    if ( fd == 0 )
      break;
    if ( cur_ns == 0 )
      cur_ns = kv_current_realtime_ns();
    if ( (n = this->result( fd, buf )) > 0 ) {
      if ( fd == STDOUT_FD )
        this->out.append( cur_ns, buf, n );
      else
        this->err.append( cur_ns, buf, n );
      this->read( fd );
    }
  }
#endif
  return b;
}

int
Logger::start( void ) noexcept
{
  LoggerContext & log = (LoggerContext &) *this;
#ifndef _MSC_VER
  if ( ::pipe( log.pout ) < 0 ||
       ::pipe( log.perr ) < 0 ||
       ::dup2( log.pout[ 1 ], STDOUT_FD ) < 0 ||
       ::dup2( log.perr[ 1 ], STDERR_FD ) < 0 )
    return -1;
  ::close( log.pout[ 1 ] );
  ::close( log.perr[ 1 ] );

  ::fcntl( STDOUT_FD, F_SETPIPE_SZ, 1024 * 1024 );
  ::fcntl( STDERR_FD, F_SETPIPE_SZ, 1024 * 1024 );
  int fd[ 4 ] = { log.pout[ 0 ], log.perr[ 0 ], STDOUT_FD, STDERR_FD };
  for ( int i = 0; i < 4; i++ )
    ::fcntl( fd[ i ], F_SETFL, O_NONBLOCK | ::fcntl( fd[ i ], F_GETFL ) );
#else
  if ( ! create_named_pipe( log.pout ) ||
       ! create_named_pipe( log.perr ) ||
       ! dup_pipe_to_fd( log.pout[ 1 ], STDOUT_FD ) ||
       ! dup_pipe_to_fd( log.perr[ 1 ], STDERR_FD ) )
    return -1;
  log.read( STDOUT_FD );
  log.read( STDERR_FD );
#endif
  ::setvbuf( stdout, NULL, _IOLBF, 1024 );
  ::setvbuf( stderr, NULL, _IOLBF, 1024 );
  return 0;
}

void
EvLogger::process( void ) noexcept
{
  size_t buflen = this->len - this->off;
  this->out.append( this->poll.current_coarse_ns(),
                    &this->recv[ this->off ], buflen );
  this->off = this->len;
  this->pop( EV_PROCESS );
}

bool
EvLogger::start( int fd,  const char *name ) noexcept
{
  this->PeerData::init_peer( fd, -1, NULL, name );
  return this->poll.add_sock( this ) == 0;
}

int
Logger::start_ev( EvPoll &p ) noexcept
{
  LoggerContext & log = (LoggerContext &) *this;
  if ( log.start() != 0 )
    return -1;
#ifndef _MSC_VER
  void * x = aligned_malloc( sizeof( EvLogger ) ),
       * y = aligned_malloc( sizeof( EvLogger ) );
  log.ev_out = new ( x ) EvLogger( p, log, log.out );
  log.ev_err = new ( y ) EvLogger( p, log, log.err );

  if ( ! log.ev_out->start( log.pout[ 0 ], "stdout" ) ||
       ! log.ev_err->start( log.perr[ 0 ], "stderr" ) )
    return -1;
#endif
  return 0;
}

int
Logger::shutdown( void ) noexcept
{
  LoggerContext & log = (LoggerContext &) *this;
  if ( log.quit == 0 ) {
    log.quit = 1;
#ifndef _MSC_VER
    if ( log.ev_out == NULL && log.pout[ 0 ] != -1 )
      ::close( log.pout[ 0 ] );
    if ( log.ev_err == NULL && log.perr[ 0 ] != -1 )
      ::close( log.perr[ 0 ] );
#else
    if ( log.pout[ 0 ] != INVALID_HANDLE_VALUE )
      CancelIo( log.pout[ 0 ] );
    if ( log.perr[ 0 ] != INVALID_HANDLE_VALUE )
      CancelIo( log.perr[ 0 ] );
#endif
  }
  return 0;
}

Logger *
Logger::create( void ) noexcept
{
  void * p = ::malloc( sizeof( LoggerContext ) );
  if ( p == NULL )
    return NULL;
  return new ( p ) LoggerContext();
}

#ifdef _MSC_VER
static bool
create_named_pipe( HANDLE *p ) noexcept
{
  static volatile long serial_no;
  static const DWORD N_PIPE_SIZE    = 1024 * 1024,
                     N_PIPE_TIMEOUT = 60 * 1000;
  HANDLE rd, wr;
  char   name[ 64 ];

  ::snprintf( name, sizeof( name ), "\\\\.\\pipe\\LOCAL.%08x.%08x",
              GetCurrentProcessId(), InterlockedIncrement( &serial_no ) );

  rd = CreateNamedPipeA( name, PIPE_ACCESS_INBOUND | FILE_FLAG_OVERLAPPED,
                         PIPE_TYPE_BYTE | PIPE_WAIT, 1,
                         N_PIPE_SIZE, N_PIPE_SIZE, N_PIPE_TIMEOUT, NULL );

  if ( rd == INVALID_HANDLE_VALUE )
    return false;

  wr = CreateFileA( name, GENERIC_WRITE, 0, NULL, OPEN_EXISTING,
                    FILE_ATTRIBUTE_NORMAL, NULL );

  if ( wr == INVALID_HANDLE_VALUE ) {
    CloseHandle( rd );
    return false;
  }

  p[ 0 ] = rd;
  p[ 1 ] = wr;
  return true;
}

static bool
dup_pipe_to_fd( HANDLE p,  int fd ) noexcept
{
  int hfd = _open_osfhandle( (intptr_t) p, 0 );
  return _dup2( hfd, fd ) == 0;
}

void
LoggerContext::read( int fd ) noexcept
{
  if ( fd == STDOUT_FD )
    ReadFile( this->pout[ 0 ], this->out_buf, sizeof( this->out_buf ),
              NULL, &this->out_ov );
  else if ( fd == STDERR_FD )
    ReadFile( this->perr[ 0 ], this->err_buf, sizeof( this->err_buf ),
              NULL, &this->err_ov );
}

int
LoggerContext::wait( void ) noexcept
{
  switch ( WaitForMultipleObjects( 2, this->event, FALSE, 0 ) ) {
    case WAIT_OBJECT_0:   return STDOUT_FD;
    case WAIT_OBJECT_0+1: return STDERR_FD;
    default:              return 0;
  }
}

int
LoggerContext::result( int fd, char *&buf ) noexcept
{
  DWORD nbytes;
  if ( fd == STDOUT_FD ) {
    if ( GetOverlappedResult( this->pout[ 0 ], &this->out_ov, &nbytes,
                              FALSE ) ) {
      buf = this->out_buf;
      ResetEvent( this->event[ 0 ] );
      return nbytes;
    }
  }
  else if ( fd == STDERR_FD ) {
    if ( GetOverlappedResult( this->perr[ 0 ], &this->err_ov, &nbytes,
                              FALSE ) ) {
      buf = this->err_buf;
      ResetEvent( this->event[ 1 ] );
      return nbytes;
    }
  }
  return 0;
}
#endif
