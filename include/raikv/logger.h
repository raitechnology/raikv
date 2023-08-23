#ifndef __rai_raikv__logger_h__
#define __rai_raikv__logger_h__

namespace rai {
namespace kv {

/* redirect stdout / stderr,
 * this closes the terminal output and redirects to pipes
 * to save the terminal, must use dup( 1 ), dup( 2 ) before start() */
struct EvPoll;
struct Logger {
  static const size_t TS_LEN         = 8,
                      TSFRACTION_LEN = 3, /* 123 */
                      /* HH:MM:SS.123 */
                      TSERR_OFF      = TS_LEN + 1 + TSFRACTION_LEN,
                      /* HH:MM:SS.123xx */
                      TSHDR_LEN      = TS_LEN + 1 + TSFRACTION_LEN + 2;
  int64_t  tz_off_sec,
           tz_off_ns;
  uint64_t last_secs,
           last_ms;
  char     ts[ TSERR_OFF ];

  Logger() : tz_off_sec( 0 ), tz_off_ns( 0 ), last_secs( 0 ), last_ms( 0 ) {}
  int start( void ) noexcept;         /* redirect stdout, stderr to pipe */
  int start_ev( EvPoll &p ) noexcept; /* redirect stdout, stderr to pipe */
  int shutdown( void ) noexcept;      /* stop thread that is reading pipe */

  /* test if any output available */
  bool avail( void ) noexcept;
  /* thse return 0 when no data, or a timestamp of the line */
  uint64_t read_stdout( char *line, size_t &linelen ) noexcept;
  uint64_t read_stderr( char *line, size_t &linelen ) noexcept;
  static Logger *create( void ) noexcept; /* allocate and init structure */
  int output_log_file( const char *fn ) noexcept;
  void update_tz( void ) noexcept;
  void update_timestamp( uint64_t stamp ) noexcept;
};

}
}

#endif
