#ifndef __rai_raikv__logger_h__
#define __rai_raikv__logger_h__

namespace rai {
namespace kv {

/* redirect stdout / stderr,
 * this closes the terminal output and redirects to pipes
 * to save the terminal, must use dup( 1 ), dup( 2 ) before start() */
struct EvPoll;
struct Logger {
  Logger() {}
  int start( void ) noexcept;         /* redirect stdout, stderr to pipe */
  int start_ev( EvPoll &p ) noexcept; /* redirect stdout, stderr to pipe */
  int shutdown( void ) noexcept;      /* stop thread that is reading pipe */
  int flush( void ) noexcept;

  /* test if any output available */
  bool avail( void ) noexcept;
  /* thse return 0 when no data, or a timestamp of the line */
  uint64_t read_stdout( char *line, size_t &linelen ) noexcept;
  uint64_t read_stderr( char *line, size_t &linelen ) noexcept;

  static Logger *create( void ) noexcept; /* allocate and init structure */
};

}
}

#endif
