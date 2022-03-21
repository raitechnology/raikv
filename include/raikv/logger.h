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
  uint64_t last_secs,
           last_ms;
  char     ts[ TSERR_OFF ];

  Logger() : last_secs( 0 ), last_ms( 0 ) {}
  int start( void ) noexcept;         /* redirect stdout, stderr to pipe */
  int start_ev( EvPoll &p ) noexcept; /* redirect stdout, stderr to pipe */
  int shutdown( void ) noexcept;      /* stop thread that is reading pipe */

  /* test if any output available */
  bool avail( void ) noexcept;
  /* thse return 0 when no data, or a timestamp of the line */
  uint64_t read_stdout( char *line, size_t &linelen ) noexcept;
  uint64_t read_stderr( char *line, size_t &linelen ) noexcept;

  static Logger *create( void ) noexcept; /* allocate and init structure */

  void update_timestamp( uint64_t stamp ) {
    uint64_t secs = stamp / (uint64_t) ( 1000 * 1000 * 1000 );
    uint64_t ms   = stamp / (uint64_t) ( 1000 * 1000 );
    if ( secs != this->last_secs ) {
      uint32_t ar[ 3 ], j = 0;
      ar[ 2 ] = secs % 60,
      ar[ 1 ] = ( secs / 60 ) % 60;
      ar[ 0 ] = ( secs / 3600 ) % 24;
      for ( int i = 0; i < 3; i++ ) {
        this->ts[ j++ ] = ( ar[ i ] / 10 ) + '0';
        this->ts[ j++ ] = ( ar[ i ] % 10 ) + '0';
        this->ts[ j++ ] = ( i == 2 ? '.' : ':' );
      }
      this->last_secs = secs;
    }
    if ( ms != this->last_ms ) {
      this->ts[ TS_LEN+1 ] = ( ( ms / 100 ) % 10 ) + '0';
      this->ts[ TS_LEN+2 ] = ( ( ms / 10 ) % 10 ) + '0';
      this->ts[ TS_LEN+3 ] = ( ms % 10 ) + '0';
      this->last_ms = ms;
    }
  }
};

}
}

#endif
