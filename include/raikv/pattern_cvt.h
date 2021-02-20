#ifndef __rai_raikv__pattern_cvt_h__
#define __rai_raikv__pattern_cvt_h__

namespace rai {
namespace kv {

/* convert glob pattern or nats/rv/capr sub pattern to pcre pattern */
struct PatternCvt {
  static const size_t MAX_PREFIX_LEN = 63; /* uint64_t bit mask */
  char   buf[ 128 ];
  char * out;          /* utf8 or utf32 char classes */
  size_t off,          /* off will be > maxlen on buf space failure */
         prefixlen;    /* size of literal prefix */
  size_t maxlen;       /* max size of output */
  void * tmp;          /* tmp alloc if needed */
  bool   match_prefix; /* if matching prefix of subject */

  PatternCvt()
    : out( this->buf ), off( 0 ), prefixlen( 0 ), maxlen( sizeof( this->buf ) ),
      tmp( 0 ), match_prefix( false ) {}
  ~PatternCvt() {
    if ( this->tmp != NULL )
      ::free( this->tmp );
  }

  void more_out( void ) {
    size_t len = this->maxlen * 2;
    void *p = ::realloc( this->tmp, len );
    if ( p != NULL ) {
      if ( this->tmp == NULL )
        ::memcpy( p, this->buf, this->off );
      this->tmp    = p;
      this->maxlen = len;
      this->out    = (char *) p;
    }
  }

  void char_out( char c ) {
    if ( this->off == this->maxlen )
      this->more_out();
    if ( ++this->off <= this->maxlen )
      this->out[ off - 1 ] = c;
  }

  void str_out( const char *s,  size_t len ) {
    for ( size_t i = 0; i < len; i++ )
      this->char_out( s[ i ] );
  }
  /* return 0 on success or -1 on failure
   * normal glob rules:
   *  * = zero or more chars
   *  ? = zero or one char
   *  [xyz] = x | y | z
   *  [^xyz] = ! ( x | y | z ) */
  int convert_glob( const char *pattern,  size_t patlen ) {
    size_t k, j = 0;
    bool   inside_bracket,
           anchor_end = ! this->match_prefix;

    this->off = 0;
    if ( patlen > 0 ) {
      this->str_out( "(?s)\\A", 6 ); /* match nl & anchor start */
      if ( pattern[ patlen - 1 ] == '*' ) {
        if ( patlen == 1 || pattern[ patlen - 2 ] != '\\' ) {
          patlen -= 1; /* no need to match trail */
          anchor_end = false;
        }
      }
      j = patlen;
      inside_bracket = false;
      for ( k = 0; k < patlen; k++ ) {
        if ( pattern[ k ] == '\\' ) {
          if ( k + 1 < patlen ) {
            switch ( pattern[ ++k ] ) {
              case '\\': case '?': case '*': case '[': case ']': case '.':
                /* leave escape in these cases, chars have special meaning */
                this->char_out( '\\' );
                /* FALLTHRU */
              default:
                /* strip escape on others: \w \s \d are special to pcre */
                this->char_out( pattern[ k ] );
                break;
            }
          }
        }
        else if ( ! inside_bracket ) {
          switch ( pattern[ k ] ) {
            case '*':
              if ( j > k ) j = k;
              if ( k > 0 && pattern[ k - 1 ] == '*' )
                k++; /* skip duplicate '*' */
              else
                this->str_out( ".*?", 3 ); /* commit and star */
              break;
            case '?':
              if ( j > k ) j = k;
              this->char_out( '.' );
              break;
            case '.':
            case '+':
            case '(':
            case ')':
            case '{':
            case '}':
              this->char_out( '\\' );
              this->char_out( pattern[ k ] );
              break;
            case '[':
              if ( j > k ) j = k;
              inside_bracket = true;
              this->char_out( '[' );
              break;
            default:
              this->char_out( pattern[ k ] );
              break;
          }
        }
        else {
          if ( pattern[ k ] == ']' )
            inside_bracket = false;
          this->char_out( pattern[ k ] );
        }
      }
      if ( anchor_end )
        this->str_out( "\\z", 2 ); /* anchor at end */
    }
    this->prefixlen = j;
    if ( this->prefixlen > MAX_PREFIX_LEN )
      this->prefixlen = MAX_PREFIX_LEN;
    if ( this->off > this->maxlen )
      return -1;
    return 0;
  }

  /* rv/nats style wildcard:
   * * = [^.]+[.]  / match one segment
   * > = .+ / match one or more segments
   */
  int convert_rv( const char *pattern,  size_t patlen ) {
    size_t k, j = 0;
    bool   anchor_end = ! this->match_prefix;

    this->off = 0;
    if ( patlen > 0 ) {
      this->str_out( "(?s)\\A", 6 ); /* match nl & anchor start */
      if ( pattern[ patlen - 1 ] == '>' ) {
        if ( patlen == 1 || pattern[ patlen - 2 ] == '.' ) {
          patlen -= 1; /* no need to match trail */
          anchor_end = false;
        }
      }
      j = patlen;
      for ( k = 0; k < patlen; k++ ) {
        if ( pattern[ k ] == '*' ) {
          if ( j > k ) j = k;
          this->str_out( "[^.]+", 5 );
        }
        else if ( pattern[ k ] == '>' ) {
          if ( j > k ) j = k;
          this->str_out( ".+", 2 );
        }
        else if ( pattern[ k ] == '.' || pattern[ k ] == '?' ||
                  pattern[ k ] == '[' || pattern[ k ] == ']' ||
                  pattern[ k ] == '\\' || pattern[ k ] == '+' ) {
          this->char_out( '\\' );
          this->char_out( pattern[ k ] );
        }
        else {
          this->char_out( pattern[ k ] );
        }
      }
      if ( anchor_end )
        this->str_out( "\\z", 2 ); /* anchor at end */
    }
    this->prefixlen = j;
    if ( this->prefixlen > MAX_PREFIX_LEN )
      this->prefixlen = MAX_PREFIX_LEN;
    if ( this->off > this->maxlen )
      return -1;
    return 0;
  }
  /* prce wildcard constructed by one of the above to prefix */
  int pcre_prefix( const char *pattern,  size_t patlen ) {
    size_t k, j;
    this->off = 0;
    if ( patlen > 6 && ::memcmp( pattern, "(?s)\\A", 6 ) == 0 ) {
      for ( k = 6; k < patlen; k++ ) {
        if ( pattern[ k ] == '[' ||
             pattern[ k ] == '.' )
          break;
        if ( pattern[ k ] == '\\' ) {
          j = k + 1;
          if ( j < patlen ) {
            if ( pattern[ j ] == 'z' ) /* end anchor */
              break;
            this->char_out( pattern[ j ] );
            k++;
          }
        }
        else {
          this->char_out( pattern[ k ] );
        }
      }
    }
    this->prefixlen = this->off;
    return 0;
  }
};

}
}

#endif
