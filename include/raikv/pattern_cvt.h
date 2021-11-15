#ifndef __rai_raikv__pattern_cvt_h__
#define __rai_raikv__pattern_cvt_h__

namespace rai {
namespace kv {

/* convert glob pattern or nats/rv/capr sub pattern to pcre pattern */
struct PatternCvt {
  static const size_t MAX_PREFIX_LEN = 63; /* uint64_t bit mask */
  char         buf[ 128 ],
             * out;          /* dynamic or static when == buf */
  const char * suffix;
  size_t       off,          /* off will be > maxlen on buf space failure */
               prefixlen,    /* size of literal prefix */
               suffixlen;
  size_t       maxlen;       /* max size of output */
  void       * tmp;          /* tmp alloc if needed for output */
  uint32_t     shard_num,    /* if [S,N] suffix */
               shard_total;
  bool         match_prefix; /* if matching prefix of subject */

  PatternCvt()
    : out( this->buf ), suffix( 0 ), off( 0 ), prefixlen( 0 ), suffixlen( 0 ),
      maxlen( sizeof( this->buf ) ), tmp( 0 ), shard_num( 0 ),
      shard_total( 0 ), match_prefix( false ) {}
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

  static bool rev_scan_int( const char *pattern,  size_t &i,  uint32_t &v ) {
    uint32_t pow = 1;
    v = 0;
    while ( i > 0 && pattern[ i - 1 ] >= '0' && pattern[ i - 1 ] <= '9') {
      v += (uint32_t) ( pattern[ --i ] - '0' ) * pow;
      pow *= 10;
    }
    return pow > 1;
  }

  template<char trail_char>
  static bool match_trail_wild( const char *pattern,  size_t patlen ) {
    if ( pattern[ patlen - 1 ] != trail_char )
      return false;
    if ( patlen == 1 )
      return true;
    if ( trail_char == '*' && pattern[ patlen - 2 ] == '\\' )
      return false;
    if ( trail_char == '>' && pattern[ patlen - 2 ] != '.' )
      return false;
    return true;
  }

  void match_shard( const char *pattern,  size_t &patlen ) {
    /* (S,N) */
    size_t i = patlen - 1;
    if ( pattern[ i ] == ')' ) { /* look for [S,N] */
      if ( rev_scan_int( pattern, i, this->shard_total ) ) {
        if ( i > 0 && pattern[ --i ] == ',' ) {
          if ( rev_scan_int( pattern, i, this->shard_num ) ) {
            if ( i > 0 && pattern[ --i ] == '(' ) {
              patlen = i;
              return;
            }
          }
        }
        this->shard_total = 0;
        this->shard_num   = 0;
      }
    }
  }
  /* return 0 on success or -1 on failure
   * normal glob rules:
   *  * = zero or more chars
   *  (S,N) = shard S of N
   *  ? = zero or one char
   *  [xyz] = x | y | z
   *  [^xyz] = ! ( x | y | z ) */
  int convert_glob( const char *pattern,  size_t patlen ) {
    size_t k, j = 0, suf = 0;
    bool   inside_bracket,
           anchor_end = ! this->match_prefix;

    this->off = 0;
    if ( patlen > 0 ) {
      this->str_out( "(?s)\\A", 6 ); /* match nl & anchor start */
      this->match_shard( pattern, patlen );
      if ( this->match_trail_wild<'*'>( pattern, patlen ) ) {
        patlen = patlen - 1; /* no need to match trail */
        anchor_end = false;
      }
      j = patlen;
      suf = patlen;
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
              else {
                suf = k + 1;
                this->str_out( ".*?", 3 ); /* commit and star */
              }
              break;
            case '?':
              if ( j > k ) j = k;
              suf = k + 1;
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
          suf = k + 1;
          this->char_out( pattern[ k ] );
        }
      }
      if ( anchor_end )
        this->str_out( "\\z", 2 ); /* anchor at end */
    }
    this->prefixlen = j;
    if ( anchor_end )
      this->suffixlen = patlen - suf;
    this->suffix = &pattern[ suf ];
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
    size_t k, j = 0, suf = 0;
    bool   anchor_end = ! this->match_prefix;

    this->off = 0;
    if ( patlen > 0 ) {
      this->str_out( "(?s)\\A", 6 ); /* match nl & anchor start */
      this->match_shard( pattern, patlen );
      if ( this->match_trail_wild<'>'>( pattern, patlen ) ) {
        patlen = patlen - 1; /* no need to match trail */
        anchor_end = false;
      }
      j = patlen;
      suf = patlen;
      for ( k = 0; k < patlen; k++ ) {
        if ( pattern[ k ] == '*' ) {
          if ( j > k ) j = k;
          suf = k + 1;
          this->str_out( "[^.]+", 5 );
        }
        else if ( pattern[ k ] == '>' ) {
          if ( j > k ) j = k;
          suf = k + 1;
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
    if ( anchor_end )
      this->suffixlen = patlen - suf;
    this->suffix = &pattern[ suf ];
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
