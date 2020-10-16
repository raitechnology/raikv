#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <raikv/util.h>
#include <raikv/key_hash.h>
#include <immintrin.h>
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>
#include <raikv/pattern_cvt.h>


#if 0
uint64_t
get_rdtsc( void )
{
   uint32_t lo, hi;
  __asm__ __volatile__("rdtsc" : "=a" (lo), "=d" (hi));
  return ( (uint64_t) hi << 32 ) | (uint64_t) lo;
}

void escape( void *p ) {
  __asm__ __volatile__ ( "" : : "g"(p) : "memory" );
}
#endif

using namespace rai;
using namespace kv;

enum reg_kind { IS_GLOB, IS_NATS };

static void
do_match( reg_kind t,  const char **pat,  size_t pc,
                       const char **match,  size_t mc )
{
  for ( size_t i = 0; i < pc; i++ ) {
    pcre2_real_code_8       * re = NULL; /* pcre regex compiled */
    pcre2_real_match_data_8 * md = NULL; /* pcre match context  */
    char       buf[ 256 ];
    PatternCvt cvt( buf, sizeof( buf ) );
    size_t     erroff;
    int        rc, rj = 0,
               error;
    
    if ( t == IS_GLOB )
      rc = cvt.convert_glob( pat[ i ], ::strlen( pat[ i ] ) );
    else
      rc = cvt.convert_rv( pat[ i ], ::strlen( pat[ i ] ) );
    printf( "\n(%s) \"%s\" : \"%.*s\" -> \"%.*s\" (%lu)\n",
            ( t == IS_GLOB ? "glob" : "rv" ), pat[ i ],
            (int) cvt.off, buf, (int) cvt.prefixlen, pat[ i ],
            cvt.prefixlen );
    if ( rc != 0 ) {
      printf( "convert failed\n" );
      continue;
    }
    re = pcre2_compile( (uint8_t *) buf, cvt.off, 0, &error, &erroff, 0 );
    if ( re != NULL )
      rj = pcre2_jit_compile( re, PCRE2_JIT_COMPLETE );
    if ( re == NULL || rj != 0 ) {
      printf( "re failed\n" );
      continue;
    }
    md = pcre2_match_data_create_from_pattern( re, NULL );
    if ( md == NULL ) {
      pcre2_code_free( re );
      printf( "md failed\n" );
      continue;
    }

    uint64_t t1 = kv_get_rdtsc();
    for ( int k = 0; k < 10000; k++ ) {
      for ( size_t j = 0; j < mc; j++ ) {
        rc = pcre2_jit_match( re, (const uint8_t *) match[ j ],
                              ::strlen( match[ j ] ), 0, 0, md, 0 );
        if ( k == 9999 ) {
          if ( j == 0 ) {
            uint64_t t2 = kv_get_rdtsc();
            printf( "%lu\n", ( t2 - t1 ) / ( 9999 * mc ) );
          }
          printf( "rc[%d]: %.*s %s\n", rc, (int) cvt.off, buf, match[ j ] );
        }
      }
    }
    pcre2_match_data_free( md );
    pcre2_code_free( re );
  }
}

int
main( int, char ** )
{
  const char *pat[] = {
    "h?ll*",
    "h*llo",
    "h[ae]llo",
    "h[^e]llo",
    "h[a-b]llo",
    "hello*"
  };
  const char *match[] = {
    "hello", "hallo", "hxllo",
    "hllo", "heeeello",
    "hillo",
    "hbllo",
    "ehello", "helloworld"
  };

  const char *patrv[] = {
    "hello.*",
    "hello.>",
    "hello.*.>",
    "*.again",
    "hello.*.again"
  };
  const char *matchrv[] = {
    "hello.world",
    "hallo",
    "hello.world.again",
    "testing.again",
    "he.world",
  };

  const size_t pc = sizeof( pat ) / sizeof( pat[ 0 ] );
  const size_t mc = sizeof( match ) / sizeof( match[ 0 ] );
  const size_t prvc = sizeof( patrv ) / sizeof( patrv[ 0 ] );
  const size_t mrvc = sizeof( matchrv ) / sizeof( matchrv[ 0 ] );

  do_match( IS_GLOB, pat, pc, match, mc );
  do_match( IS_NATS, patrv, prvc, matchrv, mrvc );

  return 0;
}

