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


using namespace rai;
using namespace kv;

enum reg_kind { IS_GLOB, IS_NATS };

static int
do_match( reg_kind t,  const char **pat,  size_t pc,
                       const char **match,  size_t mc,
                       int *mat )
{
  int fail = 0;
  for ( size_t i = 0; i < pc; i++ ) {
    pcre2_real_code_8       * re = NULL; /* pcre regex compiled */
    pcre2_real_match_data_8 * md = NULL; /* pcre match context  */
    PatternCvt cvt, cvt2;
    size_t     erroff;
    int        rc, rj = 0,
               error;
    
    if ( t == IS_GLOB )
      rc = cvt.convert_glob( pat[ i ], ::strlen( pat[ i ] ) );
    else
      rc = cvt.convert_rv( pat[ i ], ::strlen( pat[ i ] ) );
    printf( "\n(%s) \"%s\" : \"%.*s\" -> \"%.*s\" (%lu) \"%.*s\" (%lu)",
            ( t == IS_GLOB ? "glob" : "rv" ), pat[ i ],
            (int) cvt.off, cvt.out, (int) cvt.prefixlen, pat[ i ],
            cvt.prefixlen, (int) cvt.suffixlen, cvt.suffix, cvt.suffixlen );
    if ( cvt.shard_total != 0 )
      printf( " shard %u of %u\n", cvt.shard_num, cvt.shard_total );
    else
      printf( "\n" );
    if ( rc != 0 ) {
      printf( "convert failed\n" );
      fail++;
      continue;
    }
    re = pcre2_compile( (uint8_t *) cvt.out, cvt.off, 0, &error, &erroff, 0 );
    if ( re != NULL )
      rj = pcre2_jit_compile( re, PCRE2_JIT_COMPLETE );
    if ( re == NULL || rj != 0 ) {
      printf( "re failed\n" );
      fail++;
      continue;
    }
    md = pcre2_match_data_create_from_pattern( re, NULL );
    if ( md == NULL ) {
      pcre2_code_free( re );
      printf( "md failed\n" );
      fail++;
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
            printf( "avg %lu cycles\n", ( t2 - t1 ) / ( 9999 * mc ) );
          }
          printf( "%s == %s   %s", pat[ i ], match[ j ], rc == 1 ? "(yes)" : "(no)" );
          if ( rc != mat[ i * mc + j ] ) {
            printf( " (failed %d!=%d [%d])\n", rc, mat[ i * mc + j ], (int) ( i * mc + j ) );
            fail++;
          }
          else
            printf( "\n" );
        }
      }
    }
    pcre2_match_data_free( md );
    pcre2_code_free( re );

    rc = cvt2.pcre_prefix( cvt.out, cvt.off );
    printf( "\"%.*s\" -> \"%.*s\" (%lu)\n", (int) cvt.off, cvt.out,
            (int) cvt2.off, cvt2.out, cvt2.off );
    if ( rc != 0 ) {
      printf( "prefix failed\n" );
      fail++;
    }
    else if ( cvt2.off != cvt.prefixlen ||
              ::memcmp( cvt2.buf, pat[ i ], cvt2.off ) != 0 ) {
      printf( "prefix not equal\n" );
      fail++;
    }
  }
  return fail;
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
    "hello",
    "hallo",
    "hxllo",
    "hllo",
    "heeeello",
    "hbllo",
    "ehello",
    "helloworld"
  };
  int mat[] = {
                  /* "hello", "hallo", "hxllo", "hllo", "heeeello", "hbllo", "ehello", "helloworld" */
  /* "h?ll*"     */        1,       1,       1,     -1,         -1,       1,       -1,           1,
  /* "h*llo"     */        1,       1,       1,      1,          1,       1,       -1,          -1,
  /* "h[ae]llo"  */        1,       1,      -1,     -1,         -1,      -1,       -1,          -1,
  /* "h[^e]llo"  */       -1,       1,       1,     -1,         -1,       1,       -1,          -1,
  /* "h[a-b]llo" */       -1,       1,      -1,     -1,         -1,       1,       -1,          -1,
  /* "hello*"    */        1,      -1,      -1,     -1,         -1,      -1,       -1,           1,
  };

  const char *patrv[] = {
    "hello.*",
    "hello.>",
    "hello.*.>(0,4)",
    "*.again",
    "hello.*.again",
    "hello.*.*(1,4)"
  };
  const char *matchrv[] = {
    "hello.world",
    "hallo",
    "hello.world.again",
    "testing.again",
    "he.world"
  };
  int matrv[] = {
                      /* "hello.world", "hallo", "hello.world.again", "testing.again", "he.world" */
  /* "hello.*"       */              1,      -1,                  -1,              -1,         -1,
  /* "hello.>"       */              1,      -1,                   1,              -1,         -1,
  /* "hello.*.>"     */             -1,      -1,                   1,              -1,         -1,
  /* "*.again"       */             -1,      -1,                  -1,               1,         -1,
  /* "hello.*.again" */             -1,      -1,                   1,              -1,         -1,
  /* "hello.*.*      */             -1,      -1,                   1,              -1,         -1,
  };

  const size_t pc = sizeof( pat ) / sizeof( pat[ 0 ] );
  const size_t mc = sizeof( match ) / sizeof( match[ 0 ] );
  const size_t prvc = sizeof( patrv ) / sizeof( patrv[ 0 ] );
  const size_t mrvc = sizeof( matchrv ) / sizeof( matchrv[ 0 ] );

  int n = do_match( IS_GLOB, pat, pc, match, mc, mat ),
      m = do_match( IS_NATS, patrv, prvc, matchrv, mrvc, matrv );

  if ( n + m == 0 )
    printf( "success\n" );
  else
    printf( "failed : %d\n", n + m );
  return n + m == 0 ? 0 : 1;
}

