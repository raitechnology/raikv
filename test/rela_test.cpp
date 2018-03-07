#include <stdio.h>
#include <string.h>
#include <stdint.h>

#include <raikv/rela_ts.h>

using namespace rai;
using namespace kv;

/*
 * resolution at distance from base timestamp
 *
 *   shift  |   update   |   update    |   expires  |  expire
 *          | resolution |   period    | resolution |  period
 *     0    |     4ns    |     9h      |     4s     |    9h    
 *     1    |     8ns    |    19h      |     8s     |   18h    
 *     2    |    16ns    |    1d 15h   |    16s     |   1d 12h 
 *     3    |    32ns    |    3d 6h    |    32s     |   3d       shift
 *     4    |    64ns    |    6d 12h   |    64s     |   6d 1h
 *     5    |   128ns    |    13d      |   128s     |   12d 3h
 *     6    |   256ns    |    26d 1h   |   256s     |   26d 6h 
 *     7    |   512ns    |    52d 2h   |   512s     |   48d 12h
 *     8    |  1024ns    |   104d 5h   |  1024s     |   97d 1h   V
 *     9    |  1024ns    |   208d 11h  |  1024s     |  194d 3h
 *    10    |  1024ns    |   312d 17h  |  1024s     |  291d 5h
 *    11    |  1024ns    |   416d 23h  |  1024s     |  388d 7h   sum
 *    12    |  1024ns    |   521d 5h   |  1024s     |  485d 9h 
 *    13    |  1024ns    |   625d 11h  |  1024s     |  582d 11h
 *    14    |  1024ns    |   729d 17h  |  1024s     |  679d 13h
 *    15    |  1024ns    |   833d 23h  |  1024s     |  776d 15h  V
 */

char *
print_time( uint64_t ns,  char *buf,  size_t sz )
{
  char *p = buf;
  uint32_t secs = (uint32_t) ( ns / NANOS );
  int n;
  buf[ 0 ] = '\0';
  if ( secs >= 24 * 60 * 60 ) {
    n = snprintf( buf, sz, "%ud ", secs / ( 24 * 60 * 60 ) );
    secs %= 24 * 60 * 60;
    buf = &buf[ n ];
    sz -= n;
  }
  if ( secs >= 60 * 60 ) {
    n = snprintf( buf, sz, "%uh ", secs / ( 60 * 60 ) );
    secs %= 60 * 60;
    buf = &buf[ n ];
    sz -= n;
  }
  if ( secs >= 60 ) {
    n = snprintf( buf, sz, "%um ", secs / 60 );
    secs %= 60;
    buf = &buf[ n ];
    sz -= n;
  }
  if ( secs > 0 ) {
    n = snprintf( buf, sz, "%us ", secs );
    buf = &buf[ n ];
    sz -= n;
  }
  return p;
}

static const uint64_t MIN_NS      = 60 * NANOS;
static const uint64_t HOUR_NS     = 60 * MIN_NS;
static const uint64_t DAY_NS      = 24 * HOUR_NS;
static const uint64_t EXP_DAYS_NS = (uint64_t) ( 700 * 24 * 60 * 60 ) * NANOS;
static const uint64_t TWO_DAYS_NS = (uint64_t) ( 2 * 24 * 60 * 60 ) * NANOS;

int
main( int argc, char *argv[] )
{
  RelativeStamp stamp;
  char          buf[ 80 ], buf2[ 80 ];
  uint64_t      exp, upd, x, y, off, off2, base, clock;
  uint32_t      i;

  base = current_monotonic_time_ns();

  for ( off = 0, off2 = 0; off < 60 * DAY_NS; ) {
    clock = base + off2 + DAY_NS;
    x = base + off; y = base + off2;
    stamp.set( base, y, x, y );
    stamp.get( base, clock, exp, upd );

    print_time( exp - base, buf, sizeof( buf ) );
    printf( "qtr %u, %s diff = %ld secs\n", stamp.u.x.clock_base,
            buf, ( exp - x ) / NANOS );

    print_time( upd - base, buf2, sizeof( buf2 ) );
    printf( "qtr %u, %s diff = %ld ns\n", stamp.u.x.clock_base,
            buf2, ( y - upd ) );

    off2 += 7 * HOUR_NS + 912364;
    off   = off2 + 7 * HOUR_NS + 39;
  }

  printf( "shift  |   update   |    update         |   expires  |    expire\n"
          "       | resolution |    period         | resolution |    period\n"
    "-------+------------+-------------------+------------+---------------\n" );

  for ( i = 0; i < ( 1 << RELA_SHIFT_BITS ); i++ ) {
    stamp.set_expires( MAX_RELA_SECS, i );
    stamp.set_update( MAX_RELA_NS, i );
    stamp.u.x.clock_base = 0;
    stamp.get( 0, 0, exp, upd );

    printf( "%6u | %8uns | %-17s | %8us  | %s\n",
           i, stamp.upd_resolution( i ), print_time( upd, buf, sizeof( buf ) ),
           stamp.exp_resolution( i ), print_time( exp, buf2, sizeof( buf2 ) ) );
  }
  printf( "MAX_UPDATE_NS  = %s\n",
          print_time( MAX_UPDATE_NS, buf, sizeof( buf ) ) );
  printf( "MAX_EXPIRES_NS = %s\n",
          print_time( MAX_EXPIRES_NS, buf, sizeof( buf ) ) );
  for ( i = 0; i < 4; i++ ) {
    printf( "%u EXP QUARTER: (%lx) %s -> (%lx) %s\n", i,
            RELA_CLOCK_QTR * (uint64_t) i,
            print_time( RELA_CLOCK_QTR * (uint64_t) i, buf, sizeof( buf ) ),
            RELA_CLOCK_QTR * (uint64_t) i + MAX_EXPIRES_NS,
            print_time( RELA_CLOCK_QTR * (uint64_t) i + MAX_EXPIRES_NS,
                        buf2, sizeof( buf2 ) ) );
  }
  printf( "sizeof( RelativeStamp ) = %lu\n", sizeof( RelativeStamp ) );

  return 0;
}

