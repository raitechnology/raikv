#include <stdio.h>
#include <stdint.h>
#include <string.h>

#include <raikv/rela_ts.h>

using namespace rai;
using namespace kv;

void
RelativeStamp::set( uint64_t base,  uint64_t clock,  uint64_t exp,
                    uint64_t upd ) noexcept
{
  uint64_t ns        = 0;
  uint64_t secs      = 0;
  uint64_t exp_shift = 0,
           upd_shift = 0,
           qtr, qbase;

  qbase = ( clock - base ) / RELA_CLOCK_PER;
  qtr   = ( ( clock - base ) % RELA_CLOCK_PER ) / RELA_CLOCK_QTR;
  this->u.x.clock_base = qtr;
  qbase = base + ( qbase * 4 + qtr ) * RELA_CLOCK_QTR;
  /*printf( "games %lu, qtr %lu, qbase diff %lu\n",
          games, qtr, qbase - base );*/
  
  secs = ( ( exp - qbase ) / NANOS ) >> MIN_EXP_SHIFT;
  if ( secs > MAX_RELA_SECS ) {
    do {
      secs >>= 1;
      exp_shift++;
    } while ( exp_shift < MAX_EXP_SHIFT && secs > MAX_RELA_SECS );

    if ( secs > MAX_RELA_SECS ) {
      exp_shift += ( secs / MAX_RELA_SECS );
      secs      %= MAX_RELA_SECS;
    }
  }

  ns = ( upd - qbase ) >> MIN_UPD_SHIFT;
  if ( ns > MAX_RELA_NS ) {
    do {
      ns >>= 1;
      upd_shift++;
    } while ( upd_shift < MAX_UPD_SHIFT && ns > MAX_RELA_NS );

    if ( ns > MAX_RELA_NS ) {
      upd_shift += ( ns / MAX_RELA_NS );
      ns        %= MAX_RELA_NS;
    }
  }

  this->set_expires( secs, exp_shift );
  this->set_update( ns, upd_shift );
}

void
RelativeStamp::get( uint64_t base,  uint64_t clock,  uint64_t &exp,
                    uint64_t &upd ) noexcept
{
  uint64_t secs, ns, res, qtr, qbase;
  uint32_t exp_shift, upd_shift;

  qbase = ( clock - base ) / RELA_CLOCK_PER;
  qtr   = this->u.x.clock_base;
  qbase = base + ( qbase * 4 + qtr ) * RELA_CLOCK_QTR;
  if ( qbase > clock )
    qbase -= RELA_CLOCK_PER;
  /*printf( "games %lu, qtr %lu, qbase diff %lu\n",
          games, qtr, qbase - base );*/

  this->get_expires( secs, exp_shift );
  if ( exp_shift > MAX_EXP_SHIFT ) {
    secs += MAX_RELA_SECS * ( exp_shift - MAX_EXP_SHIFT );
    exp_shift = MAX_EXP_SHIFT;
  }
  if ( exp_shift > 0 )
    secs <<= exp_shift;
  res = ( 1U << ( exp_shift + MIN_EXP_SHIFT ) ) - 1;
  exp = qbase + ( ( secs << MIN_EXP_SHIFT ) + res ) * NANOS; /* use ceiling */

  this->get_update( ns, upd_shift );
  if ( upd_shift > MAX_UPD_SHIFT ) {
    ns += MAX_RELA_NS * ( upd_shift - MAX_UPD_SHIFT );
    upd_shift = MAX_UPD_SHIFT;
  }
  if ( upd_shift > 0 )
    ns <<= upd_shift;
  upd = qbase + ( ns << MIN_UPD_SHIFT ); /* use floor */
}

