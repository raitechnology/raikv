#include <stdint.h>
#include <string.h>
#include <stdlib.h>

#include <raikv/shm_ht.h>

using namespace rai;
using namespace kv;

template <class Int>
inline void
memadd( Int *t,  const Int *x,  size_t k )
{
  do { *t++ += *x++; } while ( (k -= sizeof( Int )) != 0 );
}

template <class Int>
inline void
memsub( Int *t,  const Int *x,  size_t k )
{
  do { *t++ -= *x++; } while ( (k -= sizeof( Int )) != 0 );
}

template <class Int>
inline Int
memsum( const Int *t,  size_t k )
{
  Int n = 0;
  do { n += *t++; } while ( (k -= sizeof( Int )) != 0 );
  return n;
}

HashCounters&
HashCounters::operator=( const HashCounters &x ) noexcept
{
  ::memcpy( this, &x, sizeof( *this ) );
  return *this;
}

HashCounters&
HashCounters::operator+=( const HashCounters &x ) noexcept
{
  memadd<int64_t>( &this->rd, &x.rd, sizeof( *this ) );
  return *this;
}

HashCounters&
HashCounters::operator-=( const HashCounters &x ) noexcept
{
  memsub<int64_t>( &this->rd, &x.rd, sizeof( *this ) );
  return *this;
}

bool
HashCounters::operator==( int i ) noexcept /* intended for: if ( *this == 0 ) */
{
  return memsum<int64_t>( &this->rd, sizeof( *this ) ) == (int64_t) i;
}

bool
HashCounters::operator!=( int i ) noexcept
{
  return ! this->operator==( i );
}

MemCounters&
MemCounters::operator=( const MemCounters &x ) noexcept
{
  ::memcpy( this, &x, sizeof( *this ) );
  return *this;
}

MemCounters&
MemCounters::operator+=( const MemCounters &x ) noexcept
{
  memadd<int64_t>( &this->offset, &x.offset, sizeof( *this ) );
  return *this;
}

MemCounters&
MemCounters::operator-=( const MemCounters &x ) noexcept
{
  memsub<int64_t>( &this->offset, &x.offset, sizeof( *this ) );
  return *this;
}

bool
MemCounters::operator==( int i ) noexcept /* intended for: if ( *this == 0 ) */
{
  return memsum<int64_t>( &this->offset, sizeof( *this ) ) == (uint32_t) i;
}

bool
MemCounters::operator!=( int i ) noexcept
{
  return ! this->operator==( i );
}

/* saves current state so that a delta is last time this was called */
void
HashDeltaCounters::get_ht_delta( const HashCounters &stat ) noexcept
{
  HashCounters current = stat; /* copy these, they are volatile */
  this->delta  = current;
  this->delta -= this->last;
  this->last   = current;
}

void
MemDeltaCounters::get_mem_delta( const MemCounters &cnts ) noexcept
{
  this->delta  = cnts;
  this->delta -= this->last;
  this->last   = cnts;
}

bool
HashTab::sum_ht_thr_delta( HashDeltaCounters &stat,  HashCounters &ops,
                           HashCounters &tot,  uint32_t ctx_id ) const noexcept
{
  uint32_t i;
  HashCounters tmp;
  tmp.zero();
  ops.zero();
  tot.zero();
  if ( this->ctx[ ctx_id ].ctx_id != ctx_id )
    return false;
  /* presumes that the caller owns the thread */
  for ( i = this->ctx[ ctx_id ].db_stat_hd; i != MAX_STAT_ID;
        i = this->hdr.stat_link[ i ].next ) {
    tmp += this->stats[ i ];
  }
  stat.get_ht_delta( tmp );
  ops = stat.delta;
  tot = stat.last;
  return true;
}

bool
HashTab::sum_ht_db_delta( HashDeltaCounters &stat,  HashCounters &ops,
                          HashCounters &tot,  uint8_t db ) noexcept
{
  uint32_t i;
  HashCounters tmp;

  ops.zero();
  tot.zero();

  if ( ! this->hdr.test_db_opened( db ) ) {
    tmp.zero();
    return false;
  }
  /* lock db */
  this->hdr.ht_spin_lock( db );
  /* sum retired stats */
  tmp = this->hdr.db_stat[ db ];
  /* still mutating the stat_link[] are the active contexts */
  for ( i = 0; i < MAX_STAT_ID; i++ ) {
    ThrStatLink & link = this->hdr.stat_link[ i ];
    if ( link.used == 1 && link.db_num == db )
      tmp += this->stats[ i ];
  }
  /* unlock db */
  this->hdr.ht_spin_unlock( db );

  stat.get_ht_delta( tmp );
  ops = stat.delta;
  tot = stat.last;
  return true;
}

bool
HashTab::get_db_stats( HashCounters &tot,  uint8_t db ) noexcept
{
  uint32_t i;

  if ( ! this->hdr.test_db_opened( db ) ) {
    tot.zero();
    return false;
  }
  /* lock db */
  this->hdr.ht_spin_lock( db );
  /* sum retired stats */
  tot = this->hdr.db_stat[ db ];
  /* still mutating the stat_link[] are the active contexts */
  for ( i = 0; i < MAX_STAT_ID; i++ ) {
    ThrStatLink & link = this->hdr.stat_link[ i ];
    if ( link.used == 1 && link.db_num == db )
      tot += this->stats[ i ];
  }
  /* unlock db */
  this->hdr.ht_spin_unlock( db );
  return true;
}

void
Segment::get_mem_seg_delta( MemDeltaCounters &stat,
                            uint16_t align_shift ) const noexcept
{
  MemCounters current;
  uint64_t x, y;
  this->get_position( this->ring, align_shift, x, y );
  current.offset       = x;
  current.msg_count    = this->msg_count;
  current.avail_size   = this->avail_size;
  current.move_msgs    = this->move_msgs;
  current.move_size    = this->move_size;
  current.evict_msgs   = this->evict_msgs;
  current.evict_size   = this->evict_size;
  stat.get_mem_delta( current );
}

bool
HashTab::sum_mem_deltas( MemDeltaCounters *stats,  MemCounters &chg,
                         MemCounters &tot ) const noexcept
{
  const uint16_t align_shift = this->hdr.seg_align_shift;
  chg.zero();
  tot.zero();
  for ( size_t i = 0; i < this->hdr.nsegs; i++ ) {
    this->hdr.seg[ i ].get_mem_seg_delta( stats[ i ], align_shift );
    chg += stats[ i ].delta;
    tot += stats[ i ].last;
  }
  return chg != 0;
}

void
HashTab::update_load( void ) noexcept
{
  double   ht_load    = 0,
           value_load = 0,
           max_load   = 0;
  uint64_t total_add  = 0,
           total_drop = 0,
           avail_size = 0,
           total_size;
  size_t   i, nsegs;

  this->hdr.current_stamp = current_realtime_coarse_ns();
  /* lock all dbs */
  for ( i = 0; i < DB_COUNT; i += 64 )
    this->hdr.ht_spin_lock64( i );
  HashCounters * db = this->hdr.db_stat;
  for ( i = 0; i < DB_COUNT; i++ ) {
    total_add  += db[ i ].add;
    total_drop += db[ i ].drop;
  }

  for ( i = 0; i < MAX_STAT_ID; i++ ) {
    if ( this->hdr.stat_link[ i ].used ) {
      total_add  += this->stats[ i ].add;
      total_drop += this->stats[ i ].drop;
    }
  }
  /* unlock dbs */
  for ( i = 0; i < DB_COUNT; i += 64 )
    this->hdr.ht_spin_unlock64( i );
  /* total add - total drop should be the used entries */
  ht_load = (double) ( total_add - total_drop ) / (double) this->hdr.ht_size;
  if ( (nsegs = this->hdr.nsegs) > 0 ) {
    for ( i = 0; i < nsegs; i++ )
      avail_size += this->hdr.seg[ i ].avail_size;
    total_size = nsegs * this->hdr.seg_size();
    /* calculate used size */
    value_load = (double) ( total_size - avail_size ) / (double) total_size;
  }
  if ( ht_load > value_load )
    max_load = ht_load;
  else
    max_load = value_load;
  this->hdr.last_entry_count = total_add - total_drop;
  this->hdr.ht_load          = (float) ht_load;
  this->hdr.value_load       = (float) value_load;
  this->hdr.load_percent     = (uint8_t) ( max_load * 100.0 + 0.5 );
}

HashTabStats *
HashTabStats::create( HashTab &ht ) noexcept
{
  size_t sz = sizeof( HashTabStats ) +
              sizeof( HashDeltaCounters ) * DB_COUNT +
              sizeof( MemDeltaCounters ) * ht.hdr.nsegs;
  void *p = ::malloc( sz );
  if ( p == NULL )
    return NULL;
  ::memset( p, 0, sz );
  HashTabStats * hts = new ( p ) HashTabStats( ht );
  hts->nsegs     = ht.hdr.nsegs;
  hts->db_count  = DB_COUNT;
  hts->db_stats  = (HashDeltaCounters *) (void *) &hts[ 1 ];
  hts->mem_stats = (MemDeltaCounters *) (void *)
                   &hts->db_stats[ hts->db_count ];
  return hts;
}

bool
HashTabStats::fetch( void ) noexcept
{
  bool   b = false;
  double t = current_monotonic_coarse_s();

  if ( this->nsegs > 0 )
    b |= this->ht.sum_mem_deltas( this->mem_stats, this->mops, this->mtot );
  //b |= this->ht.sum_ht_thr_deltas( this->ctx_stats, this->hops, this->htot );

  HashCounters db[ DB_COUNT ];
  size_t i;
  HashCounters last = this->htot;
  this->hops.zero();
  this->htot.zero();
  /* lock all dbs */
  for ( i = 0; i < DB_COUNT; i += 64 )
    this->ht.hdr.ht_spin_lock64( i );
  /* sum retired stats */
  for ( i = 0; i < DB_COUNT; i++ )
    db[ i ] = this->ht.hdr.db_stat[ i ];
  /* still mutating the stat_link[] are the active contexts */
  for ( i = 0; i < MAX_STAT_ID; i++ ) {
    ThrStatLink & link = this->ht.hdr.stat_link[ i ];
    if ( link.used == 1 )
      db[ link.db_num ] += this->ht.stats[ i ];
  }
  /* unlock dbs */
  for ( i = 0; i < DB_COUNT; i += 64 )
    this->ht.hdr.ht_spin_unlock64( i );

  for ( i = 0; i < DB_COUNT; i++ ) {
    this->htot += db[ i ];
    this->db_stats[ i ].get_ht_delta( db[ i ] );
  }

  this->hops = this->htot;
  this->hops -= last;
  b |= ( this->hops != 0 );

  if ( this->ival_end == 0 ) {
    this->ival_end = t;
    b = false; /* wait for an interval */
  }
  else {
    this->ival_start = this->ival_end;
    this->ival_end = t;
    this->ival = t - this->ival_start;
    if ( this->ival <= 0 )
      b = false;
  }
  return b;
}

