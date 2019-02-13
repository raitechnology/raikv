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
HashCounters::operator=( const HashCounters &x )
{
  ::memcpy( this, &x, sizeof( *this ) );
  return *this;
}

HashCounters&
HashCounters::operator+=( const HashCounters &x )
{
  memadd<int64_t>( &this->rd, &x.rd, sizeof( *this ) );
  return *this;
}

HashCounters&
HashCounters::operator-=( const HashCounters &x )
{
  memsub<int64_t>( &this->rd, &x.rd, sizeof( *this ) );
  return *this;
}

bool
HashCounters::operator==( int i ) /* intended for: if ( *this == 0 ) */
{
  return memsum<int64_t>( &this->rd, sizeof( *this ) ) == (int64_t) i;
}

bool
HashCounters::operator!=( int i )
{
  return ! this->operator==( i );
}

MemCounters&
MemCounters::operator=( const MemCounters &x )
{
  ::memcpy( this, &x, sizeof( *this ) );
  return *this;
}

MemCounters&
MemCounters::operator+=( const MemCounters &x )
{
  memadd<int64_t>( &this->offset, &x.offset, sizeof( *this ) );
  return *this;
}

MemCounters&
MemCounters::operator-=( const MemCounters &x )
{
  memsub<int64_t>( &this->offset, &x.offset, sizeof( *this ) );
  return *this;
}

bool
MemCounters::operator==( int i ) /* intended for: if ( *this == 0 ) */
{
  return memsum<int64_t>( &this->offset, sizeof( *this ) ) == (uint32_t) i;
}

bool
MemCounters::operator!=( int i )
{
  return ! this->operator==( i );
}

/* saves current state so that a delta is last time this was called */
void
HashDeltaCounters::get_ht_delta( const HashCounters &stat )
{
  HashCounters current = stat; /* copy these, they are volatile */
  this->delta  = current;
  this->delta -= this->last;
  this->last   = current;
}

void
MemDeltaCounters::get_mem_delta( const MemCounters &cnts )
{
  this->delta  = cnts;
  this->delta -= this->last;
  this->last   = cnts;
}

void
Segment::get_mem_seg_delta( MemDeltaCounters &stat,  uint16_t align_shift ) const
{
  MemCounters current;
  uint64_t x, y;
  this->get_position( this->ring.val, align_shift, x, y );
  current.offset       = x;
  current.msg_count    = this->msg_count;
  current.avail_size   = this->avail_size;
  current.move_msgs    = this->move_msgs;
  current.move_size    = this->move_size;
  current.evict_msgs   = this->evict_msgs;
  current.evict_size   = this->evict_size;
  stat.get_mem_delta( current );
}
#if 0
/* get deltas for all attached threads */
bool
HashTab::sum_ht_thr_deltas( HashDeltaCounters *stats,  HashCounters &ops,
                            HashCounters &tot ) const
{
  HashCounters mvd;
  uint32_t seqno;
  uint8_t db;
  ops.zero();
  tot.zero();
  mvd.zero();
  for ( size_t i = 0; i < MAX_CTX_ID; i++ ) {
    if ( this->ctx[ i ].get_ht_thr_delta( stats[ i ], db, seqno ) ) {
      ops += stats[ i ].delta;
      tot += stats[ i ].last;
    }
    else {
      mvd += stats[ i ].last;
      stats[ i ].zero();
    }
  }
  ops -= mvd;
  return ops != 0;
}
#endif
bool
HashTab::sum_ht_thr_delta( HashDeltaCounters &stats,  HashCounters &ops,
                           HashCounters &tot,  uint32_t ctx_id ) const
{
  uint32_t seqno;
  uint8_t db;
  ops.zero();
  tot.zero();
  if ( this->ctx[ ctx_id ].get_ht_thr_delta( stats, db, seqno ) ) {
    ops += stats.delta;
    tot += stats.last;
  }
  else {
    stats.zero();
  }
  return ops != 0;
}

bool
ThrCtx::get_ht_thr_delta( HashDeltaCounters &stat,  uint8_t &db,
                          uint32_t &seqno ) const
{
  for (;;) {
    while ( ( this->key & ZOMBIE32 ) != 0 )
      kv_sync_pause();
    db = this->db_num1; /* set db, useful when detecting retired stats */
    seqno = this->ctx_seqno;
    if ( this->ctx_id == KV_NO_CTX_ID )
      return false;
    HashCounters tmp( this->stat1 );
    /* XXX: weak sync, mutator could lock & unlock while stat copied to tmp */
    if ( ( this->key & ZOMBIE32 ) == 0 ) {
      stat.get_ht_delta( tmp );
      return true;
    }
  }
}

bool
HashTab::sum_mem_deltas( MemDeltaCounters *stats,  MemCounters &chg,
                         MemCounters &tot ) const
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

bool
HashTab::get_db_stats( HashCounters &tot,  uint8_t db_num ) const
{
  size_t i;
  tot = this->hdr.stat[ db_num ];
  for ( i = 0; i < MAX_CTX_ID; i++ ) {
    if ( this->ctx[ i ].ctx_id != KV_NO_CTX_ID ) {
      if ( this->ctx[ i ].db_num1 == db_num )
        tot += this->ctx[ i ].stat1;
      else if ( this->ctx[ i ].db_num2 == db_num )
        tot += this->ctx[ i ].stat2;
    }
  }
  return tot != 0;
}

void
HashTab::update_load( void )
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
  for ( i = 0; i < MAX_CTX_ID; i++ ) {
    if ( this->ctx[ i ].ctx_id != KV_NO_CTX_ID ) {
      total_add  += this->ctx[ i ].stat1.add;
      total_drop += this->ctx[ i ].stat1.drop;
      total_add  += this->ctx[ i ].stat2.add;
      total_drop += this->ctx[ i ].stat2.drop;
    }
  }
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
#if 0
MemDeltaCounters *
MemDeltaCounters::new_array( size_t sz )
{
  void *b = ::malloc( sizeof( MemDeltaCounters ) * sz );
  MemDeltaCounters *p = (MemDeltaCounters *) b;
  if ( p == NULL )
    return NULL;
  for ( size_t i = 0; i < sz; i++ ) {
    new ( (void *) p ) MemDeltaCounters();
    p = &p[ 1 ];
  }
  return (MemDeltaCounters *) b;
}
#endif
HashTabStats *
HashTabStats::create( HashTab &ht )
{
  size_t sz = sizeof( HashTabStats ) +
              sizeof( HashDeltaCounters ) * MAX_CTX_ID +
              sizeof( HashDeltaCounters ) * DB_COUNT +
              sizeof( MemDeltaCounters ) * ht.hdr.nsegs +
              sizeof( uint32_t ) * MAX_CTX_ID;
  void *p = ::malloc( sz );
  if ( p == NULL )
    return NULL;
  ::memset( p, 0, sz );
  HashTabStats * hts = new ( p ) HashTabStats( ht );
  hts->ctx_count = MAX_CTX_ID;
  hts->nsegs     = ht.hdr.nsegs;
  hts->db_count  = DB_COUNT;
  hts->ctx_stats = (HashDeltaCounters *) (void *) &hts[ 1 ];
  hts->db_stats  = &hts->ctx_stats[ hts->ctx_count ];
  hts->mem_stats = (MemDeltaCounters *) (void *)
                   &hts->db_stats[ hts->db_count ];
  hts->ctx_seqno = (uint32_t *) &hts->mem_stats[ hts->nsegs ];
  return hts;
}

bool
HashTabStats::fetch( void )
{
  bool   b = false;
  double t = current_monotonic_coarse_s();

  if ( this->nsegs > 0 )
    b |= this->ht.sum_mem_deltas( this->mem_stats, this->mops, this->mtot );
  //b |= this->ht.sum_ht_thr_deltas( this->ctx_stats, this->hops, this->htot );

  HashCounters db[ DB_COUNT ];
  HashCounters mvd;
  size_t i;
  uint32_t seqno;
  uint8_t db_num;
  this->hops.zero();
  this->htot.zero();
  mvd.zero();
  for ( i = 0; i < DB_COUNT; i++ )
    ::memcpy( db, this->ht.hdr.stat, sizeof( db ) );

  for ( i = 0; i < MAX_CTX_ID; i++ ) {
    const bool valid = /* true if ctx is active */
      this->ht.ctx[ i ].get_ht_thr_delta( this->ctx_stats[ i ], db_num, seqno );
    if ( valid && seqno == this->ctx_seqno[ i ] ) {
      this->hops += this->ctx_stats[ i ].delta;
      this->htot += this->ctx_stats[ i ].last;
      db[ db_num ] += this->ctx_stats[ i ].last;
    }
    else if ( this->ctx_seqno[ i ] != 0 ) {
      mvd += this->ctx_stats[ i ].last;
      this->ctx_stats[ i ].zero();
    }
    this->ctx_seqno[ i ] = ( valid ? seqno : 0 );
  }

  for ( i = 0; i < DB_COUNT; i++ )
    this->db_stats[ i ].get_ht_delta( db[ i ] );

  this->hops -= mvd;
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

