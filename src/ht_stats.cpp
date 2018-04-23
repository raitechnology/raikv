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
Segment::get_mem_delta( MemDeltaCounters &stat,  uint16_t align_shift ) const
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

/* get deltas for all attached threads */
bool
HashTab::get_ht_deltas( HashDeltaCounters *stats,  HashCounters &ops,
                        HashCounters &tot,  uint32_t ctx_id ) const
{
  ops.zero();
  tot.zero();
  if ( ctx_id >= MAX_CTX_ID ) {
    HashCounters mvd;
    mvd.zero();
    for ( size_t i = 0; i < MAX_CTX_ID; i++ ) {
      if ( this->ctx[ i ].get_ht_delta( stats[ i ] ) ) {
        ops += stats[ i ].delta;
        tot += stats[ i ].last;
      }
      else {
        mvd += stats[ i ].last;
        stats[ i ].zero();
      }
    }
    ops -= mvd;
  }
  else {
    if ( this->ctx[ ctx_id ].get_ht_delta( stats[ 0 ] ) ) {
      ops += stats[ 0 ].delta;
      tot += stats[ 0 ].last;
    }
    else {
      stats[ 0 ].zero();
    }
  }
  return ops != 0;
}

bool
ThrCtx::get_ht_delta( HashDeltaCounters &stat ) const
{
  for (;;) {
    while ( ( this->key & ZOMBIE32 ) != 0 )
      kv_sync_pause();
    if ( this->ctx_id == KV_NO_CTX_ID )
      return false;
    HashCounters tmp( this->stat );
    /* XXX: weak sync, mutator could lock & unlock while stat copied to tmp */
    if ( ( this->key & ZOMBIE32 ) == 0 ) {
      stat.get_ht_delta( tmp );
      return true;
    }
  }
}

bool
HashTab::get_mem_deltas( MemDeltaCounters *stats,  MemCounters &chg,
                         MemCounters &tot ) const
{
  chg.zero();
  tot.zero();
  for ( size_t i = 0; i < this->hdr.nsegs; i++ ) {
    this->hdr.seg[ i ].get_mem_delta( stats[ i ], this->hdr.seg_align_shift );
    chg += stats[ i ].delta;
    tot += stats[ i ].last;
  }
  return chg != 0;
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
      total_add  += this->ctx[ i ].stat.add;
      total_drop += this->ctx[ i ].stat.drop;
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
  this->hdr.load_percent     = (uint8_t) ( max_load * 100.0 /
                                           (double) this->hdr.critical_load );
}

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

