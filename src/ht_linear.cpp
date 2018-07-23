#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stddef.h>

#include <raikv/shm_ht.h>
#include <raikv/ht_search.h>

using namespace rai;
using namespace kv;

namespace rai {
namespace kv {
struct LinearPosition {
  KeyCtx &kctx;
  LinearPosition( KeyCtx &kc ) : kctx( kc ) {}

  KeyStatus acquire_incr( uint64_t &pos,  const uint64_t chains,
                          bool &/*is_next_hash*/,  const bool /*have_drop*/ ) {
    return this->find_incr( pos, chains );
  }

  KeyStatus acquire_incr_single_thread( uint64_t &pos,  const uint64_t chains,
                                        const bool /*have_drop*/ ) {
    return this->find_incr( pos, chains );
  }

  KeyStatus find_incr( uint64_t &pos,  const uint64_t chains ) {
    if ( ++pos == this->kctx.ht_size )
      pos = 0;
    if ( chains != this->kctx.max_chains )
      return KEY_OK;
    if ( chains == this->kctx.ht_size )
      return KEY_HT_FULL;
    return KEY_MAX_CHAINS;
  }

  void restore_inc( uint64_t /*pos*/ ) {}
};
}
}

/* linear hash resolve, walk entries, always keep one lock active
   so that no other thread can pass by while this thread is on the
   same chain */
KeyStatus
KeyCtx::acquire_linear_probe( const uint64_t k,  const uint64_t start_pos )
{
  LinearPosition lp( *this );

  return this->acquire<LinearPosition, true>( k, start_pos, lp );
}

KeyStatus
KeyCtx::acquire_linear_probe_single_thread( const uint64_t k,
                                            const uint64_t start_pos )
{
  LinearPosition lp( *this );

  return this->acquire_single_thread<LinearPosition>( k, start_pos, lp );
}

/* similar to acquire() but bail if can't acquire without getting in line */
KeyStatus
KeyCtx::try_acquire_linear_probe( const uint64_t k,  const uint64_t start_pos )
{
  LinearPosition lp( *this );

  return this->acquire<LinearPosition, false>( k, start_pos, lp );
}

/* find key for read only access without locking slot */
KeyStatus
KeyCtx::find_linear_probe( const uint64_t k,  const uint64_t start_pos,
                           const uint64_t spin_wait )
{
  LinearPosition lp( *this );

  return this->find<LinearPosition>( k, start_pos, spin_wait, lp );
}

KeyStatus
KeyCtx::find_linear_probe_single_thread( const uint64_t k,
                                         const uint64_t start_pos )
{
  LinearPosition lp( *this );

  return this->find_single_thread<LinearPosition>( k, start_pos, lp );
}

/* 32b aligned hash entry fetch, zero if slot is empty */
static inline uint64_t
fetch_hash_entry( void *p,  void *q,  uint32_t sz )
{
  uint64_t h = ( ((uint64_t *) p)[ 0 ] = ((uint64_t *) q)[ 0 ] );
  if ( ( h & ZOMBIE64 ) != 0 ) /* slot busy */
    return h;
  if ( h == 0 ) {
    for (;;) {
      ((uint64_t *) p)[ 1 ] = 0;
      ((uint64_t *) p)[ 2 ] = 0;
      ((uint64_t *) p)[ 3 ] = 0;
      if ( (sz -= 32) == 0 )
        return 0;
      p = &((uint64_t *) p)[ 4 ];
      ((uint64_t *) p)[ 0 ] = 0;
    }
  }
  for (;;) {
    ((uint64_t *) p)[ 1 ] = ((uint64_t *) q)[ 1 ];
    ((uint64_t *) p)[ 2 ] = ((uint64_t *) q)[ 2 ];
    ((uint64_t *) p)[ 3 ] = ((uint64_t *) q)[ 3 ];
    if ( (sz -= 32) == 0 )
      return h;
    p = &((uint64_t *) p)[ 4 ]; q = &((uint64_t *) q)[ 4 ];
    ((uint64_t *) p)[ 0 ] = ((uint64_t *) q)[ 0 ];
  }
}

/* spin on ht[ i ] until it is fetchable */
KeyStatus
KeyCtx::fetch_position( const uint64_t i,  const uint64_t spin_wait,
                        const bool is_scan )
{
  ThrCtx    & ctx  = this->thr_ctx;
  HashEntry * cpy  = this->get_work_entry();
  uint64_t    spin = 0,
              h;
  if ( cpy == NULL )
    return KEY_ALLOC_FAILED;
  HashEntry &el = *this->ht.get_entry( i, this->hash_entry_size );
  /* loop, until slot is not busy -- this algo does not lock the slot */
  for (;;) {
    for (;;) {
      h = fetch_hash_entry( cpy, &el, this->hash_entry_size );
      /* if slot is not busy */
      if ( ( h & ZOMBIE64 ) == 0 ) {
        if ( h != 0 ) /* if has data */
          goto check_seal;
        else          /* no data in slot */
          goto not_found;
      }
      /* key was locked, spin again */
      if ( ++spin == spin_wait ) {
        ctx.incr_spins( spin );
        return KEY_BUSY;
      }
      kv_sync_pause();
    }
  check_seal:;
    if ( cpy->check_seal( this->hash_entry_size ) ) {
  not_found:;
      if ( spin > 0 )
        ctx.incr_spins( spin );
      if ( ! this->test( KEYCTX_IS_CUCKOO_ACQUIRE ) )
        ctx.incr_read();
      this->inc    = cpy->cuckoo_inc();
      this->set( KEYCTX_IS_READ_ONLY );
      this->pos    = i;
      this->key    = h;
      this->key2   = cpy->hash2;
      this->lock   = h;
      this->serial = cpy->value_ctr( this->hash_entry_size ).get_serial();
      this->entry  = cpy;
      if ( h != 0 ) {
        if ( ! is_scan )
          return KEY_OK;
        if ( cpy->test( FL_DROPPED ) == 0 && this->db_num == this->get_db() )
          return KEY_OK;
      }
      return KEY_NOT_FOUND;
    }
  }
}
#if 0
/* remove slot from table and mark msg free
 * this must walk to the end of the chain and find the last element that could
 * replace the dropped entry and move it
 * if none found, then zero the dropped entry */
KeyStatus
KeyCtx::drop( void )
{
  KeyStatus status;
  if ( (status = this->tombstone()) != KEY_OK )
    return status;

  ThrCtxOwner    closure( this->ht.ctx );
  ThrCtx       & ctx       = this->thr_ctx;
  const uint64_t half_size = this->ht_size / 2;
  HashEntry    * el,
               * base,
               * save;
  uint64_t       cur_mcs_id,
                 h,
                 j,
                 base_mcs_id,
                 save_mcs_id,
                 tmp_mcs_id,
                 save_h,
                 save_pos,
                 i    = this->pos,
                 spin = 0;
  /* start search at ht[ pos + 1 ] for replacements for ht[ pos ] */
  if ( ++i == this->ht_size )
    i = 0;
continue_from_save:;
  save        = NULL;
  save_mcs_id = 0;
  save_h      = 0;
  save_pos    = 0;
  cur_mcs_id = ctx.next_mcs_lock();
  for (;;) {
    el = this->ht.get_entry( i, this->hash_entry_size );
    h  = ctx.get_mcs_lock( cur_mcs_id ).acquire( &el->hash, ZOMBIE64,
                                                 cur_mcs_id, spin, closure );
    if ( h == 0 )
      break;

    j = this->ht.hdr.ht_mod( h );
    /* test the chain for elements that could replace the dropped entry:
         first condition, if j's natural position is before pos:

           ht[0] j  <  ht[1] pos  <  ht[2] h

           if ( j <= pos && pos - j < half_size )
                0 <= 1   && 1   - 0 < half_size
         second condition, if j's natural position wraps around zero:

           ht[15] j  <  ht[0]  k   <   ht[1]  pos   <   ht[2]  h

           if ( j  > pos && j   - pos > half_size )
                15 > 1   && 15  - 1   > half_size
       the condition that must be met for these to be true is a linear probe
       chain length must be less than half_size elements
    */
    if ( ( j <= this->pos && this->pos - j < half_size ) ||
         ( j >  this->pos && j - this->pos > half_size ) ) {
      /* the new save position is closer to the end of the chain */
      if ( save_h != 0 ) {
        /* release old match, found a better one */
        ctx.get_mcs_lock( save_mcs_id ).release( &save->hash, 0, ZOMBIE64,
                                                 save_mcs_id, spin, closure );
        /* swap mcs_id */
        tmp_mcs_id  = save_mcs_id;
        save_mcs_id = cur_mcs_id;
        cur_mcs_id  = tmp_mcs_id;
      }
      else { /* need a new lock, the current one remains locked */
        save_mcs_id = cur_mcs_id;
        cur_mcs_id  = ctx.next_mcs_lock();
      }
      save_h   = h;
      save_pos = i;
      save     = el;
    }
    else { /* the position does not fit as a replacement */
      ctx.get_mcs_lock( cur_mcs_id ).release( &el->hash, h, ZOMBIE64,
                                              cur_mcs_id, spin, closure );
    }
    if ( ++i == this->ht_size )
      i = 0;
  }
  /* end of chain, replace with save entry or zero it */
  base        = this->entry;
  base_mcs_id = this->mcs_id;
  /* if a replacement was found, no need to lock out readers, no chng to data */
  if ( save != NULL ) {
    /* copy save -> entry */
    ::memcpy( &((uint8_t *) (void *) base)[ sizeof( base->hash ) ],
              &((uint8_t *) (void *) save)[ sizeof( base->hash ) ],
              this->hash_entry_size - sizeof( base->hash ) );
    /* clear save */
    ::memset( &((uint8_t *) (void *) save)[ sizeof( base->hash ) ], 0,
              this->hash_entry_size - sizeof( save->hash ) );
  }
  /* no replacement found */
  else {
    /* clear base */
    ::memset( &((uint8_t *) (void *) base)[ sizeof( base->hash ) ], 0,
              this->hash_entry_size - sizeof( base->hash ) );
    save_h = 0;
  }
  /* release base entry, with new hash, if any */
  ctx.get_mcs_lock( base_mcs_id ).release( &base->hash, save_h, ZOMBIE64,
                                           base_mcs_id, spin, closure );
  ctx.release_mcs_lock( base_mcs_id );

  /* end of chain is a zero, release that */
  ctx.get_mcs_lock( cur_mcs_id ).release( &el->hash, 0, ZOMBIE64,
                                          cur_mcs_id, spin, closure );
  ctx.release_mcs_lock( cur_mcs_id );

  /* may need to continue search */
  if ( save != NULL ) {
    if ( (j = ( save_pos + 1 )) == this->ht_size )
      j = 0;
    /* test if replacement in save is at the end of the chain, which is in i */
    if ( j != i ) {
      /* save is the new base, still locked, continue replacement search */
      this->lock   = 0;
      this->pos    = save_pos;
      this->entry  = save;
      this->mcs_id = save_mcs_id;
      i = j;
      goto continue_from_save;
    }
    /* save is at the end of the chain, zero hash, it moved */
    ctx.get_mcs_lock( save_mcs_id ).release( &save->hash, 0, ZOMBIE64,
                                             save_mcs_id, spin, closure );
    ctx.release_mcs_lock( save_mcs_id );
  }
  this->entry = NULL;
  this->msg   = NULL;
  if ( spin > 0 )
    ctx.incr_spins( spin );
  //ctx.incr_drop();  tombstone does it
  return KEY_OK;
}
#endif
