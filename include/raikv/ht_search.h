#ifndef __rai__raikv__ht_search_h__
#define __rai__raikv__ht_search_h__

namespace rai {
namespace kv {

/* Resolve by walking entries until found or empty. Always keep one lock
 active so that no other thread can pass by while this thread is on the same
 chain. If a jump to a non-linear position (where next pos != pos +1), which
 occurs with cuckoo alternate hashing; Then, on collision, it must be able to
 recover by returning KEY_BUSY to avoid the deadlock condition of two threads
 jumping into each others current position */
template <class Position, bool is_blocking>
KeyStatus
KeyCtx::acquire( Position &next )
{
  ThrCtxOwner  closure( this->ht.ctx );
  ThrCtx     & ctx     = this->ht.ctx[ this->ctx_id ];
  HashEntry  * el,              /* current ht[] ptr */
             * last,            /* last ht[] ptr */
             * drop    = NULL;  /* drop ht[] ptr */
  uint64_t     cur_mcs_id,      /* MCS lock queue for the current element */
               last_mcs_id,     /* MCS lock queue for the last element */
               drop_mcs_id = 0, /* MCS lock queue for the drop element */
               h,               /* hash val at the current element */
               last_h,          /* hash val at the last element */
               drop_h = 0,      /* hash val at the drop element */
               spin   = 0;      /* count of spinning */
  KeyStatus    status;
  bool         try_me = false;

  /* start the search, this is the busiest part of the function, most items
   * will be found at the first hash table position */
  cur_mcs_id = ctx.next_mcs_lock();
  el         = this->ht.get_entry( next.pos, this->hash_entry_size );
  if ( is_blocking ) {
    h = ctx.get_mcs_lock( cur_mcs_id ).acquire( el->hash, next.pos, ZOMBIE64,
                                                cur_mcs_id, spin, closure );
  }
  else {
    h = ctx.get_mcs_lock( cur_mcs_id ).try_acquire( el->hash, next.pos,
                                                   ZOMBIE64, cur_mcs_id, spin );
    if ( (h & ZOMBIE64) != 0 )
      goto key_is_busy3;
  }
  /* if either empty or existing item was found */
  if ( h == 0 || ( h == next.key && this->equals( *el ) ) ) {
    /* if item found was dropped, save the old hash and flags */
    if ( kv_unlikely( h != 0 && el->test( FL_DROPPED ) ) ) {
  found_drop:;
      this->drop_key   = h; /* track in case entry needs to restore tombstone */
      this->drop_key2  = el->hash2;
      this->drop_flags = el->flags;
      el->flags        = FL_NO_ENTRY; /* clear the drop */
      h                = 0; /* treat as a new entry */
    }
  found_match:;
    if ( spin > 0 )
      this->incr_spins( spin );
    if ( ! this->test( KEYCTX_IS_GC_ACQUIRE | KEYCTX_IS_CUCKOO_ACQUIRE ) )
      this->incr_write();
    this->clear( KEYCTX_IS_READ_ONLY );
    this->pos    = next.pos;
    this->lock   = h;
    this->mcs_id = cur_mcs_id;
    this->serial = el->unseal_entry( this->hash_entry_size );
    this->entry  = el;
    return ( h == 0 ? KEY_IS_NEW : KEY_OK );
  }
  /* first element not equal, linear probe */
  for (;;) {
    /* returns KEY_NOT_FOUND in cuckoo when no more buckets to search and in
     * linear when there are no more elements in table */
    if ( (status = next.acquire_incr( ++this->chains, try_me,
                                      drop != NULL )) != KEY_OK )
      break;
    /* check for tombstoned entries */
    if ( kv_unlikely( drop == NULL && ( el->test( FL_DROPPED ) ||
                                        this->test( KEYCTX_EVICT_ACQUIRE ) ) ) ) {
      drop_mcs_id = cur_mcs_id;
      drop        = el;
      drop_h      = h;

      cur_mcs_id = ctx.next_mcs_lock();
      el         = this->ht.get_entry( next.pos, this->hash_entry_size );
      if ( is_blocking && ! try_me ) {
        h = ctx.get_mcs_lock( cur_mcs_id ).acquire( el->hash, next.pos, ZOMBIE64,
                                                    cur_mcs_id, spin, closure );
      }
      else { /* next hash requires a jump to a non-linear location */
        h = ctx.get_mcs_lock( cur_mcs_id ).try_acquire( el->hash, next.pos,
                                                   ZOMBIE64, cur_mcs_id, spin );
        if ( (h & ZOMBIE64) != 0 )
          goto key_is_busy2;
      }
      /* if at the end of a chain, use the tombstone entry */
      if ( h == 0 ) {
      recycle_drop:;
        ctx.get_mcs_lock( cur_mcs_id ).release( el->hash, h, ZOMBIE64,
                                                cur_mcs_id, spin, closure );
        ctx.release_mcs_lock( cur_mcs_id );
        this->incr_chains( this->chains );
        cur_mcs_id = drop_mcs_id;
        el         = drop;
        h          = drop_h;
        next.pos   = this->ht.get_entry_pos( drop, this->hash_entry_size );
        next.restore_inc();
        goto found_drop;
      }
      /* if found the key, release the tombstone */
      if ( h == next.key && this->equals( *el ) ) {
      release_drop:;
        ctx.get_mcs_lock( drop_mcs_id ).release( drop->hash, drop_h, ZOMBIE64,
                                                 drop_mcs_id, spin, closure );
        ctx.release_mcs_lock( drop_mcs_id );
        this->incr_chains( this->chains );
        if ( el->test( FL_DROPPED ) )
          goto found_drop;
        goto found_match;
      }
      if ( (status = next.acquire_incr( ++this->chains, try_me,
                                        drop != NULL )) != KEY_OK )
        break;
    }
    /* keep one element locked by locking next and then releasing the current */
    last_mcs_id = cur_mcs_id;
    last        = el;
    last_h      = h;

    cur_mcs_id = ctx.next_mcs_lock();
    el         = this->ht.get_entry( next.pos, this->hash_entry_size );
    if ( is_blocking && ! try_me ) {
      h = ctx.get_mcs_lock( cur_mcs_id ).acquire( el->hash, next.pos, ZOMBIE64,
                                                  cur_mcs_id, spin, closure );
    }
    else { /* next hash requires a jump to a non-linear location */
      h = ctx.get_mcs_lock( cur_mcs_id ).try_acquire( el->hash, next.pos,
                                                   ZOMBIE64, cur_mcs_id, spin );
      if ( (h & ZOMBIE64) != 0 )
        goto key_is_busy1;
    }
    ctx.get_mcs_lock( last_mcs_id ).release( last->hash, last_h, ZOMBIE64,
                                             last_mcs_id, spin, closure );
    ctx.release_mcs_lock( last_mcs_id );
    /* check for match */
    if ( h == 0 || ( h == next.key && this->equals( *el ) ) ) {
      this->incr_chains( this->chains );
      /* process the tomebstone entry */
      if ( drop != NULL ) {
        if ( h == 0 )
          goto recycle_drop; /* no match */
        goto release_drop; /* matched */
      }
      if ( h != 0 && el->test( FL_DROPPED ) )
        goto found_drop;
      /* return the match */
      goto found_match;
    }
  }
  if ( drop != NULL ) {
    if ( status == KEY_USE_DROP )
      goto recycle_drop;
    ctx.get_mcs_lock( drop_mcs_id ).release( drop->hash, drop_h, ZOMBIE64,
                                             drop_mcs_id, spin, closure );
    ctx.release_mcs_lock( drop_mcs_id );
  }
  ctx.get_mcs_lock( cur_mcs_id ).release( el->hash, h, ZOMBIE64,
                                          cur_mcs_id, spin, closure );
  ctx.release_mcs_lock( cur_mcs_id );
  this->incr_chains( this->chains );
  return status;

key_is_busy1:;
  ctx.get_mcs_lock( last_mcs_id ).release( last->hash, last_h, ZOMBIE64,
                                           last_mcs_id, spin, closure );
  ctx.release_mcs_lock( last_mcs_id );

key_is_busy2:;
  if ( drop != NULL ) {
    ctx.get_mcs_lock( drop_mcs_id ).release( drop->hash, drop_h, ZOMBIE64,
                                             drop_mcs_id, spin, closure );
    ctx.release_mcs_lock( drop_mcs_id );
  }
key_is_busy3:;
  if ( spin > 0 )
    this->incr_spins( spin );
  ctx.release_mcs_lock( cur_mcs_id );
  this->incr_chains( this->chains );
  return KEY_BUSY;
}

template <class Position>
KeyStatus
KeyCtx::acquire_single_thread( Position &next )
{
  HashEntry  * el,              /* current ht[] ptr */
             * drop   = NULL;  /* drop ht[] ptr */
  uint64_t     h,               /* hash val at the current element */
               drop_h = 0;      /* hash val at the drop element */
  KeyStatus    status;

  /* start the search, this is the busiest part of the function, most items
   * will be found at the first hash table position */
  el = this->ht.get_entry( next.pos, this->hash_entry_size );
  h  = el->hash;
  /* if either empty or existing item was found */
  if ( h == 0 || ( h == next.key && this->equals( *el ) ) ) {
    /* if item found was dropped, save the old hash and flags */
    if ( kv_unlikely( h != 0 && el->test( FL_DROPPED ) ) ) {
  found_drop:;
      this->drop_key   = h; /* track in case entry needs to restore tombstone */
      this->drop_key2  = el->hash2;
      this->drop_flags = el->flags;
      el->flags        = FL_NO_ENTRY; /* clear the drop */
      h                = 0; /* treat as a new entry */
    }
  found_match:;
    if ( ! this->test( KEYCTX_IS_GC_ACQUIRE | KEYCTX_IS_CUCKOO_ACQUIRE ) )
      this->incr_write();
    this->clear( KEYCTX_IS_READ_ONLY );
    this->pos    = next.pos;
    this->lock   = h;
    this->serial = el->unseal_entry( this->hash_entry_size );
    this->entry  = el;
    return ( h == 0 ? KEY_IS_NEW : KEY_OK );
  }
  /* first element not equal, linear probe */
  for (;;) {
    /* returns KEY_NOT_FOUND in cuckoo when no more buckets to search and in
     * linear when there are no more elements in table */
    if ( (status = next.acquire_incr_single_thread( ++this->chains,
                                                    drop != NULL )) != KEY_OK )
      break;
    /* check for tombstoned entries */
    if ( kv_unlikely( drop == NULL && el->test( FL_DROPPED ) ) ) {
      drop   = el;
      drop_h = h;
      el     = this->ht.get_entry( next.pos, this->hash_entry_size );
      h      = el->hash;

      /* if at the end of a chain, use the tombstone entry */
      if ( h == 0 ) {
      recycle_drop:;
        this->incr_chains( this->chains );
        el = drop;
        h  = drop_h;
        next.pos = this->ht.get_entry_pos( drop, this->hash_entry_size );
        next.restore_inc();
        goto found_drop;
      }
      /* if found the key, release the tombstone */
      if ( h == next.key && this->equals( *el ) ) {
      release_drop:;
        this->incr_chains( this->chains );
        if ( el->test( FL_DROPPED ) )
          goto found_drop;
        goto found_match;
      }
      if ( (status = next.acquire_incr_single_thread( ++this->chains,
                                                     drop != NULL )) != KEY_OK )
        break;
    }
    el = this->ht.get_entry( next.pos, this->hash_entry_size );
    h  = el->hash;
    /* check for match */
    if ( h == 0 || ( h == next.key && this->equals( *el ) ) ) {
      this->incr_chains( this->chains );
      /* process the tomebstone entry */
      if ( drop != NULL ) {
        if ( h == 0 )
          goto recycle_drop; /* no match */
        goto release_drop; /* matched */
      }
      if ( h != 0 && el->test( FL_DROPPED ) )
        goto found_drop;
      /* return the match */
      goto found_match;
    }
  }
  if ( drop != NULL ) {
    if ( status == KEY_USE_DROP )
      goto recycle_drop;
  }
  this->incr_chains( this->chains );
  return status;
}

/* 32b aligned hash entry copy, zero if slot is empty */
static inline uint64_t
copy_hash_entry( uint64_t k,  void *p,  const void *q,  uint32_t sz )
{
  const uint64_t h = ((const uint64_t *) q)[ 0 ];
  if ( h == k ) {
    for (;;) {
      ((uint64_t *) p)[ 0 ] = ((const uint64_t *) q)[ 0 ];
      ((uint64_t *) p)[ 1 ] = ((const uint64_t *) q)[ 1 ];
      ((uint64_t *) p)[ 2 ] = ((const uint64_t *) q)[ 2 ];
      ((uint64_t *) p)[ 3 ] = ((const uint64_t *) q)[ 3 ];
      if ( (sz -= 32) == 0 )
        break;
      p = &((uint64_t *) p)[ 4 ]; q = &((const uint64_t *) q)[ 4 ];
    }
  }
  return h;
}

/* find key for read only access without locking slot */
template <class Position>
KeyStatus
KeyCtx::find( Position &next )
{
  HashEntry * cpy  = this->get_work_entry();
  uint64_t    spin = 0,
              h;
  KeyStatus   status;

  if ( kv_unlikely( cpy == NULL ) )
    return KEY_ALLOC_FAILED;
  /* loop, until found or empty slot -- this algo does not lock the slot */
  for (;;) {
    HashEntry &el = *this->ht.get_entry( next.pos, this->hash_entry_size );
    for (;;) {
      h = copy_hash_entry( next.key, cpy, &el, this->hash_entry_size );
      /* check if it is the key we're looking 4 */
      if ( kv_likely( h == next.key ) ) {
        if ( kv_likely( cpy->check_seal( this->hash_entry_size ) ) ) {
          if ( kv_likely( this->equals( *cpy ) ) ) {
            if ( kv_likely( cpy->test( FL_DROPPED ) == 0 ) ) {
              this->incr_hit();
              status = KEY_OK;
            }
            else {
        not_found:;
              this->incr_miss();
              status = KEY_NOT_FOUND;
            }
            if ( this->chains > 0 )
              this->incr_chains( this->chains );
            if ( spin > 0 )
              this->incr_spins( spin );
            this->incr_read();
            this->pos    = next.pos;
            this->lock   = h;
            this->serial = cpy->value_ctr( this->hash_entry_size ).get_serial();
            this->entry  = cpy;
            return status;
          }
          break; /* h == k, but not equal, get the next elem */
        }
        /* seal is broken, copy again */
      }
      else if ( h == 0 ) /* check if it is an empty slot */
        goto not_found;
      else if ( ( h & ZOMBIE64 ) == 0 ) /* h is not the key, get next elem */
        break;
      /* h has ZOMBIE64 set or seal is broken, spin and check hash again */
      spin++;
      kv_sync_pause();
    }
    /* get next element in chain (i += 1) */
    if ( (status = next.find_incr( ++this->chains )) != KEY_OK ) {
      this->incr_spins( spin );
      return status;
    }
  }
}

/* find key for read only access without locking slot */
template <class Position>
KeyStatus
KeyCtx::find_single_thread( Position &next )
{
  uint64_t  h;
  KeyStatus status;

  /* loop, until found or empty slot -- this algo does not lock the slot */
  for (;;) {
    HashEntry &el = *this->ht.get_entry( next.pos, this->hash_entry_size );
    h = el.hash;
    if ( kv_likely( h == next.key ) ) { /* if it is the key we're looking 4 */
      if ( kv_likely( this->equals( el ) ) ) {
        if ( kv_likely( el.test( FL_DROPPED ) == 0 ) ) {
          this->incr_hit();
          status = KEY_OK;
        }
        else {
    not_found:;
          this->incr_miss();
          status = KEY_NOT_FOUND;
        }
        if ( this->chains > 0 )
          this->incr_chains( this->chains );
        this->incr_read();
        this->pos    = next.pos;
        this->lock   = h;
        this->serial = el.value_ctr( this->hash_entry_size ).get_serial();
        this->entry  = &el;
        return status;
      }
    }
    else if ( h == 0 ) /* check if it is an empty slot */
      goto not_found;
    /* get next element in chain (i += 1) */
    if ( (status = next.find_incr( ++this->chains )) != KEY_OK )
      return status;
  }
}
}
}

#endif
