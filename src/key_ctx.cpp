#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stddef.h>

#include <raikv/shm_ht.h>

using namespace rai;
using namespace kv;

KeyCtx::KeyCtx( HashTab *t,  uint32_t id,  KeyFragment *b )
      : ht( *t ), kbuf( b ), ctx_id( id ),
        hash_entry_size( t->hdr.hash_entry_size ),
        ht_size( t->hdr.ht_size )
{
  ::memset( &this->entry, 0, (uint8_t *) (void *) &(&this->flags)[ 1 ] -
                             (uint8_t *) (void *) &this->entry );
  this->max_chains = 6;
  this->set( KEYCTX_IS_READ_ONLY );
}

KeyCtx::KeyCtx( HashTab *t,  uint32_t id,  KeyFragment &b )
      : ht( *t ), kbuf( &b ), ctx_id( id ),
        hash_entry_size( t->hdr.hash_entry_size ),
        ht_size( t->hdr.ht_size )
{
  ::memset( &this->entry, 0, (uint8_t *) (void *) &(&this->flags)[ 1 ] -
                             (uint8_t *) (void *) &this->entry );
  this->max_chains = 6;
  this->set( KEYCTX_IS_READ_ONLY );
}

KeyCtx *
KeyCtx::new_array( HashTab *t,  uint32_t id,  void *b,  size_t bsz )
{
  KeyCtxBuf *p = (KeyCtxBuf *) b;
  if ( p == NULL ) {
    p = (KeyCtxBuf *) ::malloc( sizeof( KeyCtxBuf ) * bsz );
    if ( p == NULL )
      return NULL;
    b = (void *) p;
  }
  for ( size_t i = 0; i < bsz; i++ ) {
    new ( (void *) p ) KeyCtx( t, id );
    p = &p[ 1 ];
  }
  return (KeyCtx *) b;
}

/* linear hash resolve, walk entries, always keep one lock active
   so that no other thread can pass by while this thread is on the
   same chain */
KeyStatus
KeyCtx::acquire( void )
{
  ThrCtxOwner    closure( this->ht.ctx );
  ThrCtx       & ctx     = this->ht.ctx[ this->ctx_id ];
  const uint64_t k       = this->key;
  HashEntry    * el,              /* current ht[] ptr */
               * last,            /* last ht[] ptr */
               * drop    = NULL;  /* drop ht[] ptr */
  uint64_t       cur_mcs_id,      /* MCS lock queue for the current element */
                 last_mcs_id,     /* MCS lock queue for the last element */
                 drop_mcs_id = 0, /* MCS lock queue for the drop element */
                 h,               /* hash val at the current element */
                 last_h,          /* hash val at the last element */
                 drop_h = 0,      /* hash val at the drop element */
                 i      = this->start, /* probing starts */
                 spin   = 0;      /* count of spinning */
  uint8_t        ht_evict_flag = 0; /* if max chains caused a drop */

  this->chains = 0; /* count of chains */
  cur_mcs_id = ctx.next_mcs_lock();
  el         = this->ht.get_entry( i, this->hash_entry_size );
  h          = ctx.get_mcs_lock( cur_mcs_id ).acquire( el->hash, ZOMBIE64,
                                                    cur_mcs_id, spin, closure );
  /* acquire was successful, either empty or existing item was found */
  if ( h == 0 || ( h == k && this->equals( *el ) ) ) {
    if ( h != 0 && el->test( FL_DROPPED ) ) {
  found_drop:;
      this->drop_key   = h; /* track in case entry needs to restore tombstone */
      this->drop_flags = el->flags;
      el->flags        = FL_NO_ENTRY; /* clear the drop */
      h                = 0; /* treat as a new entry */
    }
  found_match:;
    if ( spin > 0 )
      ctx.incr_spins( spin );
    if ( ! this->test( KEYCTX_IS_GC_ACQUIRE ) )
      ctx.incr_write();
    this->pos       = i;
    this->lock      = h;
    this->mcs_id    = cur_mcs_id;
    this->serial    = el->unseal( this->hash_entry_size );
    this->update_ns = 0; /* loaded on demand */
    this->expire_ns = 0;
    this->entry     = el;
    this->msg       = NULL;
    this->clear( KEYCTX_IS_READ_ONLY | KEYCTX_IS_HT_EVICT );
    this->set( ht_evict_flag );
    return ( h == 0 ? KEY_IS_NEW : KEY_OK );
  }
  /* first element not equal, linear probe */
  for ( this->chains = 1; ; this->chains++ ) {
    if ( ++i == this->ht_size )
      i = 0;
    /* went around-the-world without finding an empty slot, shouldn't happen */
    if ( i == this->start )
      goto ht_full;
    /* check for tombstoned entries or chains hits max_chains */
    if ( el->test( FL_DROPPED ) ||
         ( drop == NULL && this->chains == this->max_chains ) ) {
      if ( drop != NULL ) {
        /* if max_chains caused a drop and found an actual drop, use that */
        if ( ht_evict_flag && el->test( FL_DROPPED ) ) {
          ctx.get_mcs_lock( drop_mcs_id ).release( drop->hash, drop_h, ZOMBIE64,
                                                   drop_mcs_id, spin, closure );
          ctx.release_mcs_lock( drop_mcs_id );
          drop = NULL;
          ht_evict_flag = 0;
        }
      }
      if ( drop == NULL ) {
        drop_mcs_id    = cur_mcs_id;
        drop           = el;
        drop_h         = h;
        /* must be max_chains */
        ht_evict_flag  = el->test( FL_DROPPED ) ? 0 : KEYCTX_IS_HT_EVICT;

        cur_mcs_id = ctx.next_mcs_lock();
        el         = this->ht.get_entry( i, this->hash_entry_size );
        h          = ctx.get_mcs_lock( cur_mcs_id ).acquire( el->hash, ZOMBIE64,
                                                    cur_mcs_id, spin, closure );
        /* if at the end of a chain, use the tombstone entry */
        if ( h == 0 ) {
        recycle_drop:;
          ctx.get_mcs_lock( cur_mcs_id ).release( el->hash, h, ZOMBIE64,
                                                  cur_mcs_id, spin, closure );
          ctx.release_mcs_lock( cur_mcs_id );
          ctx.incr_chains( this->chains );
          cur_mcs_id = drop_mcs_id;
          el         = drop;
          h          = drop_h;
          goto found_drop;
        }
        /* if found the key, release the tombstone */
        if ( h == k && this->equals( *el ) ) {
        release_drop:;
          ctx.get_mcs_lock( drop_mcs_id ).release( drop->hash, drop_h, ZOMBIE64,
                                                   drop_mcs_id, spin, closure );
          ctx.release_mcs_lock( drop_mcs_id );
          ctx.incr_chains( this->chains );
          if ( el->test( FL_DROPPED ) )
            goto found_drop;
          goto found_match;
        }
        if ( ++i == this->ht_size )
          i = 0;
      }
    }
    /* keep one element locked by locking next and then releasing the current */
    last_mcs_id = cur_mcs_id;
    last        = el;
    last_h      = h;

    cur_mcs_id = ctx.next_mcs_lock();
    el         = this->ht.get_entry( i, this->hash_entry_size );
    h          = ctx.get_mcs_lock( cur_mcs_id ).acquire( el->hash, ZOMBIE64,
                                                    cur_mcs_id, spin, closure );
    ctx.get_mcs_lock( last_mcs_id ).release( last->hash, last_h, ZOMBIE64,
                                             last_mcs_id, spin, closure );
    ctx.release_mcs_lock( last_mcs_id );
    /* check for match */
    if ( h == 0 || ( h == k && this->equals( *el ) ) ) {
      ctx.incr_chains( this->chains );
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
ht_full:;
  if ( drop != NULL ) /* ht full, but have tombstones */
    goto recycle_drop;
  ctx.get_mcs_lock( cur_mcs_id ).release( el->hash, h, ZOMBIE64,
                                          cur_mcs_id, spin, closure );
  ctx.release_mcs_lock( cur_mcs_id );
  ctx.incr_chains( this->chains );
  return KEY_HT_FULL;
}

/* similar to acquire() but bail if can't acquire without getting in line */
KeyStatus
KeyCtx::try_acquire( void )
{
  ThrCtxOwner    closure( this->ht.ctx );
  ThrCtx       & ctx     = this->ht.ctx[ this->ctx_id ];
  const uint64_t k       = this->key;
  HashEntry    * el,          /* current ht[] ptr */
               * last;        /* last ht[] ptr */
  uint64_t       cur_mcs_id,  /* MCS lock queue for the current element */
                 last_mcs_id, /* MCS lock queue for the last element */
                 h,           /* hash val at the current element */
                 last_h,      /* hash val at the last element */
                 i      = this->start, /* where the ht linear probe starts */
                 spin   = 0;  /* count of spinning */

  this->chains = 0;  /* count of chains */
  cur_mcs_id = ctx.next_mcs_lock();
  el         = this->ht.get_entry( i, this->hash_entry_size );
  h          = ctx.get_mcs_lock( cur_mcs_id ).try_acquire( el->hash, ZOMBIE64,
                                                           cur_mcs_id, spin );
  if ( (h & ZOMBIE64) != 0 ) {
  key_is_busy:;
    if ( spin > 0 )
      ctx.incr_spins( spin );
    ctx.release_mcs_lock( cur_mcs_id );
    return KEY_BUSY;
  }
  /* acquire was successful, either empty or existing item was found */
  if ( h == 0 || ( h == k && this->equals( *el ) ) ) {
  key_is_matched:;
    if ( spin > 0 )
      ctx.incr_spins( spin );
    if ( ! this->test( KEYCTX_IS_GC_ACQUIRE ) )
      ctx.incr_write();
    this->pos       = i;
    this->lock      = h;
    this->mcs_id    = cur_mcs_id;
    this->serial    = el->unseal( this->hash_entry_size );
    this->update_ns = 0; /* loaded on demand */
    this->expire_ns = 0;
    this->entry     = el;
    this->msg       = NULL;
    this->clear( KEYCTX_IS_READ_ONLY | KEYCTX_IS_HT_EVICT );
    return ( this->lock == 0 ? KEY_IS_NEW : KEY_OK );
  }
  /* first element not equal, linear probe */
  for ( this->chains = 1; ; this->chains++ ) {
    if ( ++i == this->ht_size )
      i = 0;
    /* went around-the-world without finding an empty slot, shouldn't happen */
    if ( i == this->start )
      break;
    /* keep one element locked by locking next and then releasing the current */
    last_mcs_id   = cur_mcs_id;
    last          = el;
    last_h        = h;

    cur_mcs_id = ctx.next_mcs_lock();
    el         = this->ht.get_entry( i, this->hash_entry_size );
    h          = ctx.get_mcs_lock( cur_mcs_id ).try_acquire( el->hash, ZOMBIE64,
                                                             cur_mcs_id, spin );
    ctx.get_mcs_lock( last_mcs_id ).release( last->hash, last_h, ZOMBIE64,
                                             last_mcs_id, spin, closure );
    ctx.release_mcs_lock( last_mcs_id );
    /* check for match */
    if ( (h & ZOMBIE64) != 0 || h == 0 || ( h == k && this->equals( *el ) ) ) {
      ctx.incr_chains( this->chains );
      if ( (h & ZOMBIE64) != 0 )
        goto key_is_busy;
      goto key_is_matched;
    }
  }
  ctx.get_mcs_lock( cur_mcs_id ).release( el->hash, h, ZOMBIE64,
                                          cur_mcs_id, spin, closure );
  ctx.release_mcs_lock( cur_mcs_id );
  return KEY_HT_FULL;
}

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
  ThrCtx       & ctx       = ht.ctx[ this->ctx_id ];
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
    h  = ctx.get_mcs_lock( cur_mcs_id ).acquire( el->hash, ZOMBIE64,
                                                 cur_mcs_id, spin, closure );
    if ( h == 0 )
      break;

    j = h % this->ht_size;
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
        ctx.get_mcs_lock( save_mcs_id ).release( save->hash, 0, ZOMBIE64,
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
      ctx.get_mcs_lock( cur_mcs_id ).release( el->hash, h, ZOMBIE64,
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
  ctx.get_mcs_lock( base_mcs_id ).release( base->hash, save_h, ZOMBIE64,
                                           base_mcs_id, spin, closure );
  ctx.release_mcs_lock( base_mcs_id );

  /* end of chain is a zero, release that */
  ctx.get_mcs_lock( cur_mcs_id ).release( el->hash, 0, ZOMBIE64,
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
    ctx.get_mcs_lock( save_mcs_id ).release( save->hash, 0, ZOMBIE64,
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

KeyStatus
KeyCtx::tombstone( void )
{
  if ( this->lock != 0 ) { /* if it's not new */
    ThrCtx  & ctx = ht.ctx[ this->ctx_id ];
    KeyStatus status;
    if ( (status = this->release_data()) != KEY_OK )
      return status;
    this->serial = 0;
    this->entry->set( FL_DROPPED );
    ctx.incr_drop();
  }
  return KEY_OK;
}

/* 32b aligned hash entry copy, zero if slot is empty */
static inline uint64_t
copy_hash_entry( uint64_t k,  void *p,  void *q,  uint32_t sz )
{
  uint64_t h = ( ((uint64_t *) p)[ 0 ] = ((uint64_t *) q)[ 0 ] );
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
  if ( h != k ) /* could be busy, or could be ! match */
    return h;
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

KeyStatus
KeyCtx::find( KeyCtxAlloc *a,  const uint64_t spin_wait )
{
  this->init_work( a ); /* buffer used for copying hash entry & data */
  return this->find2( spin_wait );
}

/* find key for read only access without locking slot */
KeyStatus
KeyCtx::find2( const uint64_t spin_wait )
{
  ThrCtx       & ctx     = this->ht.ctx[ this->ctx_id ];
  const uint64_t k       = this->key;
  HashEntry    * cpy     = this->get_work_entry();
  uint64_t       i       = this->start,
                 spin    = 0,
                 h;
  this->chains = 0;
  if ( cpy == NULL )
    return KEY_ALLOC_FAILED;
  /* loop, until found or empty slot -- this algo does not lock the slot */
  for (;;) {
    HashEntry &el = *this->ht.get_entry( i, this->hash_entry_size );
    for (;;) {
      h = copy_hash_entry( k, cpy, &el, this->hash_entry_size );
      if ( ( h & ZOMBIE64 ) == 0 ) {
        if ( h == k ) {    /* check if it is the key we're looking for */
          if ( cpy->check_seal( this->hash_entry_size ) )
            goto check_equals;
        }
        else if ( h == 0 ) /* check if it is an empty slot */
          goto not_found;
        else               /* h is not the key */
          goto not_equal;
      }
      /* spin, check hash again */
      if ( ++spin == spin_wait ) {
        ctx.incr_spins( spin );
        return KEY_BUSY;
      }
      kv_sync_pause();
    }
  check_equals:;
    if ( this->equals( *cpy ) ) {
  not_found:;
      if ( this->chains > 0 )
        ctx.incr_chains( this->chains );
      if ( spin > 0 )
        ctx.incr_spins( spin );
      ctx.incr_read();
      this->pos       = i;
      this->lock      = h;
      this->serial    = cpy->value_ctr( this->hash_entry_size ).get_serial();
      this->update_ns = 0; /* loaded on demand */
      this->expire_ns = 0;
      this->entry     = cpy;
      this->msg       = NULL;
      this->set( KEYCTX_IS_READ_ONLY );
      if ( h != 0 ) {
        ctx.incr_hit();
        return KEY_OK;
      }
      ctx.incr_miss();
      return KEY_NOT_FOUND;
    }
  not_equal:;
    this->chains++;
    if ( ++i == this->ht_size )
      i = 0;
    if ( i == this->start )
      return KEY_HT_FULL;
  }
}

bool
KeyCtx::equals( const HashEntry &el ) const
{
  if ( this->kbuf == NULL )
    return true;
  KeyFragment &kb = *this->kbuf;
  if ( el.test( FL_IMMEDIATE_KEY ) )
    return el.key.frag_equals( kb );
  if ( el.key.keylen == kb.keylen ) {
  /* 95 bits of hash, 1 billion keys birthday paradox:
     k^2 / 2N = ( 1e9 * 1e9 ) / ( 2 << 95 ) =
     probability of a collision is 1 / 80 billion,
     where all keys are the same length, since keylen is compared
     and large enough to overflow HashEntry(64b) or keylen > 32b
     ... may need to use HashEntry(128b) if 512bit SHA hashes are the keys */
    uint32_t check = kv_crc_c( kb.buf, kb.keylen, 0 );
    return check == ( ( (uint32_t) el.key.x.b1 << 16 ) |
                        (uint32_t) el.key.x.b2 );
  }
  return false;
}

KeyStatus
KeyCtx::fetch( KeyCtxAlloc *a,  uint64_t i,
               const uint64_t spin_wait )
{
  this->init_work( a ); /* buffer used for copying hash entry & data */
  return this->fetch2( i, spin_wait );
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
KeyCtx::fetch2( const uint64_t i,  const uint64_t spin_wait )
{
  ThrCtx    & ctx  = this->ht.ctx[ this->ctx_id ];
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
      ctx.incr_read();
      this->pos       = i;
      this->key       = h;
      this->lock      = h;
      this->serial    = cpy->value_ctr( this->hash_entry_size ).get_serial();
      this->update_ns = 0; /* loaded on demand */
      this->expire_ns = 0;
      this->entry     = cpy;
      this->msg       = NULL;
      this->set( KEYCTX_IS_READ_ONLY );
      if ( h != 0 )
        return KEY_OK;
      return KEY_NOT_FOUND;
    }
  }
}

HashEntry *
KeyCtx::get_work_entry( void )
{
  return (HashEntry *) ( this->wrk == NULL ? NULL :
                         this->wrk->alloc( this->hash_entry_size ) );
}

void *
KeyCtx::copy_data( void *data,  uint64_t sz )
{
  void *p = ( this->wrk == NULL ? NULL : this->wrk->alloc( sz ) );
  if ( p != NULL ) {
    ::memcpy( p, data, sz );
    return p;
  }
  return NULL;
}

KeyStatus
KeyCtx::get_key( KeyFragment *&b )
{
  KeyStatus mstatus;

  if ( this->entry == NULL || this->lock == 0 )
    return KEY_NOT_FOUND;
  if ( this->entry->test( FL_DROPPED ) ) {
    if ( this->entry->test( FL_IMMEDIATE_KEY ) )
      b = &this->entry->key;
    else
      b = NULL;
    return KEY_TOMBSTONE;
  }
  if ( ! this->entry->test( FL_IMMEDIATE_KEY ) ) {
    if ( this->entry->test( FL_SEGMENT_VALUE ) ) {
      if ( this->msg == NULL &&
           (mstatus = this->attach_msg( ATTACH_READ )) != KEY_OK )
        return mstatus;
      b = &this->msg->key;
    }
    else {
      b = NULL;
      if ( this->entry->test( FL_PART_KEY ) )
        return KEY_PART_ONLY;
      return KEY_NOT_FOUND;
    }
  }
  else
    b = &this->entry->key;
  return KEY_OK;
}

void
KeyCtx::prefetch( uint64_t cnt ) const
{
  /* I believe rw is ignored, locality may have moderate effects, but it is
   * hard to measure accurately since memory systems have lots of noise */
  static const int rw = 1;       /* 0 is prepare for write, 1 is read */
  static const int locality = 2; /* 0 is non, 1 is low, 2 is moderate, 3 high*/
  const uint8_t * p = (uint8_t *) (void *)
                      this->ht.get_entry( this->start, this->hash_entry_size ),
                * e = (uint8_t *) &p[ cnt * this->hash_entry_size ];
  do {
    __builtin_prefetch( p, rw, locality );
    p = &p[ 64 ];
  } while ( p < e );
}

/* release the Key's entry in ht[] */
void
KeyCtx::release( void )
{
  if ( this->entry == NULL )
    return;
  HashEntry & el   = *this->entry;
  ThrCtx    & ctx  = this->ht.ctx[ this->ctx_id ];
  ThrCtxOwner closure( this->ht.ctx );
  uint64_t    spin = 0,
              k    = this->key;
  /* if no data was inserted, mark the entry as tombstone */
  if ( this->lock == 0 ) { /* if it's new */
    if ( el.flags == FL_NO_ENTRY ) { /* don't keep keys with no data */
      this->entry = NULL;
      /* was already dropped, use pre-existing key and flags */
      if ( this->drop_key != 0 ) {
        k = this->drop_key;
        el.flags = this->drop_flags;
      }
      /* mark entry as tombstone */
      else {
        el.flags = FL_DROPPED;
      }
      el.seal( this->hash_entry_size, 0 );
      goto done; /* skip over the seals, they will be tossed */
    }
    ctx.incr_add(); /* counter for added elements */
  }
  /* allow readers to access */
  el.seal( this->hash_entry_size, this->serial );
  if ( el.test( FL_SEGMENT_VALUE ) )
    this->seal_msg();
done:;
  ctx.get_mcs_lock( this->mcs_id ).release( el.hash, k, ZOMBIE64,
                                            this->mcs_id, spin, closure );
  ctx.release_mcs_lock( this->mcs_id );
  //__sync_mfence(); /* push the updates to memory */
  ctx.incr_spins( spin );
  this->entry     = NULL;
  this->msg       = NULL;
  this->drop_key  = 0;
  this->set( KEYCTX_IS_READ_ONLY );
  /*this->update_ns = 0;
  this->expire_ns = 0;*/
}

KeyStatus
KeyCtx::attach_msg( AttachType upd )
{
  /* if result of find(), do not have write access */
  if ( this->test( KEYCTX_IS_READ_ONLY ) ) {
    void *p;
    if ( upd == ATTACH_WRITE ) /* no can do */
      return KEY_WRITE_ILLEGAL;
    this->entry->get_value_geom( this->hash_entry_size, this->geom,
                                 this->ht.hdr.seg_align_shift );
    /* copy msg data into buffer */
    if ( (p = this->copy_data( 
            this->ht.seg_data( this->geom.segment, this->geom.offset ),
            this->geom.size )) == NULL )
      return KEY_ALLOC_FAILED;
    /* check that msg is valid */
    this->msg = (MsgHdr *) p;
    if ( ! this->msg->check_seal( this->key, this->geom.serial,
                                  this->geom.size ) ) {
      this->msg = NULL;
      return KEY_MUTATED;
    }
  }
  else {
    /* locked, exclusive access to message */
    this->entry->get_value_geom( this->hash_entry_size, this->geom,
                                 this->ht.hdr.seg_align_shift );
    this->msg = (MsgHdr *) this->ht.seg_data( this->geom.segment,
                                              this->geom.offset );
    if ( ! this->msg->check_seal( this->key, this->geom.serial,
                                  this->geom.size ) ) {
      this->msg = NULL;
      return KEY_MUTATED;
    }
    /* this clears the seal, locking out getters */
    this->msg->unseal();
  }
  return KEY_OK;
}

/* release the data in the segment for GC */
KeyStatus
KeyCtx::release_data( void )
{
  if ( this->test( KEYCTX_IS_READ_ONLY ) )
    return KEY_WRITE_ILLEGAL;

  HashEntry & el = *this->entry;
  switch ( el.test( FL_SEGMENT_VALUE | FL_IMMEDIATE_VALUE ) ) {
    case FL_SEGMENT_VALUE: {
      KeyStatus mstatus;
      if ( this->msg == NULL &&
           ( (mstatus = this->attach_msg( ATTACH_WRITE )) != KEY_OK ) )
        return mstatus;
      /* release segment data */
      Segment &seg = this->ht.segment( this->geom.segment );
      this->msg->release();
      this->msg = NULL;
      /* clear hash entry geometry */
      el.clear( FL_SEGMENT_VALUE );
      el.value_ptr( this->hash_entry_size ).zero();
      el.value_ctr( this->hash_entry_size ).size = 0;
      seg.msg_count  -= 1;
      seg.avail_size += this->geom.size;
      break;
    }
    case FL_IMMEDIATE_VALUE: {
      el.clear( FL_IMMEDIATE_VALUE );
      el.value_ctr( this->hash_entry_size ).size = 0;
      break;
    }
    default:
      break;
  }
  return KEY_OK;
}

KeyStatus
KeyCtx::release_evict( void )
{
  HashEntry & el  = *this->entry;
  ThrCtx    & ctx = this->ht.ctx[ this->ctx_id ];
  switch ( this->drop_flags & ( FL_SEGMENT_VALUE | FL_IMMEDIATE_VALUE ) ) {
    case FL_SEGMENT_VALUE: {
      MsgHdr * tmp;
      el.get_value_geom( this->hash_entry_size, this->geom,
                         this->ht.hdr.seg_align_shift );
      tmp = (MsgHdr *) this->ht.seg_data( this->geom.segment,
                                          this->geom.offset );
      if ( ! tmp->check_seal( this->drop_key, this->geom.serial,
                              this->geom.size ) ) {
        return KEY_MUTATED;
      }
      Segment &seg = this->ht.segment( this->geom.segment );
      tmp->release();
      this->drop_flags &= ~FL_SEGMENT_VALUE;
      el.value_ptr( this->hash_entry_size ).zero();
      el.value_ctr( this->hash_entry_size ).size = 0;
      seg.msg_count  -= 1;
      seg.avail_size += this->geom.size;
      seg.evict_msgs += 1;
      seg.evict_size += this->geom.size;
      break;
    }
    case FL_IMMEDIATE_VALUE: {
      this->drop_flags &= ~FL_IMMEDIATE_VALUE;
      el.value_ctr( this->hash_entry_size ).size = 0;
      break;
    }
    default:
      break;
  }
  ctx.incr_drop();
  ctx.incr_htevict();
  this->clear( KEYCTX_IS_HT_EVICT );
  return KEY_OK;
}

/* validate the msg data by setting the crc */
void
KeyCtx::seal_msg( void )
{
  if ( this->msg == NULL && this->attach_msg( ATTACH_WRITE ) != KEY_OK )
    return;
  const bool has_ts = ( this->update_ns | this->expire_ns ) != 0;
  if ( has_ts ) {
    RelativeStamp & rs = this->entry->rela_stamp( this->hash_entry_size );
    this->msg->rela_stamp().u.stamp = rs.u.stamp;
  }
  this->msg->seal( this->serial, this->entry->flags );
}

void
KeyCtx::update_stamps( void )
{
  HashEntry     & el = *this->entry;
  RelativeStamp & rs = el.rela_stamp( this->hash_entry_size );
  uint64_t        exp_ns,
                  upd_ns;

  if ( this->get_expire_time( exp_ns ) == KEY_OK ) {
    uint16_t fl = FL_EXPIRE_STAMP;
    if ( this->get_update_time( upd_ns ) == KEY_OK ) {
      rs.set( this->ht.hdr.create_stamp, this->ht.hdr.current_stamp,
              exp_ns, upd_ns );
      fl |= FL_UPDATE_STAMP;
    }
    else {
      rs.u.stamp = exp_ns;
    }
    el.set( fl );
  }
  else if ( this->get_update_time( rs.u.stamp ) == KEY_OK ) {
    el.set( FL_UPDATE_STAMP );
  }
}

KeyStatus
KeyCtx::update_entry( void *res,  uint64_t size,  uint8_t alignment )
{
  HashEntry    & el       = *this->entry;
  KeyFragment  & kb       = *this->kbuf;
  const uint32_t hdr_size = HashEntry::hdr_size( kb );
  uint8_t      * hdr_end  = &((uint8_t *) (void *) &el)[ hdr_size ];
  ValueCtr     & ctr      = el.value_ctr( this->hash_entry_size );
  uint8_t      * trail    = (uint8_t *) (void *) &ctr;
  const bool     has_ts   = ( this->update_ns | this->expire_ns ) != 0;

  if ( has_ts || el.test( FL_EXPIRE_STAMP | FL_UPDATE_STAMP ) ) {
    if ( has_ts )
      this->update_stamps();
    trail -= sizeof( RelativeStamp );
  }
  /* if both key + value fit in the hash entry */
  if ( res != NULL && &hdr_end[ size ] <= trail ) {
    if ( el.test( FL_IMMEDIATE_KEY ) == 0 )
      el.copy_key( kb );
    el.clear( FL_PART_KEY | FL_DROPPED );
    el.set( FL_IMMEDIATE_KEY | FL_IMMEDIATE_VALUE | FL_UPDATED );
    ctr.size = size;
    *(void **) res = (void *) hdr_end; /* return ptr where value is stored */
    return KEY_OK;
  }
  /* if the key fits in the hash entry, value in segment */
  if ( this->ht.hdr.nsegs > 0 ) {
    if ( hdr_end <= trail - sizeof( ValuePtr ) ) {
      if ( el.test( FL_IMMEDIATE_KEY ) == 0 )
        el.copy_key( kb );
      el.clear( FL_PART_KEY | FL_IMMEDIATE_VALUE | FL_DROPPED );
      el.set( FL_IMMEDIATE_KEY | FL_UPDATED );
    }
    else {
      /* only part of the key fits */
      if ( el.test( FL_PART_KEY ) == 0 ) {
        uint32_t check = kv_crc_c( kb.buf, kb.keylen, 0 );
        el.key.keylen = kb.keylen;
        el.key.x.b1   = (uint16_t) ( check >> 16 );
        el.key.x.b2   = (uint16_t) check;
      }
      el.clear( FL_IMMEDIATE_VALUE | FL_IMMEDIATE_KEY | FL_DROPPED );
      el.set( FL_PART_KEY | FL_UPDATED );
    }
  }
  /* key doesn't fit and no segment data, check if value fits */
  else {
    hdr_end = &((uint8_t *) (void *) &el)[ sizeof( HashEntry ) ];
    if ( &hdr_end[ size ] > trail ) {
      this->ht.ctx[ this->ctx_id ].incr_afail();
      return KEY_ALLOC_FAILED;
    }
    /* only part of the key fits */
    if ( el.test( FL_PART_KEY ) == 0 ) {
      uint32_t check = kv_crc_c( kb.buf, kb.keylen, 0 );
      el.key.keylen = kb.keylen;
      el.key.x.b1   = (uint16_t) ( check >> 16 );
      el.key.x.b2   = (uint16_t) check;
    }
    el.clear( FL_IMMEDIATE_KEY | FL_DROPPED );
    el.set( FL_PART_KEY | FL_IMMEDIATE_VALUE | FL_UPDATED );
    ctr.size = size;
    *(void **) res = (void *) hdr_end;
    return KEY_OK;
  }
  ctr.size = 0;
  return KEY_SEG_VALUE;
}

/* return a contiguous memory space for data attached to the Key ht[] entry */
KeyStatus
KeyCtx::alloc( void *res,  uint64_t size,  uint8_t alignment )
{
  if ( this->test( KEYCTX_IS_READ_ONLY | KEYCTX_IS_HT_EVICT ) ) {
    if ( this->test( KEYCTX_IS_READ_ONLY ) )
      return KEY_WRITE_ILLEGAL;
    this->release_evict();
  }
  HashEntry & el = *this->entry;
  /* if something is already allocated, release it */
  if ( el.test( FL_SEGMENT_VALUE ) )
    this->release_data();

  KeyStatus status;
  status = this->update_entry( res, size, alignment );
  this->next_serial( ValueCtr::SERIAL_MASK );

  if ( status == KEY_SEG_VALUE ) {
    /* allocate mem from a segment */
    MsgCtx msg_ctx( &this->ht, this->ctx_id, this->hash_entry_size );
    msg_ctx.set_key( *this->kbuf );
    msg_ctx.set_hash( this->key );
    if ( (status = msg_ctx.alloc_segment( res, size, alignment )) == KEY_OK ) {
      el.set( FL_SEGMENT_VALUE );
      msg_ctx.geom.serial = this->serial;
      this->geom = msg_ctx.geom;
      el.set_value_geom( this->hash_entry_size, msg_ctx.geom,
                         this->ht.hdr.seg_align_shift );
      el.value_ctr( this->hash_entry_size ).size = 1;
      this->msg = msg_ctx.msg;
    }
    else if ( status == KEY_ALLOC_FAILED ) {
      this->ht.ctx[ this->ctx_id ].incr_afail();
    }
  }
  return status;
}

KeyStatus
KeyCtx::load( MsgCtx &msg_ctx )
{
  if ( this->test( KEYCTX_IS_READ_ONLY | KEYCTX_IS_HT_EVICT ) ) {
    if ( this->test( KEYCTX_IS_READ_ONLY ) )
      return KEY_WRITE_ILLEGAL;
    this->release_evict();
  }
  HashEntry & el = *this->entry;

  if ( el.test( FL_SEGMENT_VALUE ) )
    this->release_data();

  this->update_entry( NULL, 0, 0 );
  this->next_serial( ValueCtr::SERIAL_MASK );

  el.set( FL_SEGMENT_VALUE );
  msg_ctx.geom.serial = this->serial;
  el.set_value_geom( this->hash_entry_size, msg_ctx.geom,
                     this->ht.hdr.seg_align_shift );
  el.value_ctr( this->hash_entry_size ).size = 1;
  this->msg = msg_ctx.msg;

  return KEY_OK;
}

KeyStatus
KeyCtx::resize( void *res,  uint64_t size,  uint8_t alignment )
{
  if ( this->test( KEYCTX_IS_READ_ONLY | KEYCTX_IS_HT_EVICT ) ) {
    if ( this->test( KEYCTX_IS_READ_ONLY ) )
      return KEY_WRITE_ILLEGAL;
    this->release_evict();
  }

  HashEntry & el = *this->entry;

  /* check if resize fits within current segment mem */
  if ( el.test( FL_SEGMENT_VALUE ) ) {
    KeyStatus mstatus;
    if ( this->msg == NULL &&
         (mstatus = this->attach_msg( ATTACH_WRITE )) != KEY_OK )
      return mstatus;

    uint32_t hdr_size   = MsgHdr::hdr_size( this->msg->key );
    uint64_t alloc_size = MsgHdr::alloc_size( hdr_size, size,
                                              this->ht.hdr.seg_align() );
    if ( alloc_size == this->msg->size ) {
      const bool has_ts = ( this->update_ns | this->expire_ns ) != 0;
      if ( has_ts ) {
        if ( el.test( FL_EXPIRE_STAMP | FL_UPDATE_STAMP ) == 0 )
          goto needs_new_layout;
        this->update_stamps();
      }
      this->next_serial( ValueCtr::SERIAL_MASK );
      this->geom.serial = el.value_ptr( this->hash_entry_size ).
                             set_serial( this->serial );
      this->msg->msg_size = size;
      *(void **) res = this->msg->ptr( hdr_size );
      return KEY_OK;
    }
  }
needs_new_layout:;
  /* reallocate, could do better if key already copied */
  return this->alloc( res, size, alignment );
}

/* get the value associated with the key */
KeyStatus
KeyCtx::value( void *data,  uint64_t &size )
{
  if ( this->entry == NULL )
    return KEY_NO_VALUE;

  HashEntry & el = *this->entry;

  switch ( el.test( FL_SEGMENT_VALUE | FL_IMMEDIATE_VALUE ) ) {
    case FL_SEGMENT_VALUE: {
      KeyStatus mstatus;
      if ( this->msg == NULL &&
           ( (mstatus = this->attach_msg( ATTACH_READ )) != KEY_OK ) )
        return mstatus;
      /* data starts after hdr */
      *(void **) data = this->msg->ptr( this->msg->hdr_size() );
      size = this->msg->msg_size;
      return KEY_OK;
    }
    case FL_IMMEDIATE_VALUE: {
      /* size stashed at end,  XXX: fix if hash_entry_size > 256 */
      size = el.value_ctr( this->hash_entry_size ).size;
      /* data after hdr in hash entry */
      *(void **) data = (void *) el.immediate_value();
      return KEY_OK;
    }
    default:
      return KEY_NO_VALUE;
  }
}

KeyStatus
KeyCtx::get_update_time( uint64_t &update_time_ns )
{
  if ( this->update_ns != 0 ) {
    update_time_ns = this->update_ns;
    return KEY_OK;
  }
  if ( this->entry != NULL ) {
    switch ( this->entry->test( FL_EXPIRE_STAMP | FL_UPDATE_STAMP ) ) {

      case FL_EXPIRE_STAMP:
        if ( this->expire_ns == 0 )
          this->entry->get_expire_stamp( this->hash_entry_size,
                                         this->expire_ns );
        break;

      case FL_UPDATE_STAMP:
        this->entry->get_update_stamp( this->hash_entry_size, this->update_ns );
        update_time_ns = this->update_ns;
        return KEY_OK;

      case FL_UPDATE_STAMP | FL_EXPIRE_STAMP: {
        uint64_t exp_ns;
        this->entry->get_updexp_stamp( this->hash_entry_size,
                                       this->ht.hdr.create_stamp,
                                       this->ht.hdr.current_stamp,
                                       exp_ns, this->update_ns );
        update_time_ns = this->update_ns;
        if ( this->expire_ns == 0 )
          this->expire_ns = exp_ns;
        return KEY_OK;
      }
    }
  }
  return KEY_NOT_FOUND;
}

KeyStatus
KeyCtx::get_expire_time( uint64_t &expire_time_ns )
{
  if ( this->expire_ns != 0 ) {
    expire_time_ns = this->expire_ns;
    return KEY_OK;
  }
  if ( this->entry != NULL ) {
    switch ( this->entry->test( FL_EXPIRE_STAMP | FL_UPDATE_STAMP ) ) {

      case FL_EXPIRE_STAMP:
        this->entry->get_expire_stamp( this->hash_entry_size, this->expire_ns );
        expire_time_ns = this->expire_ns;
        return KEY_OK;

      case FL_UPDATE_STAMP:
        if ( this->update_ns == 0 )
          this->entry->get_update_stamp( this->hash_entry_size,
                                         this->update_ns );
        break;

      case FL_UPDATE_STAMP | FL_EXPIRE_STAMP: {
        uint64_t upd_ns;
        this->entry->get_updexp_stamp( this->hash_entry_size,
                                       this->ht.hdr.create_stamp,
                                       this->ht.hdr.current_stamp,
                                       this->expire_ns, upd_ns );
        expire_time_ns = this->expire_ns;
        if ( this->update_ns == 0 )
          this->update_ns = upd_ns;
        return KEY_OK;
      }
    }
  }
  return KEY_NOT_FOUND;
}

const char *
rai::kv::key_status_string( KeyStatus status )
{
  return kv_key_status_string( status );
}

const char *
rai::kv::key_status_description( KeyStatus status )
{
  return kv_key_status_description( status );
}

extern "C" {
const char *
kv_key_status_string( kv_key_status_t status )
{
  switch ( status ) {
    case KEY_OK:            return "KEY_OK";
    case KEY_IS_NEW:        return "KEY_IS_NEW";
    case KEY_WAITING:       return "KEY_WAITING";
    case KEY_NOT_FOUND:     return "KEY_NOT_FOUND";
    case KEY_BUSY:          return "KEY_BUSY";
    case KEY_ALLOC_FAILED:  return "KEY_ALLOC_FAILED";
    case KEY_HT_FULL:       return "KEY_HT_FULL";
    case KEY_MUTATED:       return "KEY_MUTATED";
    case KEY_WRITE_ILLEGAL: return "KEY_WRITE_ILLEGAL";
    case KEY_NO_VALUE:      return "KEY_NO_VALUE";
    case KEY_SEG_FULL:      return "KEY_SEG_FULL";
    case KEY_TOO_BIG:       return "KEY_TOO_BIG";
    case KEY_SEG_VALUE:     return "KEY_SEG_VALUE";
    case KEY_TOMBSTONE:     return "KEY_TOMBSTONE";
    case KEY_PART_ONLY:     return "KEY_PART_ONLY";
  }
  return "unknown";
}

const char *
kv_key_status_description( kv_key_status_t status )
{
  switch ( status ) {
    case KEY_OK:            return "key ok";
    case KEY_IS_NEW:        return "key did not exist, is newly acquired";
    case KEY_WAITING:       return "waiting for external event";
    case KEY_NOT_FOUND:     return "not found";
    case KEY_BUSY:          return "key timeout waiting for lock";
    case KEY_ALLOC_FAILED:  return "allocate key or data failed";
    case KEY_HT_FULL:       return "looked at all ht entries";
    case KEY_MUTATED:       return "another thread updated entry";
    case KEY_WRITE_ILLEGAL: return "no exclusive lock for write";
    case KEY_NO_VALUE:      return "key has no value attached";
    case KEY_SEG_FULL:      return "no space in allocation segments";
    case KEY_TOO_BIG:       return "key + value + alignment is too big";
    case KEY_SEG_VALUE:     return "value is in segment";
    case KEY_TOMBSTONE:     return "key was dropped";
    case KEY_PART_ONLY:     return "no key attached, hash only";
  }
  return "unknown";
}

void *
kv_key_ctx_big_alloc( void *closure,  size_t item_size ) /* KeyCtx::big_alloc*/
{
  return ::malloc( item_size );
}

void
kv_key_ctx_big_free( void *closure,  void *item )
{
  return ::free( item );
}

kv_key_alloc_t *
kv_create_ctx_alloc( size_t sz,  kv_alloc_func_t ba,  kv_free_func_t bf,
                     void *closure )
{
  size_t off = align<size_t>( sizeof( KeyCtxAlloc ), sizeof( CacheLine ) );
  size_t sz2 = align<size_t>( off + sz, sizeof( CacheLine ) );
  void * ptr = ::aligned_alloc( sizeof( CacheLine ), sz2 );
  if ( ptr == NULL )
    return NULL;
  if ( ba == NULL )
    ba = kv_key_ctx_big_alloc;
  if ( bf == NULL )
    bf = kv_key_ctx_big_free;
  new ( ptr ) KeyCtxAlloc( (CacheLine *) &((uint8_t *) ptr)[ off ],
                           sz2 - off, ba, bf, closure );
  return (kv_key_alloc_t *) ptr;
}

void
kv_release_ctx_alloc( kv_key_alloc_t *ctx_alloc )
{
  delete reinterpret_cast<KeyCtxAlloc *>( ctx_alloc );
}

kv_key_frag_t *
kv_make_key_frag( uint16_t sz,  size_t avail_in,  void *in,  void *out )
{
  size_t used = align<size_t>( (size_t) sz + 2, 2 );
  if ( used > avail_in )
    return NULL;
  *(void **) out = &((uint8_t *) in)[ used ];
  return (kv_key_frag_t *) (void *) in;
}

size_t
kv_get_key_frag_mem_size( kv_key_frag_t *frag )
{
  return align<size_t>( (size_t)
    reinterpret_cast<KeyFragment *>( frag )->keylen + 2, 2 );
}

void
kv_set_key_frag_bytes( kv_key_frag_t *frag,  const void *p,  uint16_t sz )
{
  void *w = reinterpret_cast<KeyFragment *>( frag )->buf;
  reinterpret_cast<KeyFragment *>( frag )->keylen = sz;
  for ( uint16_t i = 0; i < sz; i += 2 )
    ((uint16_t *) w)[ i / 2 ] = ((const uint16_t *) p)[ i / 2 ];
}

void
kv_set_key_frag_string( kv_key_frag_t *frag,  const char *s,  uint16_t slen )
{
  void *w = frag->buf;
  for ( uint16_t i = 0; i < slen; i += 2 )
    ((uint16_t *) w)[ i / 2 ] = ((const uint16_t *) (void *) s)[ i / 2 ];
  ((char *) w)[ slen ] = '\0';
  frag->keylen = slen + 1;
}

uint64_t
kv_hash_key_frag( kv_key_frag_t *frag )
{
  return ((KeyFragment *) frag )->hash();
}

kv_key_ctx_t *
kv_create_key_ctx( kv_hash_tab_t *ht,  uint32_t ctx_id )
{
  void * ptr = ::malloc( sizeof( KeyCtx ) );
  if ( ptr == NULL )
    return NULL;
  new ( ptr ) KeyCtx( reinterpret_cast<HashTab *>( ht ),  ctx_id );
  return (kv_key_ctx_t *) ptr;
}

void
kv_release_key_ctx( kv_key_ctx_t *kctx )
{
  delete reinterpret_cast<KeyCtx *>( kctx );
}

void
kv_set_key( kv_key_ctx_t *kctx,  kv_key_frag_t *kbuf )
{
  reinterpret_cast<KeyCtx *>( kctx )->set_key(
                                  *reinterpret_cast<KeyFragment *>( kbuf ) );
}

void
kv_set_hash( kv_key_ctx_t *kctx,  uint64_t h )
{
  reinterpret_cast<KeyCtx *>( kctx )->set_hash( h );
}

void
kv_prefetch( kv_key_ctx_t *kctx,  uint64_t cnt )
{
  reinterpret_cast<KeyCtx *>( kctx )->prefetch( cnt );
}

kv_key_status_t
kv_acquire( kv_key_ctx_t *kctx )
{
  return reinterpret_cast<KeyCtx *>( kctx )->acquire();
}

kv_key_status_t
kv_try_acquire( kv_key_ctx_t *kctx )
{
  return reinterpret_cast<KeyCtx *>( kctx )->try_acquire();
}

kv_key_status_t
kv_drop( kv_key_ctx_t *kctx )
{
  return reinterpret_cast<KeyCtx *>( kctx )->drop();
}

kv_key_status_t
kv_tombstone( kv_key_ctx_t *kctx )
{
  return reinterpret_cast<KeyCtx *>( kctx )->tombstone();
}

void
kv_release( kv_key_ctx_t *kctx )
{
  return reinterpret_cast<KeyCtx *>( kctx )->release();
}

kv_key_status_t
kv_find( kv_key_ctx_t *kctx,  kv_key_alloc_t *a,  const uint64_t spin_wait )
{
  return reinterpret_cast<KeyCtx *>( kctx )->find(
           reinterpret_cast<KeyCtxAlloc *>( a ), spin_wait );
}

kv_key_status_t
kv_fetch( kv_key_ctx_t *kctx,  kv_key_alloc_t *a,
          const uint64_t pos,  const uint64_t spin_wait )
{
  return reinterpret_cast<KeyCtx *>( kctx )->fetch(
           reinterpret_cast<KeyCtxAlloc *>( a ), pos, spin_wait );
}

kv_key_status_t
kv_value( kv_key_ctx_t *kctx,  void *ptr,  uint64_t *size )
{
  return reinterpret_cast<KeyCtx *>( kctx )->value( ptr, *size );
}

kv_key_status_t
kv_alloc( kv_key_ctx_t *kctx,  void *ptr,  uint64_t size,  uint8_t alignment )
{
  return reinterpret_cast<KeyCtx *>( kctx )->alloc( ptr, size, alignment );
}

kv_key_status_t
kv_load( kv_key_ctx_t *kctx,  kv_msg_ctx_t *mctx )
{
  return reinterpret_cast<KeyCtx *>( kctx )->load( 
           *reinterpret_cast<MsgCtx *>( mctx ) );
}

kv_key_status_t
kv_resize( kv_key_ctx_t *kctx,  void *ptr,  uint64_t size,  uint8_t alignment )
{
  return reinterpret_cast<KeyCtx *>( kctx )->resize( ptr, size, alignment );
}

kv_key_status_t
kv_release_data( kv_key_ctx_t *kctx )
{
  return reinterpret_cast<KeyCtx *>( kctx )->release_data();
}

void
kv_set_update_time( kv_key_ctx_t *kctx,  uint64_t update_time_ns )
{
  reinterpret_cast<KeyCtx *>( kctx )->update_ns = update_time_ns;
}

void
kv_set_expire_time( kv_key_ctx_t *kctx,  uint64_t expire_time_ns )
{
  reinterpret_cast<KeyCtx *>( kctx )->expire_ns = expire_time_ns;
}

kv_key_status_t
kv_get_update_time( kv_key_ctx_t *kctx,  uint64_t *update_time_ns )
{
  return reinterpret_cast<KeyCtx *>( kctx )->get_update_time( *update_time_ns );
}

kv_key_status_t
kv_get_expire_time( kv_key_ctx_t *kctx,  uint64_t *expire_time_ns )
{
  return reinterpret_cast<KeyCtx *>( kctx )->get_expire_time( *expire_time_ns );
}

} /* extern "C" */

