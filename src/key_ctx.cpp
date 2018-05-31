#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stddef.h>

#include <raikv/shm_ht.h>

using namespace rai;
using namespace kv;

/* KeyFragment b is usually null */
KeyCtx::KeyCtx( HashTab &t, uint32_t id, KeyFragment *b )
  : ht( t )
  , kbuf( b )
  , ctx_id( id )
  , hash_entry_size( t.hdr.hash_entry_size )
  , ht_size( t.hdr.ht_size )
  , cuckoo_buckets( t.hdr.cuckoo_buckets )
  , cuckoo_arity( t.hdr.cuckoo_arity )
  , inc( 0 )
  , drop_flags( 0 )
  , flags( KEYCTX_IS_READ_ONLY )
  , max_chains( t.hdr.ht_size )
{
  ::memset( &this->chains, 0,
            (uint8_t *) (void *) &( &this->wrk )[ 1 ] -
              (uint8_t *) (void *) &this->chains );
}

KeyCtx::KeyCtx( HashTab &t, uint32_t id, KeyFragment &b )
  : ht( t )
  , kbuf( &b )
  , ctx_id( id )
  , hash_entry_size( t.hdr.hash_entry_size )
  , ht_size( t.hdr.ht_size )
  , cuckoo_buckets( t.hdr.cuckoo_buckets )
  , cuckoo_arity( t.hdr.cuckoo_arity )
  , inc( 0 )
  , drop_flags( 0 )
  , flags( KEYCTX_IS_READ_ONLY )
  , max_chains( t.hdr.ht_size )
{
  ::memset( &this->chains, 0,
            (uint8_t *) (void *) &( &this->wrk )[ 1 ] -
              (uint8_t *) (void *) &this->chains );
}

void
KeyCtx::set_hash( uint64_t k,  uint64_t k2 )
{
  this->key   = k;
  this->key2  = k2;
  this->start = this->ht.hdr.ht_mod( k );
}

void
KeyCtx::set_key_hash( KeyFragment &b )
{
  uint64_t k  = this->ht.hdr.hash_key_seed,
           k2 = this->ht.hdr.hash_key_seed2;
  this->set_key( b );
  b.hash( k, k2 );
  this->set_hash( k, k2 );
}

KeyCtx *
KeyCtx::new_array( HashTab &t,  uint32_t id,  void *b,  size_t bsz )
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

/* acquire lock for a key, if KEY_OK, set entry at &ht[ key % ht_size ] */
KeyStatus
KeyCtx::acquire( void )
{
  this->init_acquire();
  if ( this->test( KEYCTX_IS_SINGLE_THREAD ) == 0 ) {
    if ( this->cuckoo_buckets <= 1 )
      return this->acquire_linear_probe( this->key, this->start );
    return this->acquire_cuckoo( this->key, this->start );
  }
  /* single thread version */
  if ( this->cuckoo_buckets <= 1 )
    return this->acquire_linear_probe_single_thread( this->key, this->start );
  return this->acquire_cuckoo_single_thread( this->key, this->start );
}

/* try to acquire lock for a key without waiting */
KeyStatus
KeyCtx::try_acquire( void )
{
  this->init_acquire();
  if ( this->test( KEYCTX_IS_SINGLE_THREAD ) == 0 ) {
    if ( this->cuckoo_buckets <= 1 )
      return this->try_acquire_linear_probe( this->key, this->start );
    return this->try_acquire_cuckoo( this->key, this->start );
  }
  /* single thread version */
  if ( this->cuckoo_buckets <= 1 )
    return this->acquire_linear_probe_single_thread( this->key, this->start );
  return this->acquire_cuckoo_single_thread( this->key, this->start );
}

/* if find locates key, returns KEY_OK, sets entry at &ht[ key % ht_size ] */
KeyStatus
KeyCtx::find( const uint64_t spin_wait )
{
  this->init_find();
  if ( this->test( KEYCTX_IS_SINGLE_THREAD ) == 0 ) {
    if ( this->cuckoo_buckets <= 1 )
      return this->find_linear_probe( this->key, this->start, spin_wait );
    return this->find_cuckoo( this->key, this->start, spin_wait );
  }
  /* single thread version */
  if ( this->cuckoo_buckets <= 1 )
    return this->find_linear_probe_single_thread( this->key, this->start );
  return this->find_cuckoo_single_thread( this->key, this->start );
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

bool
KeyCtx::frag_equals( const HashEntry &el ) const
{
  KeyFragment &kb = *this->kbuf;
  if ( el.test( FL_IMMEDIATE_KEY ) )
    return el.key.frag_equals( kb );

  /* 95 bits of hash, 1 billion keys birthday paradox:
     k^2 / 2N = ( 1e9 * 1e9 ) / ( 2 << 95 ) =
     probability of a collision is 1 / 80 billion,
     where all keys are the same length, since keylen is compared
     and large enough to overflow HashEntry(64b) or keylen > 32b
     ... may need to use HashEntry(128b) if 512bit SHA hashes are the keys */
  return ( el.key.keylen == kb.keylen ) &&
         ( kv_crc_c( kb.u.buf, kb.keylen, 0 ) ==
                    ( ( (uint32_t) el.key.u.x.b1 << 16 ) |
                        (uint32_t) el.key.u.x.b2 ) );
}

void *
KeyCtx::copy_data( void *data,  uint64_t sz )
{
  void *p = this->wrk->alloc( sz );
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
  if ( this->test( KEYCTX_IS_READ_ONLY | KEYCTX_IS_SINGLE_THREAD ) != 0 ) {
    if ( this->test( KEYCTX_IS_READ_ONLY ) == 0 )
      this->release_single_thread();
    return;
  }
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
        k        = this->drop_key;
        el.hash2 = this->drop_key2;
        el.flags = this->drop_flags;
      }
      /* mark entry as tombstone */
      else {
        k        = DROPPED_HASH;
        el.flags = FL_DROPPED;
      }
      el.seal_entry( this->hash_entry_size, 0 );
      goto done; /* skip over the seals, they will be tossed */
    }
    ctx.incr_add(); /* counter for added elements */
  }
  /* allow readers to access */
  el.hash2 = this->key2;
  el.set_cuckoo_inc( this->inc );
  el.seal_entry( this->hash_entry_size, this->serial );
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

void
KeyCtx::release_single_thread( void )
{
  if ( this->test( KEYCTX_IS_READ_ONLY ) != 0 )
    return;
  HashEntry & el   = *this->entry;
  ThrCtx    & ctx  = this->ht.ctx[ this->ctx_id ];
  uint64_t    k    = this->key;
  /* if no data was inserted, mark the entry as tombstone */
  if ( this->lock == 0 ) { /* if it's new */
    /* don't keep keys with no data */
    if ( kv_unlikely( el.flags == FL_NO_ENTRY ) ) {
      this->entry = NULL;
      /* was already dropped, use pre-existing key and flags */
      if ( this->drop_key != 0 ) {
        k        = this->drop_key;
        el.hash2 = this->drop_key2;
        el.flags = this->drop_flags;
      }
      /* mark entry as tombstone */
      else {
        k        = DROPPED_HASH;
        el.hash2 = 0;
        el.flags = FL_DROPPED;
      }
      el.seal_entry( this->hash_entry_size, 0 );
      goto done; /* skip over the seals, they will be tossed */
    }
    ctx.incr_add(); /* counter for added elements */
  }
  /* allow readers to access */
  el.hash2 = this->key2;
  el.set_cuckoo_inc( this->inc );
  el.seal_entry( this->hash_entry_size, this->serial );
  if ( el.test( FL_SEGMENT_VALUE ) )
    this->seal_msg();
done:;
  el.hash = k;
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
    if ( ! this->msg->check_seal( this->key, this->key2, this->geom.serial,
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
    if ( ! this->msg->check_seal( this->key, this->key2, this->geom.serial,
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
      if ( ! tmp->check_seal( this->drop_key, this->drop_key2,
                              this->geom.serial, this->geom.size ) ) {
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
        uint32_t check = kv_crc_c( kb.u.buf, kb.keylen, 0 );
        el.key.keylen = kb.keylen;
        el.key.u.x.b1 = (uint16_t) ( check >> 16 );
        el.key.u.x.b2 = (uint16_t) check;
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
      uint32_t check = kv_crc_c( kb.u.buf, kb.keylen, 0 );
      el.key.keylen = kb.keylen;
      el.key.u.x.b1 = (uint16_t) ( check >> 16 );
      el.key.u.x.b2 = (uint16_t) check;
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
  if ( this->test( KEYCTX_IS_READ_ONLY ) )
    return KEY_WRITE_ILLEGAL;
  HashEntry & el = *this->entry;
  /* if something is already allocated, release it */
  if ( el.test( FL_SEGMENT_VALUE ) )
    this->release_data();

  KeyStatus status;
  status = this->update_entry( res, size, alignment );
  this->next_serial( ValueCtr::SERIAL_MASK );

  if ( status == KEY_SEG_VALUE ) {
    /* allocate mem from a segment */
    MsgCtx msg_ctx( this->ht, this->ctx_id, this->hash_entry_size );
    msg_ctx.set_key( *this->kbuf );
    msg_ctx.set_hash( this->key, this->key2 );
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
  if ( this->test( KEYCTX_IS_READ_ONLY ) )
    return KEY_WRITE_ILLEGAL;
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
  if ( this->test( KEYCTX_IS_READ_ONLY ) )
    return KEY_WRITE_ILLEGAL;

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

extern "C" {
const char *
kv_key_status_string( kv_key_status_t status )
{
  switch ( status ) {
    case KEY_OK:            return "KEY_OK";
    case KEY_IS_NEW:        return "KEY_IS_NEW";
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
    case KEY_MAX_CHAINS:    return "KEY_MAX_CHAINS";
    case KEY_PATH_SEARCH:   return "KEY_PATH_SEARCH";
    case KEY_USE_DROP:      return "KEY_USE_DROP";
    case KEY_MAX_STATUS:    return "KEY_MAX_STATUS";
  }
  return "unknown";
}

const char *
kv_key_status_description( kv_key_status_t status )
{
  switch ( status ) {
    case KEY_OK:            return "key ok";
    case KEY_IS_NEW:        return "key did not exist, is newly acquired";
    case KEY_NOT_FOUND:     return "not found";
    case KEY_BUSY:          return "key timeout waiting for lock";
    case KEY_ALLOC_FAILED:  return "allocate key or data failed";
    case KEY_HT_FULL:       return "no more ht entries";
    case KEY_MUTATED:       return "another thread updated entry";
    case KEY_WRITE_ILLEGAL: return "no exclusive lock for write";
    case KEY_NO_VALUE:      return "key has no value attached";
    case KEY_SEG_FULL:      return "no space in allocation segments";
    case KEY_TOO_BIG:       return "key + value + alignment is too big";
    case KEY_SEG_VALUE:     return "value is in segment";
    case KEY_TOMBSTONE:     return "key was dropped";
    case KEY_PART_ONLY:     return "no key attached, hash only";
    case KEY_MAX_CHAINS:    return "nothing found before entry count hit "
                                   "max chains";
    case KEY_PATH_SEARCH:   return "need a path search to acquire cuckoo entry";
    case KEY_USE_DROP:      return "ok to use drop, end of chain";
    case KEY_MAX_STATUS:    return "maximum status";
  }
  return "unknown";
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
  void *w = reinterpret_cast<KeyFragment *>( frag )->u.buf;
  reinterpret_cast<KeyFragment *>( frag )->keylen = sz;
  for ( uint16_t i = 0; i < sz; i += 2 )
    ((uint16_t *) w)[ i / 2 ] = ((const uint16_t *) p)[ i / 2 ];
}

void
kv_set_key_frag_string( kv_key_frag_t *frag,  const char *s,  uint16_t slen )
{
  void *w = frag->u.buf;
  for ( uint16_t i = 0; i < slen; i += 2 )
    ((uint16_t *) w)[ i / 2 ] = ((const uint16_t *) (void *) s)[ i / 2 ];
  ((char *) w)[ slen ] = '\0';
  frag->keylen = slen + 1;
}

void
kv_hash_key_frag( kv_hash_tab_t *ht,  kv_key_frag_t *frag,
                  uint64_t *k,  uint64_t *k2 )
{
  *k  = reinterpret_cast<HashTab *>( ht )->hdr.hash_key_seed;
  *k2 = reinterpret_cast<HashTab *>( ht )->hdr.hash_key_seed2;
  ((KeyFragment *) frag )->hash( *k, *k2 );
}

kv_key_ctx_t *
kv_create_key_ctx( kv_hash_tab_t *ht,  uint32_t ctx_id )
{
  void * ptr = ::malloc( sizeof( KeyCtx ) );
  if ( ptr == NULL )
    return NULL;
  new ( ptr ) KeyCtx( *reinterpret_cast<HashTab *>( ht ),  ctx_id );
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
kv_set_hash( kv_key_ctx_t *kctx,  uint64_t k,  uint64_t k2 )
{
  reinterpret_cast<KeyCtx *>( kctx )->set_hash( k, k2 );
}

void
kv_prefetch( kv_key_ctx_t *kctx,  uint64_t cnt )
{
  reinterpret_cast<KeyCtx *>( kctx )->prefetch( cnt );
}

kv_key_status_t
kv_acquire( kv_key_ctx_t *kctx,  kv_work_alloc_t *a )
{
  return reinterpret_cast<KeyCtx *>( kctx )->acquire(
           reinterpret_cast<ScratchMem *>( a ) );
}

kv_key_status_t
kv_try_acquire( kv_key_ctx_t *kctx,  kv_work_alloc_t *a )
{
  return reinterpret_cast<KeyCtx *>( kctx )->try_acquire(
           reinterpret_cast<ScratchMem *>( a ) );
}
#if 0
kv_key_status_t
kv_drop( kv_key_ctx_t *kctx )
{
  return reinterpret_cast<KeyCtx *>( kctx )->drop();
}
#endif
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
kv_find( kv_key_ctx_t *kctx,  kv_work_alloc_t *a,  const uint64_t spin_wait )
{
  return reinterpret_cast<KeyCtx *>( kctx )->find(
           reinterpret_cast<ScratchMem *>( a ), spin_wait );
}

kv_key_status_t
kv_fetch( kv_key_ctx_t *kctx,  kv_work_alloc_t *a,
          const uint64_t pos,  const uint64_t spin_wait )
{
  return reinterpret_cast<KeyCtx *>( kctx )->fetch(
           reinterpret_cast<ScratchMem *>( a ), pos, spin_wait );
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
