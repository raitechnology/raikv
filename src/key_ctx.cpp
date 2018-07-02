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
  , thr_ctx( t.ctx[ id ] )
  , kbuf( b )
  , ht_size( t.hdr.ht_size )
  , hash_entry_size( t.hdr.hash_entry_size )
  , cuckoo_buckets( t.hdr.cuckoo_buckets )
  , cuckoo_arity( t.hdr.cuckoo_arity )
  , seg_align_shift( t.hdr.seg_align_shift )
  , db_num( t.ctx[ id ].db_num )
  , inc( 0 )
  , drop_flags( 0 )
  , flags( KEYCTX_IS_READ_ONLY )
  , entry( 0 )
  , msg( 0 )
  , max_chains( t.hdr.ht_size )
{
  ::memset( &this->chains, 0,
            (uint8_t *) (void *) &( &this->wrk )[ 1 ] -
              (uint8_t *) (void *) &this->chains );
}

KeyCtx::KeyCtx( HashTab &t, uint32_t id, KeyFragment &b )
  : ht( t )
  , thr_ctx( t.ctx[ id ] )
  , kbuf( &b )
  , ht_size( t.hdr.ht_size )
  , hash_entry_size( t.hdr.hash_entry_size )
  , cuckoo_buckets( t.hdr.cuckoo_buckets )
  , cuckoo_arity( t.hdr.cuckoo_arity )
  , seg_align_shift( t.hdr.seg_align_shift )
  , db_num( t.ctx[ id ].db_num )
  , inc( 0 )
  , drop_flags( 0 )
  , flags( KEYCTX_IS_READ_ONLY )
  , entry( 0 )
  , msg( 0 )
  , max_chains( t.hdr.ht_size )
{
  ::memset( &this->chains, 0,
            (uint8_t *) (void *) &( &this->wrk )[ 1 ] -
              (uint8_t *) (void *) &this->chains );
}

void
KeyCtx::switch_db( uint8_t db_num )
{
  this->ht.retire_ht_thr_stats( this->thr_ctx.ctx_id );
  this->db_num = db_num;
  this->thr_ctx.db_num = db_num; /* must account for other KeyCtx in app */
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
  uint64_t k, k2;
  this->ht.hdr.get_hash_seed( this->db_num, k, k2 );
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
    ThrCtx  & ctx = this->thr_ctx;
    KeyStatus status;
    if ( (status = this->release_data()) != KEY_OK )
      return status;
    this->serial = 0;
    this->entry->set( FL_DROPPED );
    this->entry->clear( FL_EXPIRE_STAMP | FL_UPDATE_STAMP );
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
  if ( this->entry->test( FL_IMMEDIATE_KEY ) ) {
    b = &this->entry->key;
    return KEY_OK;
  }
  if ( this->entry->test( FL_SEGMENT_VALUE ) ) {
    if ( this->msg == NULL &&
         (mstatus = this->attach_msg( ATTACH_READ )) != KEY_OK )
      return mstatus;
    /* copy key */
    if ( this->test( KEYCTX_NO_COPY_ON_READ ) ) {
      uint16_t keylen = this->msg->key.keylen;
      if ( this->is_msg_valid() ) {
        b = (KeyFragment *) this->wrk->alloc( sizeof( KeyFragment ) + keylen );
        b->keylen = keylen;
        ::memcpy( b->u.buf, this->msg->key.u.buf, keylen );
        if ( this->is_msg_valid() )
          return KEY_OK;
      }
      return KEY_MUTATED;
    }
    /* key already copied */
    else {
      b = &this->msg->key;
    }
    return KEY_OK;
  }
  b = NULL;
  if ( this->entry->test( FL_PART_KEY ) )
    return KEY_PART_ONLY;
  return KEY_NOT_FOUND;
}

void
KeyCtx::prefetch( uint64_t cnt ) const
{
  /* I believe rw is ignored, locality may have moderate effects, but it is
   * hard to measure accurately */
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
  ThrCtx    & ctx  = this->thr_ctx;
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
      el.seal_entry( this->hash_entry_size, 0, 0 );
      goto done; /* skip over the seals, they will be tossed */
    }
    ctx.incr_add(); /* counter for added elements */
  }
  /* allow readers to access */
  el.hash2 = this->key2;
  el.set_cuckoo_inc( this->inc );
  el.seal_entry( this->hash_entry_size, this->serial, this->db_num );
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
}

void
KeyCtx::release_single_thread( void )
{
  if ( this->test( KEYCTX_IS_READ_ONLY ) != 0 )
    return;
  HashEntry & el   = *this->entry;
  ThrCtx    & ctx  = this->thr_ctx;
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
      el.seal_entry( this->hash_entry_size, 0, 0 );
      goto done; /* skip over the seals, they will be tossed */
    }
    ctx.incr_add(); /* counter for added elements */
  }
  /* allow readers to access */
  el.hash2 = this->key2;
  el.set_cuckoo_inc( this->inc );
  el.seal_entry( this->hash_entry_size, this->serial, this->db_num );
  if ( el.test( FL_SEGMENT_VALUE ) )
    this->seal_msg();
done:;
  el.hash = k;
  this->entry     = NULL;
  this->msg       = NULL;
  this->drop_key  = 0;
  this->set( KEYCTX_IS_READ_ONLY );
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
                                 this->seg_align_shift );
    if ( this->test( KEYCTX_NO_COPY_ON_READ ) ) {
      p = this->ht.seg_data( this->geom.segment, this->geom.offset );
      this->msg = (MsgHdr *) p;
    }
    else {
      /* copy msg data into buffer */
      if ( (p = this->copy_data( 
              this->ht.seg_data( this->geom.segment, this->geom.offset ),
              this->geom.size )) == NULL )
        return KEY_ALLOC_FAILED;
      /* check that msg is valid */
      this->msg = (MsgHdr *) p;
      if ( ! this->is_msg_valid() ) {
        this->msg = NULL;
        return KEY_MUTATED;
      }
    }
  }
  else {
    /* locked, exclusive access to message */
    this->entry->get_value_geom( this->hash_entry_size, this->geom,
                                 this->seg_align_shift );
    this->msg = (MsgHdr *) this->ht.seg_data( this->geom.segment,
                                              this->geom.offset );
    if ( ! this->is_msg_valid() ) {
      this->msg = NULL;
      return KEY_MUTATED;
    }
    /* this clears the seal, locking out getters */
    this->msg->unseal();
  }
  return KEY_OK;
}

KeyStatus
KeyCtx::get_msg_size( uint64_t &sz )
{
  KeyStatus mstatus;
  if ( this->msg == NULL &&
       (mstatus = this->attach_msg( ATTACH_READ )) != KEY_OK )
    return mstatus;
  if ( this->test( KEYCTX_IS_READ_ONLY ) ) {
    /* attach msg, no seal check */
    sz = this->msg->msg_size;
    if ( this->test( KEYCTX_NO_COPY_ON_READ ) && ! this->is_msg_valid() )
      return KEY_MUTATED;
  }
  /* attach and unseal msg */
  else {
    sz = this->msg->msg_size;
  }
  return KEY_OK;
}

bool
KeyCtx::is_msg_valid( void )
{
  return this->msg->check_seal( this->key, this->key2, this->geom.serial,
                                this->geom.size );
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
  ThrCtx    & ctx = this->thr_ctx;
  switch ( this->drop_flags & ( FL_SEGMENT_VALUE | FL_IMMEDIATE_VALUE ) ) {
    case FL_SEGMENT_VALUE: {
      MsgHdr * tmp;
      el.get_value_geom( this->hash_entry_size, this->geom,
                         this->seg_align_shift );
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
  if ( this->entry->test( FL_EXPIRE_STAMP | FL_UPDATE_STAMP ) ) {
    RelativeStamp & rs = this->entry->rela_stamp( this->hash_entry_size );
    this->msg->rela_stamp().u.stamp = rs.u.stamp;
  }
  ValueCtr &ctr = this->entry->value_ctr( this->hash_entry_size );
  this->msg->seal( this->serial, ctr.db, ctr.type, this->entry->flags );
}

KeyStatus
KeyCtx::update_entry( void *res,  uint64_t size,  uint8_t alignment,
                      HashEntry &el )
{
  KeyFragment  & kb       = *this->kbuf;
  const uint32_t hdr_size = HashEntry::hdr_size( kb );
  uint8_t      * hdr_end  = &((uint8_t *) (void *) &el)[ hdr_size ];
  ValueCtr     & ctr      = el.value_ctr( this->hash_entry_size );
  uint8_t      * trail    = (uint8_t *) (void *) &ctr;

  if ( el.test( FL_EXPIRE_STAMP | FL_UPDATE_STAMP ) )
    trail -= sizeof( RelativeStamp );

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
    ctr.size = 0;
    return KEY_SEG_VALUE;
  }
  /* key doesn't fit and no segment data, check if value fits */
  hdr_end = &((uint8_t *) (void *) &el)[ sizeof( HashEntry ) ];
  if ( &hdr_end[ size ] > trail ) {
    this->thr_ctx.incr_afail();
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

/* return a contiguous memory space for data attached to the Key ht[] entry */
KeyStatus
KeyCtx::alloc( void *res,  uint64_t size,  bool copy,  uint8_t alignment )
{
  if ( this->test( KEYCTX_IS_READ_ONLY ) )
    return KEY_WRITE_ILLEGAL;
  HashEntry & el = *this->entry;
  CopyData cp, *cpp = NULL;
  KeyStatus status;

  /* if something is already allocated, copy or release it */
  if ( el.test( FL_SEGMENT_VALUE ) ) {
    if ( ! copy )
      this->release_data();
    else {
      KeyStatus mstatus;
      if ( this->msg == NULL &&
           ( (mstatus = this->attach_msg( ATTACH_WRITE )) != KEY_OK ) )
        return mstatus;
      cpp     = &cp;
      cp.data = NULL;
      cp.size = 0;
      cp.msg  = this->msg;
      cp.geom = this->geom;
      this->msg = NULL;
      el.clear( FL_SEGMENT_VALUE );
    }
  }
  else if ( copy ) {
    cp.msg = NULL;
    if ( el.test( FL_IMMEDIATE_VALUE | FL_DROPPED ) == FL_IMMEDIATE_VALUE ) {
      ValueCtr & ctr = el.value_ctr( this->hash_entry_size );
      cpp     = &cp;
      cp.data = this->copy_data( el.immediate_value(), ctr.size );
      cp.size = ctr.size;
      if ( cp.data == NULL )
        return KEY_ALLOC_FAILED;
    }
    else {
      cp.data = NULL;
      cp.size = 0;
    }
  }
  status = this->update_entry( res, size, alignment, el );

  if ( status == KEY_SEG_VALUE ) {
    /* allocate mem from a segment */
    MsgCtx msg_ctx( this->ht, this->thr_ctx.ctx_id, this->hash_entry_size );
    msg_ctx.set_key( *this->kbuf );
    msg_ctx.set_hash( this->key, this->key2 );
    if ( (status = msg_ctx.alloc_segment( res, size, alignment )) == KEY_OK ) {
      el.set( FL_SEGMENT_VALUE );
      msg_ctx.geom.serial = this->serial;
      this->geom = msg_ctx.geom;
      el.set_value_geom( this->hash_entry_size, msg_ctx.geom,
                         this->seg_align_shift );
      el.value_ctr( this->hash_entry_size ).size = 1;
      this->msg = msg_ctx.msg;
    }
    else if ( status == KEY_ALLOC_FAILED ) {
      this->thr_ctx.incr_afail();
    }
  }
  if ( cpp != NULL ) {
    if ( status == KEY_OK ) {
      if ( cp.msg != NULL ) {
        cp.data = cp.msg->ptr( cp.msg->hdr_size() );
        cp.size = cp.msg->msg_size;
      }
      if ( cp.data != NULL ) {
        if ( cp.size > size )
          cp.size = size;
        if ( cp.size > 0 )
          ::memcpy( *(void **) res, cp.data, cp.size );
      }
    }
    if ( cp.msg != NULL ) {
      /* release segment data */
      Segment &seg = this->ht.segment( cp.geom.segment );
      cp.msg->release();
      /* clear hash entry geometry */
      seg.msg_count  -= 1;
      seg.avail_size += cp.geom.size;
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

  this->update_entry( NULL, 0, 0, el );

  el.set( FL_SEGMENT_VALUE );
  msg_ctx.geom.serial = this->serial;
  el.set_value_geom( this->hash_entry_size, msg_ctx.geom,
                     this->seg_align_shift );
  el.value_ctr( this->hash_entry_size ).size = 1;
  this->msg = msg_ctx.msg;

  return KEY_OK;
}

KeyStatus
KeyCtx::resize( void *res,  uint64_t size,  bool copy,  uint8_t alignment )
{
  if ( this->test( KEYCTX_IS_READ_ONLY ) )
    return KEY_WRITE_ILLEGAL;

  HashEntry & el = *this->entry;

  switch ( el.test( FL_SEGMENT_VALUE | FL_IMMEDIATE_VALUE ) ) {
    case FL_IMMEDIATE_VALUE: {
      uint8_t  * value = el.immediate_value(),
               * trail = (uint8_t *) el.ptr( this->hash_entry_size );
      trail -= sizeof( ValueCtr );
      if ( el.test( FL_EXPIRE_STAMP | FL_UPDATE_STAMP ) )
        trail -= sizeof( RelativeStamp );
      if ( &value[ size ] <= trail ) {
        el.value_ctr( this->hash_entry_size ).size = size;
        this->next_serial( ValueCtr::SERIAL_MASK );
        *(void **) res = (void *) value;
        return KEY_OK;
      }
      break;
    }
    case FL_SEGMENT_VALUE: {
      /* check if resize fits within current segment mem */
      KeyStatus mstatus;
      if ( this->msg == NULL &&
           (mstatus = this->attach_msg( ATTACH_WRITE )) != KEY_OK )
        return mstatus;

      uint32_t hdr_size   = MsgHdr::hdr_size( this->msg->key );
      uint64_t alloc_size = MsgHdr::alloc_size( hdr_size, size,
                                                this->seg_align() );
      if ( alloc_size == this->msg->size ) {
        this->next_serial( ValueCtr::SERIAL_MASK );
        this->geom.serial = el.value_ptr( this->hash_entry_size ).
                               set_serial( this->serial );
        this->msg->msg_size = size;
        *(void **) res = this->msg->ptr( hdr_size );
        return KEY_OK;
      }
      break;
    }
    default:
      break;
  }
  /* reallocate, could do better if key already copied */
  this->next_serial( ValueCtr::SERIAL_MASK );
  KeyStatus status = this->alloc( res, size, copy, alignment );
  return status;
}

/* get the value associated with the key */
KeyStatus
KeyCtx::value( void *data,  uint64_t &size )
{
  if ( this->entry == NULL )
    return KEY_NO_VALUE;

  HashEntry & el = *this->entry;

  switch ( el.test( FL_SEGMENT_VALUE | FL_IMMEDIATE_VALUE ) ) {
    case FL_IMMEDIATE_VALUE: {
      /* size stashed at end */
      size = el.value_ctr( this->hash_entry_size ).size;
      /* data after hdr in hash entry */
      *(void **) data = (void *) el.immediate_value();
      return KEY_OK;
    }
    case FL_SEGMENT_VALUE: {
      KeyStatus mstatus;
      if ( this->msg == NULL &&
           ( (mstatus = this->attach_msg( ATTACH_READ )) != KEY_OK ) )
        return mstatus;
      uint64_t hdr_size = this->msg->hdr_size();
      size = this->msg->msg_size;
      if ( this->test( KEYCTX_NO_COPY_ON_READ ) ) {
        if ( ! this->is_msg_valid() ) /* check that hdr and size are correct */
          return KEY_MUTATED;
      }
      /* data starts after hdr */
      *(void **) data = this->msg->ptr( hdr_size );
      return KEY_OK;
    }
    default:
      return KEY_NO_VALUE;
  }
}

KeyStatus
KeyCtx::update_stamps( uint64_t exp_ns,  uint64_t upd_ns )
{
  HashEntry     & el   = *this->entry;
  RelativeStamp & rela = el.rela_stamp( this->hash_entry_size );

  if ( ( exp_ns | upd_ns ) == 0 )
    return KEY_OK;
  /* make room for rela stamp by moving value ptr up */
  if ( el.test( FL_EXPIRE_STAMP | FL_UPDATE_STAMP ) == 0 ) {
    uint32_t tl = this->hash_entry_size - sizeof( ValueCtr ),
             hd = el.hdr_size2();
    if ( el.test( FL_IMMEDIATE_VALUE ) )
      hd += el.value_ctr( this->hash_entry_size ).size;
    if ( el.test( FL_SEGMENT_VALUE ) )
      tl -= sizeof( ValuePtr );
    if ( hd + sizeof( RelativeStamp ) <= tl ) {
      if ( el.test( FL_SEGMENT_VALUE ) ) {
        ValuePtr * cur_ptr = (ValuePtr *) el.ptr( tl );
        ValuePtr * new_ptr = (ValuePtr *) el.ptr( tl - sizeof( RelativeStamp ));
        ::memmove( new_ptr, cur_ptr, sizeof( ValuePtr ) );
      }
    }
    else { /* doesn't fit */
      void *tmp = NULL;
      ValueCtr &ctr = el.value_ctr( this->hash_entry_size );
      KeyFragment *kb = this->kbuf;
      uint16_t has_seg;
      /* copy in case of null kbuf */
      bool is_tmp_kb  = false;
      if ( kb == NULL && el.test( FL_IMMEDIATE_KEY ) ) {
        kb = (KeyFragment *) this->copy_data( &el.key, el.key.keylen + 2 );
        if ( kb == NULL )
          return KEY_ALLOC_FAILED;
        this->kbuf = kb;
        is_tmp_kb = true;
      }
      /* copy seg, it may be erased */
      if ( (has_seg = el.test( FL_SEGMENT_VALUE )) != 0 )
        el.get_value_geom( this->hash_entry_size, this->geom,
                           this->seg_align_shift );
      /* make room */
      if ( exp_ns != 0 ) el.set( FL_EXPIRE_STAMP );
      if ( upd_ns != 0 ) el.set( FL_UPDATE_STAMP );
      KeyStatus status = this->alloc( &tmp, ctr.size, true, 8 );
      if ( is_tmp_kb )
        this->kbuf = NULL;
      if ( status != KEY_OK )
        return status;
      if ( has_seg != 0 )
        el.set_value_geom( this->hash_entry_size, this->geom,
                           this->seg_align_shift );
    }
  }
  /* insert stamps that are not zero */
  if ( exp_ns != 0 || el.test( FL_EXPIRE_STAMP ) ) {
    if ( upd_ns != 0 || el.test( FL_UPDATE_STAMP ) ) {
      /* get the current stamps */
      if ( exp_ns == 0 || upd_ns == 0 ) {
        uint64_t old_exp_ns, old_upd_ns;
        this->get_stamps( old_exp_ns, old_upd_ns );
        if ( exp_ns == 0 ) exp_ns = old_exp_ns;
        if ( upd_ns == 0 ) upd_ns = old_upd_ns;
      }
      /* two stamps now exist */
      rela.set( this->ht.hdr.create_stamp, this->ht.hdr.current_stamp,
                exp_ns, upd_ns );
      el.set( FL_EXPIRE_STAMP | FL_UPDATE_STAMP );
    }
    /* only expire exists */
    else {
      rela.u.stamp = exp_ns;
      el.set( FL_EXPIRE_STAMP );
      el.clear( FL_UPDATE_STAMP );
    }
  }
  /* only update exists */
  else {
    rela.u.stamp = upd_ns;
    el.set( FL_UPDATE_STAMP );
    el.clear( FL_EXPIRE_STAMP );
  }
  return KEY_OK;
}

KeyStatus
KeyCtx::clear_stamps( bool clr_exp,  bool clr_upd )
{
  HashEntry     & el   = *this->entry;
  RelativeStamp & rela = el.rela_stamp( this->hash_entry_size );

  switch ( el.test( FL_EXPIRE_STAMP | FL_UPDATE_STAMP ) ) {
    case FL_EXPIRE_STAMP:
      if ( ! clr_exp ) /* if update doesn't exist and expire is not cleared */
        return KEY_OK;
      break; /* clear all stamps */
    case FL_UPDATE_STAMP:
      if ( ! clr_upd ) /* if expire doesn't exist and update is not cleared */
        return KEY_OK;
      break; /* clear all stamps */
    case FL_UPDATE_STAMP | FL_EXPIRE_STAMP: {
      uint64_t exp_ns, upd_ns;
      if ( clr_exp && clr_upd )
        break; /* clear all stamps */
      rela.get( this->ht.hdr.create_stamp, this->ht.hdr.current_stamp,
                exp_ns, upd_ns );
      /* only remove one stamp */
      if ( clr_exp ) {
        rela.u.stamp = upd_ns; /* update still exists */
        el.clear( FL_EXPIRE_STAMP );
        return KEY_OK;
      }
      rela.u.stamp = exp_ns; /* expire still exists */
      el.clear( FL_UPDATE_STAMP );
      return KEY_OK;
    }
    default:
      return KEY_OK;
  }
  /* move value ptr down */
  if ( el.test( FL_SEGMENT_VALUE ) ) {
    uint32_t tl = this->hash_entry_size - sizeof( ValueCtr );
    ValuePtr * cur_ptr = (ValuePtr *) el.ptr( tl - sizeof( RelativeStamp ) );
    ValuePtr * new_ptr = (ValuePtr *) el.ptr( tl );
    ::memmove( new_ptr, cur_ptr, sizeof( ValuePtr ) );
  }
  /* both deleted */
  el.clear( FL_EXPIRE_STAMP | FL_UPDATE_STAMP );
  return KEY_OK;
}

KeyStatus
KeyCtx::get_stamps( uint64_t &exp_ns,  uint64_t &upd_ns )
{
  HashEntry     & el   = *this->entry;
  RelativeStamp & rela = el.rela_stamp( this->hash_entry_size );

  switch ( el.test( FL_EXPIRE_STAMP | FL_UPDATE_STAMP ) ) {
    case FL_EXPIRE_STAMP:
      exp_ns = rela.u.stamp;
      upd_ns = 0;
      break;
    case FL_UPDATE_STAMP:
      exp_ns = 0;
      upd_ns = rela.u.stamp;
      break;
    case FL_UPDATE_STAMP | FL_EXPIRE_STAMP: {
      rela.get( this->ht.hdr.create_stamp, this->ht.hdr.current_stamp,
                exp_ns, upd_ns );
      break;
    }
    default:
      exp_ns = 0;
      upd_ns = 0;
      break;
  }
  return KEY_OK;
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
  HashTab *map = reinterpret_cast<HashTab *>( ht );
  map->hdr.get_hash_seed( 0, *k, *k2 );
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
#if 0
void
kv_set_update_time( kv_key_ctx_t *kctx,  uint64_t update_time_ns )
{
}

void
kv_set_expire_time( kv_key_ctx_t *kctx,  uint64_t expire_time_ns )
{
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
#endif
} /* extern "C" */
