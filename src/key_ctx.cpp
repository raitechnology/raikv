#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stddef.h>

#include <raikv/shm_ht.h>

using namespace rai;
using namespace kv;

/* KeyFragment b is usually null */
KeyCtx::KeyCtx( HashTab &t, ThrCtx &ctx, KeyFragment *b )
  : ht( t )
  , thr_ctx( ctx )
  , kbuf( b )
  , ht_size( t.hdr.ht_size )
  , hash_entry_size( t.hdr.hash_entry_size )
  , cuckoo_buckets( t.hdr.cuckoo_buckets )
  , cuckoo_arity( t.hdr.cuckoo_arity )
  , seg_align_shift( t.hdr.seg_align_shift )
  , db_num( ctx.db_num1 )
  , inc( 0 )
  , msg_chain_size( 0 )
  , drop_flags( 0 )
  , flags( KEYCTX_IS_READ_ONLY )
  , entry( 0 )
  , msg( 0 )
  , stat( ctx.stat1 )
  , max_chains( t.hdr.ht_size )
{
  ::memset( &this->chains, 0,
            (uint8_t *) (void *) &( &this->wrk )[ 1 ] -
              (uint8_t *) (void *) &this->chains );
}

KeyCtx::KeyCtx( HashTab &t, ThrCtx &ctx, HashCounters &dbstat,  uint8_t dbn,
                KeyFragment *b )
  : ht( t )
  , thr_ctx( ctx )
  , kbuf( b )
  , ht_size( t.hdr.ht_size )
  , hash_entry_size( t.hdr.hash_entry_size )
  , cuckoo_buckets( t.hdr.cuckoo_buckets )
  , cuckoo_arity( t.hdr.cuckoo_arity )
  , seg_align_shift( t.hdr.seg_align_shift )
  , db_num( dbn )
  , inc( 0 )
  , msg_chain_size( 0 )
  , drop_flags( 0 )
  , flags( KEYCTX_IS_READ_ONLY )
  , entry( 0 )
  , msg( 0 )
  , stat( dbstat )
  , max_chains( t.hdr.ht_size )
{
  ::memset( &this->chains, 0,
            (uint8_t *) (void *) &( &this->wrk )[ 1 ] -
              (uint8_t *) (void *) &this->chains );
}

KeyCtx::KeyCtx( HashTab &t, uint32_t id, KeyFragment *b )
  : ht( t )
  , thr_ctx( t.ctx[ id ] )
  , kbuf( b )
  , ht_size( t.hdr.ht_size )
  , hash_entry_size( t.hdr.hash_entry_size )
  , cuckoo_buckets( t.hdr.cuckoo_buckets )
  , cuckoo_arity( t.hdr.cuckoo_arity )
  , seg_align_shift( t.hdr.seg_align_shift )
  , db_num( t.ctx[ id ].db_num1 )
  , inc( 0 )
  , msg_chain_size( 0 )
  , drop_flags( 0 )
  , flags( KEYCTX_IS_READ_ONLY )
  , entry( 0 )
  , msg( 0 )
  , stat( t.ctx[ id ].stat1 )
  , max_chains( t.hdr.ht_size )
{
  ::memset( &this->chains, 0,
            (uint8_t *) (void *) &( &this->wrk )[ 1 ] -
              (uint8_t *) (void *) &this->chains );
}

KeyCtx::KeyCtx( KeyCtx &kctx )
  : ht( kctx.ht )
  , thr_ctx( kctx.thr_ctx )
  , kbuf( 0 )
  , ht_size( kctx.ht_size )
  , hash_entry_size( kctx.hash_entry_size )
  , cuckoo_buckets( kctx.cuckoo_buckets )
  , cuckoo_arity( kctx.cuckoo_arity )
  , seg_align_shift( kctx.seg_align_shift )
  , db_num( kctx.db_num )
  , inc( 0 )
  , msg_chain_size( 0 )
  , drop_flags( 0 )
  , flags( KEYCTX_IS_READ_ONLY )
  , entry( 0 )
  , msg( 0 )
  , stat( kctx.stat )
  , max_chains( kctx.ht_size )
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
  this->thr_ctx.db_num1 = db_num; /* must account for other KeyCtx in app */
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
    KeyStatus status;
    if ( (status = this->release_data()) != KEY_OK )
      return status;
    this->serial = 0;
    this->entry->set( FL_DROPPED );
    this->entry->clear( FL_EXPIRE_STAMP | FL_UPDATE_STAMP |
                        FL_SEQNO | FL_MSG_LIST );
    this->incr_drop();
  }
  return KEY_OK;
}

bool
KeyCtx::frag_equals( const HashEntry &el ) const
{
  KeyFragment &kb = *this->kbuf;
  if ( el.test( FL_IMMEDIATE_KEY ) )
    return el.key.frag_equals( kb );
  return el.key.keylen == kb.keylen;
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
    this->incr_add(); /* counter for added elements */
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
  this->incr_spins( spin );
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
    this->incr_add(); /* counter for added elements */
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

MsgHdr *
KeyCtx::get_chain_msg( ValueGeom &cgeom )
{
  MsgHdr * cmsg;
  void   * p;
  uint8_t  tmp_size;
  /* copy msg data into buffer */
  p = this->ht.seg_data( cgeom.segment, cgeom.offset );
  if ( ! this->ht.is_valid_region( p, cgeom.size ) ||
       (p = this->copy_data( p, cgeom.size )) == NULL )
    return NULL;
  /* check that msg is valid */
  cmsg = (MsgHdr *) p;
  if ( cmsg->check_seal( this->key, this->key2, cgeom.serial,
                         cgeom.size, tmp_size ) )
    return cmsg;
  return NULL;
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
                                this->geom.size, this->msg_chain_size );
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
      if ( this->msg_chain_size != 0 ) {
        for ( uint8_t i = 0; i < this->msg_chain_size; i++ ) {
          ValueGeom mchain;
          this->msg->get_next( i, mchain, this->seg_align_shift );
          if ( mchain.size != 0 ) {
            void * p = this->ht.seg_data( mchain.segment, mchain.offset );
            if ( this->ht.is_valid_region( p, mchain.size ) ) {
              MsgHdr * tmp = (MsgHdr *) p;
              uint8_t tmp_size;
              if ( tmp->check_seal( this->key, this->key2, mchain.serial,
                                     mchain.size, tmp_size ) ) {
                Segment &seg = this->ht.segment( mchain.segment );
                tmp->release();
                seg.msg_count -= 1;
                seg.avail_size += mchain.size;
              }
            }
          }
        }
      }
      /* release segment data */
      Segment &seg = this->ht.segment( this->geom.segment );
      this->msg->release();
      this->msg = NULL;
      this->msg_chain_size = 0;
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
#if 0
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
                              this->geom.serial, this->geom.size,
                              this->msg_chain_size ) ) {
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
  this->incr_drop();
  this->incr_htevict();
  this->clear( KEYCTX_IS_HT_EVICT );
  return KEY_OK;
}
#endif
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
  this->msg->seal( this->serial, ctr.db, ctr.type, this->entry->flags,
                   this->msg_chain_size );
}

KeyStatus
KeyCtx::update_entry( void *res,  uint64_t size,  HashEntry &el )
{
  KeyFragment  & kb       = *this->kbuf;
  const uint32_t hdr_size = HashEntry::hdr_size( kb );
  uint8_t      * hdr_end  = &((uint8_t *) (void *) &el)[ hdr_size ];
  ValueCtr     & ctr      = el.value_ctr( this->hash_entry_size );
  uint8_t      * trail    = (uint8_t *) (void *) &ctr;

  if ( el.test( FL_EXPIRE_STAMP | FL_UPDATE_STAMP | FL_SEQNO ) ) {
    if ( el.test( FL_SEQNO ) )
      trail -= sizeof( uint64_t );
    if ( el.test( FL_EXPIRE_STAMP | FL_UPDATE_STAMP  ) )
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
      if ( el.test( FL_PART_KEY ) == 0 )
        el.key.keylen = kb.keylen;
      el.clear( FL_IMMEDIATE_VALUE | FL_IMMEDIATE_KEY | FL_DROPPED );
      el.set( FL_PART_KEY | FL_UPDATED );
    }
    ctr.size = 0;
    return KEY_SEG_VALUE;
  }
  /* key doesn't fit and no segment data, check if value fits */
  hdr_end = &((uint8_t *) (void *) &el)[ sizeof( HashEntry ) ];
  if ( &hdr_end[ size ] > trail ) {
    this->incr_afail();
    return KEY_ALLOC_FAILED;
  }
  /* only part of the key fits */
  if ( el.test( FL_PART_KEY ) == 0 )
    el.key.keylen = kb.keylen;
  el.clear( FL_IMMEDIATE_KEY | FL_DROPPED );
  el.set( FL_PART_KEY | FL_IMMEDIATE_VALUE | FL_UPDATED );
  ctr.size = size;
  *(void **) res = (void *) hdr_end;
  return KEY_OK;
}

/* return a contiguous memory space for data attached to the Key ht[] entry */
KeyStatus
KeyCtx::alloc( void *res,  uint64_t size,  bool copy )
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
  status = this->update_entry( res, size, el );

  if ( status == KEY_SEG_VALUE ) {
    /* allocate mem from a segment */
    MsgCtx msg_ctx( *this );
    msg_ctx.set_key( *this->kbuf );
    msg_ctx.set_hash( this->key, this->key2 );
    if ( (status = msg_ctx.alloc_segment( res, size,
                                          this->msg_chain_size )) == KEY_OK ) {
      el.set( FL_SEGMENT_VALUE );
      msg_ctx.geom.serial = this->serial;
      this->geom = msg_ctx.geom;
      el.set_value_geom( this->hash_entry_size, msg_ctx.geom,
                         this->seg_align_shift );
      if ( this->msg_chain_size > 0 && cpp != NULL && cp.msg != NULL ) {
        this->msg_chain_size = MsgCtx::copy_chain( cp.msg, msg_ctx.msg, 0, 0,
                                                   this->msg_chain_size,
                                                   this->seg_align_shift );
      }
      else {
        this->msg_chain_size = 0;
      }
      el.value_ctr( this->hash_entry_size ).size = 0;
      this->msg = msg_ctx.msg;
    }
    else if ( status == KEY_ALLOC_FAILED ) {
      this->incr_afail();
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

  this->update_entry( NULL, 0, el );

  el.set( FL_SEGMENT_VALUE );
  msg_ctx.geom.serial = this->serial;
  el.set_value_geom( this->hash_entry_size, msg_ctx.geom,
                     this->seg_align_shift );
  el.value_ctr( this->hash_entry_size ).size = 0;
  this->msg = msg_ctx.msg;
  this->msg_chain_size = 0;

  return KEY_OK;
}

KeyStatus
KeyCtx::add_msg_chain( MsgCtx &msg_ctx )
{
  MsgChain next;
  if ( this->test( KEYCTX_IS_READ_ONLY ) )
    return KEY_WRITE_ILLEGAL;

  HashEntry & el = *this->entry;
  if ( el.test( FL_SEGMENT_VALUE ) ) {
    KeyStatus mstatus;
    if ( this->msg == NULL &&
         ( (mstatus = this->attach_msg( ATTACH_WRITE )) != KEY_OK ) )
      return mstatus;

    next.geom = this->geom;
    next.msg  = this->msg;
    el.set_cuckoo_inc( this->inc );
    this->seal_msg();
    this->geom.zero();
    this->msg = NULL;
    el.clear( FL_SEGMENT_VALUE );
    el.value_ptr( this->hash_entry_size ).zero();
    el.value_ctr( this->hash_entry_size ).size = 0;
  }
  this->update_entry( NULL, 0, el );

  el.set( FL_SEGMENT_VALUE );
  msg_ctx.geom.serial = this->serial;
  el.set_value_geom( this->hash_entry_size, msg_ctx.geom,
                     this->seg_align_shift );
  el.value_ctr( this->hash_entry_size ).size = 0;
  if ( next.msg != NULL )
    this->msg_chain_size = msg_ctx.add_chain( next );
  this->msg = msg_ctx.msg;

  return KEY_OK;
}

KeyStatus
KeyCtx::resize( void *res,  uint64_t size,  bool copy )
{
  if ( this->test( KEYCTX_IS_READ_ONLY ) )
    return KEY_WRITE_ILLEGAL;

  HashEntry & el = *this->entry;

  switch ( el.test( FL_SEGMENT_VALUE | FL_IMMEDIATE_VALUE ) ) {
    case FL_IMMEDIATE_VALUE: {
      uint8_t  * value = el.immediate_value(),
               * trail = (uint8_t *) el.trail_ptr( this->hash_entry_size );
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
  KeyStatus status = this->alloc( res, size, copy );
  return status;
}

/* get the value associated with the key */
KeyStatus
KeyCtx::value( void *data,  uint64_t &size )
{
  if ( this->entry == NULL )
    return KEY_NO_VALUE;

  HashEntry & el = *this->entry;
  KeyStatus   mstatus;
  uint8_t   * msg;

  switch ( el.test( FL_SEGMENT_VALUE | FL_IMMEDIATE_VALUE ) ) {
    case FL_IMMEDIATE_VALUE: {
      /* size stashed at end */
      size = el.value_ctr( this->hash_entry_size ).size;
      /* data after hdr in hash entry */
      msg = el.immediate_value();
      break;
    }
    case FL_SEGMENT_VALUE: {
      if ( this->msg == NULL &&
           ( (mstatus = this->attach_msg( ATTACH_READ )) != KEY_OK ) )
        return mstatus;
      /* fetch the sizes before testing is_msg_valid() */
      uint64_t hdr_size = this->msg->hdr_size();
      size = this->msg->msg_size;
      /* data starts after hdr */
      msg  = (uint8_t *) this->msg->ptr( hdr_size );
      /* no need to test is_valid with write access or msg is copied */
      if ( this->test( KEYCTX_NO_COPY_ON_READ | KEYCTX_IS_READ_ONLY ) ==
                     ( KEYCTX_NO_COPY_ON_READ | KEYCTX_IS_READ_ONLY ) ) {
        if ( ! this->is_msg_valid() ) /* check that hdr and size are correct */
          return KEY_MUTATED;
      }
      break;
    }
    default:
      return KEY_NO_VALUE;
  }
  *(void **) data = (void *) msg;
  return KEY_OK;
}

/* get the value associated with the key */
KeyStatus
KeyCtx::value_copy( void *data,  uint64_t &size,  void *cp,  uint64_t &cplen )
{
  if ( this->entry == NULL )
    return KEY_NO_VALUE;

  HashEntry & el = *this->entry;
  void      * p;

  switch ( el.test( FL_SEGMENT_VALUE | FL_IMMEDIATE_VALUE ) ) {
    case FL_IMMEDIATE_VALUE: {
      /* size stashed at end */
      size = el.value_ctr( this->hash_entry_size ).size;
      /* data after hdr in hash entry */
      p = (void *) el.immediate_value();
      *(void **) data = p;
      if ( cplen > size )
        cplen = size;
      ::memcpy( cp, p, cplen );
      return KEY_OK;
    }
    case FL_SEGMENT_VALUE: {
      KeyStatus mstatus;
      if ( this->msg == NULL &&
           ( (mstatus = this->attach_msg( ATTACH_READ )) != KEY_OK ) )
        return mstatus;
      /* fetch the sizes before testing is_msg_valid() */
      uint64_t hdr_size = this->msg->hdr_size();
      size = this->msg->msg_size;
      p    = this->msg->ptr( hdr_size );
      /* no need to test is_valid with write access or msg is copied */
      if ( this->test( KEYCTX_NO_COPY_ON_READ | KEYCTX_IS_READ_ONLY ) ==
                     ( KEYCTX_NO_COPY_ON_READ | KEYCTX_IS_READ_ONLY ) ) {
        if ( ! this->ht.is_valid_region( p, size ) )
          return KEY_MUTATED;
        if ( cplen > size )
          cplen = size;
        ::memcpy( cp, p, cplen );
        if ( ! this->is_msg_valid() ) /* check that hdr and size are correct */
          return KEY_MUTATED;
      }
      else {
        if ( cplen > size )
          cplen = size;
        ::memcpy( cp, p, cplen );
      }
      /* data starts after hdr */
      *(void **) data = p;
      return KEY_OK;
    }
    default:
      return KEY_NO_VALUE;
  }
}

typedef uint32_t msg_size_t;

/* append a message to entry */
KeyStatus
KeyCtx::append_msg( void *res,  uint64_t size )
{
  if ( this->test( KEYCTX_IS_READ_ONLY ) )
    return KEY_WRITE_ILLEGAL;

  HashEntry & el = *this->entry;
  uint8_t   * buf;
  uint64_t    cur_size,
              msg_off,
              new_size,
              msg_size;
  KeyStatus   mstatus,
              status = KEY_OK;

  el.set( FL_MSG_LIST );
  switch ( el.test( FL_SEGMENT_VALUE | FL_IMMEDIATE_VALUE ) ) {
    case FL_IMMEDIATE_VALUE: {
      uint8_t  * value = el.immediate_value(),
               * trail = (uint8_t *) el.trail_ptr( this->hash_entry_size );
      cur_size = el.value_ctr( this->hash_entry_size ).size;
      msg_off  = align<uint64_t>( cur_size, sizeof( msg_size_t ) );
      msg_size = msg_off + sizeof( msg_size_t ) + size;
      new_size = msg_size;
      if ( &value[ new_size ] <= trail ) {
        el.value_ctr( this->hash_entry_size ).size = new_size;
        this->next_serial( ValueCtr::SERIAL_MASK );
        buf = &value[ msg_off ];
        goto return_msg;
      }
      break;
    }
    case FL_SEGMENT_VALUE: {
      /* check if resize fits within current segment mem */
      if ( this->msg == NULL &&
           (mstatus = this->attach_msg( ATTACH_WRITE )) != KEY_OK )
        return mstatus;
      /* power of 2 allocator */
      cur_size = this->msg->msg_size;
      msg_off  = align<uint64_t>( cur_size, sizeof( msg_size_t ) );
      msg_size = msg_off + sizeof( msg_size_t ) + size;
      new_size = (uint64_t) 1 << ( 64 - __builtin_clzl( msg_size ) );
      uint32_t hdr_size   = MsgHdr::hdr_size( this->msg->key );
      uint64_t alloc_size = MsgHdr::alloc_size( hdr_size, new_size,
                                                this->seg_align(),
                                                this->msg_chain_size );
      if ( alloc_size == this->msg->size ) {
        this->next_serial( ValueCtr::SERIAL_MASK );
        this->geom.serial = el.value_ptr( this->hash_entry_size ).
                               set_serial( this->serial );
        this->msg->msg_size = msg_size;
        buf = (uint8_t *) this->msg->ptr( hdr_size + msg_off );
        goto return_msg;
      }
      /* split into chains at 16k, 2x32k, 4x64, 8x128... (total 384M) */
      if ( this->msg_chain_size < 0xff &&
           cur_size > 8 * 1024 * ( (uint64_t) this->msg_chain_size + 1 ) ) {
        MsgCtx mctx( *this );
        mctx.set_key( *this->kbuf );
        mctx.set_hash( this->key, this->key2 );
        msg_off  = 0;
        msg_size = sizeof( msg_size_t ) + size;
        new_size = (uint64_t) 1 << ( 64 - __builtin_clzl( msg_size ) );
        void * tmp;
        if ( (mstatus = mctx.alloc_segment( &tmp, new_size,
                                       this->msg_chain_size + 1 )) == KEY_OK ) {
          this->add_msg_chain( mctx );
          this->next_serial( ValueCtr::SERIAL_MASK );
          this->geom.serial = el.value_ptr( this->hash_entry_size ).
                                 set_serial( this->serial );
          this->msg->msg_size = sizeof( msg_size_t ) + size;
          buf = (uint8_t *) this->msg->ptr( hdr_size );
          goto return_msg;
        }
        return mstatus;
      }
      break;
    }
    default:
      msg_off  = 0;
      msg_size = size + sizeof( msg_size_t );
      new_size = msg_size;
      break;
  }
  /* reallocate, could do better if key already copied */
  this->next_serial( ValueCtr::SERIAL_MASK );
  status = this->alloc( res, new_size, true );
  if ( status == KEY_OK ) {
    buf = (uint8_t *) *(void **) res;
    buf = &buf[ msg_off ];
    if ( el.test( FL_SEGMENT_VALUE ) ) {
      this->msg->msg_size = msg_size;
    }
return_msg:;
    *(uint32_t *) (void *) buf = (uint32_t) size;
    *(void **) res = &buf[ sizeof( msg_size_t ) ];
  }
  return status;
}

KeyStatus
KeyCtx::append_vector( uint64_t count,  void *vec,  uint64_t *size )
{
  if ( this->test( KEYCTX_IS_READ_ONLY ) )
    return KEY_WRITE_ILLEGAL;
  if ( count == 0 )
    return KEY_OK;

  HashEntry & el = *this->entry;
  uint8_t   * buf;
  void      * res;
  uint64_t    cur_size,
              msg_off,
              next_off,
              new_size,
              msg_size,
              vec_size,
              i = 0;
  KeyStatus   mstatus,
              status = KEY_OK;

  next_off = 0;
  for (;;) {
    vec_size = next_off + sizeof( msg_size_t ) + size[ i ];
    if ( ++i == count )
      break;
    next_off = align<uint64_t>( vec_size, sizeof( msg_size_t ) );
  }

  el.set( FL_MSG_LIST );
  switch ( el.test( FL_SEGMENT_VALUE | FL_IMMEDIATE_VALUE ) ) {
    case FL_IMMEDIATE_VALUE: {
      uint8_t  * value = el.immediate_value(),
               * trail = (uint8_t *) el.trail_ptr( this->hash_entry_size );
      cur_size = el.value_ctr( this->hash_entry_size ).size;
      msg_off  = align<uint64_t>( cur_size, sizeof( msg_size_t ) );
      msg_size = msg_off + vec_size;
      new_size = msg_size;
      if ( &value[ new_size ] <= trail ) {
        el.value_ctr( this->hash_entry_size ).size = new_size;
        this->more_serial( count, ValueCtr::SERIAL_MASK );
        buf = &value[ msg_off ];
        goto copy_vector;
      }
      break;
    }
    case FL_SEGMENT_VALUE: {
      /* check if resize fits within current segment mem */
      if ( this->msg == NULL &&
           (mstatus = this->attach_msg( ATTACH_WRITE )) != KEY_OK )
        return mstatus;
      /* power of 2 allocator */
      cur_size = this->msg->msg_size;
      msg_off  = align<uint64_t>( cur_size, sizeof( msg_size_t ) );
      msg_size = msg_off + vec_size;
      new_size = (uint64_t) 1 << ( 64 - __builtin_clzl( msg_size ) );
      uint32_t hdr_size   = MsgHdr::hdr_size( this->msg->key );
      uint64_t alloc_size = MsgHdr::alloc_size( hdr_size, new_size,
                                                this->seg_align(),
                                                this->msg_chain_size );
      if ( alloc_size == this->msg->size ) {
        this->more_serial( count, ValueCtr::SERIAL_MASK );
        this->geom.serial = el.value_ptr( this->hash_entry_size ).
                               set_serial( this->serial );
        this->msg->msg_size = msg_size;
        buf = (uint8_t *) this->msg->ptr( hdr_size + msg_off );
        goto copy_vector;
      }
      /* split into chains at 16k, 2x32k, 4x64, 8x128... (total 384M) */
      if ( this->msg_chain_size < 0xff &&
           cur_size > 8 * 1024 * ( (uint64_t) this->msg_chain_size + 1 ) ) {
        MsgCtx mctx( *this );
        mctx.set_key( *this->kbuf );
        mctx.set_hash( this->key, this->key2 );
        msg_off  = 0;
        msg_size = vec_size;
        new_size = (uint64_t) 1 << ( 64 - __builtin_clzl( msg_size ) );
        if ( (mstatus = mctx.alloc_segment( &res, new_size,
                                       this->msg_chain_size + 1 )) == KEY_OK ) {
          this->add_msg_chain( mctx );
          this->more_serial( count, ValueCtr::SERIAL_MASK );
          this->geom.serial = el.value_ptr( this->hash_entry_size ).
                                 set_serial( this->serial );
          this->msg->msg_size = msg_size;
          buf = (uint8_t *) this->msg->ptr( hdr_size );
          goto copy_vector;
        }
        return mstatus;
      }
      break;
    }
    default:
      msg_off  = 0;
      msg_size = vec_size;
      new_size = vec_size;
      break;
  }
  /* reallocate, could do better if key already copied */
  this->more_serial( count, ValueCtr::SERIAL_MASK );
  status = this->alloc( &res, new_size, true );
  if ( status == KEY_OK ) {
    buf = (uint8_t *) res;
    buf = &buf[ msg_off ];
    if ( el.test( FL_SEGMENT_VALUE ) ) {
      this->msg->msg_size = msg_size;
    }
copy_vector:;
    for ( i = 0; ; i++ ) {
      *(uint32_t *) (void *) buf = (uint32_t) size[ i ];
      buf = &buf[ sizeof( msg_size_t ) ];
      ::memcpy( buf, ((void **) vec)[ i ], size[ i ] );
      if ( i + 1 == count )
        break;
      buf = &buf[ align<uint64_t>( size[ i ], sizeof( msg_size_t ) ) ];
    }
  }
  return status;
}

bool
MsgIter::init( KeyCtx &kctx,  uint64_t idx )
{
  HashEntry & el = *kctx.entry;
  /* serial is inclusive */
  uint64_t end_idx = kctx.get_serial_count( ValueCtr::SERIAL_MASK );

  if ( el.test( FL_SEQNO ) )
    this->seqno = el.seqno( kctx.hash_entry_size );
  else
    this->seqno = 0;
  if ( idx > end_idx ) {
    this->status = KEY_NOT_FOUND;
    return false;
  }
  switch ( el.test( FL_SEGMENT_VALUE | FL_IMMEDIATE_VALUE ) ) {
    case FL_IMMEDIATE_VALUE:
      this->setup( el.immediate_value(),
                   el.value_ctr( kctx.hash_entry_size ).size );
      return true;

    case FL_SEGMENT_VALUE: {
      if ( kctx.msg == NULL ) {
        KeyStatus mstatus = kctx.attach_msg( KeyCtx::ATTACH_READ );
        if ( mstatus != KEY_OK ) {
          this->status = mstatus;
          return false;
        }
      }
      this->msg = kctx.msg;
      if ( kctx.msg_chain_size > 0 ) {
        ValueGeom mchain, last_mchain;
        mchain.zero(); last_mchain.zero();
        for ( uint8_t i = 0; ; i++ ) {
          uint64_t seq;
          last_mchain = mchain;
          if ( i < kctx.msg_chain_size ) {
            kctx.msg->get_next( i, mchain, kctx.seg_align_shift );
            seq = ( ( mchain.serial - kctx.key ) & ValueCtr::SERIAL_MASK ) + 1;
          }
          else
            seq = this->seqno;
          if ( idx >= seq || seq <= this->seqno ) {
            if ( i != 0 ) {
              this->chain_num = i;
              this->msg = kctx.get_chain_msg( last_mchain );
              if ( this->msg == NULL ) {
                this->status = KEY_MUTATED;
                return false;
              }
            }
            if ( seq > this->seqno )
              this->seqno = seq;
            break;
          }
        }
      }
      this->setup( this->msg->ptr( this->msg->hdr_size() ),
                   this->msg->msg_size );
      return true;
    }
    default:
      this->status = KEY_NO_VALUE;
      return false;
  }
}

bool
MsgIter::trim( KeyCtx &kctx,  uint64_t &idx )
{
  HashEntry & el = *kctx.entry;
  if ( this->seek( idx ) ) {
    if ( this->msg_off < this->buf_size ) {
      this->buf_size -= this->msg_off;
      ::memmove( this->buf, &this->buf[ this->msg_off ], this->buf_size );
      if ( this->msg == NULL )
        el.value_ctr( kctx.hash_entry_size ).size = this->buf_size;
      else
        this->msg->msg_size = this->buf_size;
    }
    if ( this->chain_num > 0 ) {
      for ( uint8_t i = this->chain_num; i < kctx.msg_chain_size; i++ ) {
        ValueGeom mchain;
        kctx.msg->get_next( i, mchain, kctx.seg_align_shift );
        if ( mchain.size != 0 ) {
          MsgHdr *tmp = (MsgHdr *) kctx.ht.seg_data( mchain.segment,
                                                     mchain.offset );
          uint8_t tmp_size;
          if ( tmp->check_seal( kctx.key, kctx.key2, mchain.serial,
                                 mchain.size, tmp_size ) ) {
            Segment &seg = kctx.ht.segment( mchain.segment );
            tmp->release();
            seg.msg_count -= 1;
            seg.avail_size += mchain.size;
          }
          mchain.size = 0;
          kctx.msg->set_next( i, mchain, kctx.seg_align_shift );
        }
      }
    }
  }
  return true;
}

bool
MsgIter::seek( uint64_t &idx )
{
  if ( idx < this->seqno )
    idx = this->seqno;
  if ( ! this->first() )
    return false;
  while ( idx > this->seqno )
    if ( ! this->next() )
      return false;
  return true;
}

bool
MsgIter::first( void )
{
  if ( this->msg_off + sizeof( msg_size_t ) > this->buf_size ) {
    this->status = KEY_NOT_FOUND;
    return false;
  }
  this->msg_size = *(msg_size_t *) (void *) &this->buf[ this->msg_off ];
  uint64_t msg_end = this->msg_off + this->msg_size + sizeof( msg_size_t );
  if ( msg_end > this->buf_size ) {
    this->status = KEY_NOT_MSG;
    return false;
  }
  return true;
}

bool
MsgIter::next( void )
{
  this->msg_off += align<msg_size_t>( this->msg_size, sizeof( msg_size_t ) ) +
                   sizeof( msg_size_t );
  this->seqno++;
  return this->first();
}

/* get the value associated with the msg at index idx */
KeyStatus
KeyCtx::msg_value( uint64_t &from_idx,  uint64_t &to_idx,
                   void *data,  uint64_t *size )
{
  if ( this->entry == NULL )
    return KEY_NO_VALUE;

  MsgIter iter;
  if ( ! iter.init( *this, from_idx ) )
    return iter.status;

  uint64_t max_count = to_idx - from_idx,
           count     = 0;
  if ( iter.seek( from_idx ) ) {
    do {
      iter.get_msg( size[ count ], ((void **) data)[ count ] );
      if ( ++count >= max_count )
        break;
    } while ( iter.next() );
  }
  if ( count == 0 )
    return iter.status;
  to_idx = from_idx + count;
  return KEY_OK;
}

KeyStatus
KeyCtx::trim_msg( uint64_t new_seqno )
{
  if ( this->entry == NULL )
    return KEY_NO_VALUE;
  /* serial is inclusive */
  HashEntry & el = *this->entry;
  KeyStatus   status;

  uint64_t end_idx = this->get_serial_count( ValueCtr::SERIAL_MASK );
  if ( new_seqno >= end_idx + 1 ) {
    new_seqno = end_idx + 1;
    if ( (status = this->release_data()) != KEY_OK )
      return status;
    el.set( FL_IMMEDIATE_VALUE ); /* size = 0 */
  }
  else {
    MsgIter iter;
    if ( ! iter.init( *this, new_seqno ) )
      return iter.status;
    if ( ! iter.trim( *this, new_seqno ) )
      return iter.status;
  }
  if ( el.test( FL_SEQNO ) == 0 )
    this->reorganize_entry( el, FL_SEQNO );
  el.seqno( this->hash_entry_size ) = new_seqno;
  return KEY_OK;
}

KeyStatus
KeyCtx::update_stamps( uint64_t exp_ns,  uint64_t upd_ns )
{
  HashEntry & el = *this->entry;

  if ( ( exp_ns | upd_ns ) == 0 )
    return KEY_OK;
  /* make room for rela stamp by moving value ptr up */
  if ( el.test( FL_EXPIRE_STAMP | FL_UPDATE_STAMP ) == 0 ) {
    uint8_t fl = 0;
    if ( exp_ns != 0 ) fl |= FL_EXPIRE_STAMP;
    if ( upd_ns != 0 ) fl |= FL_UPDATE_STAMP;
    this->reorganize_entry( el, fl );
  }
  RelativeStamp & rela = el.rela_stamp( this->hash_entry_size );
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
KeyCtx::reorganize_entry( HashEntry &el,  uint16_t new_fl )
{
  HashEntry *cpy = (HashEntry *) this->copy_data( &el, this->hash_entry_size );
  if ( cpy == NULL )
    return KEY_ALLOC_FAILED;
  el.flags |= new_fl;
  /* copy existing fields to new hash entry layout */
  if ( cpy->test( FL_SEQNO ) )
    el.seqno( this->hash_entry_size ) = cpy->seqno( this->hash_entry_size );
  if ( cpy->test( FL_EXPIRE_STAMP | FL_UPDATE_STAMP ) )
    el.rela_stamp( this->hash_entry_size ) =
      cpy->rela_stamp( this->hash_entry_size );
  if ( cpy->test( FL_SEGMENT_VALUE ) )
    el.value_ptr( this->hash_entry_size ) =
      cpy->value_ptr( this->hash_entry_size );

  uint32_t tl = el.trail_offset( this->hash_entry_size ),
           hd = el.hdr_size2();
  if ( el.test( FL_IMMEDIATE_VALUE ) ) {
    uint32_t  sz       = el.value_ctr( this->hash_entry_size ).size;
    KeyStatus mstatus;
    /* if immediate fits */
    if ( hd + sz <= tl )
      return KEY_OK;
    /* move immediate to segment */
    MsgCtx mctx( *this );
    uint64_t hdr_size = MsgHdr::hdr_size( *this->kbuf );
    mctx.set_key( *this->kbuf );
    mctx.set_hash( this->key, this->key2 );
    uint64_t new_size = (uint64_t) 1 << ( 64 - __builtin_clzl( sz ) );
    void * tmp;
    if ( (mstatus = mctx.alloc_segment( &tmp, new_size, 0 )) != KEY_OK )
      return mstatus;
    el.clear( FL_IMMEDIATE_VALUE );
    el.set( FL_SEGMENT_VALUE );
    mctx.geom.serial = this->serial;
    this->geom = mctx.geom;
    el.set_value_geom( this->hash_entry_size, mctx.geom,
                       this->seg_align_shift );
    this->msg = mctx.msg;
    this->msg->msg_size = sz;
    ::memcpy( this->msg->ptr( hdr_size ), cpy->immediate_value(), sz );
  }
  if ( hd > tl ) { /* can only have part key */
    el.set( FL_PART_KEY );
    el.clear( FL_IMMEDIATE_KEY );
  }
  else {
    el.set( FL_IMMEDIATE_KEY );
    el.clear( FL_PART_KEY );
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
  uint8_t * cur_ptr = (uint8_t *) el.trail_ptr( this->hash_entry_size ),
          * rel_ptr = (uint8_t *) el.ptr( this->hash_entry_size -
                           ( sizeof( ValueCtr ) + sizeof( RelativeStamp ) ) );
  /* if there exist entries above RelativeStamp, move them */
  if ( rel_ptr > cur_ptr )
    ::memmove( cur_ptr, &cur_ptr[ sizeof( RelativeStamp ) ], rel_ptr - cur_ptr );
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
    case KEY_NOT_MSG:       return "KEY_NOT_MSG";
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
    case KEY_NOT_MSG:       return "message size out of range";
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
kv_alloc( kv_key_ctx_t *kctx,  void *ptr,  uint64_t size )
{
  return reinterpret_cast<KeyCtx *>( kctx )->alloc( ptr, size );
}

kv_key_status_t
kv_load( kv_key_ctx_t *kctx,  kv_msg_ctx_t *mctx )
{
  return reinterpret_cast<KeyCtx *>( kctx )->load( 
           *reinterpret_cast<MsgCtx *>( mctx ) );
}

kv_key_status_t
kv_resize( kv_key_ctx_t *kctx,  void *ptr,  uint64_t size )
{
  return reinterpret_cast<KeyCtx *>( kctx )->resize( ptr, size );
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
