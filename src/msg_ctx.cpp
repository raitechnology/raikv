#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stddef.h>

#include <raikv/shm_ht.h>

using namespace rai;
using namespace kv;

MsgCtx::MsgCtx( HashTab &t,  ThrCtx &thr,  uint32_t sz )
      : ht( t ), thr_ctx( thr ), kbuf( 0 ), hash_entry_size( sz ), key( 0 ),
        msg( 0 ), prefetch_ptr( 0 ) {}

MsgCtx::MsgCtx( HashTab &t,  ThrCtx &thr )
      : ht( t ), thr_ctx( thr ), kbuf( 0 ),
        hash_entry_size( t.hdr.hash_entry_size ), key( 0 ),
        msg( 0 ), prefetch_ptr( 0 ) {}
#if 0
void
MsgCtx::set_key_hash( KeyFragment &b )
{
  uint64_t k  = this->ht.hdr.hash_key_seed,
           k2 = this->ht.hdr.hash_key_seed2;
  this->set_key( b );
  b.hash( k, k2 );
  this->set_hash( k, k2 );
}
#endif
MsgCtx *
MsgCtx::new_array( HashTab &t,  uint32_t id,  void *b,  size_t bsz )
{
  MsgCtxBuf *p = (MsgCtxBuf *) b;
  if ( p == NULL ) {
    p = (MsgCtxBuf *) ::malloc( sizeof( MsgCtxBuf ) * bsz );
    if ( p == NULL )
      return NULL;
    b = (void *) p;
  }
  for ( size_t i = 0; i < bsz; i++ ) {
    new ( (void *) p ) MsgCtx( t, t.ctx[ id ] );
    p = &p[ 1 ];
  }
  return (MsgCtx *) (void *) b;
}

void
MsgCtx::prefetch_segment( uint64_t /*size*/ )
{
  static const int rw = 1;       /* 0 is prepare for write, 1 is read */
  static const int locality = 2; /* 0 is non, 1 is low, 2 is moderate, 3 high*/
  /* isn't really effective, since the streamers may be activated by alloc() */
  if ( this->prefetch_ptr != NULL ) {
    __builtin_prefetch( this->prefetch_ptr, rw, locality);
#if 0
    uint8_t * p = (uint8_t *) this->prefetch_ptr,
            * e = &p[ size < 128 ? size : 128 ];
    do {
      __builtin_prefetch( p, rw, locality );
      p = &p[ 64 ];
    } while ( p < e );
#endif
  }
}

KeyStatus
MsgCtx::alloc_segment( void *res,  uint64_t size,  uint8_t /*alignment*/ )
{
  ThrCtx         & ctx       = this->thr_ctx;
  KeyCtx           mv_kctx( this->ht, ctx );
  WorkAllocT<1024> wrk;
  const uint64_t   seg_size  = this->ht.hdr.seg_size();
  const uint32_t   nsegs     = this->ht.hdr.nsegs,
                   algn_shft = this->ht.hdr.seg_align_shift,
                   hdr_size  = MsgHdr::hdr_size( *this->kbuf );

  if ( nsegs == 0 )
    return KEY_TOO_BIG;

  /* find free space in a segment to put put key + data */
  const uint64_t alloc_size = MsgHdr::alloc_size( hdr_size, size,
                                                  this->ht.hdr.seg_align() );
  /* starting segment */
  uint32_t       spins          = 0;
  const uint32_t max_tries      = nsegs * 4;
  uint64_t       how_aggressive = 3,
                 htevict        = 0;

  if ( alloc_size > seg_size )
    return KEY_TOO_BIG;
  if ( ctx.seg_pref < (uint64_t) nsegs )
    ctx.seg_pref = ctx.rng.next();
  this->geom.segment = ctx.seg_pref % nsegs;
  this->geom.size    = alloc_size;

  for (;;) {
    Segment &seg = this->ht.segment( this->geom.segment );
    uint64_t hd, tl, old;

    /* if this fails, seg is busy */
    if ( seg.try_alloc( how_aggressive, alloc_size, seg_size, algn_shft, hd,
                        tl, old ) ) {
      /* evict any messages within the region */
      uint64_t  mv_hash,
                msgsize,
                i = hd,
                j = hd; /* i is leading edge, j is trailing edge */
      uint8_t * segptr = (uint8_t *) this->ht.seg_data( this->geom.segment, 0 );

      for (;;) {
        MsgHdr &msgptr = *(MsgHdr *) (void *) &segptr[ i ];
        if ( (msgsize = msgptr.size) == 0 ) {
          i = seg_size;
          break;
        }
        if ( (mv_hash = msgptr.hash) != ZOMBIE64 ) {
          uint64_t mv_hash2 = msgptr.hash2;
          /* move messages to the head of segment */
          if ( i > j || how_aggressive == 0 ) {
            bool success = false;
            /* key is still alive, try to acquire it */
            mv_kctx.set_key( msgptr.key );
            mv_kctx.set_hash( mv_hash, mv_hash2 );
            mv_kctx.set( KEYCTX_IS_GC_ACQUIRE );
            if ( mv_kctx.try_acquire( &wrk ) <= KEY_IS_NEW ) {
              mv_kctx.db_num = mv_kctx.get_db();
              /* make sure it didn't move */
              if ( mv_kctx.entry->test( FL_SEGMENT_VALUE ) ) {
                mv_kctx.entry->get_value_geom( this->hash_entry_size,
                                               mv_kctx.geom, algn_shft );
                /* if in the same location */
                if ( mv_kctx.geom.segment == this->geom.segment &&
                     mv_kctx.geom.offset == i &&
                     mv_kctx.geom.size == msgsize ) {
                  if ( msgptr.check_seal( mv_hash, mv_hash2, mv_kctx.serial,
                                          msgsize ) ) {
                    bool isexp = msgptr.is_expired( this->ht );
                    if ( isexp || how_aggressive == 0 ) {
                      if ( ! isexp ) {
                        seg.evict_msgs++;
                        seg.evict_size += msgsize;
                        htevict++;
                        /* XXX: send to evict handler */
                      }
                      mv_kctx.tombstone();
                      i += msgsize;
                      success = true;
                    }
                    else {
                      msgptr.unseal();
                      /* move it and update geom */
                      ::memmove( &segptr[ j ], &segptr[ i ], mv_kctx.geom.size );
                      mv_kctx.geom.offset = j;
                      mv_kctx.entry->set_value_geom( this->hash_entry_size,
                                                     mv_kctx.geom, algn_shft );
                      msg = (MsgHdr *) (void *) &segptr[ j ];
                      msg->seal2( mv_kctx.serial, mv_kctx.entry->flags );
                      /* advance both leading and trailing edge */
                      j += msgsize;
                      i += msgsize;
                      seg.move_msgs++;
                      seg.move_size += msgsize;
                      /* mark the size of the chunk after message */
                      MsgHdr &frag = *(MsgHdr *) (void *) &segptr[ j ];
                      frag.size = i - j;
                      frag.msg_size = 0;
                      frag.release();
                      success = true;
                    }
                  }
                }
              }
              mv_kctx.release();
            }
            if ( ! success ) {
              i += msgsize; /* skip over immovable object */
              j  = i;
            }
          }
          else { /* skip over msg, it's already at the head */
            i += msgsize;
            j += msgsize;
          }
        }
        else {
          i += msgsize;
        }
        if ( i - j >= alloc_size ) /* if have enough space */
          break;
        if ( i >= seg_size ) /* should not be > seg_size */
          break;
      }
      if ( i - j < alloc_size ) /* if not enough space */
        goto alloc_failed;
      /* fragment the tail msg region */
      if ( i - j > alloc_size ) {
        MsgHdr &frag  = *(MsgHdr *) (void *) &segptr[ j + alloc_size ];
        frag.size     = i - ( j + alloc_size );
        frag.msg_size = 0;
        frag.release();
      }
      /* region is free, use it */
      this->geom.offset = j;
      this->msg = (MsgHdr *) (void *) &segptr[ j ];
      this->msg->init( FL_SEGMENT_VALUE, this->key, this->key2, alloc_size, size );
      *(void **) res = this->msg->copy_key( *this->kbuf, hdr_size );
      j += alloc_size;
      if ( j + 1024 < seg_size )
        this->prefetch_ptr = &segptr[ j ];
      else
        this->prefetch_ptr = NULL;
      /* set up flags and entry index into segment */
      seg.avail_size -= alloc_size;
      seg.msg_count += 1;
      seg.release( j, algn_shft );
      ctx.incr_htevict( htevict );
      return KEY_OK;
  alloc_failed:;
      seg.release( j, algn_shft );
    }
    /* try next seg */
    if ( ++spins == max_tries ) {
      ctx.incr_htevict( htevict );
      return KEY_ALLOC_FAILED;
    }
    ctx.rng.incr();
    ctx.seg_pref = ctx.seg_pref / nsegs;
    if ( ctx.seg_pref < (uint64_t) nsegs )
      ctx.seg_pref = ctx.rng.next();
    this->geom.segment = ctx.seg_pref % nsegs;
    if ( spins >= nsegs / 4 ) {
      this->ht.update_load();
      kv_sync_pause();
    }
    if ( this->ht.hdr.load_percent >= this->ht.hdr.critical_load )
      how_aggressive = 0;
    /*else if ( this->ht.hdr.load_percent >= 90 )
      how_aggressive = 1;*/
    else
      how_aggressive = 2;
#if 0
    else if ( this->ht.hdr.load_percent >= 70 )
      how_aggressive = 2;
    else if ( this->ht.hdr.load_percent >= 50 )
      how_aggressive = 3;
    else
      how_aggressive = 4;
#endif
  }
}

bool
MsgHdr::is_expired( const HashTab &ht )
{
  switch ( this->test( FL_EXPIRE_STAMP | FL_UPDATE_STAMP ) ) {
    case FL_EXPIRE_STAMP:
      return this->rela_stamp().u.stamp < ht.hdr.current_stamp;
    default:
      return false;
    case FL_UPDATE_STAMP | FL_EXPIRE_STAMP: {
      uint64_t expire_ns, update_ns;
      this->rela_stamp().get( ht.hdr.create_stamp, ht.hdr.current_stamp,
                              expire_ns, update_ns );
      return expire_ns < ht.hdr.current_stamp;
    }
  }
}

void
MsgCtx::nevermind( void )
{
  if ( this->msg != NULL ) {
    this->msg->release();
    this->msg = NULL;
  }
}

