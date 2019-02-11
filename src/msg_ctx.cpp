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

uint8_t
MsgCtx::add_chain( MsgChain &next )
{
  const uint32_t algn_shft = this->ht.hdr.seg_align_shift;
  uint8_t        cnt       = next.msg->value_ctr().size;
  this->msg->set_next( 0, next.geom, algn_shft );
  return (uint8_t) MsgCtx::copy_chain( next.msg, this->msg, 0, 1, cnt,
                                       algn_shft );
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

struct GCRunCtx {
  static const uint16_t seg_lock_max = 31;
  const uint64_t   seg_size;
  const uint32_t   hash_entry_size,
                   algn_shft;
  const uint16_t   seg_num;

  HashTab        & ht;
  KeyCtx           kctx;
  Segment        & seg;
  uint8_t        * segptr;
  MsgHdr         * frag;
  uint64_t         frag_start,
                   frag_size,
                   pos,
                   i,
                   j; /* i is leading edge, j is trailing edge */
  uint16_t         seg_lock[ seg_lock_max ], seg_lock_cnt;
  uint64_t         seg_lock_pos[ seg_lock_max ];
  WorkAllocT<1024> wrk;

  /* used when space is allocated */
  GCRunCtx( HashTab &tab,  ThrCtx &ctx,  Segment &s,  uint8_t *sptr,
            const uint64_t seg_sz,  const uint32_t he_sz,
            const uint32_t seg_algn,  const uint16_t segment_num,
            uint64_t hd,  uint64_t tl ) :
    seg_size( seg_sz ),
    hash_entry_size( he_sz ),
    algn_shft( seg_algn ),
    seg_num( segment_num ),
    ht( tab ), kctx( tab, ctx ), 
    seg( s ),
    segptr( sptr ),
    frag( 0 ), frag_start( 0 ), frag_size( 0 ),
    pos( 0 ), i( hd ), j( tl ), seg_lock_cnt( 0 ) {}

  /* used when segment is compacted */
  GCRunCtx( HashTab &tab,  ThrCtx &ctx,  uint32_t segment_num ) :
    seg_size( tab.hdr.seg_size() ),
    hash_entry_size( tab.hdr.hash_entry_size ),
    algn_shft( tab.hdr.seg_align_shift ),
    seg_num( segment_num ),
    ht( tab ), kctx( tab, ctx ), 
    seg( tab.segment( segment_num ) ),
    segptr( (uint8_t *) tab.seg_data( segment_num, 0 ) ),
    frag( 0 ), frag_start( 0 ), frag_size( 0 ),
    pos( 0 ), i( 0 ), j( 0 ), seg_lock_cnt( 0 ) {}

  bool lock( void ) {
    return this->seg.try_lock( this->algn_shft, this->pos );
  }

  void release_exsegs( void ) {
    for ( uint16_t n = 0; n < this->seg_lock_cnt; n++ ) {
      Segment & seg2 = this->ht.segment( this->seg_lock[ n ] );
      seg2.release( this->seg_lock_pos[ n ], this->algn_shft );
    }
  }

  void release( void ) {
    this->release_exsegs();
    this->seg.release( this->j, this->algn_shft );
  }

  bool gc( GCStats &stats ) {
    MsgHdr & msgptr  = *(MsgHdr *) (void *) &this->segptr[ this->i ];
    KeyCtx & mv_kctx = this->kctx;
    uint64_t mv_hash, msgsize;

    if ( (msgsize = msgptr.size) == 0 ) /* this should never occur, assert? */
      return false;
    if ( (mv_hash = msgptr.hash) != ZOMBIE64 ) {
      uint64_t mv_hash2 = msgptr.hash2;
      /* move messages to the head of segment */
      if ( this->i > this->j ) {
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
                                           mv_kctx.geom, this->algn_shft );
            uint8_t  do_mv_msg      = 0;
            uint8_t  msg_chain_size = 0;
            uint64_t msg_serial     = mv_kctx.geom.serial;
            if ( msgptr.check_seal( mv_hash, mv_hash2, msg_serial,
                                    msgsize, msg_chain_size ) &&
                 mv_kctx.geom.segment == this->seg_num &&
                 mv_kctx.geom.offset == this->i ) {
              bool isexp = msgptr.is_expired( this->ht );
              if ( isexp ) {
                mv_kctx.tombstone();
                success = true;
                stats.expired++;
                stats.expired_size += msgsize;
                this->frag = &msgptr;
                this->frag_start = this->i;
                this->frag_size  = msgsize;
                this->i += msgsize;
              }
              else {
                do_mv_msg = 1;
              }
            }
            else if ( mv_kctx.entry->test( FL_MSG_LIST ) &&
                      msgptr.check_seal_msg_list( mv_hash, mv_hash2,
                                                  msg_serial, msgsize,
                                                  msg_chain_size ) ) {
              bool      seg_is_locked = false;
              uint16_t  seg_num2 = mv_kctx.geom.segment;
              uint8_t * refseg = segptr;
              stats.msglist++;
              stats.msglist_size += msgsize;
              /* find the segment where refmsg is located */
              if ( seg_num2 == this->seg_num )
                seg_is_locked = true;
              else {
                refseg = (uint8_t *) this->ht.seg_data( seg_num2, 0 );
                for ( uint16_t n = 0; n < this->seg_lock_cnt; n++ ) {
                  if ( seg_num2 == this->seg_lock[ n ] ) {
                    seg_is_locked = true;
                    break;
                  }
                }
                if ( ! seg_is_locked &&
                     this->seg_lock_cnt < this->seg_lock_max ) {
                  Segment & seg2 = this->ht.segment( seg_num2 );
                  if ( seg2.try_lock( this->algn_shft,
                                    this->seg_lock_pos[ this->seg_lock_cnt ] ) ) {
                    this->seg_lock[ this->seg_lock_cnt++ ] = seg_num2;
                    seg_is_locked = true;
                  }
                }
              }
              /* if segment where refmsg is located is locked */
              if ( seg_is_locked ) {
                MsgHdr &ref_msg = *(MsgHdr *) (void *)
                                  &refseg[ mv_kctx.geom.offset ];
                uint8_t ref_chains;
                if ( ref_msg.check_seal( mv_hash, mv_hash2, mv_kctx.geom.serial,
                                         mv_kctx.geom.size, ref_chains ) ) {
                  if ( ref_chains >= msg_chain_size + 1 ) {
                    uint8_t k = ref_chains - ( msg_chain_size + 1 );
                    ValueGeom ref_geom;
                    /* unsealing ref_msg may not matter if reader has a copy
                     * already, it may find the chained msg is mutated and then
                     * retry */
                    ref_msg.get_next( k, ref_geom, this->algn_shft );
                    /* link to cur msg */
                    if ( ref_geom.segment == this->seg_num &&
                         ref_geom.offset == this->i &&
                         ref_geom.size == msgsize ) {
                      stats.chains++;
                      do_mv_msg = 2;
                      ref_geom.offset = this->j;
                      ref_msg.set_next( k, ref_geom, this->algn_shft );
                    }
                  }
                }
              }
            }
            else {
              goto is_orphan;
            }
            /* do_mv_msg == 1 when msg is not in a list
             * do_mv_msg == 2 when msg is refed by a msg list */
            if ( do_mv_msg > 0 ) {
              msgptr.unseal();
              /* move it and update geom */
              ::memmove( &segptr[ this->j ], &segptr[ this->i ], msgsize );
              if ( do_mv_msg != 2 ) { /* already set geom in ref msg link */
                mv_kctx.geom.offset = this->j;
                mv_kctx.entry->set_value_geom( this->hash_entry_size,
                                               mv_kctx.geom, this->algn_shft );
              }
              MsgHdr &msg_mv = *(MsgHdr *) (void *) &segptr[ this->j ];
              msg_mv.seal2( msg_serial, mv_kctx.entry->flags,
                            msg_chain_size );
              /* advance both leading and trailing edge */
              this->j += msgsize;
              this->i += msgsize;
              this->seg.move_msgs++;
              this->seg.move_size += msgsize;
              stats.moved++;
              stats.moved_size += msgsize;
              /* mark the size of the chunk after message */
              this->frag = (MsgHdr *) (void *) &segptr[ this->j ];
              this->frag_start = this->j;
              this->frag_size  = this->i - this->j;
              this->frag->size = this->frag_size;
              this->frag->msg_size = 0;
              this->frag->release();
              success = true;
            }
          }
          else {
          is_orphan:;
            stats.orphans++;
            stats.orphans_size += msgsize;
          }
          mv_kctx.release();
        }
        if ( ! success ) {
          this->i += msgsize; /* skip over immovable object */
          this->j  = this->i;
          stats.immovable++;
          stats.immovable_size += msgsize;
        }
      }
      else { /* skip over msg, it's already at the head */
        this->i += msgsize;
        this->j += msgsize;
        stats.compact++;
        stats.compact_size += msgsize;
      }
    }
    else {
      stats.zombie++;
      stats.zombie_size += msgsize;
      /* defragment zombies */
      if ( this->frag != NULL &&
           this->frag_start + this->frag_size == this->i ) {
        this->frag->size += msgsize;
        this->frag_size += msgsize;
      }
      else {
        this->frag = &msgptr;
        this->frag_start = this->i;
        this->frag_size  = msgsize;
      }
      this->i += msgsize;
    }
    return this->i < this->seg_size; /* should not be > seg_size */
  }
};

KeyStatus
MsgCtx::alloc_segment( void *res,  uint64_t size,  uint8_t chain_size )
{
  const uint32_t   hdr_size  = MsgHdr::hdr_size( *this->kbuf ),
                   algn_shft = this->ht.hdr.seg_align_shift;
  const uint64_t   seg_algn  = this->ht.hdr.seg_align(),
                   seg_size  = this->ht.hdr.seg_size();
  const uint16_t   nsegs     = this->ht.hdr.nsegs;

  if ( nsegs == 0 )
    return KEY_TOO_BIG;
  /* find free space in a segment to put put key + data */
  const uint64_t alloc_size = MsgHdr::alloc_size( hdr_size, size, seg_algn,
                                                  chain_size );
  if ( alloc_size > seg_size )
    return KEY_TOO_BIG;

  const uint32_t max_tries      = (uint32_t) nsegs * 4;
  uint32_t       spins          = 0;
  uint8_t        how_aggressive = 3;
  ThrCtx       & ctx            = this->thr_ctx;

  this->geom.segment = ctx.seg_num;
  this->geom.size    = alloc_size;

  for (;;) {
    Segment & seg = this->ht.segment( this->geom.segment );
    uint8_t * segptr = (uint8_t *) this->ht.seg_data( this->geom.segment, 0 );
    uint64_t  hd, tl, pos;

    if ( seg.try_alloc( how_aggressive, alloc_size, seg_size, algn_shft,
                        tl, pos ) ) {
      MsgHdr & msgptr = *(MsgHdr *) (void *) &segptr[ tl ];
      hd = msgptr.size;
      if ( hd == 0 )
        hd = seg_size;  /* end of segment */
      else if ( msgptr.hash == ZOMBIE64 )
        hd += tl;       /* free msg space from tl -> hd */
      else
        hd = tl;        /* nothing free, need to find space  */
      /* run gc, try to make space for allocation */
      if ( hd - tl < alloc_size ) {
        GCRunCtx gcrun( this->ht, ctx, seg, segptr, seg_size, 
                        this->hash_entry_size, seg_algn, this->geom.segment,
                        hd, tl );
        GCStats stats;
        stats.zero();
        while ( gcrun.gc( stats ) ) {
          if ( gcrun.j + alloc_size <= gcrun.i )
            break;
        }
        hd = gcrun.i;
        tl = gcrun.j;
        gcrun.release_exsegs();
      }
      /* if there is space available in this segment */
      if ( hd - tl >= alloc_size ) {
        if ( hd - tl > alloc_size ) {
          MsgHdr &frag  = *(MsgHdr *) (void *) &segptr[ tl + alloc_size ];
          frag.size     = hd - ( tl + alloc_size );
          frag.msg_size = 0;
          frag.release();
        }
        /* region is free, use it */
        this->geom.offset = tl;
        this->msg = (MsgHdr *) (void *) &segptr[ tl ];
        this->msg->init( FL_SEGMENT_VALUE, this->key, this->key2, alloc_size,
                         size );
        *(void **) res = this->msg->copy_key( *this->kbuf, hdr_size );
        tl += alloc_size;
        if ( tl < seg_size ) /* in case app wants to prefetch for allocations */
          this->prefetch_ptr = &segptr[ tl ];
        else
          this->prefetch_ptr = NULL;
        /* set up flags and entry index into segment */
        seg.avail_size -= alloc_size;
        seg.msg_count += 1;
        seg.release( tl, algn_shft );
        return KEY_OK;
      }
      else {
        /* did not find space, try another segment */
        seg.release( tl, algn_shft );
      }
    }
    /* try next seg */
    if ( ++spins == max_tries ) {
      /*ctx.incr_htevict( htevict );*/
      return KEY_ALLOC_FAILED;
    }
    ctx.seg_num  = (uint16_t) ( ctx.rng.next() % nsegs );
    this->geom.segment = ctx.seg_num;
    if ( spins >= nsegs / 4 ) {
      this->ht.update_load();
      kv_sync_pause();
    }
    if ( this->ht.hdr.load_percent >= this->ht.hdr.critical_load )
      how_aggressive = 0;
    else
      how_aggressive = 2;
  }
}

bool
HashTab::gc_segment( ThrCtx &ctx,  uint32_t seg_num,
                     GCStats &stats )
{
  if ( seg_num >= this->hdr.nsegs )
    return false;

  GCRunCtx gcrun( *this, ctx, seg_num );
  if ( ! gcrun.lock() )
    return false;
  stats.seg_pos = gcrun.pos;
  while ( gcrun.gc( stats ) )
    ;
  stats.new_pos = gcrun.j;
  gcrun.release();
  return true;
}
