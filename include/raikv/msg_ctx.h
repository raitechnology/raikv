#ifndef __rai__raikv__msg_ctx_h__
#define __rai__raikv__msg_ctx_h__

/* also include stdint.h, string.h */
#include <raikv/key_ctx.h>
#include <raikv/hash_entry.h>
#include <raikv/ht_stats.h>
#include <raikv/atom.h>

#ifdef __cplusplus
namespace rai {
namespace kv {

struct HashTab;

struct MsgHdr {
  uint32_t    size,     /* alloc size including hdr, key, data */
              msg_size; /* data size */
  uint64_t    hash,     /* key hash value */
              hash2;    /* second hash value */
  uint8_t     db,
              type;
  uint16_t    flags;    /* where is data, how is it aligned */
  KeyFragment key;      /* all of the key */

  /* offset to the data */
  static uint64_t hdr_size( const KeyFragment &kb ) {
    return align<uint64_t>( sizeof( uint32_t ) * 2 + /* size, msg_size */
                            sizeof( uint64_t ) * 2 + /* hash, hash2 */
                            sizeof( uint8_t )  * 2 + /* db, type */
                            sizeof( uint16_t ) +     /* flags */
                            kb.keylen + sizeof( kb.keylen ), 8 );
  }
  uint64_t hdr_size( void ) const {
    return MsgHdr::hdr_size( this->key );
  }
  /* calculate space needed, hdr_size calculated from key above */
  static uint64_t alloc_size( uint32_t hdr_size,  uint64_t size,
                              uint64_t seg_align,  uint16_t chain_size = 0 ) {
    const size_t ch_size = (size_t) chain_size * sizeof( ValuePtr );
    return align<uint64_t>( hdr_size + size + ch_size +
                      sizeof( RelativeStamp ) + sizeof( ValueCtr ), seg_align );
  }
  void clear( uint32_t fl )          { this->flags &= ~fl; }
  void set( uint32_t fl )            { this->flags |= fl; }
  uint32_t test( uint32_t fl ) const { return this->flags & fl; }
  /* disallow readers from accessing */
  void unseal( void ) {
    ValueCtr &ctr = this->value_ctr();
    this->set( FL_BUSY );
    ctr.set_serial( 0 );
    ctr.seal = 0;
  }
  /* update serial and set seal, allowing readers to access */
  void seal( uint64_t serial,  uint8_t db,  uint8_t type,  uint16_t flags,
             uint16_t chain_size ) {
    ValueCtr &ctr = this->value_ctr();
    ctr.set_serial( serial );
    ctr.size    = chain_size;
    ctr.seal    = 1;
    this->db    = db;
    this->type  = type;
    this->flags = flags; /* clears FL_BUSY */
  }
  /* update serial and set seal, allowing readers to access */
  void seal2( uint64_t serial,  uint16_t flags,  uint16_t chain_size ) {
    ValueCtr &ctr = this->value_ctr();
    ctr.set_serial( serial );
    ctr.size    = chain_size;
    ctr.seal    = 1;
    this->flags = flags; /* clears FL_BUSY */
  }
  /* check that the message has same serial as hash_entry and is not unsealed */
  bool check_seal( uint64_t h,  uint64_t h2,  uint64_t serial,
                   uint32_t sz,  uint16_t &chain_size ) volatile {
    const bool hdr_sealed = ( this->size == sz ) &
                            ( this->hash == h ) &
                            ( this->hash2 == h2 ) &
                            ( ( this->flags & FL_BUSY ) == 0 );
    if ( hdr_sealed ) { /* size is correct, it is safe to dereference */
      /* check serial number */
      volatile ValueCtr &ctr = *(volatile ValueCtr *) (void *)
               &((uint8_t *) (void *) this)[ sz - sizeof( ValueCtr ) ];
      uint32_t seriallo = (uint32_t) serial,
               serialhi = (uint16_t) ( serial >> 32 );
      chain_size = ctr.size;
      return ( ctr.seriallo == seriallo ) &
             ( ctr.serialhi == serialhi ) &
             ( ctr.seal == 1 );
    }
    return false;
  }
  bool check_seal_msg_list( uint64_t h,  uint64_t h2,  uint64_t &serial,
                            uint32_t sz,  uint16_t &chain_size ) volatile {
    const bool hdr_sealed = ( this->size == sz ) &
                            ( this->hash == h ) &
                            ( this->hash2 == h2 ) &
                            ( ( this->flags & FL_BUSY ) == 0 );
    if ( hdr_sealed ) { /* size is correct, it is safe to dereference */
      /* check serial number */
      ValueCtr &ctr = *(ValueCtr *) (void *)
               &((uint8_t *) (void *) this)[ sz - sizeof( ValueCtr ) ];
      uint64_t ser = ctr.get_serial();
      chain_size = ctr.size;
      if ( ( ser < serial ) &&
           ( ser >= ( h & ValueCtr::SERIAL_MASK ) ) &&
           ( ctr.seal == 1 ) ) {
        serial = ser;
        return true;
      }
    }
    return false;
  }
  /* set the header flags, unseal() to prevent msg from going anywhere */
  void init( uint16_t fl,  uint64_t h,  uint64_t h2,  uint32_t sz,
             uint32_t msz ) {
    this->size     = sz;
    this->msg_size = msz;
    this->hash     = h;
    this->hash2    = h2;
    this->db       = 0;
    this->type     = 0;
    this->flags    = fl;
    this->unseal();
  }
  /* copy the key material and return pointer to msg data */
  void *copy_key( KeyFragment &kb,  uint32_t hsz ) {
    uint16_t * p = (uint16_t *) (void *) &this->key,
             * k = (uint16_t *) (void *) &kb,
             * e = (uint16_t *) (void *) &kb.u.buf[ kb.keylen ];
    do {
      *p++ = *k++;
    } while ( k < e );
    return this->ptr( hsz );
  }
  /* no longer need the memory, mark freed */
  void release( void ) {
    this->seal2( 0, 0, 0 );
    this->hash = ZOMBIE64;
  }
  /* the serial and seal are at the end of the msg data */
  ValueCtr &value_ctr( void ) const {
    return *(ValueCtr *) this->ptr( this->size - sizeof( ValueCtr ) );
  }
  RelativeStamp &rela_stamp( void ) const {
    return *(RelativeStamp *) this->ptr( this->size -
                             ( sizeof( ValueCtr ) + sizeof( RelativeStamp ) ) );
  }
  void *ptr( uint32_t off ) const {
    return &((uint8_t *) (void *) this)[ off ];
  }
  void set_next( uint8_t i,  ValueGeom &geom,  uint32_t align_shift ) {
    ValuePtr & vp = *(ValuePtr *) this->ptr( this->size -
                             ( sizeof( ValueCtr ) + sizeof( RelativeStamp ) +
                             ( ( (size_t) i + 1 ) * sizeof( ValuePtr ) ) ) );
    vp.set( geom, align_shift );
  }
  void get_next( uint8_t i,  ValueGeom &geom,  uint32_t align_shift ) const {
    const ValuePtr & vp = *(const ValuePtr *) this->ptr( this->size -
                             ( sizeof( ValueCtr ) + sizeof( RelativeStamp ) +
                             ( ( (size_t) i + 1 ) * sizeof( ValuePtr ) ) ) );
    vp.get( geom, align_shift );
  }
  bool is_expired( const HashTab &ht );
};

struct Segment {
  AtomUInt64 ring,       /* a ptr to the next allocation, also infers a busy
                            state when hi != lo, encoded using align_shift,
                            of the table */
  /* thse are updated only when ring is locked, but read without locking
   * which presumes atomic 8 byte updates without tearing (ie x86_64) */
             msg_count,  /* count of messages in this segment */
             avail_size; /* amount of bytes available for alloc */
  uint64_t   move_msgs,  /* stats, msgs moved for GC compaction */
             move_size,  /* stats, bytes moved for GC compaction */
             evict_msgs, /* stats, msgs evicted when GC load > critical */
             evict_size, /* stats, bytes evicted when GC load > critical */
             seg_off;    /* offset from start of shared mem for segment */

  void init( uint64_t off,  uint64_t size ) {
    ::memset( this, 0, sizeof( *this ) );
    this->avail_size = size;
    this->seg_off    = off;
  }
  static void get_position( uint64_t val,  uint16_t align_shift,
                            uint64_t &x,  uint64_t &y ) {
     x = ( ( val >> 32 ) & 0xffffffffU ) << align_shift;
     y = ( val & 0xffffffffU ) << align_shift;
  }
  /* the ring is split in 32 bit parts.  when in use, these will be the area
   * that is being updated (start->end).  when not in use, they will be equal
   * and at the point that will be allocated next (off->off) */
  bool try_alloc( uint8_t how_aggressive,  uint64_t alloc_size,
                  uint64_t ring_size,  uint16_t align_shift,
                  uint64_t &start,  uint64_t &old ) {
    uint64_t avail = this->avail_size;
    if ( how_aggressive >= 1 && alloc_size > avail )
      return false;
    uint64_t cur  = this->ring.val,
             x, y,
             used = ring_size - avail;
    /* presume that the caller does check for size < ring_size */
    get_position( cur, align_shift, x, y );
    if ( x != y ) /* is busy when not equal */
      return false;
    old = x; /* existing location where space is likely to be available */
    y += alloc_size; /* after allocation, new position */
    /* if size wraps around ring or space before cursor, depending on:
     * how_aggressive  ==  determines how CPU is used to scan existing items
     *   0    = wrap always
     *   1    = wrap when any free item exists
     *   2    = wrap when x > 2 times of that of used, so used=100, x=200
     *   3    = wrap when x > 3 times of that of used, so used=100, x=300
     *   4    = wrap when x > 4 times of that of used, so used=100, x=400 */
    if ( y > ring_size || x > used * (uint64_t) how_aggressive ) {
      x = 0;
      y = alloc_size;
    }
    uint64_t newx = ( x >> align_shift ) << 32,
             newy = ( y >> align_shift );
    if ( ! this->ring.cmpxchg( cur, newx | newy ) )
      return false;
    start = x; /* start looking for space here */
    return true;
  }

  bool try_lock( uint16_t align_shift,  uint64_t &pos ) {
    uint64_t cur  = this->ring.val,
             x, y;
    /* presume that the caller does check for size < ring_size */
    get_position( cur, align_shift, x, y );
    if ( x != y ) /* is busy when not equal */
      return false;
    pos = x;
    uint64_t newx = ( x >> align_shift ) << 32,
             newy = newx + 1;
    if ( ! this->ring.cmpxchg( cur, newx | newy ) )
      return false;
    return true;
  }

  void release( uint64_t new_off,  uint16_t align_shift ) {
    uint64_t newx = ( new_off >> align_shift ) << 32,
             newy = ( new_off >> align_shift );
    this->ring = newx | newy; /* set x == y, then others can lock segment */
  }
  void get_mem_seg_delta( MemDeltaCounters &stat,  uint16_t align_shift ) const;
};

/* a context to allocate memory from a segment for later linking into ht[] */
/* Example:
   KeyBuf kbuf( "hello world" );
   KeyCtx kctx( ht, ctx_id );
   MsgCtx mctx( ht, ctx_id );
   void * ptr;
   mctx.set_key_hash( kbuf );
   if ( mctx.alloc_segment( &ptr, 100, 8 ) == KEY_OK ) {
     ::memset( ptr, 'x', 100 );
     kctx.set_key( kbuf );
     kctx.set_hash( mctx.key, mctx.key2 );
     if ( kctx.acquire() == KEY_OK ) {
       kctx.load( mctx );
       kctx.release();
     }
     else {
       mctx.nevermind();
     }
   }
*/
struct MsgChain {
  ValueGeom geom;
  MsgHdr  * msg;
  MsgChain() : msg( 0 ) { this->geom.zero(); }
};

struct MsgCtx {
  HashTab      & ht;      /* operates on this table */
  ThrCtx       & thr_ctx;
  HashCounters & dbstat;
  KeyFragment  * kbuf;    /* key to place */
  const uint32_t hash_entry_size;
  const uint8_t  db_num;
  uint8_t        pad[ 3 ];
  uint64_t       key,
                 key2;
  MsgHdr       * msg;
  void         * prefetch_ptr;
  ValueGeom      geom;    /* value location */

  MsgCtx( KeyCtx &kctx );
  MsgCtx( HashTab &t,  ThrCtx &thr );
  MsgCtx( HashTab &t,  ThrCtx &thr,  uint32_t sz );
  ~MsgCtx() {}
  /* placement new to deal with broken c++ new[], for example:
   * MsgCtxBuf kctxbuf[ 8 ];
   * MsgCtx * kctx = MsgCtx::new_array( ht, thr_ctx, kctxbuf, 8 );
   * or to use malloc() instead of stack:
   * MsgCtx * kctx = MsgCtx::new_array( ht, thr_ctx, NULL, 8 );
   * if ( kctx == NULL ) fatal( "no memory" );
   * delete kctx; // same as free( kctx )
   */
  static MsgCtx * new_array( HashTab &t,  uint32_t id,  void *b,  size_t bsz );

  void * operator new( size_t, void *ptr ) { return ptr; }
  /* no allocated objects within this structure */
  void operator delete( void *ptr ) { ::free( ptr ); }

  void set_key( KeyFragment &b ) { this->kbuf = &b; }

  void set_hash( uint64_t k,  uint64_t k2 ) {
    this->key  = k;
    this->key2 = k2;
  }
  /*void set_key_hash( KeyFragment &b );*/

  void prefetch_segment( uint64_t size );

  KeyStatus alloc_segment( void *res,  uint64_t size,  uint16_t chain_size );

  void nevermind( void );

  static size_t copy_chain( const MsgHdr *from,  MsgHdr *to,  size_t i,
                            size_t j,  size_t cnt,  const uint32_t algn_shft ) {
    for ( size_t k = 0; k < cnt; k++ ) {
      ValueGeom geom;
      from->get_next( i++, geom, algn_shft );
      if ( geom.size == 0 )
        break;
      to->set_next( j++, geom, algn_shft );
    }
    return j;
  }
  uint8_t add_chain( MsgChain &next );
};

struct GCStats {
  uint64_t new_pos,
           seg_pos,
           moved_size,
           zombie_size,
           expired_size,
           mutated_size,
           orphans_size,
           immovable_size,
           msglist_size,
           compact_size;
  uint32_t moved,
           zombie,
           expired,
           mutated,
           orphans,
           immovable,
           msglist,
           compact,
           chains;
  void zero( void ) {
    ::memset( this, 0, sizeof( *this ) );
  }
};

typedef uint64_t MsgCtxBuf[ sizeof( MsgCtx ) / sizeof( uint64_t ) ];
}
}
#endif /* ifdef cplusplus */

#ifdef __cplusplus
extern "C" {
#endif
#ifndef __rai__raikv__key_ctx_h__
struct kv_hash_tab_s;
struct kv_msg_ctx_s;
typedef struct kv_hash_tab_s kv_hash_tab_t;
typedef struct kv_msg_ctx_s kv_msg_ctx_t;
#endif

kv_msg_ctx_t *kv_create_msg_ctx( kv_hash_tab_t *ht,  uint32_t ctx_id );
void kv_release_msg_ctx( kv_msg_ctx_t *mctx );

kv_key_status_t kv_alloc_segment( kv_msg_ctx_t *mctx,  void *ptr,
                                  uint64_t size );
void kv_nevermind( kv_msg_ctx_t *mctx );
#ifdef __cplusplus
}
#endif

#endif
