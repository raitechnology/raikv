#ifndef __rai__raikv__key_ctx_h__
#define __rai__raikv__key_ctx_h__

#include <raikv/util.h>
#include <raikv/hash_entry.h>
#include <raikv/work.h>
#include <raikv/ht_stats.h>

/* also include stdint.h, string.h */
#ifdef __cplusplus
extern "C" {
#endif
/* returned from KeyCtx find/acquire operations */
typedef enum kv_key_status_e {
  KEY_OK            = 0,  /* generic ok, key exists */
  KEY_IS_NEW        = 1,  /* key does not exist, newly acquired */
  KEY_NOT_FOUND     = 2,  /* not found status */
  KEY_BUSY          = 3,  /* spin lock timeout or try lock timeouts */
  KEY_ALLOC_FAILED  = 4,  /* allocate key or data failed */
  KEY_HT_FULL       = 5,  /* no more ht entries */
  KEY_MUTATED       = 6,  /* another thread updated entry, repeat find */
  KEY_WRITE_ILLEGAL = 7,  /* no exclusive lock for write */
  KEY_NO_VALUE      = 8,  /* key has no value attached */
  KEY_TOO_BIG       = 9,  /* key + value + alignment is too big (> seg_size) */
  KEY_SEG_VALUE     = 10, /* value doesn't fit in immmediate data */
  KEY_TOMBSTONE     = 11, /* key not valid, was dropped */
  KEY_PART_ONLY     = 12, /* no key attached, hashes only */
  KEY_MAX_CHAINS    = 13, /* nothing found before entry count hit max chains */
  KEY_PATH_SEARCH   = 14, /* need a path search to acquire cuckoo entry */
  KEY_USE_DROP      = 15, /* ok to use drop, end of chain */
  KEY_NOT_MSG       = 16, /* message size out of range */
  KEY_EXPIRED       = 17, /* if expire timer is less than current time */
  KEY_MSG_LIST_FULL = 18, /* appending msg msg_list where size is max_size */
  KEY_MAX_STATUS    = 19  /* maximum status code */
} kv_key_status_t;

/* string versions of the above */
const char *kv_key_status_string( kv_key_status_t status );
const char *kv_key_status_description( kv_key_status_t status );

struct kv_hash_tab_s;
struct kv_key_ctx_s;
struct kv_msg_ctx_s;
struct kv_key_frag_s;

typedef struct kv_hash_tab_s kv_hash_tab_t;
typedef struct kv_key_ctx_s  kv_key_ctx_t;
typedef struct kv_msg_ctx_s  kv_msg_ctx_t;

typedef void (*kv_evict_cb_t)( kv_key_ctx_t *kctx,  void *cl );

#ifdef __cplusplus
} /* extern "C" */
#endif

#ifdef __cplusplus
namespace rai {
namespace kv {

typedef enum kv_key_status_e KeyStatus;

struct HashTab;
struct HashSeed;
struct ThrCtx;
struct MsgHdr;
struct MsgCtx;

/* a context to put and get hash entry values */
/* Example:
   KeyBuf kbuf( "hello world" );
   KeyCtx kctx( ht, ctx_id, &kbuf );
   WorkAlloc8k wrk;
   void *data;
   uint64_t sz;
   uint64_t h1 = map->hdr.hash_key_seed,
            h2 = map->hdr.hash_key_seed2;
   kbuf.hash( h1, h2 );
   kctx.set_hash( h1, h2 );
   if ( kctx.acquire() == KEY_OK ) {
     if ( kctx.resize( &data, 80 ) == KEY_OK )
       ::memset( data, 'x', 80 );
     kctx.release();
   }
   if ( kctx.find( wrk ) == KEY_OK ) {
     if ( kctx.value( &data, sz ) ) {
       printf( "found data %.*s\n", (int) sz, data );
     }
   }
*/
enum KeyCtxFlags {
  KEYCTX_HT_READ_ONLY      = 1, /* can't mutate hash tab */
  KEYCTX_IS_READ_ONLY      = 2, /* result of find(), etc.. no lock acq */
  KEYCTX_IS_GC_ACQUIRE     = 4, /* if GC is trying to acquire for moving */
  KEYCTX_IS_CUCKOO_ACQUIRE = 8, /* if Cuckoo relocate is trying to make space */
  KEYCTX_IS_HT_EVICT       = 16,/* if chains == max_chains, is eviction */
  KEYCTX_IS_SINGLE_THREAD  = 32,/* don't use thread synchronization */
  KEYCTX_NO_COPY_ON_READ   = 64,/* don't copy message on read, use seal check */
  KEYCTX_MULTI_KEY_ACQUIRE = 128,/* when multiple keys are being acquired */
  KEYCTX_EVICT_ACQUIRE     = 256 /* when acquire should evict old on new entry */
};

typedef uint32_t msg_size_t;

struct KeyCtx {
  HashTab      & ht;      /* operates on this table */
  const uint32_t ctx_id,
                 dbx_id;
  KeyFragment  * kbuf;    /* key to lookup resolve */
  const uint64_t ht_size;
  const uint32_t hash_entry_size;
  const uint16_t cuckoo_buckets; /* how many cuckoo buckets */
  const uint8_t  cuckoo_arity,   /* how many cuckoo hash functions */
                 seg_align_shift; /* alignment of segment data */
  uint8_t        db_num,
                 inc;        /* which hash function: 0 -> cuckoo_arity */
  uint16_t       msg_chain_size,
                 drop_flags, /* flags from dropped recycle entry */
                 flags;      /* KeyCtxFlags */
  HashCounters & stat;
  const uint64_t max_chains; /* drop entries after accumulating max chains */
                 /* ^^ 8*8 ^^  vv 8*12 + 8*4(geom) + 8 vv */ 
  HashEntry    * entry;   /* the entry after lookup, may be empty entry if NF*/
  MsgHdr       * msg;     /* the msg header indexed by geom */
  uint64_t       chains,     /* number of chains used to find/acquire */
                 start,   /* key % ht_size */
                 key,     /* KeyBuf hash() */
                 key2,    /* KeyBuf hash() */
                 pos,     /* position of entry */
                 lock,    /* value stored in ht[], either zero or == key */
                 drop_key,/* the dropped key that is being recycled */
                 drop_key2,/* the dropped key2 */
                 mcs_id,  /* id of lock queue for above ht lock */
                 serial;  /* serial number of the hash ent & message */
  ValueGeom      geom;    /* values decoded from HashEntry */
  ScratchMem   * wrk;     /* temp work allocation */
  kv_evict_cb_t* evict_cb;
  void         * cl;
  size_t         end;

  void zero( void ) {
    ::memset( &this->entry, 0, (uint8_t *) (void *) &( &this->cl )[ 1 ] -
                               (uint8_t *) (void *) &this->entry ); 
  }
  void incr_read( uint64_t cnt = 1 )    { this->stat.rd      += cnt; }
  void incr_write( uint64_t cnt = 1 )   { this->stat.wr      += cnt; }
  void incr_spins( uint64_t cnt = 1 )   { this->stat.spins   += cnt; }
  void incr_chains( uint64_t cnt = 1 )  { this->stat.chains  += cnt; }
  void incr_add( uint64_t cnt = 1 )     { this->stat.add     += cnt; }
  void incr_drop( uint64_t cnt = 1 )    { this->stat.drop    += cnt; }
  void incr_expire( uint64_t cnt = 1 )  { this->stat.expire  += cnt; }
  void incr_htevict( uint64_t cnt = 1 ) { this->stat.htevict += cnt; }
  void incr_afail( uint64_t cnt = 1 )   { this->stat.afail   += cnt; }
  void incr_hit( uint64_t cnt = 1 )     { this->stat.hit     += cnt; }
  void incr_miss( uint64_t cnt = 1 )    { this->stat.miss    += cnt; }
  void incr_cuckacq( uint64_t cnt = 1 ) { this->stat.cuckacq += cnt; }
  void incr_cuckfet( uint64_t cnt = 1 ) { this->stat.cuckfet += cnt; }
  void incr_cuckmov( uint64_t cnt = 1 ) { this->stat.cuckmov += cnt; }
  void incr_cuckret( uint64_t cnt = 1 ) { this->stat.cuckret += cnt; }
  void incr_cuckmax( uint64_t cnt = 1 ) { this->stat.cuckmax += cnt; }

  uint16_t test( uint16_t fl ) const { return ( this->flags & fl ); }
  void set( uint16_t fl )            { this->flags |= fl; }
  void clear( uint16_t fl )          { this->flags &= ~fl; }
  uint64_t seg_align( void ) const {
    return (uint64_t) 1 << this->seg_align_shift;
  }
  KeyCtx( HashTab &t,  uint32_t xid,  KeyFragment *b = NULL ) noexcept;
#if 0
  KeyCtx( HashTab &t,  uint32_t cid,  uint32_t id,  uint8_t dbn,
          KeyFragment *b = NULL ) noexcept;
#endif
  KeyCtx( KeyCtx &kctx ) noexcept;

  /* placement new to deal with broken c++ new[], for example:
   * KeyCtxBuf kctxbuf[ 8 ];
   * KeyCtx * kctx = KeyCtx::new_array( ht, ctx_id, kctxbuf, 8 );
   * or to use malloc() instead of stack:
   * KeyCtx * kctx = KeyCtx::new_array( ht, ctx_id, NULL, 8 );
   * if ( kctx == NULL ) fatal( "no memory" );
   * delete kctx; // same as free( kctx )
   */
  static KeyCtx * new_array( HashTab &t,  uint32_t xid,  void *b,
                             size_t bsz ) noexcept;
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  /* set key, this does not hash the key, use set_hash() afterwards */
  void set_key( KeyFragment &b ) { this->kbuf = &b; }
  /* set hash value, no key arg, usually done as:
   * kctx.set_hash( kctx.kbuf->hash( ht.hdr.hash_key_seed ) ),
   * this function is separate from the key because there may not be a key or
   * the hash may be precomputed using vector functions or client resources */
  void set_hash( uint64_t k,  uint64_t k2 ) noexcept;
  /* set key and hash by using set_key() then b.hash() to compute hash value */
  void set_key_hash( KeyFragment &b ) noexcept;
  /* change to another db */
  void set_db( uint32_t xid ) noexcept;
  /* used for find() operations where data is copied from ht to local buffers */
  void init_work( ScratchMem *a ) {
    if ( a != NULL ) {
      this->wrk = a;
      a->reset();
    }
  }
  /* bypass the work reset, keep already alloced data */
  void set_work( ScratchMem *a ) {
    this->wrk = a;
  }
  /* if hash entry is new, initialize with new serial, otherwise increment */
  void next_serial( uint64_t serial_mask ) {
    if ( this->lock == 0 ) /* new entry, init to key */
      this->serial = this->key & serial_mask;
    else
      this->serial++;
  }
  /* increment a bunch of serials, when several messages are appended */
  void more_serial( uint64_t count,  uint64_t serial_mask ) {
    if ( this->lock == 0 ) /* new entry, init to key */
      this->serial = ( this->key + count - 1 ) & serial_mask;
    else
      this->serial += count;
  }
  /* the total serial offset from origin */
  uint64_t get_serial_count( uint64_t serial_mask ) const {
    return ( this->serial - this->key ) & serial_mask;
  }
  /* set the next serial and return it */
  uint64_t get_next_serial( uint64_t serial_mask ) {
    this->next_serial( serial_mask );
    return this->get_serial_count( serial_mask );
  }
  void get_pos_value_ctr( uint64_t &pos,  ValueCtr &ctr ) const noexcept {
    pos = this->pos;
    ctr = this->entry->value_ctr( this->hash_entry_size );
  }
  bool if_value_equals( uint64_t pos,  const ValueCtr &ctr ) const noexcept;
  /* copy on read hash entry using this->wrk->alloc() */
  HashEntry *get_work_entry( void ) {
    return (HashEntry *) this->wrk->alloc( this->hash_entry_size );
  }
  /* copy value using this->wrk->alloc() */
  void *copy_data( void *data,  uint64_t sz ) noexcept;
  /* copy current key to KeyFragment reference */
  KeyStatus get_key( KeyFragment *&b ) noexcept;
  /* compare hash entry to kbuf, true if kbuf == NULL, when hash is perfect */
  bool equals( const HashEntry &el ) const {
    return el.hash2 == this->key2;
    /*if ( this->kbuf == NULL )
      return true;
    return this->frag_equals( el );*/
  }
  bool frag_equals( const HashEntry &el ) const noexcept;
  /* use __builtin_prefetch() on hash element using this->start as a base */
  void prefetch( bool for_read = true ) const noexcept;
  /* type indicates the data type of the value */
  uint8_t get_type( void ) const {
    return this->entry->type;
  }
  /* val is a memcached like external value */
  uint16_t get_val( void ) const {
    return this->entry->val;
  }
  /* the entry is tagged with the db it belongs to */
  uint8_t get_db( void ) {
    return this->entry->db;
  }
  /* set the value type */
  void set_type( uint8_t t ) {
    this->entry->type = t;
  }
  /* set the external memcached value (a application serial or a type) */
  void set_val( uint16_t v ) {
    this->entry->val = v;
  }
  /* size of the value */
  KeyStatus get_size( uint64_t &sz ) {
    if ( this->entry->test( FL_IMMEDIATE_VALUE ) )
      sz = this->entry->value_ctr( this->hash_entry_size ).size;
    else {
      if ( this->entry->test( FL_SEGMENT_VALUE ) )
        return this->get_msg_size( sz );
      sz = 0;
    }
    return KEY_OK;
  }
  /* if in a segment, this is the size */
  KeyStatus get_msg_size( uint64_t &sz ) noexcept;
  /* check that msg data is still valid for speculative reads */
  bool is_msg_valid( void ) noexcept;
  /* whether message data was updated during a read */
  KeyStatus validate_value( void ) {
    return ( this->msg == NULL || this->is_msg_valid() ) ? KEY_OK : KEY_MUTATED;
  }
  /* acquire lock for a key, if KEY_OK, set entry at &ht[ key % ht_size ] */
  KeyStatus acquire( ScratchMem *a ) {
    this->init_work( a );
    return this->acquire();
  }
  KeyStatus acquire( void ) noexcept;
  /* try to acquire lock for a key without waiting */
  KeyStatus try_acquire( ScratchMem *a ) {
    this->init_work( a );
    return this->try_acquire();
  }
  KeyStatus try_acquire( void ) noexcept;

  void init_acquire( void ) {
    this->chains    = 0; /* count of chains */
    this->drop_key  = 0;
    this->msg       = NULL; 
  }
  /* acquire using linear probing  */
  KeyStatus acquire_linear_probe( const uint64_t k,
                                  const uint64_t start_pos ) noexcept;
  /* acquire using cuckoo */
  KeyStatus acquire_cuckoo( const uint64_t k,
                            const uint64_t start_pos ) noexcept;
  /* multi key acquire using linear probing  */
  KeyStatus multi_acquire_linear_probe( const uint64_t k,
                                        const uint64_t start_pos ) noexcept;
  /* multi key acquire using cuckoo */
  KeyStatus multi_acquire_cuckoo( const uint64_t k,
                                  const uint64_t start_pos ) noexcept;
  /* acquire using linear probing  */
  KeyStatus try_acquire_linear_probe( const uint64_t k,
                                      const uint64_t start_pos ) noexcept;
  /* acquire using cuckoo */
  KeyStatus try_acquire_cuckoo( const uint64_t k,
                                const uint64_t start_pos ) noexcept;
  /* acquire using linear probing  */
  KeyStatus acquire_linear_probe_single_thread( const uint64_t k,
                                            const uint64_t start_pos ) noexcept;
  /* acquire using cuckoo */
  KeyStatus acquire_cuckoo_single_thread( const uint64_t k,
                                          const uint64_t start_pos ) noexcept;
  /* templated for ht acquire search */
  template <class Position, bool is_blocking>
  KeyStatus acquire( Position &next );
  /* templated for ht acquire single thread search */
  template <class Position>
  KeyStatus acquire_single_thread( Position &next );
  /* drop key after lock is acquired, deletes value data */
  /*KeyStatus drop( void );*/
  /* mark key dropped after lock is acquired, deletes value data */
  KeyStatus tombstone( void ) noexcept;
  /* like tombstone and incr expired */
  KeyStatus expire( void ) noexcept;
  /* state set during acquire */
  void copy_acquire_state( const KeyCtx &kctx );
  /* start a new read only operation */
  void init_find( void ) {
    this->chains = 0; /* count of chains */
    this->msg    = NULL;
    this->set( KEYCTX_IS_READ_ONLY );
  }
  /* if find locates key, returns KEY_OK, sets entry at &ht[ key % ht_size ] */
  KeyStatus find( ScratchMem *a ) {
    this->init_work( a );
    return this->find();
  }
  KeyStatus find( void ) noexcept;
  /* templated for ht find search */
  template <class Position>
  KeyStatus find( Position &next );
  template <class Position>
  KeyStatus find_single_thread( Position &next );
  /* find in ht using linear probing */
  KeyStatus find_linear_probe( const uint64_t k,
                               const uint64_t start_pos ) noexcept;
  KeyStatus find_cuckoo( const uint64_t k,
                         const uint64_t start_pos ) noexcept;
  /* find in ht using linear probing */
  KeyStatus find_linear_probe_single_thread( const uint64_t k,
                                            const uint64_t start_pos ) noexcept;
  KeyStatus find_cuckoo_single_thread( const uint64_t k,
                                       const uint64_t start_pos ) noexcept;
  /* get item at ht[ i ] */
  KeyStatus fetch( ScratchMem *a,  const uint64_t i,
                   const bool is_scan = false ) {
    this->init_work( a ); /* buffer used for copying hash entry & data */
    return this->fetch( i, is_scan );
  }
  KeyStatus fetch( const uint64_t i,  const bool is_scan ) {
    this->init_find();
    return this->fetch_position( i, is_scan );
  }
  KeyStatus fetch_position( const uint64_t i,  const bool is_scan ) noexcept;
  /* exclusive access to a position */
  KeyStatus try_acquire_position( const uint64_t i ) noexcept;

  struct CopyData {
    void    * data;
    uint64_t  size;
    MsgHdr  * msg;
    ValueGeom geom;
  };
  /* update the hash entry */
  KeyStatus update_entry( void *res,  uint64_t size,  HashEntry &el ) noexcept;
  /* allocate memory for hash, releases data that may be allocated */
  KeyStatus alloc( void *res,  uint64_t size,  bool copy = false ) noexcept;
  /* copy value segment location to hash entry */
  KeyStatus load( MsgCtx &msg_ctx ) noexcept;
  /* chain value segment location to hash entry at head */
  KeyStatus add_msg_chain( MsgCtx &msg_ctx ) noexcept;
  /* resizes memory, could return already alloced memory if fits,
   * does not copy data to newly allocated space (maybe it should) */
  KeyStatus resize( void *res,  uint64_t size,  bool copy = false ) noexcept;
  /* value returns KEY_OK if has data and set ptrs to a reference, either
   * immediate or segment ex: char *s; if ( kctx.get( &s, sz ) == KEY_OK )
   * printf( "%s\n", s ); if find() is used, ptr will not reference data in the
   * table, but copied data;  if acquire() is used, ptr will reference the shm
   * table data */
  KeyStatus value( void *ptr,  uint64_t &size ) noexcept;
  /* same as value() but must be write mode, increments serial for update */
  KeyStatus value_update( void *ptr,  uint64_t &size ) noexcept;

  /* append message size,
   * if stream size is larger than max_size, return KEY_MSG_LIST_FULL */
  KeyStatus append_msg( const void *res,  msg_size_t size,
                        uint64_t max_size = 0 ) {
    void *p = (void *) res;
    return this->append_vector( 1, &p, &size, max_size );
  }
  /* append a vector of count elems, where vec[ i ] -> msg @ size[ i ] */
  KeyStatus append_vector( uint64_t count,  void *vec,
                           msg_size_t *size,  uint64_t max_size = 0 ) noexcept;
  /* get the geoms of the chained messages */
  ValueGeom *get_msg_chain( uint8_t i,  ValueGeom &buf ) noexcept;
  /* fetch a messsge from a message list value */
  KeyStatus msg_value( uint64_t &from_idx,  uint64_t &to_idx,
                       void *data,  msg_size_t *size ) noexcept;
  /* move hash entry elements within the struct without losing values */
  KeyStatus reorganize_entry( HashEntry &el,  uint32_t new_fl ) noexcept;
  /* set the base seqno and remove msgs < idx */
  KeyStatus trim_msg( uint64_t idx ) noexcept;
  /* value + a copy of value header which is validated as current and
   * unmolested by mutators */
  KeyStatus value_copy( void *ptr,  uint64_t &size,  void *cp,
                        uint64_t &cplen ) noexcept;
  /* update timestamp if not zero */
  KeyStatus update_stamps( uint64_t exp_ns,  uint64_t upd_ns ) noexcept;
  /* clear one or both timestamps */
  KeyStatus clear_stamps( bool clr_exp,  bool clr_upd ) noexcept;
  /* get stamps, zero if don't exist */
  KeyStatus get_stamps( uint64_t &exp_ns,  uint64_t &upd_ns ) noexcept;
  /* check if has a stamp */
  bool is_expired( void ) {
    return this->entry->test( FL_EXPIRE_STAMP ) != 0 &&
           this->check_expired() == KEY_EXPIRED;
  }
  /* test if hash entry expire timer < ht.hdr.current_stamp, then KEY_EXPIRED */
  KeyStatus check_expired( void ) noexcept;
  /* test if hash entry updaste timer < age, if true then return KEY_EXPIRED */
  KeyStatus check_update( uint64_t age_ns ) noexcept;
  /* release the data used by entry */
  KeyStatus release_data( void ) noexcept;
  /* release the data used by dropped entry when chain == max_chains */
  /*KeyStatus release_evict( void );*/
  /* set msg field to the segment data */
  enum AttachType { ATTACH_READ = 0, ATTACH_WRITE = 1 };
  /* fetch and set this->msg, validate it */
  KeyStatus attach_msg( AttachType upd ) noexcept;
  /* get any msg data attached to current key, copy and validate it */
  MsgHdr *get_chain_msg( ValueGeom &cgeom ) noexcept;
  /* crc the message */
  void seal_msg( void ) noexcept;
  /* release the hash entry */
  void release( void ) noexcept;
  /* release the hash entry */
  void release_single_thread( void ) noexcept;
  /* get the position info for the current key */
  void get_pos_info( uint64_t &natural_pos,  uint64_t &pos_offset ) noexcept;
  /* distance between x and y, where y is a linear probe position after x */
  static uint64_t calc_offset( uint64_t x,  uint64_t y,  uint64_t ht_size ) {
    if ( y >= x )
      return y - x;
    return y + ht_size - x;
  }
};

typedef uint64_t KeyCtxBuf[ sizeof( KeyCtx ) / sizeof( uint64_t ) ];
typedef uint32_t msg_size_t;

struct MsgIter {
  KeyCtx    & kctx;
  uint8_t   * buf;
  uint64_t    msg_off,
              buf_size,
              seqno;
  msg_size_t  msg_size;
  MsgHdr    * msg;
  uint8_t     chain_num;
  KeyStatus   status;

  MsgIter( KeyCtx &k ) : kctx( k ), buf( 0 ), msg_off( 0 ), buf_size( 0 ),
    seqno( 0 ), msg_size( 0 ), msg( 0 ), chain_num( 0 ), status( KEY_OK ) {}
  void setup( void *b,  uint64_t sz ) {
    this->buf      = (uint8_t *) b;
    this->buf_size = sz;
    this->status   = KEY_OK;
  }

  bool init( uint64_t idx ) noexcept;
  void trim_old_chains( void ) noexcept;
  bool seek( uint64_t &idx ) noexcept;
  bool first( void ) noexcept;
  bool next( void ) noexcept;

  void get_msg( msg_size_t &sz,  void *&b ) {
    sz = this->msg_size;
    b  = &this->buf[ this->msg_off + sizeof( msg_size_t ) ];
  }
};

} /* namespace kv */
} /* namespace rai */
#endif /* __cplusplus */

#ifdef __cplusplus
extern "C" {
#endif
/* uint16_t buf[ 1024 ];
 * void *in = (void *) buf, *out;
 * kv_key_frag_t *frag[ 2 ];
 * size_t sz = sizeof( buf );
 * frag[ 0 ] = kv_make_key_frag( 128, sz, in, &out );
 * in = out; sz = (uint8_t *) &buf[ sizeof( buf ) ] - (uint8_t *) out;
 * frag[ 1 ] = kv_make_key_frag( 128, sz, in, &out );
 * kv_set_key_frag_string( frag[ 0 ], "hello world", 11 );
 * // not allocated, no free routine, unsafe stuff ahead */
kv_key_frag_t *kv_make_key_frag( uint16_t sz,  size_t avail_in,
                                 void *in,  void *out );
size_t kv_get_key_frag_mem_size( kv_key_frag_t *frag );
/* this does not check that sz fits in frag, there is no buf length to check */
void kv_set_key_frag_bytes( kv_key_frag_t *frag,  const void *p,  uint16_t sz );
/* this null terminates a key string */
void kv_set_key_frag_string( kv_key_frag_t *frag,  const char *s, 
                             uint16_t slen /* strlen(s) */ );
void kv_hash_key_frag( kv_hash_tab_t *ht,  kv_key_frag_t *frag,
                       uint64_t *k,  uint64_t *k2 );

kv_key_ctx_t *kv_create_key_ctx( kv_hash_tab_t *ht,  uint32_t xid );
void kv_release_key_ctx( kv_key_ctx_t *kctx );

void kv_set_key( kv_key_ctx_t *kctx,  kv_key_frag_t *kbuf );
void kv_set_hash( kv_key_ctx_t *kctx,  uint64_t k,  uint64_t k2 );
void kv_prefetch( kv_key_ctx_t *kctx,  uint8_t for_read );

/* status = kv_acquire( kctx );
 * if ( status == KEY_OK ) {
 *   uint64_t * mem;
 *   status = kv_alloc( kctx, &mem, sizeof( uint64_t ) * 2, sizeof( uint64_t ));
 *   if ( status == KEY_OK ) {
 *     mem[ 0 ] = 10001;
 *     mem[ 1 ] = 10002;
 *   }
 * }
 * kv_release( kctx );
 */
kv_key_status_t kv_acquire( kv_key_ctx_t *kctx,  kv_work_alloc_t *a );
kv_key_status_t kv_try_acquire( kv_key_ctx_t *kctx,  kv_work_alloc_t *a );
/*kv_key_status_t kv_drop( kv_key_ctx_t *kctx );*/
kv_key_status_t kv_tombstone( kv_key_ctx_t *kctx );
void kv_release( kv_key_ctx_t *kctx );

kv_key_status_t kv_find( kv_key_ctx_t *kctx,  kv_work_alloc_t *a );
kv_key_status_t kv_fetch( kv_key_ctx_t *kctx,  kv_work_alloc_t *a,
                          const uint64_t pos );
kv_key_status_t kv_value( kv_key_ctx_t *kctx,  void *ptr,  uint64_t *size );

kv_key_status_t kv_alloc( kv_key_ctx_t *kctx,  void *ptr,  uint64_t size );
kv_key_status_t kv_load( kv_key_ctx_t *kctx,  kv_msg_ctx_t *mctx );
kv_key_status_t kv_resize( kv_key_ctx_t *kctx,  void *ptr,  uint64_t size );
kv_key_status_t kv_release_data( kv_key_ctx_t *kctx );
#ifdef __cplusplus
} /* extern "C" */
#endif

#endif
