#ifndef __rai__raikv__key_ctx_h__
#define __rai__raikv__key_ctx_h__

#include <raikv/util.h>
#include <raikv/hash_entry.h>
#include <raikv/work.h>

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
  KEY_SEG_FULL      = 9,  /* no space in allocation segments */
  KEY_TOO_BIG       = 10, /* key + value + alignment is too big (> seg_size) */
  KEY_SEG_VALUE     = 11, /* value doesn't fit in immmediate data */
  KEY_TOMBSTONE     = 12, /* key not valid, was dropped */
  KEY_PART_ONLY     = 13, /* no key attached, hashes only */
  KEY_MAX_CHAINS    = 14, /* nothing found before entry count hit max chains */
  KEY_PATH_SEARCH   = 15, /* need a path search to acquire cuckoo entry */
  KEY_USE_DROP      = 16, /* ok to use drop, end of chain */
  KEY_MAX_STATUS    = 17  /* maximum status code */
} kv_key_status_t;

/* string versions of the above */
const char *kv_key_status_string( kv_key_status_t status );
const char *kv_key_status_description( kv_key_status_t status );

#ifdef __cplusplus
} /* extern "C" */
#endif

#ifdef __cplusplus
namespace rai {
namespace kv {

typedef enum kv_key_status_e KeyStatus;

struct HashTab;
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
  KEYCTX_IS_READ_ONLY      = 1, /* result of find(), etc.. no lock acq */
  KEYCTX_IS_GC_ACQUIRE     = 2, /* if GC is trying to acquire for moving */
  KEYCTX_IS_CUCKOO_ACQUIRE = 4, /* if Cuckoo relocate is trying to make space */
  KEYCTX_IS_HT_EVICT       = 8, /* if chains == max_chains, is eviction */
  KEYCTX_IS_SINGLE_THREAD  = 16 /* don't use thread synchronization */
};

struct KeyCtx {
  HashTab      & ht;      /* operates on this table */
  KeyFragment  * kbuf;    /* key to lookup resolve */
  const uint32_t ctx_id,  /* which thread owns this this context */
                 hash_entry_size;
  const uint64_t ht_size;
  const uint16_t cuckoo_buckets; /* how many cuckoo buckets */
  const uint8_t  cuckoo_arity;   /* how many cuckoo hash functions */
  uint8_t        inc;        /* which hash function: 0 -> cuckoo_arity */
  uint16_t       drop_flags, /* flags from dropped recycle entry */
                 flags;      /* KeyCtxFlags */
  uint64_t       max_chains, /* drop entries after accumulating max chains */
                 chains,     /* number of chains used to find/acquire */
                 start,   /* key % ht_size */
                 key,     /* KeyBuf hash() */
                 key2,    /* KeyBuf hash() */
                 pos,     /* position of entry */
                 lock,    /* value stored in ht[], either zero or == key */
                 drop_key,/* the dropped key that is being recycled */
                 drop_key2,/* the dropped key2 */
                 mcs_id,  /* id of lock queue for above ht lock */
                 serial,  /* serial number of the hash ent & message */
                 update_ns, /* absolute ns time when updated, 0 is unset */
                 expire_ns; /* absolute ns time when expires, 0 is unset */
  HashEntry    * entry;   /* the entry after lookup, may be empty entry if NF*/
  MsgHdr       * msg;     /* the msg header indexed by geom */
  ValueGeom      geom;    /* values decoded from HashEntry */
  ScratchMem   * wrk;     /* temp work allocation */

  uint16_t test( uint16_t fl ) const { return ( this->flags & fl ); }
  void set( uint16_t fl )            { this->flags |= fl; }
  void clear( uint16_t fl )          { this->flags &= ~fl; }

  KeyCtx( HashTab &t,  uint32_t id,  KeyFragment &b );
  KeyCtx( HashTab &t,  uint32_t id,  KeyFragment *b = NULL );
  ~KeyCtx() {}

  /* placement new to deal with broken c++ new[], for example:
   * KeyCtxBuf kctxbuf[ 8 ];
   * KeyCtx * kctx = KeyCtx::new_array( ht, ctx_id, kctxbuf, 8 );
   * or to use malloc() instead of stack:
   * KeyCtx * kctx = KeyCtx::new_array( ht, ctx_id, NULL, 8 );
   * if ( kctx == NULL ) fatal( "no memory" );
   * delete kctx; // same as free( kctx )
   */
  static KeyCtx * new_array( HashTab &t,  uint32_t id,  void *b,  size_t bsz );

  void * operator new( size_t sz, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  /* set key, this does not hash the key, use set_hash() afterwards */
  void set_key( KeyFragment &b ) { this->kbuf = &b; }
  /* set hash value, no key arg, usually done as:
   * kctx.set_hash( kctx.kbuf->hash( ht.hdr.hash_key_seed ) ),
   * this function is separate from the key because there may not be a key or
   * the hash may be precomputed using vector functions or client resources */
  void set_hash( uint64_t k,  uint64_t k2 );
  /* set key and hash by using set_key() then b.hash() to compute hash value */
  void set_key_hash( KeyFragment &b );
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
  /* copy on read hash entry using this->wrk->alloc() */
  HashEntry *get_work_entry( void ) {
    return (HashEntry *) this->wrk->alloc( this->hash_entry_size );
  }
  /* copy value using this->wrk->alloc() */
  void *copy_data( void *data,  uint64_t sz );
  /* copy current key to KeyFragment reference */
  KeyStatus get_key( KeyFragment *&b );
  /* compare hash entry to kbuf, true if kbuf == NULL, when hash is perfect */
  bool equals( const HashEntry &el ) const {
    return el.hash2 == this->key2;
    /*if ( this->kbuf == NULL )
      return true;
    return this->frag_equals( el );*/
  }
  bool frag_equals( const HashEntry &el ) const;
  /* use __builtin_prefetch() on hash element using this->start as a base */
  void prefetch( uint64_t cnt = 2 ) const;
  uint8_t get_type( void ) {
    return this->entry->value_ctr( this->hash_entry_size ).type;
  }
  void set_type( uint8_t type ) {
    this->entry->value_ctr( this->hash_entry_size ).type = type;
  }
  /* acquire lock for a key, if KEY_OK, set entry at &ht[ key % ht_size ] */
  KeyStatus acquire( ScratchMem *a ) {
    this->init_work( a );
    return this->acquire();
  }
  KeyStatus acquire( void );
  /* try to acquire lock for a key without waiting */
  KeyStatus try_acquire( ScratchMem *a ) {
    this->init_work( a );
    return this->try_acquire();
  }
  KeyStatus try_acquire( void );

  void init_acquire( void ) {
    this->chains     = 0; /* count of chains */
    this->drop_key   = 0;
    this->update_ns  = 0; /* loaded on demand */
    this->expire_ns  = 0;
    this->msg        = NULL; 
  }
  /* acquire using linear probing  */
  KeyStatus acquire_linear_probe( const uint64_t k,  const uint64_t start_pos );
  /* acquire using cuckoo */
  KeyStatus acquire_cuckoo( const uint64_t k,  const uint64_t start_pos );
  /* acquire using linear probing  */
  KeyStatus try_acquire_linear_probe( const uint64_t k,
                                      const uint64_t start_pos );
  /* acquire using cuckoo */
  KeyStatus try_acquire_cuckoo( const uint64_t k,  const uint64_t start_pos );
  /* acquire using linear probing  */
  KeyStatus acquire_linear_probe_single_thread( const uint64_t k,
                                                const uint64_t start_pos );
  /* acquire using cuckoo */
  KeyStatus acquire_cuckoo_single_thread( const uint64_t k,
                                          const uint64_t start_pos );
  /* templated for ht acquire search */
  template <class Position, bool is_blocking>
  KeyStatus acquire( const uint64_t k,  uint64_t i,  Position &next );
  /* templated for ht acquire single thread search */
  template <class Position>
  KeyStatus acquire_single_thread( const uint64_t k,  uint64_t i,
                                   Position &next );
  /* drop key after lock is acquired, deletes value data */
  /*KeyStatus drop( void );*/
  /* mark key dropped after lock is acquired, deletes value data */
  KeyStatus tombstone( void );
  /* start a new read only operation */
  void init_find( void ) {
    this->chains    = 0; /* count of chains */
    this->update_ns = 0; /* loaded on demand */
    this->expire_ns = 0;
    this->msg       = NULL;
    this->set( KEYCTX_IS_READ_ONLY );
  }
  /* if find locates key, returns KEY_OK, sets entry at &ht[ key % ht_size ] */
  KeyStatus find( ScratchMem *a,  const uint64_t spin_wait = 0 ) {
    this->init_work( a );
    return this->find( spin_wait );
  }
  KeyStatus find( const uint64_t spin_wait = 0 );
  /* templated for ht find search */
  template <class Position>
  KeyStatus find( const uint64_t k,  uint64_t i,  const uint64_t spin_wait,
                  Position &next );
  template <class Position>
  KeyStatus find_single_thread( const uint64_t k, uint64_t i,  Position &next );
  /* find in ht using linear probing */
  KeyStatus find_linear_probe( const uint64_t k,  const uint64_t start_pos,
                               const uint64_t spin_wait );
  KeyStatus find_cuckoo( const uint64_t k,  const uint64_t start_pos,
                         const uint64_t spin_wait );
  /* find in ht using linear probing */
  KeyStatus find_linear_probe_single_thread( const uint64_t k,
                                             const uint64_t start_pos );
  KeyStatus find_cuckoo_single_thread( const uint64_t k,
                                       const uint64_t start_pos );
  /* get item at ht[ i ] */
  KeyStatus fetch( ScratchMem *a,  const uint64_t i,
                   const uint64_t spin_wait = 0 ) {
    this->init_work( a ); /* buffer used for copying hash entry & data */
    return this->fetch( i, spin_wait );
  }
  KeyStatus fetch( const uint64_t i,  const uint64_t spin_wait ) {
    this->init_find();
    return this->fetch_position( i, spin_wait );
  }
  KeyStatus fetch_position( const uint64_t i,  const uint64_t spin_wait );
  /* exclusive access to a position */
  KeyStatus try_acquire_position( const uint64_t i );
  /* value returns KEY_OK if has data and set ptrs to a reference, either
   * immediate or segment ex: char *s; if ( kctx.get( &s, sz ) == KEY_OK )
   * printf( "%s\n", s ); if find() is used, ptr will not reference data in the
   * table, but copied data;  if acquire() is used, ptr will reference the shm
   * table data */
  KeyStatus value( void *ptr,  uint64_t &size );
  /* copy update & expire timestamps into hash entry */
  void update_stamps( void );

  struct CopyData {
    void    * data;
    uint64_t  size;
    MsgHdr  * msg;
    ValueGeom geom;
  };
  /* update the hash entry */
  KeyStatus update_entry( void *res,  uint64_t size,  uint8_t alignment,
                          HashEntry &el,  CopyData *copy );
  /* allocate memory for hash, releases data that may be allocated, alignment
   * is a size -- sizeof( int64_t ) for example */
  KeyStatus alloc( void *res,  uint64_t size,  bool copy = false,
                   uint8_t alignment = 8 );
  /* copy value segment location to hash entry */
  KeyStatus load( MsgCtx &msg_ctx );
  /* resizes memory, could return already alloced memory if fits,
   * does not copy data to newly allocated space (maybe it should) */
  KeyStatus resize( void *res,  uint64_t size,  bool copy = false,
                    uint8_t alignment = 8 );
  /* release the data used by entry */
  KeyStatus release_data( void );
  /* release the data used by dropped entry when chain == max_chains */
  KeyStatus release_evict( void );
  /* get update time, returns KEY_NOT_FOUND when no update time is attached */
  KeyStatus get_update_time( uint64_t &update_time_ns );
  /* get expire time, returns KEY_NOT_FOUND when no expire time is attached */
  KeyStatus get_expire_time( uint64_t &expire_time_ns );
  /* set msg field to the segment data */
  enum AttachType { ATTACH_READ = 0, ATTACH_WRITE = 1 };
  KeyStatus attach_msg( AttachType upd );
  /* crc the message */
  void seal_msg( void );
  /* release the hash entry */
  void release( void );
  /* release the hash entry */
  void release_single_thread( void );
  /* get the position info for the current key */
  void get_pos_info( uint64_t &natural_pos,  uint64_t &pos_offset );
  /* distance between x and y, where y is a linear probe position after x */
  static uint64_t calc_offset( uint64_t x,  uint64_t y,  uint64_t ht_size ) {
    if ( y >= x )
      return y - x;
    return y + ht_size - x;
  }
};

typedef uint64_t KeyCtxBuf[ sizeof( KeyCtx ) / sizeof( uint64_t ) ];

} /* namespace kv */
} /* namespace rai */
#endif /* __cplusplus */

#ifdef __cplusplus
extern "C" {
#endif
struct kv_hash_tab_s;
struct kv_key_ctx_s;
struct kv_msg_ctx_s;
struct kv_key_frag_s;

typedef struct kv_hash_tab_s kv_hash_tab_t;
typedef struct kv_key_ctx_s  kv_key_ctx_t;
typedef struct kv_msg_ctx_s  kv_msg_ctx_t;

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

kv_key_ctx_t *kv_create_key_ctx( kv_hash_tab_t *ht,  uint32_t ctx_id );
void kv_release_key_ctx( kv_key_ctx_t *kctx );

void kv_set_key( kv_key_ctx_t *kctx,  kv_key_frag_t *kbuf );
void kv_set_hash( kv_key_ctx_t *kctx,  uint64_t k,  uint64_t k2 );
void kv_prefetch( kv_key_ctx_t *kctx,  uint64_t cnt );

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

kv_key_status_t kv_find( kv_key_ctx_t *kctx,  kv_work_alloc_t *a,
                         const uint64_t spin_wait );
kv_key_status_t kv_fetch( kv_key_ctx_t *kctx,  kv_work_alloc_t *a,
                          const uint64_t pos,  const uint64_t spin_wait );
kv_key_status_t kv_value( kv_key_ctx_t *kctx,  void *ptr,  uint64_t *size );

kv_key_status_t kv_alloc( kv_key_ctx_t *kctx,  void *ptr,  uint64_t size,
                          uint8_t alignment );
kv_key_status_t kv_load( kv_key_ctx_t *kctx,  kv_msg_ctx_t *mctx );
kv_key_status_t kv_resize( kv_key_ctx_t *kctx,  void *ptr,  uint64_t size,
                           uint8_t alignment );
kv_key_status_t kv_release_data( kv_key_ctx_t *kctx );

void kv_set_update_time( kv_key_ctx_t *kctx,  uint64_t update_time_ns );
void kv_set_expire_time( kv_key_ctx_t *kctx,  uint64_t expire_time_ns );
kv_key_status_t kv_get_update_time( kv_key_ctx_t *kctx,
                                    uint64_t *update_time_ns );
kv_key_status_t kv_get_expire_time( kv_key_ctx_t *kctx,
                                    uint64_t *expire_time_ns );
#ifdef __cplusplus
} /* extern "C" */
#endif

#endif
