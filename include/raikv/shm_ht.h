#ifndef __rai__raikv__shm_ht_h__
#define __rai__raikv__shm_ht_h__

/* also include stdint.h, string.h */
#include <raikv/atom.h>
#include <raikv/util.h>
#include <raikv/ht_stats.h>
#include <raikv/hash_entry.h>
#include <raikv/msg_ctx.h>

#ifdef __cplusplus
extern "C" {
#endif
typedef struct kv_geom_s {
  uint64_t map_size;         /* size of memory used by shm */
  uint32_t max_value_size,   /* max size of an data entry */
           hash_entry_size;  /* size of a hash entry, min 32b */
  float    hash_value_ratio; /* ratio of hash/data cells: hash = ratio * size */
  uint16_t cuckoo_buckets;   /* how many buckets for each hash */
  uint8_t  cuckoo_arity;     /* how many hash functions */
} kv_geom_t;

typedef enum kv_facility_e {
  KV_POSIX_SHM = 1,  /* use shm_open(), mmap() (p:shm, q:shm2m, r:shm1g) */
  KV_FILE_MMAP = 2,  /* use open(), mmap()     (f:file, g:file2m, h:file1g) */
  KV_SYSV_SHM  = 4,  /* use shmget(), shmat()  (v:sysv, w:sysv2m, x:sysv1g) */
  KV_HUGE_2MB  = 8,  /* use 2mb pages */
  KV_HUGE_1GB  = 16  /* use 1gb pages */
} kv_facility_t;

/* +----- +-----
 * | Hash | FileHdr 64 * 3     = 192
 * | Hdr  |   lockq            = 832                 -> 1 K | HT_FILE_HDR_SIZE
 * |      +-----
 * |      | DBHdr
 * |      |   seed[ 256 ]       = 256 * 16  = 4 K
 * |      |   HashStats[ 256 ]  = 256 * 128 = 32 K ( pad 12 K )
 * |      |   ThrDBStat[ 1024 ] = 1024 * 16 = 16 K   -> 64 K | DB_HDR_SIZE
 * |      +----
 * |      | Segment[ 2032 ] * 64 = 130048            -> 127 K  (192 - (1+64))
 * |      |                                          == 192 K HT_HDR_SIZE
 * +------+----
 * | ThrCtx[ 128 ]
 * |   ThrCtxHdr        = 64
 * |   ThrMCSLock[30]   = 960 -> 1024 - 64           == 128 K HT_CTX_SIZE
 * | HashStats[ 1024 ]  = 128 * 1024                 == 128 K HT_STATS_SIZE
 * +-----
 */

/* used as error return for kv_attach_ctx() */
#define KV_NO_CTX_ID        ((uint32_t) -1)
/* used as error return for kv_attach_db() */
#define KV_NO_DBSTAT_ID     ((uint32_t) -1)
/* hdr of map file */
#define KV_HT_FILE_HDR_SIZE 1024
/* how big each thread context is */
#define KV_HT_THR_CTX_SIZE  1024
/* file hdr + segment data */
#define KV_HT_HDR_SIZE      ( 192 * 1024 )
/* how much space for thread contexts */
#define KV_HT_CTX_SIZE      ( 128 * 1024 )
/* space for db stats */
#define KV_DB_HDR_SIZE      ( 64 * 1024 )
/* max db count */
#define KV_DB_COUNT         256
/* max ctx db open */
#define KV_STAT_COUNT       1024
/* ht stats count * size */
#define KV_HT_STATS_SIZE    ( 128 * 1024 )
/* the maximum thread context id */
#define KV_MAX_CTX_ID       ( KV_HT_CTX_SIZE / KV_HT_THR_CTX_SIZE )
/* shm_attach( shm_string ) */
#define KV_DEFAULT_SHM      "sysv:raikv.shm"
/* sizeof magic at first byte */
#define KV_SIG_SIZE         16

#ifdef __cplusplus
}
#endif

#ifdef __cplusplus
namespace rai {
namespace kv {

                    /* sizeof( FileHdr ) */
static const size_t HT_FILE_HDR_SIZE      = KV_HT_FILE_HDR_SIZE, /* 1024 */
                    /* sizeof( ThrCtx ) */
                    HT_THR_CTX_SIZE       = KV_HT_THR_CTX_SIZE,  /* 1024 */
                    /* FileHdr( 1024b ) + Segment[ 2032 ] each 64b */
                    HT_HDR_SIZE           = KV_HT_HDR_SIZE,      /* 192 k */
                    /* ThrCtx[ 128 ] each 1024b containing ThrMCSLock[ 56 ] */
                    HT_CTX_SIZE           = KV_HT_CTX_SIZE,      /* 128 k */
                    HT_STATS_SIZE         = KV_HT_STATS_SIZE,    /* 128 k */
                    DB_HDR_SIZE           = KV_DB_HDR_SIZE,      /* 64 k */
                    DB_COUNT              = KV_DB_COUNT,         /* 256 */
                    /* ThrCtx[] size */
                    MAX_CTX_ID            = HT_CTX_SIZE /        /* 128 k / */
                                            HT_THR_CTX_SIZE,     /* 1024 = 128*/
                    MAX_STAT_ID           = KV_STAT_COUNT,
                    /* FileHdr::name[] size */
                    MAX_FILE_HDR_NAME_LEN = 64/*sig hdr*/ - 16/*sig*/;
/* hdr size should be 256b */
struct FileHdr {
             /* first 64b, not referenced very much */
  char       sig[ KV_SIG_SIZE ];             /* version info and mem type */
  char       name[ MAX_FILE_HDR_NAME_LEN ];  /* name of this map file */

             /* second 64b, updated infreq, load calculations and stamps */
  uint64_t   last_entry_count;       /* sum of entry counts in ctx[] */
  float      hash_value_ratio,       /* ratio of ht[] mem to values mem */
             ht_load,                /* hash entry used / ht_size */
             value_load;             /* seg data used / seg size */
  uint8_t    load_percent,           /* current_load * 100 / critical_load */
             critical_load;      
  uint16_t   padh;
  AtomUInt16 next_ctx;               /* next free ctx[] */
  AtomUInt16 ctx_used;               /* number of ctx used */
  uint32_t   max_immed_value_size;   /* sizeof value in entry, including key */
  uint64_t   max_segment_value_size, /* sizeof value in segment, inc key */
             current_stamp,          /* cached current time, used for expire */
                                     /* time evictions, not high resolution */
             create_stamp,    /* time created (nanosecs), base timestamp */
             map_size,        /* map size from geom */

             /* third 64b useful read only data, referenced a lot */
             ht_size,         /* calculated size of ht[] */
             pad1,
             pad2,
             ht_mod_mask,     /* mask of bits used for mod */
             ht_mod_fraction; /* fraction of mask used in ht */
  uint32_t   seg_size_val,    /* size of segment[] ( val << seg_align_shift ) */
             seg_start_val,   /* offset of first segment ( val << seg_align ) */
             hash_entry_size, /* size of each hash table entry */
             max_value_size;  /* max value size from geom */
  uint16_t   nsegs,           /* count of segments */
             cuckoo_buckets;  /* number of buckets per hash function */
  uint8_t    seg_align_shift, /* 1 << 6 = 64, align size for vals in segment */
             log2_ht_size,    /* ht_size <= 1 << log2_ht_size */
             ht_mod_shift,    /* mod calc: ( ( k & mask ) * frac ) >> shift */
             cuckoo_arity;    /* number of hash functions used, <= 1 linear */
  /* when db attached to thr ctx[], db_opened[] is set */
  static const uint64_t DB_OPENED_SIZE = DB_COUNT / 64;
  uint64_t   db_opened[ DB_OPENED_SIZE ];
  /* spin locks */
  static const uint64_t LOCKQ_SIZE =
    ( KV_HT_FILE_HDR_SIZE - 64 * 3 ) / sizeof( uint64_t ) - DB_OPENED_SIZE;
  uint64_t   lockq[ LOCKQ_SIZE ];

  /* max( ht load, value load ) */
  float current_load( void ) const {
    return this->ht_load > this->value_load ? this->ht_load : this->value_load;
  }
  /* value segment alignment and sizes (usually is 64 byte aligned) */
  uint64_t seg_align( void ) const {
    return (uint64_t) 1 << this->seg_align_shift;
  }
  uint64_t seg_size( void ) const {
    return (uint64_t) this->seg_size_val << this->seg_align_shift;
  }
  uint64_t seg_start( void ) const {
    return (uint64_t) this->seg_start_val << this->seg_align_shift;
  }
  /* fixed point mod() */
  uint64_t ht_mod( const uint64_t k ) const {
    return ( ( k & this->ht_mod_mask ) * this->ht_mod_fraction ) >>
           this->ht_mod_shift;
  }
  bool test_db_opened( const uint8_t db ) {
    volatile uint64_t &ptr = this->db_opened[ ( db >> 6 ) % DB_OPENED_SIZE ];
    uint64_t mask = (uint64_t) 1 << ( db & 63 );
    return ( ptr & mask ) != 0;
  }
  void set_db_opened( const uint8_t db ) {
    volatile uint64_t &ptr = this->db_opened[ ( db >> 6 ) % DB_OPENED_SIZE ];
    uint64_t mask = (uint64_t) 1 << ( db & 63 );
    while ( ( ptr & mask ) == 0 )
      kv_sync_bit_try_lock( &ptr, mask );
  }
  /* spin locks using the lockq[] bits in the header, cacheline[ 13 ] at off 3*/
  bool ht_spin_trylock( const uint64_t id ) {
    return kv_sync_bit_try_lock( &this->lockq[ ( id >> 6 ) % LOCKQ_SIZE ],
                                 (uint64_t) 1 << ( id & 63 ) );
  }
  /* lock for protecting db_stat[ id ] */
  void ht_spin_lock( const uint64_t id ) {
    return kv_sync_bit_spin_lock( &this->lockq[ ( id >> 6 ) % LOCKQ_SIZE ],
                                  (uint64_t) 1 << ( id & 63 ) );
  }
  void ht_spin_unlock( const uint64_t id ) {
    return kv_sync_bit_spin_unlock( &this->lockq[ ( id >> 6 ) % LOCKQ_SIZE ],
                                    (uint64_t) 1 << ( id & 63 ) );
  }
  /* lock 64 at a time, for protecting db_stat[ id .. id + 64 ] */
  void ht_spin_lock64( const uint64_t id ) {
    volatile uint64_t &ptr = this->lockq[ ( id >> 6 ) % LOCKQ_SIZE ];
    uint64_t old_val = 0;
    for (;;) {
      while ( ptr != 0 )
        kv_sync_pause();
      if ( kv_sync_cmpxchg( &ptr, old_val, ~(uint64_t) 0 ) ) {
        kv_acquire_fence();
        return;
      }
    }
  }
  void ht_spin_unlock64( const uint64_t id ) {
    kv_release_fence();
    this->lockq[ ( id >> 6 ) % LOCKQ_SIZE ] = 0;
  }
};

typedef struct kv_geom_s HashTabGeom;

struct ThrCtxOwner; /* MCSLock used for hash table entry ownership */
struct ThrMCSLock : public MCSLock<uint64_t, ThrCtxOwner> {};

struct ThrCtxHdr {
  AtomUInt64             key; /* zero free, zombie32 dropped, otherwise used */
  uint64_t               mcs_used;  /* bit mask of used locks (64>MCS_CNT=30) */
  uint32_t               ctx_id,    /* the ctx id that holds a ht[] locks */
                         ctx_pid,   /* process id (getpid())*/
                         ctx_thrid, /* thread id (syscall(SYS_gettid)) */
                         db_stat_hd,/* list of db stat */
                         db_stat_tl,
                         ctx_seqno, /* least recently used counter */
                         pad1;
  uint16_t               seg_num,   /* use seg until exhausted */
                         ctx_flags; /* whether busy or need signal */
  rand::xoroshiro128plus rng;       /* rand state initialized on creation */
  /* 4*7=28(int32) 2*2=4(int16) + 8*2=16(int64) + 16(rng) = 64 */
};

struct ThrCtxEntry : public ThrCtxHdr {
  static const uint32_t MCS_CNT  = /* 1024 - 64 = 960 / 32 = 30 mcs */
    ( HT_THR_CTX_SIZE - sizeof( ThrCtxHdr ) ) / sizeof( ThrMCSLock );
  static const uint32_t MCS_SHIFT = 16,
                        MCS_MASK  = ( 1 << MCS_SHIFT ) - 1;
  ThrMCSLock mcs[ MCS_CNT ]; /* a queue of ctx waiting for ht.entry[ x ] */

  void zero( void ) {
    this->mcs_used = 0;
    ::memset( this->mcs, 0, sizeof( this->mcs ) );
  }
};

struct ThrCtx : public ThrCtxEntry { /* each thread needs one of these */
#if __cplusplus >= 201103L
  /* 1024b align */
  static_assert( HT_THR_CTX_SIZE == sizeof( ThrCtxEntry ), "ctx hdr size" );
#endif
  uint64_t next_mcs_lock( void ) {
    uint32_t id = ( this->mcs_used == 0 ) ? 0 :
                  ( 64 - __builtin_clzl( this->mcs_used ) );
    for ( ; ; id++ ) {
      if ( id >= MCS_CNT )
        id = 0;
      if ( ( this->mcs_used & ( (uint64_t) 1 << id ) ) == 0 ) {
        this->mcs_used |= ( (uint64_t) 1 << id );
        return ( this->ctx_id << MCS_SHIFT ) | id;
      }
    }
  }
  void release_mcs_lock( uint64_t mcs_id ) {
    this->mcs_used &= ~( (uint64_t) 1 << ( mcs_id & MCS_MASK ) );
  }
  ThrMCSLock &get_mcs_lock( uint64_t mcs_id ) {
    return this->mcs[ mcs_id & MCS_MASK ];
  }
  bool is_my_lock( uint64_t pos ) const {
    uint64_t used = this->mcs_used;
    uint8_t  i = 0;
    if ( used == 0 )
      return false;
    if ( ( used & 1 ) != 0 )
      if ( pos + 1 == this->mcs[ 0 ].lock_id )
        return true;
    for (;;) {
      uint64_t x = used >> ++i;
      if ( ( x & 1 ) == 0 ) {
        if ( x == 0 )
          return false;
        i += __builtin_ffsl( x ) - 1;
      }
      if ( pos + 1 == this->mcs[ i ].lock_id )
        return true;
    }
  }
};

struct ThrCtxOwner { /* closure for MCSLock to find the owner of a lock */
  ThrCtx * ctx;
  ThrCtxOwner( ThrCtx *p ) : ctx( p ) {}
  ThrMCSLock& owner( const uint64_t mcs_id ) {
    return this->ctx[ mcs_id >> ThrCtxEntry::MCS_SHIFT ].get_mcs_lock( mcs_id );
  }
  bool is_active( uint64_t mcs_id ) {
    uint32_t ctx_id = ( mcs_id >> ThrCtxEntry::MCS_SHIFT );
    if ( ctx_id >= MAX_CTX_ID )
      return false;
    return ( this->ctx[ ctx_id ].mcs_used &
             ( (uint64_t) 1 << ( mcs_id & ThrCtxEntry::MCS_MASK ) ) ) != 0;
  }
};

struct ThrStatLink {
  AtomUInt8 busy,
            used;
  uint8_t   db_num,
            pad;
  uint32_t  ctx_id,
            next,
            back;
  /* 4 * 3 + 4 = 16 */
};

struct HashSeed {
  uint64_t hash1, hash2;
  void get( uint64_t &h1,  uint64_t &h2 ) const {
    h1 = this->hash1; h2 = this->hash2;
  }
  void hash( KeyFragment &kb,  uint64_t &h1,  uint64_t &h2 ) const {
    h1 = this->hash1; h2 = this->hash2;
    kb.hash( h1, h2 );
  }
};

struct DBHdr {
  HashSeed     seed[ DB_COUNT ];         /* db hash seeds 4 K */
  HashCounters db_stat[ DB_COUNT ];      /* one for each db            32 K */
  ThrStatLink  stat_link[ MAX_STAT_ID ]; /* one for each open db       16 K */

  uint8_t pad[ DB_HDR_SIZE - /* 12 K */
    ( ( sizeof( HashCounters ) + sizeof( uint64_t ) * 2 ) * DB_COUNT
    + ( sizeof( ThrStatLink ) * MAX_STAT_ID ) ) ];

  void get_hash_seed( uint8_t db_num,  HashSeed &hs ) const {
    hs = this->seed[ db_num ];
  }
};

struct HashHdr : public FileHdr, public DBHdr {
  static const uint32_t SHM_MAX_SEG_COUNT = ( HT_HDR_SIZE -
    ( sizeof( FileHdr ) + sizeof( DBHdr ) ) ) / sizeof( Segment );
  Segment seg[ SHM_MAX_SEG_COUNT ]; /* pointers to segment alloc info */
};

struct HashTab {
  HashHdr      hdr; /* FileHdr, seed[], db_stat[], stat_link[], seg[] */
  ThrCtx       ctx[ MAX_CTX_ID ];
  HashCounters stats[ MAX_STAT_ID ];
#if __cplusplus >= 201103L
  static_assert( HT_HDR_SIZE == sizeof( HashHdr ), "ht hdr size");
  static_assert( HT_CTX_SIZE == sizeof( ThrCtx ) * MAX_CTX_ID, "ht ctx size" );
  static_assert( HT_STATS_SIZE == sizeof( HashCounters ) * MAX_STAT_ID, "ht stats size" );
#endif
  /* tab size is this->hdr.ht_size * this->hdr.hash_entry_size,
     determined by total shm size */
public:
  static HashEntry *get_entry( void *ht_base,  uint64_t i,
                               uint32_t hash_entry_size ) {
    return (HashEntry *) (void *)
      &((uint8_t *) ht_base)[ i * (uint64_t) hash_entry_size ];
  }
  HashEntry *get_entry( uint64_t i,  uint32_t hash_entry_size ) const {
    return get_entry( &((uint8_t *) (void *) this)[ HT_HDR_SIZE + HT_CTX_SIZE +
                                                    HT_STATS_SIZE ],
                      i, hash_entry_size );
  }
  uint64_t get_entry_pos( const HashEntry *entry,
                          uint32_t hash_entry_size ) const {
    const uint8_t *start =
      &((uint8_t *) (void *) this)[ HT_HDR_SIZE + HT_CTX_SIZE + HT_STATS_SIZE ];
    return ( (const uint8_t *) (const void *) entry - start ) / hash_entry_size;
  }
  HashEntry *get_entry( uint64_t i ) {
    return this->get_entry( i, this->hdr.hash_entry_size );
  }
  /* prefetch key k by calc ht_mod( k ) */
  void prefetch( uint64_t k,  bool for_read ) const {
    static const int locality = 1; /* 0 is non, 1 is low, 2 moderate, 3 high */
    const void * p = (void *)
            this->get_entry( this->hdr.ht_mod( k ), this->hdr.hash_entry_size );
    if ( for_read )
      __builtin_prefetch( p, 0, locality );
    else
      __builtin_prefetch( p, 1, locality );
  }
  /* mem is shm via mmap(): new HashTab( mmap( fd ) ) */
  void * operator new( size_t, void *ptr ) { return ptr; }
  /* delete does close if shm, free() if alloced */
  void operator delete( void *ptr ) noexcept;
  /* static header string for this version, sigs for different memory models */
  static const char shared_mem_sig[ 16 ], alloced_mem_sig[ 16 ];
  /* close calls munmap(), 'this' is no longer valid */
  int close_map( void ) noexcept;
private:
  /* calls initialize() */
  HashTab( const char *map_name,  const HashTabGeom &geom ) noexcept;
  /* this could be used to reinitialize */
  void initialize( const char *map_name,  const HashTabGeom &geom ) noexcept;
public:
  uint64_t ht_mod( const uint64_t k ) const {
    return this->hdr.ht_mod( k );
  }
  /* allocate new map using malloc */
  static HashTab *alloc_map( HashTabGeom &geom ) noexcept;
  /* initialize new map using shm file name, kv_facility bits and geom */
  static HashTab *create_map( const char *map_name,  uint8_t facility,
                              HashTabGeom &geom,  int map_mode ) noexcept;
  /* attaches existing map using shm file name, kv_facility bits, return geom */
  static HashTab *attach_map( const char *map_name,  uint8_t facility,
                              HashTabGeom &geom ) noexcept;
  static int remove_map( const char *map_name,  uint8_t facility ) noexcept;
  /* a shared usage context for stats and signals */
  uint32_t attach_ctx( uint64_t key ) noexcept;
  /* get a stat context for db */
  uint32_t attach_db( uint32_t ctx_id,  uint8_t db ) noexcept;
  /* release a stat context */
  void detach_db( uint32_t ctx_id,  uint8_t db ) noexcept;
  /* free the shared usage context */
  void detach_ctx( uint32_t ctx_id ) noexcept;
  /* calculate load of ht and set this->hdr.current_load */
  void update_load( void ) noexcept;
  /* accumulate stats just for ctx_id with delta change */
  bool sum_ht_thr_delta( HashDeltaCounters &stats,  HashCounters &ops,
                         HashCounters &tot,  uint32_t ctx_id ) const noexcept;
  bool sum_ht_db_delta( HashDeltaCounters &stats,  HashCounters &ops,
                        HashCounters &tot,  uint8_t db ) noexcept;
  /* accumulate stats just for db */
  bool get_db_stats( HashCounters &tot,  uint8_t db_num ) noexcept;
  /* accumulate memory usage stats of each segment and return true if changed
   * stats[] should be sized by this->hdr.nsegs */
  bool sum_mem_deltas( MemDeltaCounters *stats,  MemCounters &chg,
                       MemCounters &tot ) const noexcept;
  /* get the start of a data segment */
  Segment &segment( uint32_t i ) {
    return this->hdr.seg[ i ];
  }
  /* walk segment an reclaim memory */
  bool gc_segment( uint32_t dbx_id,  uint32_t seg_num,
                   GCStats &stats ) noexcept;
  void *seg_data( uint32_t i,  uint64_t off ) const {
    /*return &((uint8_t *) this)[ this->segment( i ).seg_off + off ];*/
    uint64_t sz = this->hdr.seg_size();
    if ( i >= this->hdr.nsegs || off >= sz )
      return NULL;
    return &((uint8_t *) this)[ this->hdr.seg_start() + (uint64_t) i*sz + off ];
  }
  bool is_valid_region( void *p,  size_t sz ) const {
    return (uint8_t *) p >= &((uint8_t *) this)[ this->hdr.seg_start() ] &&
           &((uint8_t *) p)[ sz ] <= &((uint8_t *) this)[ this->hdr.map_size ];
  }
};

struct EvShm {
  HashTab * map;
  uint32_t  ctx_id,
            dbx_id;

  EvShm( HashTab *m = 0 )
    : map( m ), ctx_id( MAX_CTX_ID ), dbx_id( MAX_STAT_ID ) {}
  EvShm( EvShm &m )
    : map( m.map ), ctx_id( m.ctx_id ), dbx_id( m.dbx_id ) {}
  ~EvShm() noexcept;

  int open( const char *map_name    = KV_DEFAULT_SHM,
            uint8_t db_num          = 0 ) noexcept;
  int create( const char * map_name = KV_DEFAULT_SHM,
              kv_geom_t  * geom     = NULL,
              int          map_mode = 0660,
              uint8_t      db_num   = 0 ) noexcept;
  void print( void ) noexcept;
  int attach( uint8_t db_num ) noexcept;
  void detach( void ) noexcept;
  void close( void ) noexcept;
};

char *print_map_geom( HashTab *map,  uint32_t ctx_id, char *buf = 0,
                      size_t buflen = 0 ) noexcept;
} /* namespace kv */
} /* namespace rai */
#endif /* __cplusplus */

#ifdef __cplusplus
extern "C" {
#endif

kv_hash_tab_t *kv_alloc_map( kv_geom_t *geom );
kv_hash_tab_t *kv_create_map( const char *map_name,  uint8_t facility,
                              kv_geom_t *geom,  int map_mode );
kv_hash_tab_t *kv_attach_map( const char *map_name,  uint8_t facility,
                              kv_geom_t *geom );
void kv_close_map( kv_hash_tab_t *ht );

/* calculate % load and return it, 0 <= load < 1.0 */
float kv_update_load( kv_hash_tab_t *ht );
/* attach a thread context, return ctx_id, which is an index to ht->ctx[], key is arbitrary */
uint32_t kv_attach_ctx( kv_hash_tab_t *ht,  uint64_t key );
/* deattach a thread context */
void kv_detach_ctx( kv_hash_tab_t *ht,  uint32_t ctx_id );
/* attach a thread context, return ctx_id, which is an index to ht->ctx[], key is arbitrary */
uint32_t kv_attach_db( kv_hash_tab_t *ht,  uint32_t ctx_id,  uint8_t db );
/* deattach a thread context */
void kv_detach_db( kv_hash_tab_t *ht,  uint32_t ctx_id,  uint8_t db );
/* total number of hash slots */
uint64_t kv_map_get_size( kv_hash_tab_t *ht );

char *kv_print_map_geom( kv_hash_tab_t *kv,  uint32_t ctx_id,  char *buf,
                         size_t buflen );
#ifdef __cplusplus
}
#endif

#endif
