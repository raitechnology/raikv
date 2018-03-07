#ifndef __rai__raikv__shm_ht_h__
#define __rai__raikv__shm_ht_h__

/* also include stdint.h, string.h */
#ifndef __rai__raikv__key_ctx_h__
#include <raikv/atom.h>
#endif

#ifndef __rai__raikv__util_h__
#include <raikv/util.h>
#endif

#ifndef __rai__raikv__ht_stats_h__
#include <raikv/ht_stats.h>
#endif

#ifndef __rai__raikv__hash_entry_h__
#include <raikv/hash_entry.h>
#endif

#ifndef __rai__raikv__msg_ctx_h__
#include <raikv/msg_ctx.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif
typedef struct kv_geom_s {
  uint64_t map_size,         /* size of memory used by shm */
           max_value_size;   /* max size of an data entry */
  uint32_t hash_entry_size;  /* size of a hash entry, min 32b */
  float    hash_value_ratio; /* ratio of hash/data cells: hash = ratio * size */
} kv_geom_t;

typedef enum kv_facility_e {
  KV_POSIX_SHM = 1,  /* use shm_open(), mmap() (p:shm, q:shm2m, r:shm1g) */
  KV_FILE_MMAP = 2,  /* use open(), mmap()     (f:file, g:file2m, h:file1g) */
  KV_SYSV_SHM  = 4,  /* use shmget(), shmat()  (v:sysv, w:sysv2m, x:sysv1g) */
  KV_HUGE_2MB  = 8,  /* use 2mb pages */
  KV_HUGE_1GB  = 16  /* use 1gb pages */
} kv_facility_t;

/* used as error return for kv_attach_ctx() */
#define KV_NO_CTX_ID        ((uint32_t) -1)
/* hdr of map file */
#define KV_HT_FILE_HDR_SIZE 256
/* how big each thread context is */
#define KV_HT_THR_CTX_SIZE  1024
/* file hdr + segment data */
#define KV_HT_HDR_SIZE      ( 128 * 1024 )
/* how much space for thread contexts */
#define KV_HT_CTX_SIZE      ( 128 * 1024 )
/* the maximum thread context id */
#define KV_MAX_CTX_ID       ( KV_HT_CTX_SIZE / KV_HT_THR_CTX_SIZE )

#ifdef __cplusplus
}
#endif

#ifdef __cplusplus
namespace rai {
namespace kv {

                    /* sizeof( FileHdr ) */
static const size_t HT_FILE_HDR_SIZE      = KV_HT_FILE_HDR_SIZE,
                    /* sizeof( ThrCtx ) */
                    HT_THR_CTX_SIZE       = KV_HT_THR_CTX_SIZE,
		    /* FileHdr(256b) + Segment[ 2044 ] each 64b */
                    HT_HDR_SIZE           = KV_HT_HDR_SIZE,
		    /* ThrCtx[ 128 ] each 1024b containing ThrMCSLock[ 56 ] */
                    HT_CTX_SIZE           = KV_HT_CTX_SIZE,
                    /* ThrCtx[] size */
                    MAX_CTX_ID            = HT_CTX_SIZE / HT_THR_CTX_SIZE,
                    /* FileHdr::name[] size */
                    MAX_FILE_HDR_NAME_LEN =
   64/*sig hdr*/ - ( 16/*sig*/ +
                      4/*hash_value_ratio*/ +
                      4/*critical_load*/ +
                      4/*current_load*/ +
                      4/*ht_load*/ +
                      4/*value_load*/ +
                      4/*load_percent + pad */ +
                      4/*next_ctx*/ +
                      4/*max_immed_value_size*/ +
                      8/*max_segment_value_size*/ +
                      8/*current_stamp*/ ) + 64 * 2; /* currently 128b */

/* hdr size should be 256b */
struct FileHdr {
  /* first part are 192b, not referenced very much */
  char       sig[ 16 ]; /* version info and mem type (a,f,g,h,p,q,r,v,w,x) */
  char       name[ MAX_FILE_HDR_NAME_LEN ];  /* name of this map file */
  float      hash_value_ratio,       /* ratio of ht[] mem to values mem */
             critical_load,          /* current > critical, forced evictions */
             current_load,           /* max( ht load, value load ) */
             ht_load,                /* hash entry used / ht_size */
             value_load;             /* seg data used / seg size */
  uint8_t    load_percent;           /* current_load * 100 / critical_load */
  uint8_t    padb;      
  AtomUInt16 next_ctx;               /* next free ctx[] */
  AtomUInt16 ctx_used;               /* number of ctx used */
  uint32_t   max_immed_value_size;   /* sizeof value in entry, including key */
  uint64_t   max_segment_value_size, /* sizeof value in segment, inc key */
             current_stamp;          /* cached current time, used for expire */
                                     /* time evictions, not high resolution */
  /* next 64b useful read only data, referenced a lot */
  uint64_t   create_stamp,   /* time created (nanosecs), base timestamp */
             map_size,       /* map size from geom */
             max_value_size, /* max value size from geom */
             ht_size,        /* calculated size of ht[] */
             seg_size,       /* size of segment[] */
             seg_start,      /* offset of first segment */
             padq;
  uint32_t   hash_entry_size; /* size of each hash table entry */
  uint16_t   nsegs,           /* count of segments */
             seg_align_shift; /* 1 << 6 = 64, align size for vals in segment */

  uint64_t seg_align( void ) const {
    return (uint64_t) 1 << this->seg_align_shift;
  }
};

typedef struct kv_geom_s HashTabGeom;
static const uint32_t ZOMBIE32 = 0x80000000U;

struct ThrCtxOwner; /* MCSLock used for hash table entry ownership */
struct ThrMCSLock : public MCSLock<uint64_t, ThrCtxOwner> {};

struct ThrCtxHdr {
  AtomUInt32             key; /* zero free, zombie32 dropped, otherwise used */
  uint32_t               ctx_id,    /* the ctx id that holds a ht[] locks */
	                 ctx_pid,   /* process id (getpid())*/
                         ctx_thrid; /* thread id (syscall(SYS_gettid)) */

  HashCounters           stat;      /* stats for this thread context */
  rand::xoroshiro128plus rand;      /* rand state initialized on creation */

  uint64_t               mcs_used,  /* bit mask of used locks (64 > MCS_CNT=56)*/
                         seg_pref;  /* insert segment pref, bits from rand */
  /* 16(int32) + 80(stat) + 16(rand) + 16(uint64) = 128 */
};

struct ThrCtxEntry : public ThrCtxHdr {
  static const uint32_t MCS_CNT  =
    ( HT_THR_CTX_SIZE - sizeof( ThrCtxHdr ) ) / sizeof( ThrMCSLock );
  static const uint32_t MCS_SHIFT = 16,
                        MCS_MASK  = ( 1 << MCS_SHIFT ) - 1;
  ThrMCSLock mcs[ MCS_CNT ]; /* a queue of ctx waiting for ht.entry[ x ] */

  void zero( void ) {
    this->stat.zero(); /* TODO: accum to a global counter, for monitoring */
    this->mcs_used = 0;
    ::memset( this->mcs, 0, sizeof( this->mcs ) );
  }
};

struct ThrCtx : public ThrCtxEntry { /* each thread needs one of these */
  char pad[ HT_THR_CTX_SIZE - sizeof( ThrCtxEntry ) ]; /* 1024b align */

  /* convenience functions for stats */
  void incr_read( uint64_t cnt = 1 )   { this->stat.rd      += cnt; }
  void incr_write( uint64_t cnt = 1 )  { this->stat.wr      += cnt; }
  void incr_spins( uint64_t cnt = 1 )  { this->stat.spins   += cnt; }
  void incr_chains( uint64_t cnt = 1 ) { this->stat.chains  += cnt; }
  void incr_add( uint64_t cnt = 1 )    { this->stat.add     += cnt; }
  void incr_drop( uint64_t cnt = 1 )   { this->stat.drop    += cnt; }
  void incr_htevict( uint64_t cnt = 1 ){ this->stat.htevict += cnt; }
  void incr_afail( uint64_t cnt = 1 )  { this->stat.afail   += cnt; }
  void incr_hit( uint64_t cnt = 1 )    { this->stat.hit     += cnt; }
  void incr_miss( uint64_t cnt = 1 )   { this->stat.miss    += cnt; }

  void get_ht_delta( HashDeltaCounters &stat ) const;

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
};

struct ThrCtxOwner { /* closure for MCSLock to find the owner of a lock */
  ThrCtx * ctx;
  ThrCtxOwner( ThrCtx *p ) : ctx( p ) {}
  ThrMCSLock& owner( const uint64_t mcs_id ) {
    return this->ctx[ mcs_id >> ThrCtxEntry::MCS_SHIFT ].get_mcs_lock( mcs_id );
  }
};

struct HashHdr : public FileHdr {
  static const uint32_t SHM_MAX_SEG_COUNT =
    ( HT_HDR_SIZE - sizeof( FileHdr ) ) / sizeof( Segment );
  Segment seg[ SHM_MAX_SEG_COUNT ];
};

struct HashTab {
  HashHdr hdr;
  char    hdr_pad[ HT_HDR_SIZE - sizeof( HashHdr ) ];

  ThrCtx ctx[ MAX_CTX_ID ];
  char   ctx_pad[ HT_CTX_SIZE - sizeof( ThrCtx ) * MAX_CTX_ID ];

  /* tab size is this->hdr.ht_size * this->hdr.hash_entry_size,
     determined by total shm size */
public:
  HashEntry *get_entry( uint64_t i,  uint32_t hash_entry_size ) {
    return (HashEntry *) (void *)
      &((uint8_t *) (void *) this)[ HT_HDR_SIZE + HT_CTX_SIZE +
                                    i * (uint64_t) hash_entry_size ];
  }
  HashEntry *get_entry( uint64_t i ) {
    return this->get_entry( i, this->hdr.hash_entry_size );
  }
  /* mem is shm via mmap(): new HashTab( mmap( fd ) ) */
  void * operator new( size_t sz, void *ptr ) { return ptr; }
  /* delete does close if shm, free() if alloced */
  void operator delete( void *ptr );
  /* static header string for this version, sigs for different memory models */
  static const char shared_mem_sig[ 16 ], alloced_mem_sig[ 16 ];
  /* close calls munmap(), 'this' is no longer valid */
  int close_map( void );
private:
  /* calls initialize() */
  HashTab( const char *map_name,  const HashTabGeom &geom );
  /* this could be used to reinitialize */
  void initialize( const char *map_name,  const HashTabGeom &geom );
public:
  /* allocate new map using malloc */
  static HashTab *alloc_map( HashTabGeom &geom );
  /* initialize new map using shm file name, kv_facility bits */
  static HashTab *create_map( const char *map_name,  uint8_t facility,
                              HashTabGeom &geom ); /* create using geom */
  /* attaches existing map using shm file name, kv_facility bits */
  static HashTab *attach_map( const char *map_name,  uint8_t facility,
                              HashTabGeom &geom ); /* return geom */
  /* a shared usage context for stats and signals */
  uint32_t attach_ctx( uint32_t key );
  /* free the shared usage context */
  void detach_ctx( uint32_t ctx_id );
  /* calculate load of ht and set this->hdr.current_load */
  void update_load( void );
  /* accumulate hash table stats and return true if changed
   * stats[] should be sized by MAX_CTX_ID */
  bool get_ht_deltas( HashDeltaCounters *stats,  HashCounters &ops,
                      HashCounters &tot, uint32_t ctx_id = KV_NO_CTX_ID ) const;
  /* accumulate memory usage stats of each segment and return true if changed
   * stats[] should be sized by this->hdr.nsegs */
  bool get_mem_deltas( MemDeltaCounters *stats,  MemCounters &chg,
                       MemCounters &tot ) const;
  /* get the start of a data segment */
  Segment &segment( uint32_t i ) {
    return this->hdr.seg[ i ];
  }
  void *seg_data( uint32_t i,  uint64_t off ) {
    //return &((uint8_t *) this)[ this->segment( i ).seg_off + off ];
    return &((uint8_t *) this)[ this->hdr.seg_start + 
                                (uint64_t) i * this->hdr.seg_size + off ];
  }
};

} // namespace kv
} // namespace rai
#endif // __cplusplus

#ifdef __cplusplus
extern "C" {
#endif
struct kv_hash_tab_s;
typedef struct kv_hash_tab_s kv_hash_tab_t;

kv_hash_tab_t *kv_alloc_map( kv_geom_t *geom );
kv_hash_tab_t *kv_create_map( const char *map_name,  uint8_t facility,
                             kv_geom_t *geom );
kv_hash_tab_t *kv_attach_map( const char *map_name,  uint8_t facility,
                             kv_geom_t *geom );
void kv_close_map( kv_hash_tab_t *ht );

/* calculate % load and return it, 0 <= load < 1.0 */
float kv_update_load( kv_hash_tab_t *ht );
/* attach a thread context, return ctx_id, which is an index to ht->ctx[], key is arbitrary */
uint32_t kv_attach_ctx( kv_hash_tab_t *ht,  uint32_t key );
/* deattach a thread context */
void kv_detach_ctx( kv_hash_tab_t *ht,  uint32_t ctx_id );
/* total number of hash slots */
uint64_t kv_map_get_size( kv_hash_tab_t *ht );

#ifdef __cplusplus
}
#endif

#endif
