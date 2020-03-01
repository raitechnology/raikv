#ifndef __rai__raikv__ht_stats_h__
#define __rai__raikv__ht_stats_h__

/* also include stdint.h, string.h */

#ifdef __cplusplus
namespace rai {
namespace kv {

struct MemCounters {
  int64_t offset,
          msg_count,
          avail_size,
          move_msgs,
          move_size,
          evict_msgs,
          evict_size;

  MemCounters() { this->zero(); }
  void zero( void ) { ::memset( this, 0, sizeof( *this ) ); }

  MemCounters& operator=( const MemCounters &x ) noexcept;
  MemCounters& operator+=( const MemCounters &x ) noexcept;
  MemCounters& operator-=( const MemCounters &x ) noexcept;
  bool operator==( int i ) noexcept;
  bool operator!=( int i ) noexcept;
};

struct MemDeltaCounters {
  MemCounters last, delta;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  MemDeltaCounters() {}

  void get_mem_delta( const MemCounters &cnts ) noexcept;
};

struct HashCounters {
  int64_t rd, wr,     /* counter of get/put operations for this ctx */
          spins,      /* count of spins to acquire locks */
          chains,     /* count of chain links traversed to find/acquire items */
          add, drop,  /* count of new entries, count of dropped entries */
          expire,     /* count of expired entries dropped */
          htevict,    /* count of evictions */
          afail,      /* write failed to allocate data (too big or no space) */
          hit, miss,  /* hit when data was found, miss when not found */
          cuckacq,    /* cuckoo path acquire ops */
          cuckfet,    /* cuckoo path fetch to find a path */
          cuckmov,    /* cuckoo path move items to new locations */
          cuckret,    /* cuckoo path retry when a path search failed */
          cuckmax;    /* cuckoo path max retry, could be cycle */

  HashCounters() { this->zero(); }
  HashCounters( const HashCounters &cnt ) { *this = cnt; }
  void zero( void ) { ::memset( this, 0, sizeof( *this ) ); }

  HashCounters& operator=( const HashCounters &x ) noexcept;
  HashCounters& operator+=( const HashCounters &x ) noexcept;
  HashCounters& operator-=( const HashCounters &x ) noexcept;
  bool operator==( int i ) noexcept;
  bool operator!=( int i ) noexcept;
};

struct HashDeltaCounters {
  HashCounters last, delta;

  HashDeltaCounters() {}
  void zero( void ) { this->last.zero(); this->delta.zero(); }

  void get_ht_delta( const HashCounters &stat ) noexcept;
};

struct HashTab;

struct HashTabStats {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  HashTab           & ht;
  HashDeltaCounters * db_stats;  /* one for each db */
  MemDeltaCounters  * mem_stats; /* one for each memory segment */
  HashCounters        hops,      /* ops in the last ival */
                      htot;      /* total ops since creation */
  MemCounters         mops,      /* mops in the last ival */
                      mtot;      /* total ops since creation */
  double              ival,      /* ival for ops */
                      ival_start,/* start time */
                      ival_end;  /* end time, end becomes start next ival */
  uint32_t            nsegs,     /* ht.hdr.nsegs */
                      db_count;  /* MAX_DB_COUNT */
  HashTabStats( HashTab &h ) : ht( h ), db_stats( 0 ),
    mem_stats( 0 ), ival( 0 ), ival_start( 0 ), ival_end( 0 ),
    nsegs( 0 ), db_count( 0 ) {
    this->hops.zero(); this->htot.zero();
    this->mops.zero(); this->mtot.zero();
  }
  static HashTabStats * create( HashTab &ht ) noexcept;

  bool fetch( void ) noexcept; /* calculate a new ival, ret true if new stats */
};

} /* namespace kv */
} /* namespace rai */
#endif
#endif
