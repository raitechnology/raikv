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

  MemCounters& operator=( const MemCounters &x );
  MemCounters& operator+=( const MemCounters &x );
  MemCounters& operator-=( const MemCounters &x );
  bool operator==( int i );
  bool operator!=( int i );
};

struct MemDeltaCounters {
  MemCounters last, delta;

  void * operator new( size_t sz, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  MemDeltaCounters() {}

  void get_mem_delta( const MemCounters &cnts );

  static MemDeltaCounters *new_array( size_t sz );
};

struct HashCounters {
  int64_t rd, wr,     /* counter of get/put operations for this ctx */
          spins,      /* count of spins to acquire locks */
          chains,     /* count of chain links */
          add, drop,  /* count of new entries, count of dropped entries */
          htevict,    /* count of evictions because of ht collisions */
          afail,      /* write failed to allocate data */
          hit, miss,  /* read when data was found, miss when not found */
          cuckacq,    /* cuckoo path acquire */
          cuckfet,    /* cuckoo path fetch */
          cuckmov,    /* cuckoo path move */
          cuckbiz,    /* cuckoo path busy */
          cuckret,    /* cuckoo path retry */
          cuckmax;    /* cuckoo path max retry, could be cycle */

  HashCounters() { this->zero(); }
  HashCounters( const HashCounters &cnt ) { *this = cnt; }
  void zero( void ) { ::memset( this, 0, sizeof( *this ) ); }

  HashCounters& operator=( const HashCounters &x );
  HashCounters& operator+=( const HashCounters &x );
  HashCounters& operator-=( const HashCounters &x );
  bool operator==( int i );
  bool operator!=( int i );
};

struct HashDeltaCounters {
  HashCounters last, delta;

  HashDeltaCounters() {}
  void zero( void ) { ::memset( this, 0, sizeof( *this ) ); }

  void get_ht_delta( const HashCounters &stat );
};

} /* namespace kv */
} /* namespace rai */
#endif
#endif
