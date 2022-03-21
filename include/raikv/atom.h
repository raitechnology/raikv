#ifndef __rai__raikv__atom_h__
#define __rai__raikv__atom_h__

/*#ifndef __x86_64__
#error TODO: other arch
#endif*/

/* also include stdint.h */
#ifdef __cplusplus
extern "C" {
#endif
/* volatile not really necessary */
typedef volatile uint64_t kv_atom_uint64_t;
typedef volatile uint32_t kv_atom_uint32_t;
typedef volatile uint16_t kv_atom_uint16_t;
typedef volatile uint8_t  kv_atom_uint8_t;

uint8_t kv_sync_xchg8( kv_atom_uint8_t *a,  uint8_t new_val );
uint16_t kv_sync_xchg16( kv_atom_uint16_t *a,  uint16_t new_val );
uint32_t kv_sync_xchg32( kv_atom_uint32_t *a,  uint32_t new_val );
uint64_t kv_sync_xchg64( kv_atom_uint64_t *a,  uint64_t new_val );

int kv_sync_cmpxchg8( kv_atom_uint8_t *a, uint8_t old_val, uint8_t new_val );
int kv_sync_cmpxchg16( kv_atom_uint16_t *a, uint16_t old_val, uint16_t new_val );
int kv_sync_cmpxchg32( kv_atom_uint32_t *a, uint32_t old_val, uint32_t new_val );
int kv_sync_cmpxchg64( kv_atom_uint64_t *a, uint64_t old_val, uint64_t new_val );

uint8_t kv_sync_add8( kv_atom_uint8_t *a,  uint8_t val );
uint16_t kv_sync_add16( kv_atom_uint16_t *a,  uint16_t val );
uint32_t kv_sync_add32( kv_atom_uint32_t *a,  uint32_t val );
uint64_t kv_sync_add64( kv_atom_uint64_t *a,  uint64_t val );

uint8_t kv_sync_sub8( kv_atom_uint8_t *a,  uint8_t val );
uint16_t kv_sync_sub16( kv_atom_uint16_t *a,  uint16_t val );
uint32_t kv_sync_sub32( kv_atom_uint32_t *a,  uint32_t val );
uint64_t kv_sync_sub64( kv_atom_uint64_t *a,  uint64_t val );

void kv_sync_store8( kv_atom_uint8_t *a, uint8_t val );
void kv_sync_store16( kv_atom_uint16_t *a, uint16_t val );
void kv_sync_store32( kv_atom_uint32_t *a, uint32_t val );
void kv_sync_store64( kv_atom_uint64_t *a, uint64_t val );

uint8_t kv_sync_load8( kv_atom_uint8_t *a );
uint16_t kv_sync_load16( kv_atom_uint16_t *a );
uint32_t kv_sync_load32( kv_atom_uint32_t *a );
uint64_t kv_sync_load64( kv_atom_uint64_t *a );

#ifndef _MSC_VER
/* alias for mfence instruction */
static inline void kv_sync_mfence( void ) { __sync_synchronize(); }
/* a compiler store fence */
static inline void kv_sync_sfence( void ) { __asm__ __volatile__( "" ::: "memory" ); }
/* alias for pause instruction
 * https://software.intel.com/en-us/articles/benefitting-power-and-performance-sleep-loops */
static inline void kv_sync_pause( void ) { __asm__ __volatile__( "pause":::"memory" ); }
/* prefetch addr, for read, locality */
static inline void kv_prefetch( const void *addr, int rdwr, int locality ) {
  (void) locality;
  if ( rdwr == 0 )
    __builtin_prefetch( addr, 0, 1 );
  else
    __builtin_prefetch( addr, 1, 1 );
}
#if __cplusplus >= 201103L || __STDC_VERSION__ >= 201112L
static inline void kv_acquire_fence( void ) { __atomic_thread_fence( __ATOMIC_ACQUIRE ); }
static inline void kv_release_fence( void ) { __atomic_thread_fence( __ATOMIC_RELEASE ); }
#else
static inline void kv_acquire_fence( void ) { kv_sync_sfence(); }
static inline void kv_release_fence( void ) { kv_sync_sfence(); }
#endif

#else
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN 1
#endif
#include <intrin.h>
static inline void kv_sync_mfence( void ) { _ReadWriteBarrier(); }
static inline void kv_sync_sfence( void ) { _mm_sfence(); }
static inline void kv_sync_pause( void ) { _mm_pause(); }
static inline void kv_prefetch( const void *addr, int rdwr, int locality ) {
  _mm_prefetch( (char *) addr, 1 );
}
static inline void kv_acquire_fence( void ) { kv_sync_sfence(); }
static inline void kv_release_fence( void ) { kv_sync_sfence(); }
#endif

#ifdef __cplusplus
}

#ifndef _MSC_VER
#if 0
/* use a pointer to force the compiler to calculate it */
#define kv_escape( p ) __asm__ __volatile__ ( "" : : "g"(p) : "memory" )
#endif
/* atomics for spin locks and counters, requires gcc -std=c11 */
#if __cplusplus >= 201103L || __STDC_VERSION__ >= 201112L
template <class Int> static inline Int kv_sync_xchg( volatile Int *a, Int new_val ) {
  return __atomic_exchange_n( a, new_val, __ATOMIC_RELAXED );
}
template <class Int> static inline Int kv_sync_cmpxchg( volatile Int *a, Int old_val, Int new_val ) {
  return __atomic_compare_exchange_n( a, &old_val, new_val, 0, __ATOMIC_RELAXED, __ATOMIC_RELAXED );
}
template <class Int> static inline Int kv_sync_add( volatile Int *a, Int val ) {
  return __atomic_add_fetch( a, val, __ATOMIC_RELAXED );
}
template <class Int> static inline Int kv_sync_sub( volatile Int *a, Int val ) {
  return __atomic_sub_fetch( a, val, __ATOMIC_RELAXED );
}
template <class Int> static inline void kv_sync_store( volatile Int *a, Int val ) {
  __atomic_store_n( a, val, __ATOMIC_RELAXED );
}
template <class Int> static inline Int kv_sync_load( volatile Int *a ) {
  return __atomic_load_n( a, __ATOMIC_ACQUIRE );
}

/* gcc 4.3 (RH5?) */
#else
template <class Int> static inline Int kv_sync_xchg( volatile Int *a, Int new_val ) {
  return __sync_lock_test_and_set( a, new_val )
}
template <class Int> static inline Int kv_sync_cmpxchg( volatile Int *a, Int old_val, Int new_val ) {
  return __sync_bool_compare_and_swap( a, old_val, new_val );
}
template <class Int> static inline Int kv_sync_add( volatile Int *a, Int val ) {
  return __sync_fetch_and_add( a, val );
}
template <class Int> static inline Int kv_sync_sub( volatile Int *a, Int val ) {
  return __sync_fetch_and_sub( a, val );
}
template <class Int> static inline void kv_sync_store( volatile Int *a, Int val ) {
  kv_sync_sfence(); *(a) = val
}
template <class Int> static inline Int kv_sync_load( volatile Int *a ) {
  Int i = *(a); kv_sync_sfence(); return i;
}
#endif

/* _MSC_VER */
#else

static inline uint8_t kv_sync_xchg( kv_atom_uint8_t *a,  uint8_t new_val ) {
  return (uint8_t) _InterlockedExchange8( (char *) a, (char) new_val );
}
static inline uint16_t kv_sync_xchg( kv_atom_uint16_t *a,  uint16_t new_val ) {
  return (uint16_t) _InterlockedExchange16( (short *) a, (short) new_val );
}
static inline uint32_t kv_sync_xchg( kv_atom_uint32_t *a,  uint32_t new_val ) {
  return (uint32_t) _InterlockedExchange( (long *) a, (long) new_val );
}
static inline uint64_t kv_sync_xchg( kv_atom_uint64_t *a,  uint64_t new_val ) {
  return (uint64_t) _InterlockedExchange64( (__int64 *) a, (__int64) new_val );
}

static inline bool kv_sync_cmpxchg( kv_atom_uint8_t *a, uint8_t old_val, uint8_t new_val ) {
  return (uint8_t) _InterlockedCompareExchange8( (char *) a, (char) new_val, (char) old_val ) == old_val;
}
static inline bool kv_sync_cmpxchg( kv_atom_uint16_t *a, uint16_t old_val, uint16_t new_val ) {
  return (uint16_t) _InterlockedCompareExchange16( (short *) a, (short) new_val, (short) old_val ) == old_val;
}
static inline bool kv_sync_cmpxchg( kv_atom_uint32_t *a, uint32_t old_val, uint32_t new_val ) {
  return (uint32_t) _InterlockedCompareExchange( (long *) a, (long) new_val, (long) old_val ) == old_val;
}
static inline bool kv_sync_cmpxchg( kv_atom_uint64_t *a, uint64_t old_val, uint64_t new_val ) {
  return (uint64_t) _InterlockedCompareExchange64( (__int64 *) a, (__int64) new_val, (__int64) old_val ) == old_val;
}

static inline uint8_t kv_sync_add( kv_atom_uint8_t *a,  uint8_t val ) {
  return (uint8_t) _InterlockedExchangeAdd8( (char *) a, (char) val );
}
static inline uint16_t kv_sync_add( kv_atom_uint16_t *a,  uint16_t val ) {
  return (uint16_t) _InterlockedExchangeAdd16( (short *) a, (short) val );
}
static inline uint32_t kv_sync_add( kv_atom_uint32_t *a,  uint32_t val ) {
  return (uint32_t) _InterlockedExchangeAdd( (long *) a, (long) val );
}
static inline uint64_t kv_sync_add( kv_atom_uint64_t *a,  uint64_t val ) {
  return (uint64_t) _InterlockedExchangeAdd64( (__int64 *) a, (__int64) val );
}

static inline uint8_t kv_sync_sub( kv_atom_uint8_t *a,  uint8_t val ) {
  return (uint8_t) _InterlockedExchangeAdd8( (char *) a, -(char) val );
}
static inline uint16_t kv_sync_sub( kv_atom_uint16_t *a,  uint16_t val ) {
  return (uint16_t) _InterlockedExchangeAdd16( (short *) a, -(short) val );
}
static inline uint32_t kv_sync_sub( kv_atom_uint32_t *a,  uint32_t val ) {
  return (uint32_t) _InterlockedExchangeAdd( (long *) a, -(long) val );
}
static inline uint64_t kv_sync_sub( kv_atom_uint64_t *a,  uint64_t val ) {
  return (uint64_t) _InterlockedExchangeAdd64( (__int64 *) a, -(__int64) val );
}
template <class Int> static inline void kv_sync_store( volatile Int *a, Int val ) {
  kv_sync_sfence(); *(a) = val;
}
template <class Int> static inline Int kv_sync_load( volatile Int *a ) {
  Int i = *(a); kv_sync_sfence(); return i;
}

#endif /* _MSC_VER */

static inline int
kv_sync_bit_try_lock( volatile uint64_t *ptr,  uint64_t mask )
{
  uint64_t old_val = *ptr,
           new_val = old_val | mask;
  if ( ( old_val & mask ) == 0 ) {
    if ( kv_sync_cmpxchg( ptr, old_val, new_val ) ) {
      kv_acquire_fence();
      return 1;
    }
  }
  return 0;
}

static inline void
kv_sync_bit_spin_lock( volatile uint64_t *ptr,  uint64_t mask )
{
  uint64_t old_val, new_val;
  for (;;) {
    while ( ( (old_val = *ptr) & mask ) != 0 )
      kv_sync_pause();
    new_val = old_val | mask;
    if ( kv_sync_cmpxchg( ptr, old_val, new_val ) ) {
      kv_acquire_fence();
      return;
    }
  }
}

static inline void
kv_sync_bit_spin_unlock( volatile uint64_t *ptr,  uint64_t mask )
{
  uint64_t old_val, new_val;
  kv_release_fence();
  for (;;) {
    old_val = *ptr;
    new_val = old_val & ~mask;
    if ( kv_sync_cmpxchg( ptr, old_val, new_val ) )
      return;
    kv_sync_pause();
  }
}

namespace rai {
namespace kv {

/*#define TEST_ATOM_SPEED 1*/
template <class Int>
struct Atom {
  private: volatile Int val;
  public:
  /* swap values, return the old value, spins until succeeds */
  Int xchg( Int new_val ) {
#ifdef TEST_ATOM_SPEED
    Int old_val = this->val; this->val = new_val; return old_val;
#else
    return kv_sync_xchg( &this->val, new_val );
#endif
  }
  /* wrapper using this->val as the exchange location */
  bool cmpxchg( Int old_val,  Int new_val ) {
#ifdef TEST_ATOM_SPEED
   if ( old_val == this->val ) {
     this->val = new_val;
     return true;
   }
   return false;
#else
    return kv_sync_cmpxchg( &this->val, old_val, new_val );
#endif
  }
  /* add to val, return the value that was in val */
  Int add( Int add_val ) {
#ifdef TEST_ATOM_SPEED
    Int old_val = this->val;
    this->val += add_val;
    return old_val;
#else
    return kv_sync_add( &this->val, add_val );
#endif
  }
  /* sub from val, return the value that was in val */
  Int sub( Int sub_val ) {
#ifdef TEST_ATOM_SPEED
    Int old_val = this->val;
    this->val -= sub_val;
    return old_val;
#else
    return kv_sync_sub( &this->val, sub_val );
#endif
  }
  void store( Int new_val ) {
#ifdef TEST_ATOM_SPEED
    this->val = new_val;
#else
    kv_sync_store( &this->val, new_val );
#endif
  }
  Int load( void ) const {
#ifdef TEST_ATOM_SPEED
    return this->val;
#else
    return kv_sync_load( &this->val );
#endif
  }
  /* conveniences */
  Atom &operator++() { this->add( 1 ); return *this; }
  Atom &operator--() { this->sub( 1 ); return *this; }
  Atom &operator+=( const Int &i ) { this->add( i ); return *this; }
  Atom &operator-=( const Int &i ) { this->sub( i ); return *this; }
  Atom &operator=( const Int &i ) { this->store( i ); return *this; }
  Int operator&( const Int &i ) const { return this->load() & i; }
  operator Int() const { return this->load(); }
};

typedef struct Atom<uint64_t> AtomUInt64; /* a hash value */
typedef struct Atom<uint32_t> AtomUInt32; /* a size or offset */
typedef struct Atom<uint16_t> AtomUInt16; /* ctx counter */
typedef struct Atom<uint8_t>  AtomUInt8;  /* a bool */

/* MCS lock derived from a similar Linux kernel technique:
 *   https://lwn.net/Articles/590243/
 *   "Non-scalable locks are dangerous" -- Boyd-Wickizer, et al.
 *   https://people.csail.mit.edu/nickolai/papers/boyd-wickizer-locks.pdf
 *
 * The properties of this lock are that it is a fair queue and that the major
 * spin contention is separate for each thread trying to acquire the same lock,
 * which should reduce the cache coherence contention compared to several
 * threads spinning on the same memory location.  The primary drawback is that
 * a release requires a cmpxchg, but a spin lock does not.
 *
 * The lock part uses an unconditional exchange to find whether an acquisition
 * attempt is successful or not and who owns the lock.  A thread uses the owner
 * information to insert it self into a queue of waiters.
 *
 * The unlock part uses a cmpxchg to find whether other threads are waiting
 * and need to be notified.  The cmpxchg is successful when the unlocker is
 * the only thread and unsuccessful when other threads are waiting.
 *
 * This implementation also communicates a value between the unlock thread and
 * the next thread by using a bit in the value to distinguish it from the list
 * links between threads.  This is useful in a hash table where the hash value
 * doubles as a lock.  For example if locked_bit = 0x80, then 0 -> 0x7f are
 * values and 0x80 -> 0xff are MCS thread lock list.
 */
enum MCSStatus {
  MCS_OK       = 0,
  MCS_WAIT     = 1,
  MCS_INACTIVE = 2
};
template <class Int, class Owner>
struct MCSLock {
  Atom<Int> val, lock, next; /* should be init to zero */
  uint64_t  lock_id;

  void reset( void ) {
    this->val     = 0;
    this->next    = 0; /* reset */
    this->lock    = 0;
    this->lock_id = 0;
  }

  /* try to recover from crashes by post-mortem unlocking */
  MCSStatus recover_lock( Atom<Int> &/*link*/,  const Int locked_bit,
                          const Int my_id,  Owner &closure ) {
    uint64_t v = this->val;
    /* if locked_bit is set, then waiting for another thread */
    if ( ( v & locked_bit ) != 0 ) {
      if ( ( this->lock & locked_bit ) == 0 ) {
        v = this->lock;
        this->val = v;
        return MCS_OK;
      }
      if ( ! closure.is_active( v & ~locked_bit ) )
        return MCS_INACTIVE;
      /* waiting for another thread */
      closure.owner( v & ~locked_bit ).next = my_id | locked_bit;
      return MCS_WAIT;
    }
    return MCS_OK;
  }

  MCSStatus recover_unlock( Atom<Int> &link,  const Int locked_bit,
                            const Int my_id,  Owner &closure ) {
    Int waiting, v = this->val;
    if ( link.cmpxchg( my_id | locked_bit, v ) ) {
      /* sucessfully unlocked */
      this->reset();
      return MCS_OK;
    }
    /* wake the other thread */
    if ( (waiting = this->next) != 0 ) {
      if ( ! closure.is_active( waiting & ~locked_bit ) )
        return MCS_INACTIVE;
      MCSLock &own = closure.owner( waiting & ~locked_bit );
      /* other thread should be waiting, communicate val */
      if ( own.lock != 0 ) {
        own.lock = v;
        this->reset();
        return MCS_OK; /* successfully communicated value */
      }
      /* other thread in list is not waiting */
      /* return MCS_INACTIVE; */
    }
    /* other thread in list needs to unlock */
    return MCS_WAIT;
  }

  /* wait for lock to become available */
  Int acquire( Atom<Int> &link,  uint64_t id,  const Int locked_bit,
               const Int my_id,  uint64_t &spin,  Owner &closure ) {
    Int v, wait;
    this->lock_id = id + 1;
    if ( ( (v = link.xchg( my_id | locked_bit )) & locked_bit ) != 0 ) {
      this->val = v; /* val has lock bit set */
      /* These two store operations could be reordered as seen by the next
       * thread if memory ordering is weak */
      this->lock = locked_bit;
      closure.owner( v & ~locked_bit ).next = my_id | locked_bit;
      for (;;) {
        /* spin on my lock, waiting for it to be free by owner signal */
        if ( ( (wait = this->lock) & locked_bit ) == 0 ) /* val communicated */
          break;
        spin++;
        kv_sync_pause();
      }
      v = wait;
    }
    this->val = v; /* val does not have lock bit set */
    kv_acquire_fence();
    return v;
  }

  /* same as acquire, except don't wait for lock */
  Int try_acquire( Atom<Int> &link,  uint64_t id,  const Int locked_bit,
                   const Int my_id,  uint64_t &spin ) {
    this->lock_id = id + 1;
    for (;;) {
      Int v = link;
      if ( ( v & locked_bit ) != 0 ) {
        this->lock_id = 0;
        return v; /* lock is not free */
      }
      /* try to acquire it */
      if ( link.cmpxchg( v, my_id | locked_bit ) ) {
        this->val = v;
        kv_acquire_fence();
        return v; /* success */
      }
      spin++;
      kv_sync_pause();
    }
  }

  /* release lock and wake waiters */
  void release( Atom<Int> &link,  const Int v,  const Int locked_bit,
                const Int my_id,  uint64_t &spin,  Owner &closure ) {
    kv_release_fence();
    /* set lock=val if no one else is waiting */
    if ( ! link.cmpxchg( my_id | locked_bit, v ) ) {
      Int waiting;
      while ( (waiting = this->next) == 0 ) { /* find next thread in queue */
        spin++;
        kv_sync_pause();
      }
      /* notify, the waiter is spinning on lock, communicate val */
      MCSLock &own = closure.owner( waiting & ~locked_bit );
      while ( own.lock == 0 ) { /* wait for locked_bit to be set */
        spin++;
        kv_sync_pause();
      }
      /* val is seen by the acquire() wait variable above */
      own.lock = v;
    }
    this->reset();
  }
};

} /* namespace kv */
} /* namespace rai */
#endif

#endif
