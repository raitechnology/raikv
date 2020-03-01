#ifndef __rai__raikv__atom_h__
#define __rai__raikv__atom_h__

/* also include stdint.h */
#ifdef __cplusplus
extern "C" {
#endif
/* alias for mfence instruction */
#define kv_sync_mfence() __sync_synchronize()

/* a compiler store fence */
#define kv_sync_sfence() __asm__ __volatile__( "" ::: "memory" )

/* use a pointer to force the compiler to calculate it */
#define kv_escape( p ) __asm__ __volatile__ ( "" : : "g"(p) : "memory" );

/* alias for pause instruction
 * https://software.intel.com/en-us/articles/benefitting-power-and-performance-sleep-loops */
#define kv_sync_pause() __asm__ __volatile__( "pause":::"memory" )

/* volatile not really necessary, use kv_sync_load(), kv_sync_store() */
typedef volatile uint64_t kv_atom_uint64_t;
typedef volatile uint32_t kv_atom_uint32_t;
typedef volatile uint16_t kv_atom_uint16_t;
typedef volatile uint8_t  kv_atom_uint8_t;

#if __cplusplus >= 201103L || __STDC_VERSION__ >= 201112L

/* atomics for spin locks and counters, requires gcc -std=c11 */
#define kv_sync_xchg( a, new_val ) \
  __atomic_exchange_n( a, new_val, __ATOMIC_RELAXED )

#define kv_sync_cmpxchg( a, old_val, new_val ) \
  __atomic_compare_exchange_n( a, &old_val, new_val, 0, __ATOMIC_RELAXED, \
                               __ATOMIC_RELAXED )
#define kv_sync_add( a, val ) \
  __atomic_add_fetch( a, val, __ATOMIC_RELAXED )

#define kv_sync_sub( a, val ) \
  __atomic_sub_fetch( a, val, __ATOMIC_RELAXED )

#define kv_sync_store( a, val ) \
  __atomic_store_n( a, val, __ATOMIC_RELAXED )

#define kv_sync_load( a ) \
  __atomic_load_n( a, __ATOMIC_RELAXED )

#define kv_acquire_fence() \
  __atomic_thread_fence( __ATOMIC_ACQUIRE )

#define kv_release_fence() \
  __atomic_thread_fence( __ATOMIC_RELEASE )

#else

/* atomics for spin locks and counters, requires gcc 4.3 */
#define kv_sync_xchg( a, new_val ) \
  __sync_lock_test_and_set( a, new_val )

#define kv_sync_cmpxchg( a, old_val, new_val ) \
  __sync_bool_compare_and_swap( a, old_val, new_val )

#define kv_sync_add( a, val ) \
  __sync_fetch_and_add( a, val )

#define kv_sync_sub( a, val ) \
  __sync_fetch_and_sub( a, val )

#define kv_sync_store( a, val ) \
  *(a) = val

#define kv_sync_load( a ) \
  *(a)

#define kv_acquire_fence() /* nothing for x86 */

#define kv_release_fence() /* nothing for x86 */
/* probably should assert arch == x86 */
#endif

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
#ifdef __cplusplus
}
#endif

#ifdef __cplusplus
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
