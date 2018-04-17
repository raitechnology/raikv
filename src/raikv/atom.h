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

/* alias for pause instruction
 * https://software.intel.com/en-us/articles/benefitting-power-and-performance-sleep-loops */
#define kv_sync_pause() __asm__ __volatile__( "pause":::"memory" )

typedef volatile uint64_t kv_atom_uint64_t;
typedef volatile uint32_t kv_atom_uint32_t;
typedef volatile uint16_t kv_atom_uint16_t;
typedef volatile uint8_t  kv_atom_uint8_t;

/* atomics for spin locks and counters, requires gcc 4.3 */
#define kv_sync_xchg( a, new_val ) \
  __sync_lock_test_and_set( a, new_val )

#define kv_sync_cmpxchg( a, old_val, new_val ) \
  __sync_bool_compare_and_swap( a, old_val, new_val )

#define kv_sync_add( a, val ) \
  __sync_fetch_and_add( a, val )

#define kv_sync_sub( a, val ) \
  __sync_fetch_and_sub( a, val )

#ifdef __cplusplus
}
#endif

#ifdef __cplusplus
namespace rai {
namespace kv {

/*#define TEST_ATOM_SPEED 1*/
template <class Int>
struct Atom {
  volatile Int val;
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
  /* conveniences */
  Atom &operator++() { this->add( 1 ); return *this; }
  Atom &operator--() { this->sub( 1 ); return *this; }
  Atom &operator+=( const Int &i ) { this->add( i ); return *this; }
  Atom &operator-=( const Int &i ) { this->sub( i ); return *this; }
  Atom &operator=( const Int &i ) { this->val = i; return *this; }
  Int operator&( const Int &i ) const { return this->val & i; }
  operator Int() const { return this->val; }
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
template <class Int, class Owner>
struct MCSLock {
  Atom<Int> lock, next; /* should be init to zero */

  /* wait for lock to become available */
  Int acquire( Atom<Int> &link,  const Int locked_bit,  const Int my_id,
               uint64_t &spin,  Owner &closure ) {
    Int val, wait;
    if ( ( (val = link.xchg( my_id | locked_bit )) & locked_bit ) != 0 ) {
      /* These two store operations could be reordered as seen by the next
       * thread if memory ordering is weak */
      this->lock = locked_bit;
      closure.owner( val & ~locked_bit ).next = my_id | locked_bit;
      for (;;) {
        /* spin on my lock, waiting for it to be free by owner signal */
        if ( ( (wait = this->lock) & locked_bit ) == 0 ) /* val communicated */
          break;
        spin++;
        kv_sync_pause();
      }
      val = wait;
    }
    return val;
  }

  /* same as acquire, except don't wait for lock */
  Int try_acquire( Atom<Int> &link,  const Int locked_bit,  const Int my_id,
                   uint64_t &spin ) {
    for (;;) {
      Int val = link.val;
      if ( ( val & locked_bit ) != 0 )
        return val;
      if ( link.cmpxchg( val, my_id | locked_bit ) )
        return val;
      spin++;
      kv_sync_pause();
    }
  }

  /* release lock and wake waiters */
  void release( Atom<Int> &link,  const Int val,  const Int locked_bit,
                const Int my_id,  uint64_t &spin,  Owner &closure ) {
    /* set lock=val if no one else is waiting */
    if ( ! link.cmpxchg( my_id | locked_bit, val ) ) {
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
      own.lock = val;
    }
    this->next = 0; /* reset */
    this->lock = 0;
  }
};

} /* namespace kv */
} /* namespace rai */
#endif

#endif
