#ifndef __rai__raikv__ht_cuckoo_h__
#define __rai__raikv__ht_cuckoo_h__

#include <raikv/key_ctx.h>

namespace rai {
namespace kv {

struct CuckooVisit {
  uint64_t from_pos,
           from_hash,
           to_pos;
  int32_t  next;
  uint16_t to_off;
  uint8_t  to_inc;

  void set( uint64_t fpos,  uint64_t fhash,  uint64_t tpos,
            uint8_t tinc,  uint16_t toff,  int32_t nxt ) {
    this->from_pos  = fpos;
    this->from_hash = fhash;
    this->to_pos    = tpos;
    this->next      = nxt;
    this->to_off    = toff;
    this->to_inc    = tinc;
  }
};

template <uint32_t NBITS>
struct PositionBits {
  static const uint64_t M  = 64 - 1, /* mask */
                        SH = 6, /* 1 << 6 == 64 */
                        SZ = ( NBITS + M ) / 64; /* how many quad words */
  uint64_t val[ SZ ];

  PositionBits( uint64_t *pos_used,  uint32_t cnt ) {
    ::memset( this->val, 0, sizeof( this->val ) );
    for ( uint32_t i = 0; i < cnt; i++ ) {
      uint64_t b = pos_used[ i ];
      this->val[ ( b >> SH ) ] |= ( (uint64_t) 1 << ( b & M ) );
    }
  }

  uint64_t test_set( uint64_t b ) {
    b %= NBITS;
    uint64_t i = ( b >> SH );
    uint64_t j = ( (uint64_t) 1 << ( b & M ) ),
             t = this->val[ i ];
    this->val[ i ] |= j;
    return t & j;
  }
};

template <uint32_t MAX_VAL, uint32_t SHFT>
struct FixedPointMod {
  uint8_t mod_frac[ MAX_VAL + 1 ]; /* mod fractions for 1 -> MAX_VAL */

  static uint8_t get_fraction( uint32_t sz ) {
    uint32_t szlog2   = 32 - __builtin_clz( sz ),
             mask     = ( (uint32_t) 1 << szlog2 ) - 1,
             fraction = (uint32_t) ( ( (double) sz / (double) mask ) *
                                   (double) ( 1U << SHFT ) ),
             max_idx  = ( mask * fraction ) >> SHFT;
    return fraction - ( ( max_idx == sz ) ? 1 : 0 );
  }

  void init( void ) {
    for ( uint32_t x = 1; x < MAX_VAL + 1; x++ )
      this->mod_frac[ x ] = get_fraction( x );
    this->mod_frac[ 0 ] = 1; /* % 0 never used */
  }
  /* 1 <= limit <= MAX_VAL */
  uint32_t mod( uint32_t limit,  uint32_t hash,  uint32_t &bits ) {
    bits = ( 32 - __builtin_clz( limit ) );
    return ( ( hash & ( ( 1U << bits ) - 1 ) ) *
           (uint32_t) this->mod_frac[ limit ] ) >> SHFT;
  }
};

struct CuckooAltHash;

/* when is_find is true, not found returned, otherwise it runs cuckoo path
 * search for acquiring empty slot when key is not found */
struct CuckooPosition {
  KeyCtx        & kctx;          /* the key context of this position */
  CuckooAltHash * h;             /* alternative positions, created on demand */
  uint16_t        buckets_off;   /* next hash when buckets_off == buckets */
  uint8_t         inc;           /* hash number of current position */
  bool            is_path_search;/* search for empty slot search path acquire */

  CuckooPosition( KeyCtx &kc )
    : kctx( kc ), h( 0 ), is_path_search( false ) {}
  /* start a new search */
  void start( void ) { this->buckets_off = 0; this->inc = 0; }
  /* go to next position, use alternate hashes when no more buckets  */
  KeyStatus acquire_incr( uint64_t &pos,  const uint64_t chains,
                          bool &is_next_hash,  const bool have_drop ) {
    if ( ++pos == this->kctx.ht_size )
      pos = 0;
    if ( ++this->buckets_off != this->kctx.cuckoo_buckets )
      return KEY_OK;
    is_next_hash = true;
    KeyStatus status = this->next_hash( pos, false );
    if ( status != KEY_PATH_SEARCH )
      return status;
    if ( this->trylock_cuckoo_path() ) {
      if ( have_drop ) {
        this->unlock_cuckoo_path();
        return KEY_USE_DROP;
      }
      return KEY_PATH_SEARCH;
    }
    return KEY_BUSY;
  }
  KeyStatus acquire_incr_single_thread( uint64_t &pos,  const uint64_t chains,
                                        const bool have_drop ) {
    if ( ++pos == this->kctx.ht_size )
      pos = 0;
    if ( ++this->buckets_off != this->kctx.cuckoo_buckets )
      return KEY_OK;
    KeyStatus status = this->next_hash( pos, false );
    if ( status != KEY_PATH_SEARCH )
      return status;
    if ( have_drop )
      return KEY_USE_DROP;
    return KEY_PATH_SEARCH;
  }
  /* the find() function uses this version */
  KeyStatus find_incr( uint64_t &pos,  const uint64_t chains ) {
    if ( ++pos == this->kctx.ht_size )
      pos = 0;
    if ( ++this->buckets_off != this->kctx.cuckoo_buckets )
      return KEY_OK;
    return this->next_hash( pos, true );
  }
  bool trylock_cuckoo_path( void ) { /* lock for cuckoo path search */
    return this->kctx.ht.hdr.ht_spin_trylock( this->kctx.key );
  }
  void unlock_cuckoo_path( void ) { /* release cuckoo path search */
    this->kctx.ht.hdr.ht_spin_unlock( this->kctx.key );
  }
  KeyStatus next_hash( uint64_t &pos,  const bool is_find );

  void restore_inc( uint64_t pos );
};

struct CuckooAltHash {
  uint64_t * alt, /* alternate hashes alt[ 0 ] == original hash */
           * pos, /* position in ht[] of the alternates */
           * num; /* position in PositionBits<> for the ht pos[] */
  CuckooAltHash( uint8_t arity ) {
    this->alt  = (uint64_t *) (void *) &this[ 1 ];
    this->pos  = &this->alt[ arity ];
    this->num  = &this->pos[ arity ];
  }
  void * operator new( size_t sz, void *ptr ) { return ptr; }

  static size_t size( uint8_t arity ) {
    return sizeof( CuckooAltHash ) + sizeof( uint64_t ) * arity * 3;
  }
  static CuckooAltHash *create( KeyCtx &kctx );

  void calc_hash( KeyCtx &kctx,  const uint64_t key,
                  const uint64_t key2,  const uint64_t start_pos );
  KeyStatus find_cuckoo_path( CuckooPosition &kpos );
};

}
}

#endif

