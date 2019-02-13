#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stddef.h>

#include <raikv/shm_ht.h>
#include <raikv/ht_search.h>
#include <raikv/ht_cuckoo.h>

using namespace rai;
using namespace kv;

static const uint32_t MAX_CUCKOO_RETRY = 5;
static const uint64_t cuckoo_position_8k = 8 * 1024;
static const uint64_t cuckoo_position_mask = cuckoo_position_8k - 1;
static const uint64_t cuckoo_hash_seed     = _U64( 0x9e3779b9U, 0x7f4a7c13U );

/* xoroshiro128plus */
static void
cuckoo_incr( uint64_t &state0,  uint64_t &state1 )
{
  const uint64_t s0 = state0;
  const uint64_t s1 = state1 ^ s0;
  state0 = kv::rand::rotl( s0, 55 ) ^ s1 ^ ( s1 << 14 ); // a, b
  state1 = kv::rand::rotl( s1, 36 ); // c
}

CuckooAltHash *
CuckooAltHash::create( KeyCtx &kctx )
{
  void * p = kctx.wrk->alloc( CuckooAltHash::size( kctx.cuckoo_arity ) );
  if ( p == NULL )
    return NULL;
  return new ( p ) CuckooAltHash( kctx.cuckoo_arity );
}

void
CuckooAltHash::calc_hash( KeyCtx &kctx,  const uint64_t key,
                          const uint64_t key2,  const uint64_t start_pos )
{
  const uint64_t buckets = kctx.cuckoo_buckets;
  uint64_t       seed = cuckoo_hash_seed ^ key2;
  uint64_t     * r    = this->alt,
               * q    = this->pos,
               * n    = this->num;
  bool           collision;
  uint8_t        inc  = 1;

  r[ 0 ] = key;
  q[ 0 ] = start_pos;
  n[ 0 ] = start_pos & cuckoo_position_mask;

  r[ 1 ] = key2;
  q[ 1 ] = kctx.ht.hdr.ht_mod( key2 );
  n[ 1 ] = q[ 1 ] & cuckoo_position_mask;

  collision = ( n[ 1 ] == n[ 0 ] ) |
    ( KeyCtx::calc_offset( q[ 1 ], q[ 0 ], kctx.ht_size ) < buckets ) |
    ( KeyCtx::calc_offset( q[ 0 ], q[ 1 ], kctx.ht_size ) < buckets );
  if ( ! collision )
    inc++;

  /* find unique spots in the ht for all of the alt hashes */
  for ( ; inc < kctx.cuckoo_arity; inc++ ) {
    r[ inc ] = r[ inc - 1 ];
    do {
      cuckoo_incr( r[ inc ], seed );
      q[ inc ] = kctx.ht.hdr.ht_mod( r[ inc ] );
      n[ inc ] = q[ inc ] & cuckoo_position_mask;
      collision = false;
      for ( uint8_t j = 0; j < inc; j++ ) {
        collision |= ( n[ inc ] == n[ j ] ) |
          ( KeyCtx::calc_offset( q[ inc ], q[ j ], kctx.ht_size ) < buckets ) |
          ( KeyCtx::calc_offset( q[ j ], q[ inc ], kctx.ht_size ) < buckets );
      }
    } while ( collision );
  }
}

KeyStatus
CuckooAltHash::find_cuckoo_path( CuckooPosition &cp )
{
  static const uint32_t node_size = /*buckets*/8 * /*arity*/4 * 36, /* 1152 */
                        stk_size  = /*buckets*/8 * /*arity*/4 * 12; /* 384 */
  static FixedPointMod<stk_size, 8> fpmod;
  KeyCtx           & kctx      = cp.kctx;
  const uint32_t     arity     = kctx.cuckoo_arity,
                     buckets   = kctx.cuckoo_buckets;
  const uint64_t     ht_size   = kctx.ht_size;
  KeyCtx             to_kctx( kctx ),
                     fr_kctx( kctx );
  WorkAllocT<1024>   wrk, wrk2;
  CuckooVisit        node[ node_size ],
                   * vis;
  uint32_t           stk[ stk_size ];
   /*= (CuckooVisit *) kctx.wrk->alloc( node_size * sizeof( CuckooVisit ) ),*/
   /*= (uint32_t *) kctx.wrk->alloc( stk_size * sizeof( uint32_t ) );*/
  CuckooAltHash    * h   = CuckooAltHash::create( kctx );
  ThrCtx           & ctx = kctx.thr_ctx;
  rand::xoroshiro128plus
                   & rng = ctx.rng;
  uint64_t           key, key2, p, rng_bits, boff;
  uint32_t           inc, off, tos, maxtos, tos_bits,
                     fetch_cnt = 0, acquire_cnt = 0, move_cnt = 0,
                     busy_cnt = 0, retry;
  KeyStatus          status;

  if ( kv_unlikely( fpmod.mod_frac[ 0 ] == 0 ) ) /* init fixed point mod */
    fpmod.init();
  rng_bits = rng.next();

  for ( retry = 0; retry < MAX_CUCKOO_RETRY; retry++ ) {
    PositionBits<cuckoo_position_8k> bits( this->num, arity );
    tos = 0;
    off = 0;

    for ( inc = 0; inc < arity; inc++ ) {
      p    = this->pos[ inc ];
      boff = 0;
      while ( boff < buckets ) {
        node[ off ].set( ZOMBIE64, 0, p, inc, boff++, -1 );
        if ( ++p == ht_size )
          p = 0;
        stk[ tos ] = off;
        off++; tos++;
      }
    }

    to_kctx.set( KEYCTX_IS_CUCKOO_ACQUIRE );
    fr_kctx.set( KEYCTX_IS_CUCKOO_ACQUIRE );
    maxtos = tos;
    while ( tos > 0 ) {
      /* randomly choose an entry to move, idx = rng % tos, rng >>= tos_bits */
      uint32_t idx = fpmod.mod( tos, rng_bits, tos_bits );
      rng_bits   >>= tos_bits;
      vis          = &node[ stk[ idx ] ];
      stk[ idx ]   = stk[ --tos ];

      /* p is the position that needs to be moved to create space */
      p      = vis->to_pos;
      status = fr_kctx.fetch( &wrk, p ); /* try to move this somewhere else */
      fetch_cnt++;
      if ( status != KEY_OK ) {
        if ( status != KEY_BUSY ) /* if KEY_NOT_FOUND, try acquire again */
          goto key_busy;
        busy_cnt++;
        continue; /* skip this node, is acquired by something else */
      }
      /* trying to move this key */
      key  = fr_kctx.key;
      key2 = fr_kctx.key2;
      inc  = fr_kctx.inc;
      /* fill alternative hash with positions based on key */
      h->calc_hash( kctx, key, key2, kctx.ht.hdr.ht_mod( key ) );
      /* get the current bucket position */
      boff = KeyCtx::calc_offset( h->pos[ inc ], p, kctx.ht_size );

      /* go to the next bucket */
      if ( ++boff >= buckets ) {
        if ( ++inc == arity )
          continue; /* no more positions left in forward direction */
        boff = 0;
        p = h->pos[ inc ];
      }
      else {
        if ( ++p == ht_size )
          p = 0;
      }
      /* loop through the alternate positions of this key */
      for (;;) {
        status = to_kctx.fetch( &wrk2, p );
	fetch_cnt++;
        /* if another key to move, push it to the search stack */
        if ( status == KEY_OK ) {
	  if ( off < node_size && tos < stk_size ) {
	    node[ off ].set( vis->to_pos, key, p, inc, boff,
                             (int32_t) ( vis - node ) );
	    stk[ tos ] = off;
	    off++; tos++;
	  }
        }
        /* if slot empty, it's the end of the search */
        else if ( status == KEY_NOT_FOUND ) {
          status = to_kctx.try_acquire_position( p );
	  acquire_cnt++;
          if ( status == KEY_IS_NEW ) {
            status = fr_kctx.try_acquire_position( vis->to_pos );
	    acquire_cnt++;
            /* make sure it didn't change */
            if ( status == KEY_OK && fr_kctx.key == key ) {
              HashEntry * frent = fr_kctx.entry,
                        * toent = to_kctx.entry;
              /* copy from -> to */
              ::memcpy( &((uint8_t *) (void *) toent)[ sizeof( toent->hash ) ],
                        &((uint8_t *) (void *) frent)[ sizeof( toent->hash ) ],
                        kctx.hash_entry_size - sizeof( toent->hash ) );
              /* clear from */
              ::memset( &((uint8_t *) (void *) frent)[ sizeof( frent->hash ) ],
                        0, kctx.hash_entry_size - sizeof( frent->hash ) );
              move_cnt++;
              //toent->set( FL_BUSY );
              to_kctx.inc    = inc;
              to_kctx.lock   = fr_kctx.lock;
              to_kctx.key    = fr_kctx.key;
              to_kctx.key2   = fr_kctx.key2;
              to_kctx.db_num = fr_kctx.db_num;
              to_kctx.serial = fr_kctx.serial;
              to_kctx.release();

              /* fr_kctx now empty, move fr_kctx to to_kctx */
              to_kctx.pos    = fr_kctx.pos;
              to_kctx.mcs_id = fr_kctx.mcs_id;
              to_kctx.entry  = frent;
              to_kctx.clear( KEYCTX_IS_READ_ONLY );

              /* make a path back to original key */
              for (;;) {
                /* no more moves left, this is a usable empty spot */
                if ( vis->from_pos == ZOMBIE64 ) {
                  status = KEY_IS_NEW;
                  cp.inc         = vis->to_inc;
                  cp.buckets_off = vis->to_off + 1;
                  if ( cp.buckets_off == buckets ) {
                    cp.inc++;
                    cp.buckets_off = 0;
                  }
                  /* if not at the end of the chain, jumping backwards
                   * requires a check that no other thread added the same
                   * hash entry key */
                  if ( cp.inc < arity ) {
                    /* when acq position incremented, path_search is checked */
                    cp.is_path_search = true;
                    key = kctx.key;
                    p = this->pos[ cp.inc ] + cp.buckets_off;
                    if ( p >= ht_size )
                      p -= ht_size;
                    /* this set causes acquire to not incr write counter */
                    kctx.set( KEYCTX_IS_CUCKOO_ACQUIRE );
                    /* search after this location, see if another thread
                     * did a cuckoo search and found a position */
                    status = kctx.acquire<CuckooPosition, false>( key, p, cp );
                    kctx.clear( KEYCTX_IS_CUCKOO_ACQUIRE );
                    cp.is_path_search = false;
                    /* if another key exists, then drop this entry and use
                     * the other key */
                    if ( status != KEY_NOT_FOUND ) {
                      /* release the empty slot, found another or busy */
                      to_kctx.drop_flags = FL_DROPPED;
                      to_kctx.lock       = 0;
                      to_kctx.drop_key   = DROPPED_HASH;
                      to_kctx.drop_key2  = 0;
                      to_kctx.release();
                      /*if ( status == KEY_OK )
                        kctx.entry->set( FL_BUSY );*/
                      /* either KEY_IS_NEW, KEY_BUSY, KEY_OK or error status */
                      goto skip_new_slot_init;
                    }
                    else {
                      /* no other slot found, use the newly empty slot */
                      status = KEY_IS_NEW;
                    }
                  }
                  /* initialize kctx with the new hash entry */
                  if ( status == KEY_IS_NEW ) {
                    kctx.drop_flags = FL_DROPPED;
                    kctx.clear( KEYCTX_IS_READ_ONLY );
                    kctx.inc       = vis->to_inc;
                    kctx.pos       = to_kctx.pos;
                    kctx.lock      = 0;
                    kctx.drop_key  = DROPPED_HASH;
                    kctx.drop_key2 = 0;
                    kctx.mcs_id    = to_kctx.mcs_id;
                    kctx.entry     = to_kctx.entry;
                    kctx.entry->set( FL_MOVED );
                  }
                skip_new_slot_init:;
                  kctx.incr_cuckacq( acquire_cnt );
                  kctx.incr_cuckfet( fetch_cnt );
                  kctx.incr_cuckmov( move_cnt );
                  kctx.incr_cuckbiz( busy_cnt );
                  return status;
                }
                status = fr_kctx.try_acquire_position( vis->from_pos );
		acquire_cnt++;
                /* if key still there and is movable to new location */
                if ( status == KEY_OK && fr_kctx.key == vis->from_hash ) {
                  frent = fr_kctx.entry,
                  toent = to_kctx.entry;
                  /* copy from -> to */
                  ::memcpy(
                    &((uint8_t *) (void *) toent)[ sizeof( toent->hash ) ],
                    &((uint8_t *) (void *) frent)[ sizeof( toent->hash ) ],
                    kctx.hash_entry_size - sizeof( toent->hash ) );
                  /* clear from */
                  ::memset(
                    &((uint8_t *) (void *) frent)[ sizeof( frent->hash ) ],
                    0, kctx.hash_entry_size - sizeof( frent->hash ) );
		  move_cnt++;
                  //toent->set( FL_BUSY );
                  to_kctx.inc    = vis->to_inc;
                  to_kctx.lock   = fr_kctx.lock;
                  to_kctx.key    = fr_kctx.key;
                  to_kctx.key2   = fr_kctx.key2;
                  to_kctx.db_num = fr_kctx.db_num;
                  to_kctx.serial = fr_kctx.serial;
                  to_kctx.release();

                  /* fr_kctx now empty, move fr_kctx to to_kctx */
                  to_kctx.pos    = fr_kctx.pos;
                  to_kctx.mcs_id = fr_kctx.mcs_id;
                  to_kctx.entry  = frent;
                  to_kctx.clear( KEYCTX_IS_READ_ONLY );
                  vis = &node[ vis->next ];
                }
                /* busy or key changed, mark empty spot as dropped */
                else {
                  to_kctx.drop_flags = FL_DROPPED;
                  to_kctx.clear( KEYCTX_IS_READ_ONLY );
                  to_kctx.lock       = 0;
                  to_kctx.drop_key   = DROPPED_HASH;
                  to_kctx.drop_key2  = 0;
                  busy_cnt++;
                  break;
                }
              }
            }
            /* to_ctx key is new, mark as dropped to fill holes */
            else if ( to_kctx.drop_key == 0 ) {
              to_kctx.drop_flags = FL_DROPPED;
              to_kctx.drop_key   = DROPPED_HASH;
              to_kctx.drop_key2  = 0;
              busy_cnt++;
            }
            fr_kctx.release();
          }
          to_kctx.release();
        }
        if ( ++boff == buckets ) { /* if no more buckets to search */
          /* if no more hashes to search */
          if ( ++inc == arity || bits.test_set( h->pos[ inc ] ) != 0 )
            break;
          boff = 0;
          p = h->pos[ inc ]; /* next hash start position */
        }
        else {
          if ( ++p == ht_size )
            p = 0;
        }
      }
      if ( tos > maxtos )
        maxtos = tos;
      if ( rng_bits == 0 )
        rng_bits = rng.next();
    }
    //printf( "retrying %u off %u maxtos %u\n", retry, off, maxtos );
    kctx.incr_cuckret( 1 );
  }
key_busy:;
  kctx.incr_cuckacq( acquire_cnt );
  kctx.incr_cuckfet( fetch_cnt );
  kctx.incr_cuckmov( move_cnt );
  kctx.incr_cuckbiz( busy_cnt );
  if ( retry == MAX_CUCKOO_RETRY ) {
    kctx.incr_cuckmax( 1 );
    return KEY_HT_FULL;
  }
  return KEY_BUSY;
}

KeyStatus
KeyCtx::acquire_cuckoo( const uint64_t k,  const uint64_t start_pos )
{
  CuckooPosition cp( *this );
  KeyStatus status;
  for (;;) {
    cp.start();
    this->inc = 0;
    status = this->acquire<CuckooPosition, true>( k, start_pos, cp );
    if ( status == KEY_PATH_SEARCH ) {
      if ( cp.h == NULL ) {
        cp.h = CuckooAltHash::create( *this );
        if ( cp.h != NULL )
          cp.h->calc_hash( *this, this->key, this->key2, this->start );
        else
          status = KEY_ALLOC_FAILED;
      }
      if ( status == KEY_PATH_SEARCH )
        status = cp.h->find_cuckoo_path( cp );
      cp.unlock_cuckoo_path();
    }
    if ( status != KEY_BUSY )
      return status;
  }
}

KeyStatus
KeyCtx::try_acquire_cuckoo( const uint64_t k,  const uint64_t start_pos )
{
  CuckooPosition cp( *this );
  KeyStatus status;
  cp.start();
  this->inc = 0;
  status = this->acquire<CuckooPosition, false>( k, start_pos, cp );
  if ( status == KEY_PATH_SEARCH ) {
    status = cp.h->find_cuckoo_path( cp );
    cp.unlock_cuckoo_path();
  }
  return status;
}

KeyStatus
KeyCtx::acquire_cuckoo_single_thread( const uint64_t k,
                                      const uint64_t start_pos )
{
  CuckooPosition cp( *this );
  KeyStatus status;
  cp.start();
  this->inc = 0;
  status = this->acquire_single_thread<CuckooPosition>( k, start_pos, cp );
  if ( status == KEY_PATH_SEARCH ) {
    if ( cp.h == NULL ) {
      cp.h = CuckooAltHash::create( *this );
      if ( cp.h != NULL )
        cp.h->calc_hash( *this, this->key, this->key2, this->start );
      else
        status = KEY_ALLOC_FAILED;
    }
    if ( status == KEY_PATH_SEARCH )
      status = cp.h->find_cuckoo_path( cp );
  }
  return status;
}

KeyStatus
KeyCtx::find_cuckoo( const uint64_t k,  const uint64_t start_pos,
                     const uint64_t spin_wait )
{
  CuckooPosition cp( *this );
  cp.start();
  this->inc = 0;
  return this->find<CuckooPosition>( k, start_pos, spin_wait, cp );
}

KeyStatus
KeyCtx::find_cuckoo_single_thread( const uint64_t k,  const uint64_t start_pos )
{
  CuckooPosition cp( *this );
  cp.start();
  this->inc = 0;
  return this->find_single_thread<CuckooPosition>( k, start_pos, cp );
}

KeyStatus
CuckooPosition::next_hash( uint64_t &pos,  const bool is_find )
{
  if ( ++this->inc != this->kctx.cuckoo_arity ) {
    if ( this->h == NULL ) {
      this->h = CuckooAltHash::create( this->kctx );
      if ( this->h == NULL )
        return KEY_ALLOC_FAILED;
      this->h->calc_hash( this->kctx, this->kctx.key, this->kctx.key2,
                          this->kctx.start );
    }
    pos = this->h->pos[ this->inc ];
    this->kctx.inc    = this->inc;
    this->buckets_off = 0;
    return KEY_OK;
  }
  /* find or path search ends here */
  if ( is_find || this->is_path_search )
    return KEY_NOT_FOUND;
  return KEY_PATH_SEARCH;
}

void
CuckooPosition::restore_inc( uint64_t pos )
{
  if ( this->h != NULL ) {
    for ( uint8_t inc = 0; inc < this->kctx.cuckoo_arity; inc++ ) {
      uint64_t pos_off = KeyCtx::calc_offset( this->h->pos[ inc ], pos,
                                              this->kctx.ht_size );
      if ( pos_off < this->kctx.cuckoo_buckets ) {
        this->kctx.inc = inc;
        return;
      }
    }
  }
  this->kctx.inc = 0;
}

void
KeyCtx::get_pos_info( uint64_t &natural_pos,  uint64_t &pos_off )
{
  natural_pos = this->ht.hdr.ht_mod( this->key );
  pos_off = KeyCtx::calc_offset( natural_pos, this->pos, this->ht_size );
  if ( this->cuckoo_buckets > 1 && pos_off >= (uint64_t) this->cuckoo_buckets ) {
    CuckooAltHash *h = CuckooAltHash::create( *this );
    h->calc_hash( *this, this->key, this->key2, natural_pos );
    for ( uint8_t inc = 1; inc < this->cuckoo_arity; inc++ ) {
      pos_off = KeyCtx::calc_offset( h->pos[ inc ], this->pos, this->ht_size );
      if ( pos_off < this->cuckoo_buckets ) {
        pos_off += ( inc * this->cuckoo_buckets );
        break;
      }
    }
  }
}

/* spin on ht[ i ] until it is fetchable */
KeyStatus
KeyCtx::try_acquire_position( const uint64_t i )
{
  ThrCtx    & ctx = this->thr_ctx;
  HashEntry * el;          /* current ht[] ptr */
  uint64_t    cur_mcs_id,  /* MCS lock queue for the current element */
              h,           /* hash val at the current element */
              spin = 0;  /* count of spinning */

  this->init_acquire();
  cur_mcs_id = ctx.next_mcs_lock();
  el         = this->ht.get_entry( i, this->hash_entry_size );
  h          = ctx.get_mcs_lock( cur_mcs_id ).try_acquire( el->hash, i,
                                                   ZOMBIE64, cur_mcs_id, spin );
  if ( spin > 0 )
    this->incr_spins( spin );
  if ( (h & ZOMBIE64) == 0 ) {
    if ( ! this->test( KEYCTX_IS_CUCKOO_ACQUIRE ) )
      this->incr_write();
    else
      this->db_num = el->value_ctr( this->hash_entry_size ).db;
    if ( el->test( FL_DROPPED ) ) {
      this->drop_key   = h;
      this->drop_key2  = el->hash2;
      this->drop_flags = el->flags;
      el->flags        = FL_NO_ENTRY;
      h                = 0;
    }
    this->inc    = el->cuckoo_inc();
    this->clear( KEYCTX_IS_READ_ONLY );
    this->pos    = i;
    this->key    = h;
    this->key2   = el->hash2;
    this->lock   = h;
    this->mcs_id = cur_mcs_id;
    this->serial = el->value_ctr( this->hash_entry_size ).get_serial();
    this->entry  = el;
    return ( h == 0 ? KEY_IS_NEW : KEY_OK );
  }
  ctx.release_mcs_lock( cur_mcs_id );
  this->set( KEYCTX_IS_READ_ONLY );
  return KEY_BUSY;
}

