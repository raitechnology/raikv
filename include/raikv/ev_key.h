#ifndef __rai_raikv__ev_key_h__
#define __rai_raikv__ev_key_h__

#include <raikv/shm_ht.h>
#include <raikv/key_buf.h>
#include <raikv/prio_queue.h>

namespace rai {
namespace kv {

struct EvSocket;

struct EvKeyTempResult {
  size_t  mem_size, /* alloc size */
          size;     /* data size */
  char * data( size_t x ) const {
    return &((char *) (void *) &this[ 1 ])[ x ];
  }
};

enum EvKeyStatus {/* status for exec_key_continue(), memcached_key_continue() */
  EK_SUCCESS  = 20, /* statement finished */
  EK_DEPENDS  = 21, /* key depends on another */
  EK_CONTINUE = 22  /* more keys to process */
};
/* the key operations (ex. set, lpush, del, xadd, etc) set these flags --
 * when something is subscribed to a keyspace event, it causes a publish to
 * occur for the event if the bits subscribed & flags != 0 */
enum EvKeyFlags { /* bits for EvKeyCtx::flags */
  EKF_IS_READ_ONLY    = 1,     /* key is read only (no write access) */
  EKF_IS_NEW          = 2,     /* key doesn't exist */
  EKF_UNUSED          = 4,     /* placeholder for something else */
  EKF_IS_EXPIRED      = 8,     /* a key read oper caused expired data */
  EKF_KEYSPACE_FWD    = 0x10,  /* operation resulted in a keyspace mod */
  EKF_KEYEVENT_FWD    = 0x20,  /* operation resulted in a keyevent mod */
  EKF_KEYSPACE_EVENT  = 0x30,  /* when a key is updated, this is set */
  EKF_KEYSPACE_DEL    = 0x40,  /* keyspace event (pop, srem, etc) -> key del */
  EKF_KEYSPACE_TRIM   = 0x80,  /* xadd with maxcnt caused a trim event */
  EKF_LISTBLKD_NOT    = 0x100, /* notify a blocked list an element available */
  EKF_ZSETBLKD_NOT    = 0x200, /* notify a blocked zset */
  EKF_STRMBLKD_NOT    = 0x400, /* notify a blocked stream */
  EKF_MONITOR         = 0x800, /* command monitor is enabled */

#define EKF_TYPE_SHIFT 12
  /* the type of the key that event occurred */
  EKF_KEYSPACE_STRING = ( 1 << EKF_TYPE_SHIFT ), /* 4092 */
  EKF_KEYSPACE_LIST   = ( 2 << EKF_TYPE_SHIFT ),
  EKF_KEYSPACE_HASH   = ( 3 << EKF_TYPE_SHIFT ),
  EKF_KEYSPACE_SET    = ( 4 << EKF_TYPE_SHIFT ),
  EKF_KEYSPACE_ZSET   = ( 5 << EKF_TYPE_SHIFT ),
  EKF_KEYSPACE_GEO    = ( 6 << EKF_TYPE_SHIFT ),
  EKF_KEYSPACE_HLL    = ( 7 << EKF_TYPE_SHIFT ),
  EKF_KEYSPACE_STREAM = ( 8 << EKF_TYPE_SHIFT )  /* 32768 */
  /* must fit into 16 bits (no more left) */
};

enum EvKeyState { /* state of key */
  EKS_INIT          = 0,     /* initialize */
  EKS_IS_SAVED_CONT = 1,     /* key has saved data on continuation */
  EKS_NO_UPDATE     = 2      /* key doesn't need an update stamp */
};

struct EvKeyCtx {
  void * operator new( size_t, void *ptr ) { return ptr; }

  static bool is_greater( EvKeyCtx *ctx,  EvKeyCtx *ctx2 ) noexcept;
  kv::HashTab     & ht;
  EvSocket        * owner;   /* parent connection */
  uint64_t          hash1,   /* 128 bit hash of key */
                    hash2;
  int64_t           ival;    /* if it returns int */
  EvKeyTempResult * part;    /* saved data for key */
  const int         argn;    /* which arg number of command */
  int               status;  /* result of exec for this key */
  kv::KeyStatus     kstatus; /* result of key lookup */
  uint16_t          flags;   /* ekf flags is new, is read only */
  uint8_t           dep,     /* depends on another key */
                    type;    /* value type, string, list, hash, etc */
  const uint32_t    key_idx; /* if cmd has multiple keys */
  uint16_t          state;   /* eks state flags */
  kv::KeyFragment   kbuf;    /* key material, extends past structure */

  EvKeyCtx( kv::HashTab &h,  EvSocket *own,  const void *key,  size_t keystrlen,
            const int n,  const uint32_t idx,  const kv::HashSeed &hs )
     : ht( h ), owner( own ), ival( 0 ), part( 0 ), argn( n ), status( 0 ),
       kstatus( KEY_OK ), flags( EKF_IS_READ_ONLY ), dep( 0 ), type( 0 ),
       key_idx( idx ), state( EKS_INIT ) {
    this->copy_key( key, keystrlen );
    hs.hash( this->kbuf, this->hash1, this->hash2 );
  }

  EvKeyCtx( EvKeyCtx &ctx )
     : ht( ctx.ht ), owner( ctx.owner ), hash1( ctx.hash1 ), hash2( ctx.hash2 ),
       ival( 0 ), part( 0 ), argn( ctx.argn ), status( 0 ), kstatus( KEY_OK ),
       flags( EKF_IS_READ_ONLY ), dep( 0 ), type( ctx.type ),
       key_idx( ctx.key_idx ), state( EKS_INIT ) {
    this->copy_key( ctx.kbuf.u.buf, ctx.kbuf.keylen - 1 );
  }

  EvKeyCtx( kv::HashTab &h )
     : ht( h ), owner( 0 ), hash1( 0 ), hash2( 0 ), ival( 0 ), part( 0 ),
       argn( 0 ), status( 0 ), kstatus( KEY_OK ), flags( EKF_IS_READ_ONLY ),
       dep( 0 ), type( 0 ), key_idx( 0 ), state( EKS_INIT ) {}

  void copy_key( const void *key,  size_t keystrlen ) {
    uint16_t       * p = (uint16_t *) (void *) this->kbuf.u.buf;
    const uint16_t * k = (const uint16_t *) key,
                   * e = &k[ ( keystrlen + 1 ) / 2 ];
    do {
      *p++ = *k++;
    } while ( k < e );
    this->kbuf.u.buf[ keystrlen ] = '\0'; /* keys terminate with nul char */
    this->kbuf.keylen = keystrlen + 1;
  }
  static size_t size( size_t keystrlen ) {
    /* alloc size of *this (kbuf has 4 bytes pad) */
    return sizeof( EvKeyCtx ) + keystrlen;
  }
  const char *get_type_str( void ) const noexcept;
  EvKeyCtx *set( kv::KeyCtx &kctx ) {
    kctx.set_key( this->kbuf );
    kctx.set_hash( this->hash1, this->hash2 );
    return this;
  }
  void prefetch( kv::HashTab &ht,  bool for_read ) {
    ht.prefetch( this->hash1, for_read );
  }
  bool is_new( void ) const {
    return ( this->flags & EKF_IS_NEW ) != 0;
  }
  bool is_read_only( void ) const {
    return ( this->flags & EKF_IS_READ_ONLY ) != 0;
  }
};

inline bool
EvKeyCtx::is_greater( EvKeyCtx *ctx,  EvKeyCtx *ctx2 ) noexcept
{
  if ( ctx->dep > ctx2->dep ) /* fetch other keys first if depends on another */
    return true;
  if ( ctx->dep == ctx2->dep ) {
    return ctx->ht.hdr.ht_mod( ctx->hash1 ) > /* if ht position greater */
           ctx->ht.hdr.ht_mod( ctx2->hash1 );
  }
  return false;
}

struct EvPrefetchQueue {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  EvKeyCtx ** ar;      /* ring buffer queue */
  size_t      ar_size, /* count of queue elems */
              hd,      /* next to prefetch */
              cnt;     /* hd + cnt push to queue */
  EvKeyCtx  * ini[ 1024 ]; /* prefetch will fit in this array in most cases */
  EvPrefetchQueue() : ar( this->ini ), ar_size( 1024 ), hd( 0 ), cnt( 0 ) {}

  bool more_queue( void ) noexcept;

  bool push( EvKeyCtx *k ) {
    /* if full, realloc some more */
    if ( this->cnt == this->ar_size ) {
      if ( ! this->more_queue() )
        return false;
    }
    this->ar[ ( this->hd + this->cnt ) & ( this->ar_size - 1 ) ] = k;
    this->cnt++;
    return true;
  }

  bool is_empty( void ) const { return this->cnt == 0; }
  size_t count( void ) const  { return this->cnt; }

  EvKeyCtx *pop( void ) {
    size_t    n = this->hd & ( this->ar_size - 1 );
    EvKeyCtx *k = this->ar[ n ];
    this->ar[ n ] = NULL;
    this->hd++; this->cnt--;
    return k;
  }

  static EvPrefetchQueue *create( void ) {
    void *p = ::malloc( sizeof( EvPrefetchQueue ) );
    return new ( p ) EvPrefetchQueue();
  }
};

#if 0
struct EvPrefetchQueue :
    public kv::PrioQueue<EvKeyCtx *, EvKeyCtx::is_greater> {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  static EvPrefetchQueue *create( void ) {
    void *p = ::malloc( sizeof( EvPrefetchQueue ) );
    return new ( p ) EvPrefetchQueue();
  }
};
#endif
}
}

#endif
