#ifndef __rai__raikv__hash_entry_h__
#define __rai__raikv__hash_entry_h__

/* also include stdint.h, string.h */
#include <raikv/atom.h>
#include <raikv/rela_ts.h>
#include <raikv/key_hash.h>

#ifdef __cplusplus
extern "C" {
#endif

/* choice of hash function matters less as memory latency dominates access,
 * especially when the hash computation is hidden within mem prefetching */
#ifndef KV_DEFAULT_HASH
/*#define KV_DEFAULT_HASH kv_hash_murmur128*/
/*#define KV_DEFAULT_HASH_STR "murmur128"*/
/*#define KV_DEFAULT_HASH kv_hash_citymur128*/
/*#define KV_DEFAULT_HASH_STR "citymur128"*/
/*#define KV_DEFAULT_HASH kv_hash_spooky128*/
/*#define KV_DEFAULT_HASH_STR "spooky128"*/
/*#define KV_DEFAULT_HASH kv_hash_aes128*/
/*#define KV_DEFAULT_HASH_STR "aes128"*/
#define KV_DEFAULT_HASH kv_hash_meow128
#define KV_DEFAULT_HASH_STR "meow128"
#endif

typedef struct kv_key_frag_s {
  uint16_t keylen;
  union {
    char buf[ 4 /* KEY_FRAG_SIZE-2 */]; /* sized to fit into HashEntry */
    struct {
      uint16_t b1, b2;
    } x;
  } u;
} kv_key_frag_t;

#ifdef __cplusplus
} /* extern "C" */
#endif

#ifdef __cplusplus
namespace rai {
namespace kv {

/* high bit in hash that signifies entity is not present
   (not in the main ht[] index, that doesn't have tombstones) */
static const uint64_t ZOMBIE64     = (uint64_t) 0x80000000 << 32,
                      /*EMPTY_HASH   = 0, -- unused hash for empty */
                      DROPPED_HASH = 1; /* unused hash for drops */

static const size_t KEY_FRAG_SIZE = 6; /* must fit in HashEntry % 8 */

/* KeyFragment is used everywhere internally, but externally KeyBufT<N>
 * provides more key buffer space, since keylen=N could be large */
struct KeyFragment : public kv_key_frag_s {

  bool frag_equals( const KeyFragment &k ) const {
    const uint32_t  j = this->keylen;
    bool           eq = ( j == k.keylen );
    if ( eq ) {
      const uint8_t *p1 = (const uint8_t *) this->u.buf;
      const uint8_t *p2 = (const uint8_t *) k.u.buf;
      uint32_t i = 0;
      while ( i + 4 <= j ) {
        /* presuming keys are eq since hash is eq */
        eq &= *(const uint32_t *) &p1[ i ] == *(const uint32_t *) &p2[ i ];
        i += 4;
      }
      if ( ( j & 2 ) != 0 ) {
        eq &= *(const uint16_t *) &p1[ i ] == *(const uint16_t *) &p2[ i ];
        i += 2;
      }
      if ( ( j & 1 ) != 0 )
        eq &= ( p1[ i ] == p2[ i ] );
    }
    return eq;
  }
  /* 127 bit hash */
  void hash( uint64_t &seed,  uint64_t &seed2 /*,
             kv_hash128_func_t func = KV_DEFAULT_HASH*/ ) {
    /*func*/
    KV_DEFAULT_HASH( this->u.buf, this->keylen, &seed, &seed2 );
    if ( (seed &= ~ZOMBIE64) <= DROPPED_HASH) /* clear tombstone */
      seed = DROPPED_HASH + 1; /* zero & one are reserved for empty, dropped */
  }
};

/* flags attached to hash entry and msg value */
enum KeyValueFlags {
  FL_NO_ENTRY        =   0x00, /* initialize */
#define FL_CUCKOO_SHIFT 0
  FL_CUCKOO_INC      =   0x0f, /* 0x0f << FL_CUCKOO_SHIFT, X=[0->f], hash num */
  FL_SEQNO           =   0x10,
  FL_MSG_LIST        =   0x20,
  /* FL_ALIGNMENT       =   0x03, X=[0->3], data alignment: 2 << X, 2 -> 16 */
  FL_SEGMENT_VALUE   =   0x40, /* value is in segment */
  FL_UPDATED         =   0x80, /* was updated recently, useful for persistence*/
  FL_IMMEDIATE_VALUE =  0x100, /* has immediate data, no segment data used */
  FL_IMMEDIATE_KEY   =  0x200, /* full key is in hash entry */
  FL_PART_KEY        =  0x400, /* part of key is in hash entry */
  FL_DROPPED         =  0x800, /* dropped key + data */
  FL_EXPIRE_STAMP    = 0x1000, /* has expiration time stamp */
  FL_UPDATE_STAMP    = 0x2000, /* has update time stamp */
  FL_MOVED           = 0x4000, /* cuckoo move */
  FL_BUSY            = 0x8000  /* msg is busy */
};

struct ValueGeom {
  /* a pointer to the value stored with a key, segment max 1 << 16 */
  uint32_t segment;
  uint64_t size, offset; /* off/size max 1 << ( 24 + table alignment(def=6) ) */
  uint64_t serial;
  void zero( void ) { this->segment = 0; this->size =
                      this->offset = this->serial = 0; }
};

struct ValuePtr { /* packed 128 bit pointer with seg & off & size & serial */
  static const uint32_t VALUE_SIZE_BITS = 32; /* number of bits in offset/size*/
  uint16_t segment, /* seg=16, off=32, size=32, serial=48 = 128 */
           serialhi;
  uint32_t seriallo,
           size,
           offset;

  void set( const ValueGeom &geom,  uint32_t align_shift ) {
    this->segment  = geom.segment;
    this->serialhi = (uint16_t) ( geom.serial >> 32 );
    this->seriallo = (uint32_t) geom.serial;
    this->size     = (uint32_t) ( geom.size >> align_shift ),
    this->offset   = (uint32_t) ( geom.offset >> align_shift );
  }
  void get( ValueGeom &geom,  uint32_t align_shift ) const {
    geom.segment = this->segment;
    geom.serial  = ( (uint64_t) this->serialhi << 32 ) |
                     (uint64_t) this->seriallo;
    geom.size    = (uint64_t) this->size << align_shift;
    geom.offset  = (uint64_t) this->offset << align_shift;
  }
  uint64_t set_serial( uint64_t serial ) {
    this->serialhi = (uint16_t) ( serial >> 32 );
    this->seriallo = (uint32_t) serial;
    return serial;
  }
  void zero( void ) {
    ::memset( this, 0, sizeof( *this ) );
  }
};

struct ValueCtr { /* packed 64 bit immediate size & serial & seal */
  static const uint64_t SERIAL_MASK = ( (uint64_t) 1 << 48 ) - 1;
  uint32_t size     : 15, /* size of immediate or number of values */
           seal     : 1,  /* seal check, data is valid */
           serialhi : 16, /* update serial */
           seriallo;

  uint64_t get_serial( void ) const {
    return ( (uint64_t) this->serialhi << 32 ) | (uint64_t) this->seriallo;
  }
  void set_serial( uint64_t ser ) {
    this->seriallo = (uint32_t) ser;
    this->serialhi = (uint16_t) ( ser >> 32 );
  }
  void zero( void ) {
    ::memset( this, 0, sizeof( *this ) );
  }
};

/* HashEntry layout (segment data, when hash_entry_size=64):
     What       |  Fields             | Offset
    ------------+---------------------+----------
     Header     |  8b = hash          |  0 ->  8   Always present
                |  8b = hash2         |  8 -> 16       "
                |  2b = val           | 16 -> 18       "
                |  1b = db            | 18 -> 19       "
                |  1b = type          | 19 -> 20       "
                |  2b = flags         | 20 -> 22       "
                |  2b = key len       | 22 -> 24       "
                |  8b = key part      | 24 -> 32   (Optional)
                |                     |
     Value PTR  |  2b = segment       | 32 -> 34
                |  6b = serial cnt    | 34 -> 40   When value is in segment
                |  4b = size          | 40 -> 44
                |  4b = offset        | 44 -> 48
                |                     |
     Rela Stamp |  8b = timestamp     | 48 -> 56   (Optional)
                |                     |
     Value CTR  |  2b = size/seal     | 56 -> 58   Always present
                |  6b = serial cnt    | 58 -> 64       "

   HashEntry layout (immediate data, when hash_entry_size=64):
     What       |  Fields             | Offset
    ------------+---------------------+----------
     Header     |  8b = hash          |  0 ->  8   Always present
                |  8b = hash2         |  8 -> 16       "
                |  2b = val           | 16 -> 18       "
                |  1b = db            | 18 -> 19       "
                |  1b = type          | 19 -> 20       "
                |  2b = flags         | 20 -> 22       "
                |  2b = key len       | 22 -> 24       "
                |  8b = key part      | 24 -> 32   (Optional if key material
                |                     |                      is not required)
     Value      | 16b = value         | 32 -> 48   When value is immediate
                |                     |              key len + value len <= 24
     Rela Stamp |  8b = timestamp     | 48 -> 56   (Optional, then len <= 32)
                |                     |
     Value CTR  |  2b = size/seal     | 56 -> 58   Always present
                |  6b = serial cnt    | 58 -> 64       "
*/
struct HashEntry {
  AtomUInt64  hash;   /* the lock and the hash value */
  uint64_t    hash2;  /* more hash (64 -> 128 bit) */
  uint16_t    val;    /* extra value, for memcached external flags */
  uint8_t     db,     /* which db entry belongs to */
              type;   /* the type assigned to the object value */
  uint16_t    flags;  /* KeyValueFlags, where is data, alignment */
  KeyFragment key;    /* key, or just the prefix of the key */

  void copy_key( KeyFragment &kb ) {
    uint16_t       * k = (uint16_t *) (void *) &kb;
    const uint16_t * e = (uint16_t *) (void *) &kb.u.buf[ kb.keylen ];
    uint16_t       * p = (uint16_t *) (void *) &this->key;
    /* copy all of key, 2b at a time, 1b overruns won't hurt */
    do {
      *p++ = *k++;
    } while ( k < e );
  }
  static uint32_t hdr_size_part( void ) { /* 24 */
    return sizeof( AtomUInt64 ) + /* hash */
           sizeof( uint64_t )   + /* hash2 */
           sizeof( uint16_t )   + /* val */
           sizeof( uint8_t  )   + /* db */
           sizeof( uint8_t  )   + /* type */
           sizeof( uint16_t )   + /* flags */
           sizeof( uint16_t );    /* keylen */
  }
  static uint32_t hdr_size( const KeyFragment &kb ) {
    return hdr_size_part() + align<uint32_t>( kb.keylen, 8 );
  }
  uint32_t hdr_size2( void ) const {
    if ( this->test( FL_PART_KEY ) == 0 )
      return HashEntry::hdr_size( this->key );
    return hdr_size_part();
    /*return sizeof( HashEntry );*/
  }
  void clear( uint32_t fl )          { this->flags &= ~fl; }
  void set( uint32_t fl )            { this->flags |= fl; }
  uint32_t test( uint32_t fl ) const { return this->flags & fl; }
  uint64_t unseal_entry( uint32_t hash_entry_size ) {
    ValueCtr &ctr = this->value_ctr( hash_entry_size );
    ctr.seal = 0;
    return ctr.get_serial();
  }
  void seal_entry( uint32_t hash_entry_size,  uint64_t serial,  uint8_t db ) {
    ValueCtr &ctr = this->value_ctr( hash_entry_size );
    this->db = db;
    ctr.set_serial( serial );
    ctr.seal = 1;
  }
  bool check_seal( uint32_t hash_entry_size ) {
    const ValueCtr &ctr = this->value_ctr( hash_entry_size );
    return ctr.seal == 1;
  }
  uint8_t cuckoo_inc( void ) const {
    return this->test( FL_CUCKOO_INC ) >> FL_CUCKOO_SHIFT;
  }
  void set_cuckoo_inc( uint8_t inc ) {
    this->clear( FL_CUCKOO_INC );
    this->set( inc << FL_CUCKOO_SHIFT );
  }
  /* ptr after key, only valid when FL_IMMEDIATE_VALUE is set */
  uint8_t *immediate_value( void ) const {
    return &((uint8_t *) (void *) this)[ this->hdr_size2() ];
  }
  /* ptr to end of hash entry */
  void *ptr( uint32_t off ) const {
    return (void *) &((uint8_t *) (void *) this)[ off ];
  }
  /* ValuePtr      ? ( FL_SEGMENT_VALUE )
   * Seqno         ? ( FL_SEQNO )
   * RelativeStamp ? ( FL_EXPIRE_STAMP | FL_UPDATE_STAMP )
   * ValueCtr */
  /* before value ctr, relative stamp */
  uint32_t trail_offset( uint32_t hash_entry_size ) const {
    uint32_t sz = hash_entry_size - sizeof( ValueCtr );
    if ( this->test( FL_SEGMENT_VALUE ) )
      sz -= sizeof( ValuePtr );
    if ( this->test( FL_SEQNO ) )
      sz -= sizeof( uint64_t );
    if ( this->test( FL_EXPIRE_STAMP | FL_UPDATE_STAMP ) )
      sz -= sizeof( RelativeStamp );
    return sz;
  }
  void *trail_ptr( uint32_t hash_entry_size ) const {
    return this->ptr( this->trail_offset( hash_entry_size ) );
  }
  ValuePtr &value_ptr( uint32_t hash_entry_size ) {
    uint32_t sz = hash_entry_size - ( sizeof( ValueCtr ) + sizeof( ValuePtr ) );
    if ( this->test( FL_EXPIRE_STAMP | FL_UPDATE_STAMP ) )
      sz -= sizeof( RelativeStamp );
    if ( this->test( FL_SEQNO ) )
      sz -= sizeof( uint64_t );
    return *(ValuePtr *) this->ptr( sz );
  }
  RelativeStamp &rela_stamp( uint32_t hash_entry_size ) {
    uint32_t sz = hash_entry_size - ( sizeof( ValueCtr ) +
                                      sizeof( RelativeStamp ) );
    if ( this->test( FL_SEQNO ) )
      sz -= sizeof( uint64_t );
    return *(RelativeStamp *) this->ptr( sz );
  }
  uint64_t &seqno( uint32_t hash_entry_size ) {
    uint32_t sz = hash_entry_size - ( sizeof( ValueCtr ) + sizeof( uint64_t ) );
    if ( this->test( FL_EXPIRE_STAMP | FL_UPDATE_STAMP ) )
      sz -= sizeof( RelativeStamp );
    return *(uint64_t *) this->ptr( sz );
  }
  /* at the the end of the entry */
  ValueCtr &value_ctr( uint32_t hash_entry_size ) {
    return *(ValueCtr *) this->ptr( hash_entry_size - sizeof( ValueCtr ) );
  }
  /* expand geom bits into value geom */
  void get_value_geom( uint32_t hash_entry_size,  ValueGeom &geom,
                       uint32_t align_shift ) {
    this->value_ptr( hash_entry_size ).get( geom, align_shift );
  }
  /* compress value geom into geom bits */
  void set_value_geom( uint32_t hash_entry_size,  const ValueGeom &geom,
                       uint32_t align_shift ) {
    this->value_ptr( hash_entry_size ).set( geom, align_shift );
  }
};

} /* kv */
} /* rai */

#endif
#endif
