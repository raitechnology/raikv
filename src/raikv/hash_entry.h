#ifndef __rai__raikv__hash_entry_h__
#define __rai__raikv__hash_entry_h__

/* also include stdint.h, string.h */
#ifndef __rai__raikv__key_ctx_h__
#include <raikv/key_ctx.h>
#endif

#ifndef __rai__raikv__atom_h__
#include <raikv/atom.h>
#endif

#ifndef __rai__raikv__rela_ts_h__
#include <raikv/rela_ts.h>
#endif

#ifdef __cplusplus
namespace rai {
namespace kv {

/* flags attached to hash entry and msg value */
enum KeyValueFlags {
  FL_NO_ENTRY	     =   0x00, /* initialize */
  FL_ALIGNMENT	     =   0x07, /* X=[0->7], data alignment: 1 << X */
  FL_SEGMENT_VALUE   =   0x08, /* value is in segment */
  FL_UPDATED         =   0x10, /* was updated recently, useful for persistence */
  FL_IMMEDIATE_VALUE =   0x20, /* has immediate data, no segment data used */
  FL_IMMEDIATE_KEY   =   0x40, /* full key is in hash entry */
  FL_PART_KEY        =   0x80, /* part of key is in hash entry */
  FL_DROPPED         =  0x100, /* dropped key + data */
  FL_EXPIRE_STAMP    =  0x200, /* has expiration time stamp */
  FL_UPDATE_STAMP    =  0x400, /* has update time stamp */
  FL_CRC_KEY         =  0x800, /* key to big for hash entry, use crc */
  FL_CRC_VALUE       = 0x1000, /* use crc to seal value */
  FL_BUSY            = 0x8000  /* msg is busy */
};

struct ValuePtr { /* packed 128 bit pointer with seg & off & size & serial */
  static const uint32_t VALUE_SIZE_BITS = 32; /* (sizehi << 16) | sizelo */
  uint16_t segment, /* seg=16, off=32, size=32, serial=48 = 128 */
           sizehi,
           sizelo,
           offsethi,
           offsetlo,
           serialhi;
  uint32_t seriallo;

  void set( const ValueGeom &geom,  uint32_t align_shift ) {
    uint32_t sz  = (uint32_t) ( geom.size >> align_shift ),
             off = (uint32_t) ( geom.offset >> align_shift );
    this->segment  = geom.segment;
    this->sizehi   = (uint16_t) ( sz >> 16 );
    this->sizelo   = (uint16_t) sz;
    this->offsethi = (uint16_t) ( off >> 16 );
    this->offsetlo = (uint16_t) off;
    this->serialhi = (uint16_t) ( geom.serial >> 32 );
    this->seriallo = (uint32_t) geom.serial;
  }
  void get( ValueGeom &geom,  uint32_t align_shift ) const {
    geom.segment = this->segment;
    geom.size    = ( (uint64_t) ( (uint32_t) this->sizehi << 16 ) |
                                  (uint32_t) this->sizelo ) << align_shift;
    geom.offset  = ( (uint64_t) ( (uint32_t) this->offsethi << 16 ) |
                                  (uint32_t) this->offsetlo ) << align_shift;
    geom.serial  = ( (uint64_t) this->serialhi << 32 ) |
                     (uint64_t) this->seriallo;
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
  void set_serial( uint64_t ctr ) {
    this->seriallo = (uint32_t) ctr;
    this->serialhi = (uint16_t) ( ctr >> 32 );
  }
  void zero( void ) {
    ::memset( this, 0, sizeof( *this ) );
  }
};

struct HashEntry { /* min sizeof HashEntry is 40b: 16 key, 16 val ptr, 8 ctr */
  AtomUInt64  hash;   /* the lock and the hash value */
  uint16_t    flags;  /* KeyValueFlags, where is data, alignment */
  KeyFragment key;    /* key, or just the prefix of the key */

  void copy_key( KeyFragment &kb ) {
    uint16_t       * k = (uint16_t *) (void *) &kb;
    const uint16_t * e = (uint16_t *) (void *) &kb.buf[ kb.keylen ];
    uint16_t       * p = (uint16_t *) (void *) &this->key;
    /* copy all of key, 2b at a time, 1b overruns won't hurt */
    do {
      *p++ = *k++;
    } while ( k < e );
  }
  static uint32_t hdr_size( const KeyFragment &kb ) {
    return align<uint32_t>( sizeof( uint64_t ) + sizeof( uint16_t ) +
                            kb.keylen + sizeof( kb.keylen ), 8 );
  }
  uint32_t hdr_size2( void ) const {
    if ( this->test( FL_PART_KEY ) == 0 )
      return HashEntry::hdr_size( this->key );
    return sizeof( HashEntry );
  }
  void clear( uint32_t fl )          { this->flags &= ~fl; }
  void set( uint32_t fl )            { this->flags |= fl; }
  uint32_t test( uint32_t fl ) const { return this->flags & fl; }
  uint64_t unseal( uint32_t hash_entry_size ) {
    ValueCtr &ctr = this->value_ctr( hash_entry_size );
    ctr.seal = 0;
    return ctr.get_serial();
  }
  void seal( uint32_t hash_entry_size,  uint64_t serial ) { 
    ValueCtr &ctr = this->value_ctr( hash_entry_size );
    ctr.set_serial( serial );
    ctr.seal = 1;
  }
  bool check_seal( uint32_t hash_entry_size ) {
    return this->value_ctr( hash_entry_size ).seal == 1;
  }
  /* ptr after key, only valid when FL_IMMEDIATE_VALUE is set */
  uint8_t *immediate_value( void ) const {
    return &((uint8_t *) (void *) this)[ this->hdr_size2() ];
  }
  /* ptr to end of hash entry */
  void *ptr( uint32_t off ) const {
    return (void *) &((uint8_t *) (void *) this)[ off ];
  }
  /* before value ctr, relative stamp */
  ValuePtr &value_ptr( uint32_t hash_entry_size,  uint32_t idx = 0 ) {
    return *(ValuePtr *) this->ptr( hash_entry_size -
                                    ( sizeof( ValueCtr ) +
                                      sizeof( ValuePtr ) * ( idx + 1 ) +
                                      ( this->test( FL_EXPIRE_STAMP |
                                                    FL_UPDATE_STAMP ) ?
                                        sizeof( RelativeStamp ) : 0 )
                                      ) );
  }
  /* at the the end of the entry */
  ValueCtr &value_ctr( uint32_t hash_entry_size ) {
    return *(ValueCtr *) this->ptr( hash_entry_size - sizeof( ValueCtr ) );
  }
  /* before value ctr */
  RelativeStamp &rela_stamp( uint32_t hash_entry_size ) {
    return *(RelativeStamp *) this->ptr( hash_entry_size -
                                         ( sizeof( ValueCtr ) +
                                           sizeof( RelativeStamp ) ) );
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
  void get_expire_stamp( uint32_t hash_entry_size,  uint64_t &expire_ns ) {
    expire_ns = this->rela_stamp( hash_entry_size ).u.stamp;
  }
  void get_update_stamp( uint32_t hash_entry_size,  uint64_t &update_ns ) {
    update_ns = this->rela_stamp( hash_entry_size ).u.stamp;
  }
  void get_updexp_stamp( uint32_t hash_entry_size,  uint64_t base,
                         uint64_t clock,  uint64_t &expire_ns,
                         uint64_t &update_ns ) {
    this->rela_stamp( hash_entry_size ).get( base, clock, expire_ns, update_ns);
  }
};

} // kv
} // rai

#endif
#endif
