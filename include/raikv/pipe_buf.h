#ifndef __rai__raikv__pipe_buf_h__
#define __rai__raikv__pipe_buf_h__

/* also include stdint.h, string.h */
#include <raikv/atom.h>
#include <raikv/util.h>

#ifdef __cplusplus
namespace rai {
namespace kv {

template <class Int>
static inline void sync_set( volatile Int &a,  const Int val ) {
  kv_sync_store( &a, val );
}

template <class Int>
static inline Int sync_get( volatile Int &val ) {
  Int a = kv_sync_load( &val );
  return a;
}

/* PipeBuf is a single producer, single consumer structure */
struct PipePtr {
  union {
    uint64_t val; /* location of head or tail in buffer */
    uint8_t  cache_line[ 64 ];
  };

  operator uint64_t () {
    return sync_get<uint64_t>( this->val );
  }
  uint64_t operator=( const uint64_t x ) {
    sync_set<uint64_t>( this->val, x );
    return x;
  }
};

struct PipeMsg {
  uint64_t length; /* 0 = terminates msg list, >= IS_PADDING pads out to edge */
  /* data follows */
};

struct PipeInfo { /* the producer, src ident, and consumer, dest ident */
  union {
    struct {
      uint32_t src_ctx_id, /* uniquely identify endpoints */
               src_pid,
               src_seqno,
               dest_ctx_id,
               dest_pid,
               dest_seqno;
    } x;
    uint8_t cache_line[ 64 ];
  };
};

/* this is loosely based on Aeron single producer, single consumer */
struct PipeBuf {
  static const uint64_t CAPACITY   = 64 * 1024, /* must be power ^ 2 */
                        MASK       = CAPACITY - 1,
                        HDR_LEN    = sizeof( PipeMsg ),
                        IS_PADDING = CAPACITY;
  uint8_t  buf[ CAPACITY ];

  struct PipeMeta {
    PipeInfo owner;
    /* producer write msgs here */
    PipePtr  tail_position;
    /* producer reads here, until full, then reads the consumer side */
    PipePtr  producer_head_position;
    /* consumer reads here, and updates after consuming */
    PipePtr  consumer_head_position;
  } x;

  static PipeBuf *open( const char *name,  bool do_create ) noexcept;
  void init( void ) noexcept;
  void close( void ) noexcept;
  static int unlink( const char *name ) noexcept;

  void set_rec_length( uint64_t off,  uint64_t length ) {
    PipeMsg & r = *(PipeMsg *) &this->buf[ off ];
    sync_set<uint64_t>( r.length, length );
  }

  uint64_t get_rec_length( uint64_t off ) {
    PipeMsg & r = *(PipeMsg *) &this->buf[ off ];
    return sync_get<uint64_t>( r.length );
  }

  uint64_t read( void *data,  size_t data_size ) {
    for (;;) {
      const uint64_t head     = this->x.consumer_head_position;
      const uint64_t head_idx = head & MASK;
      const uint64_t data_len = this->get_rec_length( head_idx );

      if ( data_len < IS_PADDING ) {
        if ( data_len == 0 )
          return 0;
        if ( data_size > data_len )
          data_size = data_len;
        ::memcpy( data, &this->buf[ head_idx + HDR_LEN ], data_size );
        this->x.consumer_head_position =
          head + align<uint64_t>( HDR_LEN + data_len, HDR_LEN );
        return data_size;
      }
      /* is padding, consume it */
      this->x.consumer_head_position = head + HDR_LEN + ( data_len & MASK );
    }
  }

  uint64_t write( const void *data,  size_t data_len ) {
    /* |<- used ->|<-- capacity ---------------------------->|
     * +----...---+------------------------------------------+
     * |   data   | PipeMsg |  data_len   | | PipeMsg |      |
     * +----...---+-----------------------|-|---------|------+
     * |          | <- rec_len ------------>|         |
     * |          | <- required --------------------->|
     **/
    /* rec_len must be aligned with HDR_LEN, so there is never a buffer
     * fragment smaller or larger than HDR_LEN */
    const uint64_t rec_len  = align<uint64_t>( HDR_LEN + data_len, HDR_LEN );
    const uint64_t required = rec_len + HDR_LEN; /* terminate record w/zero */
    const uint64_t tail     = this->x.tail_position; /* write msgs here */

    uint64_t head       = this->x.producer_head_position;
    uint64_t available  = CAPACITY - ( ( tail - head ) & MASK );
    uint64_t record_idx = tail & MASK;
    uint64_t padding    = 0;

    /* check if have space */
    if ( required > available ) {
      head      = this->x.consumer_head_position; /* load from read side */
      available = CAPACITY - ( ( tail - head ) & MASK );
      if ( required > available ) /* full */
        return 0;
      this->x.producer_head_position = head;
    }
    /* check if wraps around buffer */
    if ( required > CAPACITY - record_idx ) {
      uint64_t head_idx = head & MASK;
      padding = CAPACITY - record_idx;
      /* head is offset from zero, where rec starts */
      if ( required > head_idx ) {
        head     = this->x.consumer_head_position;
        head_idx = head & MASK;

        if ( required > head_idx ) /* rec_len + padding exceeds avail */
          return 0;
        this->x.producer_head_position = head;
      }
      this->set_rec_length( 0, 0 );
      this->set_rec_length( record_idx, ( padding - HDR_LEN ) | IS_PADDING );
      record_idx = 0;
    }

    ::memcpy( &this->buf[ record_idx + HDR_LEN ], data, data_len );
    this->set_rec_length( record_idx + rec_len, 0 ); /* terminate w/zero */
    /* mem fence here if not total store ordered cpu */
    this->set_rec_length( record_idx, data_len );    /* data is valid now */
    this->x.tail_position = tail + rec_len + padding;

    return data_len;
  }
};

}
}
#endif

#endif
