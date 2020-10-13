#ifndef __rai__raikv__pipe_buf_h__
#define __rai__raikv__pipe_buf_h__

/* also include stdint.h, string.h */
#include <raikv/atom.h>
#include <raikv/util.h>

#ifdef __cplusplus
namespace rai {
namespace kv {

/* these sync_set()/get() don't do much on x86_64 other than make sure the
 * compiler doesn't reorder the lines of code when optimizing */
template <class Int>
static inline void sync_set( volatile Int &a,  const Int val ) {
  kv_sync_store( &a, val );
}

template <class Int>
static inline Int sync_get( volatile Int &val ) {
  Int a = kv_sync_load( &val );
  return a;
}

/* A pointer into the pipe buffer has it's own cache line */
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

/* A message begins with a length header */
struct PipeMsg {
  uint64_t length; /* 0 = terminates msg list, high bits can contain flags */
  /* data or padding follows */
};

/* Useful to kill( pid ), see if the other process is still alive */
struct PipeOwner {
  uint32_t snd_ctx_id, /* uniquely identify endpoints */
           snd_pid,
           rcv_ctx_id,
           rcv_pid;
};

struct PipeInfo { /* the producer, src ident, and consumer, dest ident */
  union {
    PipeOwner x;
    uint8_t   cache_line[ 64 ];
  };
};

/* Consumers can use this for zero copy reading, access the buffer, then
 * update the consumer_head pointer when finished */
struct PipeReadCtx {
  void   * data;
  uint64_t data_len,
           next_head,
           fl;
};

/* Producer use this for writing vectors of messages */
struct PipeWriteCtx {
  uint64_t rec_len,
           required,
           tail,
           record_idx,
           available,
           padding;
};

/* This is loosely based on Aeron's single producer, single consumer ringbuf */
struct PipeBuf {
  static const uint64_t CAPACITY   = 512 * 1024, /* must be power ^ 2 */
                        MASK       = CAPACITY - 1,
                        HDR_LEN    = sizeof( PipeMsg ),
                        IS_PADDING = ( CAPACITY << 1 ),
                        FLAG2      = ( CAPACITY << 2 ),
                        FLAG3      = ( CAPACITY << 3 );
  uint8_t  buf[ CAPACITY ];

  /* These are always incrementing, wrapping is done by masking the values to
   * the length of the pipe
   *
   *   pipe buffer
   * +------------------------------------------------------------+
   * |         ^               ^                      ^           |
   * |         |               |                      |           |
   * |       producer_head   consumer_head          producer_tail |
   *
   * The producer_head is always behind or equal to the consumer_head
   * because it is only copied from the consumer when the producer_tail
   * wraps around and no space is left.  This avoids unnecessary cache line
   * bouncing from producer to consumer.  The producer_tail is never read
   * by the consumer, instead the terminating record is zero length.  A
   * consumer can check whether data is available by reading the record
   * lenght at the consumer_head -- when non-zero, it is available.  */
  struct PipeMeta {
    PipeInfo owner;
    PipePtr  tail_counter,          /* producer write msgs here */
             producer_head_counter, /* producer reads here */
             consumer_head_counter; /* consumer reads here */
  } x;

  static PipeBuf *open( const char *name,  bool do_create,
                        int pipe_mode ) noexcept;
  void init( void ) noexcept;
  void close( void ) noexcept;
  static int unlink( const char *name ) noexcept;

  void set_rec_length( uint64_t off,  uint64_t length,  uint64_t fl = 0 ) {
    PipeMsg * r = (PipeMsg *) (void *) &this->buf[ off ];
    sync_set<uint64_t>( r->length, length | fl );
  }

  uint64_t get_rec_length( uint64_t off,  uint64_t &fl ) {
    PipeMsg * r = (PipeMsg *) (void *) &this->buf[ off ];
    fl = sync_get<uint64_t>( r->length );
    return fl & MASK;
  }

  bool read_ctx( PipeReadCtx &ctx ) {
    for (;;) {
      const uint64_t head     = this->x.consumer_head_counter,
                     head_idx = head & MASK;
      ctx.data_len = this->get_rec_length( head_idx, ctx.fl );

      if ( (ctx.fl & IS_PADDING) == 0 ) {
        if ( ctx.data_len == 0 )
          return false;
        ctx.data = &this->buf[ head_idx + HDR_LEN ];
        ctx.next_head =
          head + align<uint64_t>( HDR_LEN + ctx.data_len, HDR_LEN );
        return true;
      }
      /* is padding, consume it */
      this->x.consumer_head_counter = head + HDR_LEN + ctx.data_len;
    }
  }
  void consume_ctx( PipeReadCtx &ctx ) {
    this->x.consumer_head_counter = ctx.next_head;
  }

  uint64_t read( void *data,  size_t data_size ) {
    for (;;) {
      uint64_t       fl;
      const uint64_t head     = this->x.consumer_head_counter,
                     head_idx = head & MASK,
                     data_len = this->get_rec_length( head_idx, fl );

      if ( (fl & IS_PADDING) == 0 ) {
        if ( data_len == 0 )
          return 0;
        if ( data_size > data_len )
          data_size = data_len;
        ::memcpy( data, &this->buf[ head_idx + HDR_LEN ], data_size );
        this->x.consumer_head_counter =
          head + align<uint64_t>( HDR_LEN + data_len, HDR_LEN );
        return data_size;
      }
      /* is padding, consume it */
      this->x.consumer_head_counter = head + HDR_LEN + data_len;
    }
  }

  static uint64_t wrap_size( const uint64_t tail, const uint64_t head ) {
    return CAPACITY - ( ( tail - head ) & MASK );
  }

  bool alloc_space( PipeWriteCtx &ctx,  uint64_t data_len ) {
    /* |<- used ->|<-- capacity ---------------------------->|
     * +----...---+------------------------------------------+
     * |   data   | PipeMsg |  data_len   | | PipeMsg |      |
     * +----...---+-----------------------|-|---------|------+
     * |          | <- rec_len ------------>|         |
     * |          | <- required --------------------->|
     **/
    /* rec_len must be aligned with HDR_LEN, so there is never a buffer
     * fragment smaller or larger than HDR_LEN */
    uint64_t head = this->x.producer_head_counter;

    ctx.rec_len    = align<uint64_t>( HDR_LEN + data_len, HDR_LEN );
    ctx.required   = ctx.rec_len + HDR_LEN; /* terminate record w/zero */
    ctx.tail       = this->x.tail_counter; /* write msgs here */
    ctx.record_idx = ctx.tail & MASK;
    ctx.available  = wrap_size( ctx.tail, head );
    ctx.padding    = 0;

    /* check if have space */
    if ( ctx.required > ctx.available ) {
      head = this->x.consumer_head_counter; /* load from read side */
      ctx.available = wrap_size( ctx.tail, head );
      if ( ctx.required > ctx.available ) /* full */
        return false;
      this->x.producer_head_counter = head;
    }
    /* check if wraps around buffer */
    if ( ctx.required > CAPACITY - ctx.record_idx ) {
      ctx.available = head & MASK;
      ctx.padding   = CAPACITY - ctx.record_idx;
      /* head is offset from zero, where rec starts */
      if ( ctx.required > ctx.available ) {
        head          = this->x.consumer_head_counter;
        ctx.available = head & MASK;

        if ( ctx.required > ctx.available ) /* rec_len + padding exceeds avail*/
          return false;
        this->x.producer_head_counter = head;
      }
      this->set_rec_length( 0, 0 );
      this->set_rec_length( ctx.record_idx, ctx.padding - HDR_LEN, IS_PADDING );
      ctx.record_idx = 0;
    }
    return true;
  }

  uint64_t write( const void *data,  size_t data_len,  uint64_t fl = 0 ) {
    PipeWriteCtx ctx;
    if ( ! this->alloc_space( ctx, data_len ) )
      return 0;

    ::memcpy( &this->buf[ ctx.record_idx + HDR_LEN ], data, data_len );
    /* terminate w/zero */
    this->set_rec_length( ctx.record_idx + ctx.rec_len, 0 );
    /* mem fence here if not total store ordered cpu */
    this->set_rec_length( ctx.record_idx, data_len, fl );    /* data is valid now */
    this->x.tail_counter = ctx.tail + ctx.rec_len + ctx.padding;

    return data_len;
  }
};

}
}
#endif

#endif
