#ifndef __rai_raikv__stream_buf_h__
#define __rai_raikv__stream_buf_h__

#include <sys/uio.h>
#include <raikv/work.h>

namespace rai {
namespace kv {

struct StreamBuf {
  /* buffering items */
  struct BufList {
    BufList * next;
    size_t    off,
              used,
              buflen;
    char    * buf( size_t x ) const {
      return &((char *) (void *) &this[ 1 ])[ this->off + x ];
    }
  };
  /* allocate a BufList */
  BufList *alloc_buf_list( BufList *&hd,  BufList *&tl,  size_t len,
                           size_t pad = 0 ) noexcept;
  /* queue of items, usually an array */
  struct BufQueue {
    StreamBuf & strm;
    BufList   * hd,
              * tl;
    BufQueue( StreamBuf &s ) : strm( s ), hd( 0 ), tl( 0 ) {}
    /* put used into this->tl */
    void append_list( BufQueue &q ) {
      if ( q.hd != NULL ) {
        if ( this->tl != NULL )
          this->tl->next = q.hd;
        else
          this->hd = q.hd;
        this->tl = q.tl;
      }
    }
    BufList * get_buf( size_t len ) {
      if ( this->tl == NULL ||
           len + this->tl->used + this->tl->off > this->tl->buflen )
        return this->append_buf( len );
      return this->tl;
    }
    /* allocate a new BufList that fits at least len and append to list */
    BufList * append_buf( size_t len ) noexcept;
    /* a nil string: $-1\r\n or *-1\r\n if is_null true */
    size_t append_nil( bool is_null = false ) noexcept;
    /* a zero length array: *0\r\n */
    size_t append_zero_array( void ) noexcept;
    /* one string item, appended with decorations: $<strlen>\r\n<string>\r\n */
    size_t append_string( const void *str,  size_t len,  const void *str2=0,
                          size_t len2=0 ) noexcept;
    /* an int, appended with decorations: :<val>\r\n */
    size_t append_uint( uint64_t val ) noexcept;
    /* arbitrary bytes, no formatting */
    size_t append_bytes( const void *buf,  size_t len ) noexcept;
    /* make array: [item1, item2, ...], prepends decorations *<arraylen>\r\n */
    size_t prepend_array( size_t nitems ) noexcept;
    /* cursor is two arrays: [cursor,[items]] */
    size_t prepend_cursor_array( size_t curs,  size_t nitems ) noexcept;
  };

  static const size_t SND_BUFSIZE = 32 * 1024;
  static const size_t BUFSIZE     = 1600;
  size_t  wr_pending;  /* how much is in send buffers total */
  char  * out_buf;     /* current buffer to fill, up to BUFSIZE */
  size_t  sz,          /* sz bytes in out_buf */
          idx,         /* head data in iov[] to send */
          woff;        /* offset of iov[] sent, tail, idx >= woff */
  bool    alloc_fail;  /* if alloc send buffers below failed */
  iovec * iov;         /* output vectors written to stream */
  size_t  vlen;        /* length of iov[] */

  kv::WorkAllocT< SND_BUFSIZE > tmp;
  struct iovec iovbuf[ 32 ]; /* vec of send buffers */

  StreamBuf() { this->reset(); }

  void release( void ) {
    this->reset();
    this->tmp.release_all();
  }

  void truncate( size_t offset ) {
    /* normal case, reset sizes */
    if ( offset == 0 ) {
      this->sz         = 0;
      this->idx        = 0;
      this->woff       = 0;
      this->wr_pending = 0;
      this->out_buf    = NULL;
      return;
    }
    this->truncate2( offset );
  }
  void truncate2( size_t offset ) noexcept;

  /* return the end of the output buffer */
  char *trunc_copy( size_t offset,  size_t &len ) {
    char *s;
    len = this->pending() - offset;
    if ( len == 0 )
      return NULL;
    if ( this->sz == len ) { /* most likely */
      s = this->out_buf;
      this->sz      = 0;
      this->out_buf = NULL;
      return s;
    }
    s = (char *) this->tmp.alloc( len ); /* concat or shrink buffers */
    if ( s == NULL ) {
      this->alloc_fail = true;
      len = 0;
      return NULL;
    }
    if ( this->sz > len ) {
      ::memcpy( s, &this->out_buf[ this->sz - len ], len );
      this->sz -= len;
      return s;
    }
    /* sz < len */
    size_t j = len - this->sz;
    ::memcpy( &s[ j ], this->out_buf, this->sz );
    this->sz = 0;
    for (;;) {
      iovec & io = this->iov[ --this->idx ];
      if ( io.iov_len >= j ) {
        ::memcpy( s, &((char *) io.iov_base)[ io.iov_len - j ], j );
        if ( io.iov_len > j ) {
          this->idx += 1;
          io.iov_len -= j;
        }
        this->wr_pending -= j;
        return s;
      }
      j -= io.iov_len;
      ::memcpy( &s[ j ], io.iov_base, io.iov_len );
      this->wr_pending -= io.iov_len;
    }
  }

  size_t pending( void ) const { /* how much is read to send */
    return this->wr_pending + this->sz;
  }
  void reset_pending( void ) {
    this->iov        = this->iovbuf;
    this->vlen       = sizeof( this->iovbuf ) / sizeof( this->iovbuf[ 0 ] );
    this->wr_pending = 0;
    this->out_buf    = NULL;
    this->sz         = 0;
    this->idx        = 0;
    this->woff       = 0;
    this->alloc_fail = false;
  }
  void reset( void ) {
    this->reset_pending();
    this->tmp.reset();
  }
  void expand_iov( void ) noexcept;

  void prepend_flush( size_t i ) { /* move work buffer to front of iov */
    this->flush();
    if ( i < this->idx ) {
      iovec v = this->iov[ this->idx - 1 ];
      ::memmove( &this->iov[ i + 1 ], &this->iov[ i ],
                 sizeof( this->iov[ 0 ] ) * ( ( this->idx - i ) - 1 ) );
      this->iov[ i ] = v;
    }
  }
  void flush( void ) { /* move work buffer to send iov */
    if ( this->idx == this->vlen )
      this->expand_iov();
    this->iov[ this->idx ].iov_base  = this->out_buf;
    this->iov[ this->idx++ ].iov_len = this->sz;

    this->wr_pending += this->sz;
    this->out_buf     = NULL;
    this->sz          = 0;
  }
  void append_iov( void *p,  size_t amt ) {
    if ( this->out_buf != NULL && this->sz > 0 )
      this->flush();
    if ( this->idx == this->vlen )
      this->expand_iov();
    this->iov[ this->idx ].iov_base  = p;
    this->iov[ this->idx++ ].iov_len = amt;
    this->wr_pending += amt;
  }
  void append_iov( BufQueue &q ) {
    if ( q.hd != NULL ) {
      BufList *p = q.hd;
      if ( p->next != NULL || this->out_buf == NULL ||
           this->sz + p->used > BUFSIZE ) {
        for (;;) {
          this->append_iov( p->buf( 0 ), p->used );
          if ( (p = p->next) == NULL )
            break;
        }
      }
      else {
        this->append( p->buf( 0 ), p->used );
      }
    }
  }
  bool concat_iov( void ) {
    if ( this->sz > 0 )
      this->flush();
    if ( this->idx == 0 )
      return false;
    if ( this->idx > 1 )
      this->merge_iov();
    return this->idx > 0;
  }
  void merge_iov( void ) noexcept;

  char *alloc_temp( size_t amt ) noexcept;

  uint8_t *alloc_bytes( size_t amt ) {
    return (uint8_t *) this->alloc( amt );
  }
  char *alloc( size_t amt ) {
    if ( this->out_buf != NULL && this->sz + amt > BUFSIZE )
      this->flush();
    if ( this->out_buf == NULL ) {
      this->out_buf = (char *) this->alloc_temp( amt < BUFSIZE ? BUFSIZE : amt);
      if ( this->out_buf == NULL )
        return NULL;
    }
    return &this->out_buf[ this->sz ];
  }
  void append( const void *p,  size_t amt ) {
    char *b = this->alloc( amt );
    if ( b != NULL ) {
      ::memcpy( b, p, amt );
      this->sz += amt;
    }
    else {
      this->alloc_fail = true;
    }
  }
  void append2( const void *p,  size_t amt,
                const void *p2,  size_t amt2 ) {
    char *b = this->alloc( amt + amt2 );
    if ( b != NULL ) {
      ::memcpy( b, p, amt );
      ::memcpy( &b[ amt ], p2, amt2 );
      this->sz += amt + amt2;
    }
    else {
      this->alloc_fail = true;
    }
  }
};

#if __cplusplus >= 201103L
  /* 64b align */
  static_assert( 0 == sizeof( StreamBuf ) % 64, "stream buf size" );
#endif

}
}
#endif
