#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <raikv/stream_buf.h>
#include <raikv/util.h>

using namespace rai;
using namespace kv;

char *
StreamBuf::alloc_temp( size_t amt ) noexcept
{
  char *spc = (char *) this->tmp.alloc( amt );
#if 0
  if ( spc == NULL ) {
    this->wr_used++;
    this->alloc_fail = true;
    return NULL;
  }
#endif
  this->wr_used += amt;
  if ( this->wr_used > this->wr_max )
    this->wr_max = this->wr_used;
  return spc;
}

void
StreamBuf::expand_iov( void ) noexcept
{
  void *p;
  p = this->alloc_temp( sizeof( struct iovec ) * this->vlen * 2 );
  ::memcpy( p, this->iov, sizeof( struct iovec ) * this->vlen );
  this->iov   = (struct iovec *) p;
  this->vlen *= 2;
}

void
StreamBuf::expand_refs( void ) noexcept
{
  void *p;
  p = this->alloc_temp( sizeof( this->refs[ 0 ] ) * this->ref_size * 2 );
  ::memcpy( p, this->refs, sizeof( this->refs[ 0 ] ) * this->ref_size );
  this->refs  = (uint32_t *) p;
  this->ref_size *= 2;
}

void
StreamBuf::temp_gc( void ) noexcept
{
  /*printf( "wr_pending %lu block_cnt %u vlen %lu free %lu\n",
          this->wr_pending, this->tmp.block_cnt, this->vlen,
          this->wr_free );*/
  size_t x = this->wr_gc;
  this->merge_iov();
  this->wr_gc = x;
  if ( x < (size_t) 256 * 1024 * 1024 )
    this->wr_gc += x / 2;
}

void
StreamBuf::merge_iov( void ) noexcept
{
  size_t i, len = 0;
  void * buf;
  for ( i = 0; i < this->idx; i++ )
    len += this->iov[ i ].iov_len;
  buf = this->tmp.make_big_buf( len );
  len = 0;
  if ( buf == NULL ) {
    this->reset_pending();
    this->alloc_fail = true;
    return;
  }
  for ( i = 0; i < this->idx; i++ ) {
    size_t add = this->iov[ i ].iov_len;
    ::memcpy( &((char *) buf)[ len ], this->iov[ i ].iov_base, add );
    len += add;
  }

  this->reset();
  this->wr_pending = len;
  this->iov[ 0 ].iov_base = buf;
  this->iov[ 0 ].iov_len  = len;
  this->idx = 1;
  this->tmp.push_big_buf( buf );
  this->wr_used = len;
}

void
StreamBuf::truncate2( size_t offset ) noexcept
{
  /* find truncate offset, add iovs together with out_buf/sz */
  size_t i, len = 0, off = offset;
  for ( i = 0; i < this->idx; i++ ) {
    len = this->iov[ i ].iov_len;
    if ( len >= off ) {
      this->iov[ i ].iov_len = off;
      this->idx = i + 1;
      off = 0;
      break;
    }
    off -= len;
  }
  if ( (this->sz = off) == 0 ) {
    this->out_buf = NULL;
    this->sz      = 0;
  }
  this->wr_pending = offset - this->sz;
}

StreamBuf::BufList *
StreamBuf::alloc_buf_list( BufList *&hd,  BufList *&tl,  size_t len,
                           size_t pad ) noexcept
{
  BufList *p = (BufList *) this->alloc_temp( sizeof( BufList ) + len + pad );
  if ( p == NULL )
    return NULL;
  if ( tl != NULL )
    tl->next = p;
  else
    hd = p;
  tl = p;
  p->next   = NULL;
  p->off    = pad;
  p->used   = 0;
  p->buflen = len + pad;
  return p;
}

StreamBuf::BufList *
StreamBuf::BufQueue::append_buf( size_t len ) noexcept
{
  if ( this->hd == NULL )
    return this->append_buf2( len, this->pad_sz );
  return this->strm.alloc_buf_list( this->hd, this->tl,
                          ( len < this->min_sz ? this->min_sz : len ), 0 );
}

StreamBuf::BufList *
StreamBuf::BufQueue::append_buf2( size_t len,  size_t pad ) noexcept
{
  size_t alsz = ( pad < this->min_sz ? this->min_sz - pad : 0 );
  return this->strm.alloc_buf_list( this->hd, this->tl,
                                    ( len < alsz ? alsz : len ), pad );
}

size_t
StreamBuf::BufQueue::append_bytes( const void *buf,  size_t len ) noexcept
{
  BufList * p = this->get_buf( len );
  if ( p == NULL )
    return 0;
  char * bufp = p->buf( 0 );
  ::memcpy( &bufp[ p->used ], buf, len );
  p->used += len;
  return len;
}

char *
StreamBuf::BufQueue::prepend_buf( size_t len ) noexcept
{
  BufList * p;
  if ( (p = this->hd) != NULL && p->off >= len ) {
    p->off  -= len;
    p->used += len;
  }
  else {
    p = (BufList *) this->strm.alloc_temp( sizeof( BufList ) + len );
    if ( p == NULL )
      return 0;
    p->off    = 0;
    p->used   = len;
    p->buflen = len;
    p->next = this->hd;
    this->hd = p;
    if ( this->tl == NULL )
      this->tl = p;
  }
  return p->buf( 0 );
}

