#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <malloc.h>
#include <raikv/work.h>

using namespace rai;
using namespace kv;

extern "C" {

/* KeyCtx::big_alloc*/
void *
kv_key_ctx_big_alloc( void * /*closure*/,  size_t item_size )
{
  return ::malloc( item_size );
}

void
kv_key_ctx_big_free( void * /*closure*/,  void *item,  size_t )
{
  return ::free( item );
}

kv_work_alloc_t *
kv_create_ctx_alloc( size_t sz,  kv_alloc_func_t ba,  kv_free_func_t bf,
                     void *closure )
{
  size_t off = align<size_t>( sizeof( ScratchMem ), sizeof( BufAlign64 ) );
  size_t sz2 = align<size_t>( off + sz, sizeof( BufAlign64 ) );
  void * ptr = aligned_malloc( sz2 );
  if ( ptr == NULL )
    return NULL;
  new ( ptr ) ScratchMem( 2, 16 * 1024 - 32,
                          (BufAlign64 *) &((uint8_t *) ptr)[ off ],
                           sz2 - off, ba, bf, closure );
  return (kv_work_alloc_t *) ptr;
}

void
kv_release_ctx_alloc( kv_work_alloc_t *ctx_alloc )
{
  delete reinterpret_cast<ScratchMem *>( ctx_alloc );
}

}

ScratchMem::MBlock *
ScratchMem::alloc_block( void ) noexcept
{
  MBlock *blk = NULL;
  if ( this->free_cnt > 0 ) {
    this->free_cnt--;
    blk = this->blk_list.pop_tl();
  }
  if ( blk == NULL ) {
    blk = (MBlock *) this->kv_big_alloc( this->closure, this->alloc_size );
    if ( blk == NULL )
      return NULL;
    this->block_cnt++;
  }
  blk->init( this, this->alloc_size );
  this->off = sizeof( MBlock );
  this->blk_list.push_hd( blk );
  return blk;
}

void 
ScratchMem::release_block( MBlock *blk ) noexcept
{
  this->blk_list.pop( blk );
  if ( this->free_cnt < this->slack_count ) {
    this->free_cnt++;
    this->blk_list.push_tl( blk );
  }
  else {
    this->kv_big_free( this->closure, blk, blk->size );
    this->block_cnt--;
  }
}

size_t
ScratchMem::mem_size( void ) noexcept
{
  size_t blk_size = this->alloc_size * this->block_cnt,
         big_size = 0;
  for ( BigBlock *big = this->big_list.hd; big != NULL; big = big->next )
    big_size += big->hdr.size;
  return this->fast_len + blk_size + big_size;
}

void
ScratchMem::release_all( void ) noexcept
{
  this->reset();
  this->free_cnt  = 0;
  this->block_cnt = 0;
  while ( ! this->blk_list.is_empty() ) {
    MBlock *blk = this->blk_list.pop_hd();
    this->kv_big_free( this->closure, blk, blk->size );
  }
}

void
ScratchMem::reset_slow( void ) noexcept
{
  if ( this->block_cnt > 0 ) {
    this->blk_list.hd->off2 = (uint32_t) this->alloc_size;
    while ( this->block_cnt > this->slack_count )
      this->release_block( this->blk_list.hd );
    this->free_cnt = this->block_cnt;
  }
  this->off = (uint32_t) this->alloc_size;
  while ( ! this->big_list.is_empty() ) {
    BigBlock *big = this->big_list.pop_hd();
    this->kv_big_free( this->closure, big, big->hdr.size );
  }
  this->fast = ( this->fast_len != 0 );
}

void *
ScratchMem::alloc_slow( size_t sz ) noexcept
{
  MemHdr * hdr;
  size_t   used = align<size_t>( sz + sizeof( MemHdr ), sizeof( MemHdr ) );
  if ( this->closure == NULL )
    this->init_big();
  this->fast = false;
  /* if too large, just malloc it */
  if ( used <= this->alloc_size - sizeof( MBlock ) ) {
    /* find a new object */
    if ( this->off >= this->alloc_size )
      if ( this->alloc_block() == NULL )
        return NULL;
    for (;;) {
      uint32_t j = this->off;
      this->off += (uint32_t) used;
      if ( this->off <= this->alloc_size ) {
        hdr = (MemHdr *) this->blk_list.hd->arena_ptr( j );
        hdr->size = used;
        void *p = hdr->user_ptr();
        hdr->set_offset( (uint8_t *) p - (uint8_t *) this->blk_list.hd );
        return p;
      }
      if ( j < this->alloc_size )
        this->blk_list.hd->off2 += (uint32_t) ( this->alloc_size - j );
      if ( this->alloc_block() == NULL )
        return NULL;
    }
  }
  return this->big_alloc( sz );
}

void *
ScratchMem::big_alloc( size_t sz ) noexcept
{
  size_t used = align<size_t>( sz + sizeof( BigBlock ), sizeof( BigBlock ) );
  BigBlock *big = (BigBlock *) this->kv_big_alloc( this->closure, used );
  if ( big == NULL )
    return NULL;
  big->hdr.size = used;
  big->hdr.set_offset( 0 );
  big->next = big->back = NULL;
  big->owner = this;
  this->big_list.push_hd( big );
  return big->hdr.user_ptr();
}

void
ScratchMem::init_big( void ) noexcept
{
  if ( this->closure == NULL ) {
    if ( this->kv_big_alloc == NULL ) {
      this->kv_big_alloc = kv_key_ctx_big_alloc;
      this->kv_big_free  = kv_key_ctx_big_free;
    }
    this->closure = this;
  }
  this->fast = false;
}

void *
ScratchMem::make_big_buf( size_t sz ) noexcept
{
  this->init_big();
  void * p = this->big_alloc( sz );
  if ( p == NULL )
    return NULL;
  this->big_list.pop_hd();
  return p;
}

void
ScratchMem::push_big_buf( void *p ) noexcept
{
  this->init_big();
  BigBlock *big = (BigBlock *) ( (uint8_t *) p - sizeof( BigBlock ) );
  this->big_list.push_tl( big );
}

void
ScratchMem::release( void *p ) noexcept
{
  MemHdr * hdr = (MemHdr *) ( (uint8_t *) p - sizeof( MemHdr ) );
  size_t   off;

  if ( hdr->get_offset( off ) ) {
    if ( off != 0 ) {
      MBlock * blk = (MBlock *) ( (uint8_t *) p - off );
      ScratchMem &_this = *blk->owner;
      blk->off2 += (uint32_t) hdr->size;
      if ( blk->off2 == _this.alloc_size )
	_this.release_block( blk );
    }
    else { /* if too large for ScratchMem, was directly from malloc() */
      BigBlock *big = (BigBlock *) ( (uint8_t *) p - sizeof( BigBlock ) );
      ScratchMem &_this = *big->owner;
      _this.big_list.pop( big );
      _this.kv_big_free( _this.closure, big, big->hdr.size );
    }
  }
  else {
    fprintf( stderr, "Bad pointer of ScratchMem object: %p\n", p );
  }
}

