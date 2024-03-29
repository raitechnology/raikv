#ifndef __rai__raikv__work_h__
#define __rai__raikv__work_h__

#ifdef __cplusplus
extern "C" {
#endif
typedef void *(*kv_alloc_func_t)( void *closure,  size_t item_size );
typedef void (*kv_free_func_t)( void *closure,  void *item,  size_t item_size );
/* WorkAlloc::big_alloc */
extern void *kv_key_ctx_big_alloc( void *closure,  size_t item_size );
extern void kv_key_ctx_big_free( void *closure,  void *item,  size_t item_size);

struct kv_work_alloc_s;
typedef struct kv_work_alloc_s kv_work_alloc_t;
/* kv_work_alloc_t alloc = kc_create_ctx_alloc( 8 * 1024, NULL, NULL, NULL ); */
kv_work_alloc_t *kv_create_ctx_alloc( size_t sz,  kv_alloc_func_t ba,
                                     kv_free_func_t bf,  void *closure );
void kv_release_ctx_alloc( kv_work_alloc_t *ctx_alloc );

#ifdef __cplusplus
}
#endif

#ifdef __cplusplus
#include <raikv/dlinklist.h>
#include <raikv/util.h>

namespace rai {
namespace kv {

struct ScratchMem {
  /* mblock for pkt mem that usually alloced and freed in order */
  struct MBlock {
    ScratchMem * owner;
    MBlock     * next,
               * back;
    uint32_t     off2, size;

    void * arena_ptr( size_t i ) {
      return &((uint8_t *) this )[ i ];
    }
    void init( ScratchMem *o,  uint32_t sz ) {
      this->owner = o;
      this->next  = this->back = NULL;
      this->off2  = sizeof( MBlock );
      this->size  = sz;
    }
  };
  struct MemHdr {
    static const size_t MEM_MARKER = 0xdad00000U,
                        MEM_MASK   = 0x000fffffU;
    size_t size, offset;
    void * user_ptr( void ) {
      return &this[ 1 ];
    }
    bool get_offset( size_t &off ) {
      if ( (this->offset & ~MEM_MASK) != MEM_MARKER )
        return false;
      off = this->offset & MEM_MASK;
      this->offset = 0;
      return true;
    }
    void set_offset( size_t off ) {
      this->offset = off | MEM_MARKER;
    }
  };
  struct BigBlock {
    ScratchMem * owner;
    BigBlock   * next,
               * back;
    MemHdr       hdr;
  };
  void              * fast_buf;
  size_t              fast_off;
  const size_t        fast_len;
  DLinkList<MBlock>   blk_list; /* alloc from head */
  DLinkList<BigBlock> big_list; /* big blocks in use > alloc_size */
  bool                fast;
  uint32_t            off,     /* offset within hd */
                      free_cnt,/* num of blocks available (up to slack_count) */
                      block_cnt;
  const uint32_t      slack_count;
  const size_t        alloc_size;
  /* external allocation functions to acquire heap mem */
  kv_alloc_func_t     kv_big_alloc;
  kv_free_func_t      kv_big_free;
  void              * closure;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { aligned_free( ptr ); }
  static ScratchMem *create( void ) {
    return reinterpret_cast<ScratchMem *>(
      kv_create_ctx_alloc( 8 * 1024, 0, 0, 0 ) );
  }

  ScratchMem( uint32_t cnt = 2,  uint32_t sz = 16 * 1024 - 32,
              void *fb = 0,  size_t fbsz = 0,
              kv_alloc_func_t ba = 0/*kv_key_ctx_big_alloc*/,
              kv_free_func_t bf = 0/*kv_key_ctx_big_free*/,  void *cl = 0 )
    : fast_buf( fb ), fast_off( 0 ), fast_len( fbsz ),
      fast( fbsz != 0 ), off( sz ), free_cnt( 0 ), block_cnt( 0 ),
      slack_count( cnt ), alloc_size( sz ), kv_big_alloc( ba ),
      kv_big_free( bf ), closure( cl ) {}
  ~ScratchMem() {
    if ( ! this->blk_list.is_empty() || ! this->big_list.is_empty() )
      this->release_all();
  }

  bool is_valid( const void *p ) const {
    const char * s = (const char *) p;
    if ( s >= (char *) this->fast_buf ) {
      if ( s < &((char *) this->fast_buf)[ this->fast_len ] )
        return true;
    }
    for ( MBlock *blk = this->blk_list.hd; blk != NULL; blk = blk->next ) {
      if ( s >= (char *) (void *) &blk[ 1 ] &&
           s < &((char *) (void *) blk)[ this->alloc_size ] )
        return true;
    }
    for ( BigBlock *bb = this->big_list.hd; bb != NULL; bb = bb->next ) {
      if ( s >= (char *) (void *) &bb[ 1 ] &&
           s < &((char *) (void *) bb)[ bb->hdr.size ] )
        return true;
    }
    return false;
  }
  size_t mem_size( void ) noexcept;          /* amount of mem alloced + fast */
  void release_all( void ) noexcept;         /* free all blocks */
  MBlock *alloc_block( void ) noexcept;      /* alloc or reuse a block */
  void release_block( MBlock *m ) noexcept;  /* free or save an empty block */
  void reset( void ) {              /* reset all allocs */
    this->fast_off = 0;
    if ( ! this->fast )
      this->reset_slow();
  }
  void reset_slow( void ) noexcept;
  void *alloc( size_t sz ) {        /* get new mem */
    if ( this->fast ) {
      void *cur = &((char *) this->fast_buf)[ this->fast_off ];
      sz = rai::kv::align<size_t>( sz, 16 );
      if ( (this->fast_off += sz) <= this->fast_len )
        return cur;
    }
    return this->alloc_slow( sz );
  }
  void *alloc_slow( size_t sz ) noexcept;
  void *big_alloc( size_t sz ) noexcept;   /* get mem larger than alloc_size */
  void init_big( void ) noexcept;
  void *make_big_buf( size_t sz ) noexcept;
  void push_big_buf( void *p ) noexcept;
  static void release( void *p ) noexcept; /* release ptr returned by alloc() */
};

#ifdef _MSC_VER
__declspec(align(64)) struct BufAlign64 {
  uint8_t line[ 64 ];
};
#else
struct BufAlign64 {
  uint8_t line[ 64 ];
} __attribute__((__aligned__(64)));
#endif

template <size_t fast_size>
struct WorkAllocT : public ScratchMem {
  BufAlign64 spc[ fast_size / sizeof( BufAlign64 ) ];

  WorkAllocT( uint32_t cnt = 2, uint32_t sz = 16 * 1024 - 32,
              kv_alloc_func_t ba = 0/*kv_key_ctx_big_alloc*/,
              kv_free_func_t bf = 0/*kv_key_ctx_big_free*/,  void *cl = 0 )
    : ScratchMem( cnt, sz, this->spc, sizeof( this->spc ), ba, bf, cl ) {}
};
}
}
#endif
#endif
