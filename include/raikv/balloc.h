#ifndef __rai_raikv__balloc_h__
#define __rai_raikv__balloc_h__

#include <raikv/util.h>
#include <raikv/dlinklist.h>

namespace rai {
namespace kv {
 /* BA_BLOCK_SIZE, BA_ALLOC_SIZE  should be powers of 2 */
template <size_t BA_BLOCK_SIZE, size_t BA_ALLOC_SIZE>
struct Balloc {
  static const size_t BA_COUNT = BA_ALLOC_SIZE / BA_BLOCK_SIZE,
                      N        = BA_COUNT / 64;
  uint64_t free_size[ N ];
  size_t   alloced, malloced;
  uint8_t  buf[ BA_ALLOC_SIZE ];

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  Balloc() : alloced( 0 ), malloced( 0 ) {
    for ( size_t i = 0; i < N; i++ )
      this->free_size[ i ] = 0;
  }

  bool in_block( const void *ptr ) const {
    return ( (uint8_t *) ptr >= this->buf &&
             (uint8_t *) ptr < &this->buf[ BA_ALLOC_SIZE ] );
  }

  void *alloc_block( size_t i,  size_t bits,  uint64_t mask ) {
    uint64_t fsz = this->free_size[ i ];
    size_t   free_bits, used_bits;
    if ( kv_popcountl( ~fsz ) < bits )
      return NULL;
    for ( size_t shift = 0; ; ) {
      if ( fsz == 0 )
        free_bits = 64 - shift;
      else
        free_bits = kv_ffsl( fsz ) - 1;
      if ( free_bits >= bits ) {
        this->free_size[ i ] |= ( mask << shift );
        this->alloced        += bits;
        return (void *) &this->buf[ BA_BLOCK_SIZE * ( shift + 64 * i ) ];
      }
      shift += free_bits;
      if ( shift + bits >= 64 )
        return NULL;
      if ( free_bits < 64 )
        fsz >>= free_bits;
      else
        fsz = 0;
      used_bits = kv_ffsl( ~fsz ) - 1;
      shift += used_bits;
      fsz >>= used_bits;
    }
  }

  void *alloc( size_t size ) {
    if ( size == 0 )
      return NULL;
    void * ptr = this->try_alloc( size );
    if ( ptr != NULL )
      return ptr;
    this->malloced++;
    return ::malloc( size );
  }

  void *try_alloc( size_t size ) {
    if ( size == 0 )
      return NULL;
    size_t   asize = align<size_t>( size, BA_BLOCK_SIZE ),
             bits  = asize / BA_BLOCK_SIZE;
    uint64_t mask  = ~(uint64_t) 0;
    if ( bits > 64 || this->alloced + bits > BA_COUNT )
      return NULL;
    if ( bits < 64 )
      mask = ( (uint64_t) 1 << bits ) - 1;
    for ( size_t i = 0; i < N; i++ ) {
      void * ptr = this->alloc_block( i, bits, mask );
      if ( ptr != NULL )
        return ptr;
    }
    return NULL;
  }

  void release( void *ptr,  size_t size ) {
    if ( size == 0 )
      return;
    if ( ! this->in_block( ptr ) ) {
      this->malloced--;
      ::free( ptr );
      return;
    }
    size = align<size_t>( size, BA_BLOCK_SIZE );
    size_t   bits  = size / BA_BLOCK_SIZE;
    size_t   shift = (size_t) ( (uint8_t *) ptr - this->buf ) / BA_BLOCK_SIZE;
    uint64_t mask  = ~(uint64_t) 0;
    if ( bits < 64 )
      mask = ( (uint64_t) 1 << bits ) - 1;
    this->free_size[ shift / 64 ] &= ~( mask << ( shift % 64 ) );
    this->alloced -= bits;
  }

  void *try_resize( void *ptr,  size_t osize,  size_t nsize ) {
    size_t   asize = align<size_t>( osize, BA_BLOCK_SIZE ),
             bsize = align<size_t>( nsize, BA_BLOCK_SIZE ),
             obits = asize / BA_BLOCK_SIZE,
             nbits = bsize / BA_BLOCK_SIZE;
    uint64_t nmask = ~(uint64_t) 0,
             omask = ~(uint64_t) 0;
    size_t   shift, i, j;

    if ( nbits < 64 )
      nmask = ( (uint64_t) 1 << nbits ) - 1;
    if ( obits < 64 )
      omask = ( (uint64_t) 1 << obits ) - 1;

    if ( this->in_block( ptr ) ) {
      if ( asize == bsize )
        return ptr;
      shift = (size_t) ( (uint8_t *) ptr - this->buf ) / BA_BLOCK_SIZE,
      i     = shift / 64,
      j     = shift % 64;

      if ( nbits < obits || ( j + nbits <= 64 &&
           ( ( this->free_size[ i ] >> j ) & nmask ) == omask ) ) {
        this->free_size[ i ] &= ~( omask << j );
        this->free_size[ i ] |=  ( nmask << j );
        this->alloced        -=  obits;
        this->alloced        +=  nbits;
        return ( nbits == 0 ) ? NULL : ptr;
      }
    }
    return NULL;
  }

  void *resize( void *ptr,  size_t osize,  size_t nsize ) {
    size_t   asize = align<size_t>( osize, BA_BLOCK_SIZE ),
             bsize = align<size_t>( nsize, BA_BLOCK_SIZE ),
             obits = asize / BA_BLOCK_SIZE,
             nbits = bsize / BA_BLOCK_SIZE,
             msize = min_int<size_t>( osize, nsize );
    uint64_t nmask = ~(uint64_t) 0,
             omask = ~(uint64_t) 0;
    size_t   shift, i, j;
    void   * sav = NULL, * ptr2;

    if ( nbits < 64 )
      nmask = ( (uint64_t) 1 << nbits ) - 1;
    if ( obits < 64 )
      omask = ( (uint64_t) 1 << obits ) - 1;

    if ( this->in_block( ptr ) ) {
      if ( asize == bsize )
        return ptr;
      shift = (size_t) ( (uint8_t *) ptr - this->buf ) / BA_BLOCK_SIZE,
      i     = shift / 64,
      j     = shift % 64;

      if ( nbits < obits || ( j + nbits <= 64 &&
           ( ( this->free_size[ i ] >> j ) & nmask ) == omask ) ) {
        this->free_size[ i ] &= ~( omask << j );
        this->free_size[ i ] |=  ( nmask << j );
        this->alloced        -=  obits;
        this->alloced        +=  nbits;
        return ( nbits == 0 ) ? NULL : ptr;
      }
      sav = ptr;
      this->free_size[ i ] &= ~( omask << j );
      this->alloced -= obits;
    }
    if ( nbits <= 64 ) {
      for ( i = 0; i < N; i++ ) {
        if ( (ptr2 = this->alloc_block( i, nbits, nmask )) != NULL ) {
          if ( sav == NULL ) {
            if ( osize > 0 ) {
              ::memcpy( ptr2, ptr, msize );
              this->malloced--;
              ::free( ptr );
            }
            return ptr2;
          }
          ::memmove( ptr2, sav, msize );
          return ptr2;
        }
      }
    }
    ptr2 = ( sav == NULL ? ptr : NULL );
    ptr2 = ::realloc( ptr2, nsize );
    if ( sav != NULL ) {
      this->malloced++;
      ::memcpy( ptr2, sav, msize );
    }
    return ptr2;
  }

  void *compact( void *ptr,  size_t size ) {
    size = align<size_t>( size, BA_BLOCK_SIZE );

    size_t   bits  = size / BA_BLOCK_SIZE;
    size_t   shift;
    uint64_t mask  = ~(uint64_t) 0;
    void   * sav   = NULL, * ptr2;
    if ( bits > 64 )
      return ptr;
    if ( bits < 64 )
      mask = ( (uint64_t) 1 << bits ) - 1;

    if ( this->in_block( ptr ) ) {
      sav   = ptr;
      shift = (size_t) ( (uint8_t *) ptr - this->buf ) / BA_BLOCK_SIZE;
      this->free_size[ shift / 64 ] &= ~( mask << ( shift % 64 ) );
      this->alloced -= bits;
    }
    for ( size_t i = 0; i < N; i++ ) {
      if ( (ptr2 = this->alloc_block( i, bits, mask )) != NULL ) {
        if ( sav == NULL ) {
          ::memcpy( ptr2, ptr, size );
          this->malloced--;
          ::free( ptr );
          return ptr2;
        }
        if ( ptr2 != sav )
          ::memmove( ptr2, sav, size );
        return ptr2;
      }
    }
    return ptr;
  }
};

template <size_t BL_BLOCK_SIZE, size_t BL_ALLOC_SIZE>
struct BallocList {
  static const size_t BL_COUNT = BL_ALLOC_SIZE / BL_BLOCK_SIZE;
  struct Elem {
    Elem * next, * back;
    Balloc<BL_BLOCK_SIZE, BL_ALLOC_SIZE> mem;

    void * operator new( size_t, void *ptr ) { return ptr; }
    void operator delete( void *ptr ) { ::free( ptr ); }
    Elem() : next( 0 ), back( 0 ) {}
  };
  DLinkList<Elem> list;

  Elem *new_balloc( void ) {
    Elem *x = this->list.tl;
    if ( x != NULL && x->mem.alloced < BL_COUNT / 4 )
      this->list.pop( x );
    else
      x = new ( ::malloc( sizeof( Elem ) ) ) Elem();
    this->list.push_hd( x );
    return x;
  }
  void *alloc( size_t sz ) {
    void * p = NULL;
    if ( sz > 0 ) {
      Elem *x = this->list.hd;
      if ( x == NULL || (p = x->mem.try_alloc( sz )) == NULL ) {
        if ( sz <= BL_BLOCK_SIZE ) {
          x = this->new_balloc();
          p = x->mem.alloc( sz );
        }
      }
    }
    if ( p != NULL )
      return p;
    return ::malloc( sz );
  }
  bool release_if_alloced( void *p,  size_t sz ) {
    if ( sz > 0 ) {
      Elem *x = this->list.hd;
      for ( ; x != NULL; x = x->next ) {
        if ( x->mem.in_block( p ) ) {
          x->mem.release( p, sz );
          return true;
        }
      }
    }
    return false;
  }
  void release( void *p,  size_t sz ) {
    if ( sz > 0 ) {
      if ( this->release_if_alloced( p, sz ) )
        return;
      ::free( p );
    }
  }
  void *resize( void *p,  size_t osz,  size_t nsz ) {
    Elem *x = this->list.hd;
    if ( osz > 0 ) {
      for ( ; x != NULL; x = x->next ) {
        if ( x->mem.in_block( p ) ) {
          if ( nsz == 0 ) {
            x->mem.release( p, osz );
            return NULL;
          }
          void *p2 = x->mem.try_resize( p, osz, nsz );
          if ( p2 != NULL )
            return p2;
          break;
        }
      }
    }
    void * p2 = this->alloc( nsz );
    if ( osz > 0 ) {
      ::memcpy( p2, p, min_int<size_t>( osz, nsz ) );
      if ( x != NULL )
        x->mem.release( p, osz );
      else
        ::free( p );
    }
    return p2;
  }
};


}
}
#endif
