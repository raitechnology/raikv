#ifndef __rai_raikv__array_space_h__
#define __rai_raikv__array_space_h__

#include <raikv/util.h>

namespace rai {
namespace kv {

template <class T, size_t growby>
struct ArraySpace {
  size_t size;
  T    * ptr;
  ArraySpace() : size( 0 ), ptr( 0 ) {}
  ~ArraySpace() { this->reset(); }
  T *make( size_t new_sz,  bool clear = false ) {
    if ( new_sz <= this->size )
      return this->ptr;
    return this->resize( new_sz, clear );
  }
  T *resize( size_t new_sz,  bool clear = false ) {
    size_t  sz  = align<size_t>( new_sz, growby ) * sizeof( T );
    size_t  old = this->size;
    this->ptr  = (T *) ::realloc( (void *) this->ptr, sz );
    this->size = sz / sizeof( T );
    if ( clear )
      ::memset( (void *) &this->ptr[ old ], 0, sz - ( old * sizeof( T ) ) );
    return this->ptr;
  }
  void reset( void ) {
    if ( this->ptr != NULL ) {
      ::free( this->ptr );
      this->size = 0;
      this->ptr  = NULL;
    }
  }
  void zero( void ) {
    if ( this->ptr != NULL ) {
      ::memset( (void *) this->ptr, 0, this->size * sizeof( T ) );
    }
  }
  T &operator[] ( size_t n ) {
    return this->ptr[ n ];
  }
  const T &operator[] ( size_t n ) const {
    return this->ptr[ n ];
  }
};

template <class T, size_t arsz>
struct ArrayCount : public kv::ArraySpace<T, arsz> {
  size_t count;
  ArrayCount() : count( 0 ) {}

  T &operator[] ( size_t n ) {
    if ( n >= this->count ) {
      this->count = n + 1;
      this->make( n + 1, true );
    }
    return this->ptr[ n ];
  }
  T &push( T el ) {
    return this->push( &el );
  }
  T &push( T *el = NULL ) {
    this->make( this->count + 1, true );
    if ( el != NULL )
      this->ptr[ this->count ] = *el;
    return this->ptr[ this->count++ ];
  }
  void clear( void ) {
    this->reset();
    this->count = 0;
  }
  void zero( void ) {
    this->ArraySpace<T, arsz>::zero();
    this->count = 0;
  }
};

struct ArrayOutput : public ArrayCount< char, 8192 > {
  int puts( const char *s ) noexcept;
  void putchar( char c ) noexcept;
  int printf( const char *fmt,  ... ) noexcept __attribute__((format(printf,2,3)));
  int vprintf( const char *fmt, va_list args ) noexcept;

  ArrayOutput &nil( void ) noexcept;
  ArrayOutput &s( const char *buf ) noexcept;
  ArrayOutput &b( const char *buf,  size_t buf_len ) noexcept;
  ArrayOutput &u( uint64_t n ) noexcept;
  ArrayOutput &i( uint32_t n ) noexcept;
};

template <class T>
struct CatPtrT {
  char * start, * ptr;
  CatPtrT( char *out ) { this->ptr = this->start = out; }
  size_t len( void ) {
    return this->ptr - this->start;
  }
  T &b( const void *mem,  size_t len ) {
    ::memcpy( this->ptr, mem, len );
    this->ptr += len;
    return (T &) *this;
  }
  T &x( const void *mem,  size_t sz ) {
    const char *m = (const char *) mem;
    do { *this->ptr++ = *m++; } while ( --sz > 0 );
    return (T &) *this;
  }
  T &s( const char *str ) {
    while ( *str )
      *this->ptr++ = *str++;
    return (T &) *this;
  }
  T &c( char ch ) {
    *this->ptr++ = ch;
    return (T &) *this;
  }
  T &u( uint64_t n,  size_t d ) {
    this->ptr += uint64_to_string( n, this->ptr, d );
    return (T &) *this;
  }
  T &u( uint32_t n,  size_t d ) {
    this->ptr += uint32_to_string( n, this->ptr, d );
    return (T &) *this;
  }
  T &u( uint16_t n,  size_t d ) {
    this->ptr += uint16_to_string( n, this->ptr, d );
    return (T &) *this;
  }
  T &i( int64_t n,  size_t d ) {
    this->ptr += int64_to_string( n, this->ptr, d );
    return (T &) *this;
  }
  T &i( int32_t n,  size_t d ) {
    this->ptr += int32_to_string( n, this->ptr, d );
    return (T &) *this;
  }
  size_t end( void ) {
    *this->ptr = '\0';
    return this->len();
  }
};

template <size_t maxsize>
struct CatBuf : public CatPtrT<CatBuf<maxsize>> {
  char buf[ maxsize ];
  CatBuf() : CatPtrT<CatBuf<maxsize>>( this->buf ) {}
};

struct CatPtr : public CatPtrT<CatPtr> {
  CatPtr( char *out ) : CatPtrT( out ) {}
};

}
}
#endif
