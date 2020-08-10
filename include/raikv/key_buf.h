#ifndef __rai__raikv__key_buf_h__
#define __rai__raikv__key_buf_h__

/* also include stdint.h, string.h */
#include <raikv/key_ctx.h>

#ifdef __cplusplus
namespace rai {
namespace kv {

static const size_t MAX_KEY_SIZE = 128; /* could be up to 1 << 16 */

/* this is not used internally, it is defined for clients */
template <uint16_t KEY_SIZE>
struct KeyBufT : public KeyFragment {
  static const uint16_t MAX_BUF_SIZE = KEY_SIZE - 2; /* keylen */
 /* extends KeyFragment for a larger contiguous buffer */
  char morebuf[ KEY_SIZE - KEY_FRAG_SIZE ];

  KeyBufT() {
    this->keylen = 0;
  }
  KeyBufT( const char *s ) {
    this->set_string( s );
  }
  KeyBufT( const KeyFragment &x ) {
    this->copy( x );
  }
  void set_string( const char *s ) {
    this->copy( s, ::strnlen( s, MAX_BUF_SIZE ) + 1 );
  }
  /* useful for integer key types */
  template<class T> void set( T arg ) {
    this->copy( (const void *) &arg, sizeof( T ) );
  }
  /* useful for integer key types */
  template<class T> size_t get( T &arg ) const {
    return this->get( (void *) &arg, sizeof( T ) );
  }
  /* should probably not truncate silently */
  uint16_t copy( const KeyFragment &x ) {
    return this->copy( x.u.buf, x.keylen );
  }
  uint16_t copy( const void *p,  size_t len ) {
    if ( len > MAX_BUF_SIZE )
      len = MAX_BUF_SIZE;
    ::memcpy( this->u.buf, p, len ); /* if p == null then len = 0 */
    this->keylen = len;
    return len;
  }
  size_t get( void *p,  size_t len ) const {
    if ( len > this->keylen )
      len = this->keylen;
    ::memcpy( p, this->u.buf, len ); /* if p == null then len = 0 */
    return len;
  }
  void zero( void ) {
    ::memset( this, 0, sizeof( *this ) );
  }
};

typedef KeyBufT<MAX_KEY_SIZE> KeyBuf;
//typedef KeyBufT<MAX_KEY_BUF_LARGE_SIZE> KeyBufLarge;

/* buf[] aligned on 8 byte boundary, may be a slight win:
 * unaligned hash = 15.7ns 63650879.3/s
 * aligned hash = 14.7ns 68005352.5/s
 */
struct KeyBufAligned {
  uint16_t pad[ 3 ];
  KeyBuf   kb;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  KeyBufAligned() : kb() {}
  KeyBufAligned( const char *s ) : kb( s ) {}
  KeyBufAligned( const KeyFragment &x ) : kb( x ) {}
  void set_string( const char *s ) { this->kb.set_string( s ); }
  template<class T> void set( T arg ) { this->kb.set( arg ); }
  template<class T> size_t get( T &arg ) const {
    return this->kb.get( (void *) &arg, sizeof( T ) );
  }
  size_t get( void *p,  size_t len ) const { return this->kb.get( p, len ); }
  void copy( const void *p,  uint16_t len ) { this->kb.copy( p, len ); }
  void zero() { this->kb.zero(); }
  void hash( uint64_t &seed,  uint64_t &seed2/*,  kv_hash128_func_t func*/ ) {
    this->kb.hash( seed, seed2/*, func*/ );
  }
#if 0
  uint64_t hash128( kv_hash128_func_t func ) {
    return this->kb.hash128( func );
  }
#endif
  operator KeyFragment&() { return this->kb; }
  operator KeyFragment*() { return &this->kb; }

  static KeyBufAligned *new_array( void * b,  size_t sz ) {
    if ( b == NULL )
      b = ::malloc( sizeof( KeyBufAligned ) * sz );
    KeyBufAligned *p = (KeyBufAligned *) b;
    if ( p == NULL )
      return NULL;
    for ( size_t i = 0; i < sz; i++ ) {
      new ( (void *) p ) KeyBufAligned();
      p = &p[ 1 ];
    }
    return (KeyBufAligned *) b;
  }
};

/* simple version of the WorkAlloc defining 8k stack space usage */
typedef struct WorkAllocT<8192> WorkAlloc8k;

}
}
#endif
#endif
