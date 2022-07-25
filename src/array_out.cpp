#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>
#include <raikv/array_space.h>

using namespace rai;
using namespace kv;

int
ArrayOutput::puts( const char *s ) noexcept
{
  size_t n = ::strlen( s );
  char * p = this->make( this->count + n );
  ::memcpy( &p[ this->count ], s, n );
  this->count += n;
  return n;
}

void
ArrayOutput::putchar( char c ) noexcept
{
  char * p = this->make( this->count + 1 );
  p[ this->count ] = c;
  this->count += 1;
}

int
ArrayOutput::printf( const char *fmt, ... ) noexcept
{
  va_list args;
  va_start( args, fmt );
  int n = this->vprintf( fmt, args );
  va_end( args );
  return n;
}

int
ArrayOutput::vprintf( const char *fmt, va_list args ) noexcept
{
  int n, len = 1024;
  for (;;) {
    char * p = this->make( this->count + len );
    n = ::vsnprintf( &p[ this->count ], len, fmt, args );
    if ( n < len ) {
      this->count += n;
      break;
    }
    len += 1024;
  }
  return n;
}

ArrayOutput &
ArrayOutput::nil( void ) noexcept
{
  char *p = this->make( this->count + 1 );
  p[ this->count++ ] = '\0';
  return *this;
}

ArrayOutput &
ArrayOutput::s( const char *buf ) noexcept
{
  if ( buf != NULL ) {
    char * p = this->make( this->count + ::strlen( buf ) + 1 );
    while ( *buf != 0 ) { p[ this->count++ ] = *buf++; }
    p[ this->count ] = '\0';
  }
  return *this;
}

ArrayOutput &
ArrayOutput::b( const char *buf, size_t buf_len ) noexcept
{
  if ( buf != NULL && buf_len != 0 ) {
    char * p = this->make( this->count + buf_len + 1 );
    while ( buf_len != 0 ) { p[ this->count++ ] = *buf++; buf_len--; }
    p[ this->count ] = '\0';
  }
  return *this;
}

ArrayOutput &
ArrayOutput::u( uint64_t n ) noexcept
{
  size_t len = uint64_digits( n );
  char * p   = this->make( this->count + len + 1 );
  uint64_to_string( n, &p[ this->count ], len );
  this->count += len;
  p[ this->count ] = '\0';
  return *this;
}

ArrayOutput &
ArrayOutput::i( uint32_t n ) noexcept
{
  size_t len = uint32_digits( n );
  char * p   = this->make( this->count + len + 1 );
  uint32_to_string( n, &p[ this->count ], len );
  this->count += len;
  p[ this->count ] = '\0';
  return *this;
}

