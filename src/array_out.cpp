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
  *this->ptr++ = '\0';
  return *this;
}

ArrayOutput &
ArrayOutput::s( const char *buf ) noexcept
{
  this->count += ::strlen( buf );
  this->make( this->count );
  while ( *buf != 0 ) { *this->ptr++ = *buf++; }
  return *this;
}

ArrayOutput &
ArrayOutput::b( const char *buf, size_t buf_len ) noexcept
{
  this->count += buf_len;
  this->make( this->count );
  while ( buf_len != 0 ) { *this->ptr++ = *buf++; buf_len -= 1; }
  return *this;
}

ArrayOutput &
ArrayOutput::u( uint64_t n ) noexcept
{
  size_t len = uint64_digits( n );
  this->count += len;
  this->make( this->count );
  uint64_to_string( n, this->ptr, len );
  this->ptr += len;
  return *this;
}

ArrayOutput &
ArrayOutput::i( uint32_t n ) noexcept
{
  size_t len = uint32_digits( n );
  this->count += len;
  this->make( this->count );
  uint32_to_string( n, this->ptr, len );
  this->ptr += len;
  return *this;
}

