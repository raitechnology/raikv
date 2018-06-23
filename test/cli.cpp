#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <unistd.h>
#include <ctype.h>
#include <pthread.h>
#include <signal.h>

#include <raikv/shm_ht.h>
#include <raikv/key_buf.h>

using namespace rai;
using namespace kv;

HashTabGeom geom;
HashTab   * map;
uint32_t    ctx_id = MAX_CTX_ID, my_pid, no_pid = 666;

//static uint64_t max( uint64_t x,  uint64_t y ) { return ( x > y ? x : y ); }
static void print_stats( uint32_t c = ctx_id );
static void get_key_string( KeyFragment &kb,  char *key );

FILE *outfp = NULL;
bool quiet;

static int xprintf( FILE *out,  const char *format, ... )
  __attribute__((format(printf,2,3)));

static int
xprintf( FILE *out,  const char *format, ... )
{
  FILE *o1 = out,
       *o2 = ( ( out == NULL && ! quiet ) ? stdout : NULL );
  int sz = 0;
#ifndef va_copy
#define va_copy( dst, src ) memcpy( &( dst ), &( src ), sizeof( va_list ) )
#endif
  va_list args;
  va_start( args, format );
  if ( o1 != NULL ) {
    va_list aq;
    va_copy( aq, args );
    sz = vfprintf( o1, format, aq );
    va_end( aq );
  }
  if ( o2 != NULL ) {
    va_list aq;
    va_copy( aq, args );
    sz = vfprintf( o2, format, aq );
    va_end( aq );
  }
  return sz;
}

static void
shm_attach( const char *mn )
{
  map = HashTab::attach_map( mn, 0, geom );
  if ( map != NULL ) {
    ctx_id = map->attach_ctx( my_pid = ::getpid() );
    fputs( print_map_geom( map, ctx_id ), stdout );
  }
}

static void
shm_close( void )
{
  print_stats();
  if ( ctx_id != MAX_CTX_ID ) {
    map->detach_ctx( ctx_id );
    ctx_id = MAX_CTX_ID;
  }
  delete map;
}

static void
fix_locks( void )
{
  uint32_t hash_entry_size = map->hdr.hash_entry_size;
  for ( uint32_t c = 1; c < MAX_CTX_ID; c++ ) {
    uint32_t pid = map->ctx[ c ].ctx_pid;
    if ( pid == 0 ||
         map->ctx[ c ].ctx_id == KV_NO_CTX_ID ||
         ::kill( pid, 0 ) == 0 )
      continue;

    uint64_t used, recovered = 0;
    if ( (used = map->ctx[ c ].mcs_used) == 0 )
      continue;

    for ( uint32_t id = 0; id < 64; id++ ) {
      if ( ( used & ( (uint64_t) 1 << id ) ) == 0 )
        continue;
      uint64_t mcs_id = ( c << ThrCtxEntry::MCS_SHIFT ) | id;
      ThrMCSLock &mcs = map->ctx[ c ].get_mcs_lock( mcs_id );
      MCSStatus status;
      xprintf( 0,
      "ctx %u: pid %u, mcs %u, val 0x%lx, lock 0x%lx, next 0x%lx, link %lu\n",
               c, pid, id, mcs.val.val, mcs.lock.val, mcs.next.val,
               mcs.lock_id );
      if ( mcs.lock_id != 0 ) {
        HashEntry *el = map->get_entry( mcs.lock_id - 1,
                                        map->hdr.hash_entry_size );
        ThrCtxOwner  closure( map->ctx );
        status = mcs.recover_lock( el->hash, ZOMBIE64, mcs_id, closure );
        if ( status == MCS_OK ) {
          ValueCtr &ctr = el->value_ctr( hash_entry_size );
          if ( ctr.seal == 0 || el->seal != ctr.seriallo ) {
            ctr.seal = 1; /* these are lost with the context thread */
            el->seal = ctr.seriallo;
          }
          status = mcs.recover_unlock( el->hash, ZOMBIE64, mcs_id, closure );
          if ( status == MCS_OK ) {
            xprintf( 0, "mcs_id %u:%u recovered\n", c, id );
            recovered |= ( (uint64_t) 1 << id );
          }
        }
        if ( status != MCS_OK ) {
          xprintf( 0, "mcs_id %u:%u status %s\n", c, id,
                   status == MCS_WAIT ? "MCS_WAIT" : "MCS_INACTIVE" );
        }
      }
    }
    map->ctx[ c ].mcs_used &= ~recovered;
    if ( used != recovered ) {
      xprintf( 0, "ctx %u still has locks\n", c );
    }
    else {
      map->detach_ctx( c );
    }
  }
}

static void
print_stats( uint32_t c )
{
  if ( c != MAX_CTX_ID ) {
    int alive = 0;
    HashCounters & stat = map->ctx[ c ].stat;
    uint32_t pid = map->ctx[ c ].ctx_pid;
    if ( pid != 0 ) {
      if ( map->ctx[ c ].ctx_id != KV_NO_CTX_ID ) {
        alive = 1;
        if ( ::kill( pid, 0 ) == 0 )
          alive = 2;
      }
    }
    xprintf( 0, "ctx %u%c:  pid %u, read %lu, write %lu, spin %lu, chains %lu, "
            "add %lu, drop %lu, htevict %lu, afail %lu, hit %lu, miss %lu, "
            "cuckacq %lu, cuckfet %lu, cuckmov %lu, cuckbiz %lu cuckret %lu "
            "cuckmax %lu\n",
        c, ( c == ctx_id ? '*' :
             ( alive == 2 ? '+' : ( alive == 1 ? '?' : '/' ) ) ),
        pid, stat.rd, stat.wr, stat.spins, stat.chains, stat.add, stat.drop,
        stat.htevict, stat.afail, stat.hit, stat.miss, stat.cuckacq,
        stat.cuckfet, stat.cuckmov, stat.cuckbiz, stat.cuckret, stat.cuckmax );
  }
  else {
    xprintf( 0, "No ctx_id\n" );
  }
}

static void
print_mem( uint32_t s )
{
  if ( s < map->hdr.nsegs ) {
    Segment &seg = map->segment( s );
    uint64_t x, y;
    seg.get_position( seg.ring.val, map->hdr.seg_align_shift, x, y );
    xprintf( 0, "seg(%u): off=%lu, next=%lu:%lu, msg_count=%lu, "
             "avail_size=%lu, move_msgs=%lu, move_size=%lu, evict_msgs=%lu, "
             "evict_size=%lu\n", s, seg.seg_off, x, y, seg.msg_count.val,
             seg.avail_size.val, seg.move_msgs, seg.move_size, seg.evict_msgs,
             seg.evict_size );
  }
}

static void
print_seg( uint32_t s )
{
  if ( s < map->hdr.nsegs ) {
    uint64_t dead_size = 0;
    char key[ 8192 ];
    print_mem( s );
    uint64_t off = 0, seg_size = map->hdr.seg_size();
    for ( MsgHdr *msg = (MsgHdr *) map->seg_data( s, off ); ; ) {
      if ( msg->size == 0 )
        break;
      if ( msg->hash == ZOMBIE64 ) {
        dead_size += msg->size;
      }
      else {
        if ( dead_size > 0 ) {
          xprintf( 0, "[dead %lu] ", dead_size );
          dead_size = 0;
        }
        KeyFragment &kb = msg->key;
        get_key_string( kb, key );
        xprintf( 0, "[%s %u] ", key, msg->size );
      }
      off += msg->size;
      if ( off >= seg_size )
        break;
      msg = (MsgHdr *) map->seg_data( s, off );
    }
    if ( dead_size > 0 ) {
      xprintf( 0, "[dead %lu] ", dead_size );
    }
    xprintf( 0, "\n" );
  }
}

static void
print_status( const char *cmd,  const char *key,  KeyStatus status )
{
  xprintf( 0, "%s %s%s%s status=%s:%s\n", cmd,
          key[ 0 ] == 0 ? "" : "\"", key, key[ 0 ] == 0 ? "" : "\"",
          kv_key_status_string( status ),
          kv_key_status_description( status ) );
}

struct HexDump {
  static const char hex_chars[];
  char line[ 80 ];
  uint32_t boff, hex, ascii;
  uint64_t stream_off;

  HexDump() : boff( 0 ), stream_off( 0 ) {
    this->flush_line();
  }
  void flush_line( void ) {
    this->stream_off += this->boff;
    this->boff  = 0;
    this->hex   = 9;
    this->ascii = 61;
    this->init_line();
  }
  void init_line( void ) {
    uint64_t k = this->stream_off;
    ::memset( this->line, ' ', 79 );
    this->line[ 79 ] = '\0';
    for ( uint64_t j = 5; k > 0; ) {
      this->line[ j ] = hex_chars[ k & 0xf ];
      if ( j-- == 0 )
        break;
      k >>= 4;
    }
  }
  uint32_t fill_line( const void *ptr,  uint64_t off,  uint64_t len ) {
    while ( off < len && this->boff < 16 ) {
      uint8_t b = ((uint8_t *) ptr)[ off++ ];
      this->line[ this->hex ]   = hex_chars[ b >> 4 ];
      this->line[ this->hex+1 ] = hex_chars[ b & 0xf ];
      this->hex += 3;
      if ( b >= ' ' && b <= 127 )
        line[ this->ascii ] = b;
      this->ascii++;
      if ( ( ++this->boff & 0x3 ) == 0 )
        this->hex++;
    }
    return off;
  }
};

static void
dump_hex( void *ptr,  uint64_t size )
{
  HexDump hex;
  for ( uint64_t off = 0; off < size; ) {
    off = hex.fill_line( ptr, off, size );
    xprintf( 0, "%s\n", hex.line );
    hex.flush_line();
  }
}

static void
dump_value( const char *key )
{
  WorkAlloc8k wrk;
  KeyBuf      kb;
  KeyCtx      kctx( *map, ctx_id, &kb );
  void      * ptr;
  uint64_t    size, h1, h2;
  KeyStatus   status;

  kb.set_string( key );
  h1 = map->hdr.hash_key_seed;
  h2 = map->hdr.hash_key_seed2;
  kb.hash( h1, h2 );
  kctx.set_hash( h1, h2 );

  if ( (status = kctx.find( &wrk )) == KEY_OK &&
       (status = kctx.value( &ptr, size )) == KEY_OK ) {
    dump_hex( ptr, size );
  }
  if ( status != KEY_OK ) {
    xprintf( 0, "value \"%s\" status=%s:%s\n", key,
            kv_key_status_string( status ),
            kv_key_status_description( status ) );
  }
}

const char HexDump::hex_chars[] = "0123456789abcdef";

static bool
eat_token( char *in,  char *&end,  char *buf,  size_t sz )
{
  char *p = in;
  size_t i = 0, j = sz - 1;
  while ( *p == ' ' ) p++;
  if ( *p == '\0' )
    return false;
  for ( ; *p > ' '; p++ )
    if ( i < j )
      buf[ i++ ] = *p;
  buf[ i ] = '\0';
  end = p;
  return i > 0;
}

static uint8_t
hexval( char c ) {
  return ( c >= '0' && c <= '9' ) ? ( c - '0' ) :
         ( c >= 'a' && c <= 'f' ) ? ( c - 'a' + 10 ) : ( c - 'A' + 10 );
}

static bool
parse_key_value( KeyBuf &kb,  const char *key,  size_t len )
{
  uint64_t val = 0;
  size_t   j   = 0;

  if ( key[ 0 ] == '0' && key[ 1 ] == 'x' ) {
    kb.keylen = 0;
    for ( j = 2; j < len; j += 2 ) {
      if ( ! isxdigit( key[ j ] ) || ! isxdigit( key[ j + 1 ] ) )
        break;
      kb.u.buf[ kb.keylen++ ] = (char) ( hexval( key[ j ] ) << 4 ) |
                                         hexval( key[ j + 1 ] );
    }
    return j == len;
  }
  for ( j = 0; j < len; j++ ) {
    if ( ! isdigit( key[ j ] ) )
      break;
    val = val * 10 + ( key[ j ] - '0' );
  }
  if ( j == len ) {
    kb.keylen = sizeof( val );
    ::memcpy( kb.u.buf, &val, sizeof( val ) );
    return true;
  }
  return false;
}

static void
parse_key_string( KeyBuf &kb,  const char *key )
{
  size_t len = ::strlen( key );
  if ( ! parse_key_value( kb, key, len ) ) {
    if ( len > 1 && key[ 0 ] == '\"' && key[ len - 1 ] == '\"' ) {
      kb.keylen = len - 1;
      ::memcpy( kb.u.buf, &key[ 1 ], len - 2 );
      kb.u.buf[ kb.keylen ] = '\0';
    }
    else {
      kb.keylen = len + 1;
      ::memcpy( kb.u.buf, key, kb.keylen );
    }
  }
}

static void
get_key_string( KeyFragment &kb,  char *key )
{
  uint32_t j;
  for ( j = 0; j < kb.keylen; j++ )
    if ( (uint8_t) kb.u.buf[ j ] < ' ' )
      break;
  if ( j + 1 != kb.keylen || kb.u.buf[ j ] != '\0' ) {
    if ( kb.keylen == 8 ) {
      uint64_t val;
      ::memcpy( &val, kb.u.buf, 8 );
      uint64_to_string( val, key );
    }
    else {
      j = 0;
      key[ j++ ] = '0';
      key[ j++ ] = 'x';
      for ( uint16_t i = 0; i < kb.keylen; i++ ) {
        uint8_t b = (uint8_t) kb.u.buf[ i ];
        key[ j++ ] = HexDump::hex_chars[ b >> 4 ];
        key[ j++ ] = HexDump::hex_chars[ b & 0xf ];
      }
      key[ j ] = '\0';
    }
  }
  else {
    ::memcpy( key, kb.u.buf, kb.keylen );
  }
}

static char *
copy_fl( char *buf,  const char *s )
{
  while ( ( *buf = *s ) != '\0' )
    buf++, s++;
  return buf;
}

static char *
flags_string( uint16_t fl,  uint8_t type,  char *buf )
{
  char *s = buf;
  *buf++ = (char) ( fl & FL_ALIGNMENT ) + '0';
  if ( ( fl & FL_SEGMENT_VALUE ) != 0 )
    buf = copy_fl( buf, "-Seg" );
  if ( ( fl & FL_UPDATED ) != 0 )
    buf = copy_fl( buf, "-Upd" );
  if ( ( fl & FL_IMMEDIATE_VALUE ) != 0 )
    buf = copy_fl( buf, "-Ival" );
  if ( ( fl & FL_IMMEDIATE_KEY ) != 0 )
    buf = copy_fl( buf, "-Key" );
  if ( ( fl & FL_PART_KEY ) != 0 )
    buf = copy_fl( buf, "-Part" );
  if ( ( fl & FL_DROPPED ) != 0 )
    buf = copy_fl( buf, "-Drop" );
  if ( ( fl & FL_EXPIRE_STAMP ) != 0 )
    buf = copy_fl( buf, "-Exp" );
  if ( ( fl & FL_UPDATE_STAMP ) != 0 )
    buf = copy_fl( buf, "-Stmp" );
  if ( ( fl & FL_MOVED ) != 0 )
    buf = copy_fl( buf, "-Mved" );
  if ( ( fl & FL_BUSY ) != 0 )
    buf = copy_fl( buf, "-Busy" );
  if ( type != 0 ) {
    buf = copy_fl( buf, "," );
    switch ( type ) {
      default: buf = copy_fl( buf, "MD_NODATA" );    break;
      case 1:  buf = copy_fl( buf, "MD_MESSAGE" );   break;
      case 2:  buf = copy_fl( buf, "MD_STRING" );    break;
      case 3:  buf = copy_fl( buf, "MD_OPAQUE" );    break;
      case 4:  buf = copy_fl( buf, "MD_BOOLEAN" );   break;
      case 5:  buf = copy_fl( buf, "MD_INT" );       break;
      case 6:  buf = copy_fl( buf, "MD_UINT" );      break;
      case 7:  buf = copy_fl( buf, "MD_REAL" );      break;
      case 8:  buf = copy_fl( buf, "MD_ARRAY" );     break;
      case 9:  buf = copy_fl( buf, "MD_PARTIAL" );   break;
      case 10: buf = copy_fl( buf, "MD_IPDATA" );    break;
      case 11: buf = copy_fl( buf, "MD_SUBJECT" );   break;
      case 12: buf = copy_fl( buf, "MD_ENUM" );      break;
      case 13: buf = copy_fl( buf, "MD_TIME" );      break;
      case 14: buf = copy_fl( buf, "MD_DATE" );      break;
      case 15: buf = copy_fl( buf, "MD_DATETIME" );  break;
      case 16: buf = copy_fl( buf, "MD_STAMP" );     break;
      case 17: buf = copy_fl( buf, "MD_DECIMAL" );   break;
      case 18: buf = copy_fl( buf, "MD_LIST" );      break;
      case 19: buf = copy_fl( buf, "MD_HASH" );      break;
      case 20: buf = copy_fl( buf, "MD_SET" );       break;
      case 21: buf = copy_fl( buf, "MD_SORTEDSET" ); break;
      case 22: buf = copy_fl( buf, "MD_STREAM" );    break;
      case 23: buf = copy_fl( buf, "MD_GEO" );       break;
      case 24: buf = copy_fl( buf, "MD_HYPERLOGLOG");break;
      case 25: buf = copy_fl( buf, "MD_PUBSUB" );    break;
      case 26: buf = copy_fl( buf, "MD_SCRIPT" );    break;
      case 27: buf = copy_fl( buf, "MD_SERVER" );    break;
      case 28: buf = copy_fl( buf, "MD_TRANSACTION");break;
    }
  }
  *buf = '\0';
  return s;
}

static bool
match( const char *s,  const char *u, ... )
{
  bool b = false;
  va_list ap;
  va_start( ap, u );
  for ( const char *x = u; x != NULL; x = va_arg( ap, const char * ) ) {
    const char *v = x;
    for ( const char *t = s; ; t++, v++ ) {
      if ( *t == '\0' || *v == '\0' ) {
        if ( *t == '\0' && *v == '\0' )
          b = true;
        if ( *t == 's' && t[ 1 ] == '\0' )
          b = true;
        break;
      }
      if ( tolower( *t ) != tolower( *v ) )
        break;
    }
    if ( b )
      break;
  }
  va_end( ap );
  return b;
}

static const uint64_t MIN_NS   = 60 * NANOS;
static const uint64_t HOUR_NS  = 60 * MIN_NS;
static const uint64_t DAY_NS   = 24 * HOUR_NS;
static const uint64_t WEEK_NS  = 7 * DAY_NS;
static const uint64_t MONTH_NS = 4 * WEEK_NS;

static uint64_t
time_unit( const char *s )
{
  for ( ; ; s++ ) {
    if ( *s == '\0' )
      return 0;
    if ( ! isspace( *s ) )
      break;
  }
  switch ( tolower( s[ 0 ] ) ) {
    case 'm': if ( match( s, "ms", "msec", "millisec", "millisecond", 0 ) )
                return NANOS / 1000;
              if ( match( s, "m", "min", "minute", 0 ) )
                return MIN_NS;
              if ( match( s, "microsec", "microsecond", 0 ) )
                return NANOS / 1000 / 1000;
              if ( match( s, "month", 0 ) )
                return MONTH_NS;
              break;
    case 'w': if ( match( s, "w", "wk", "week", 0 ) )
                return WEEK_NS;
              break;
    case 'd': if ( match( s, "d", "day", 0 ) )
                return DAY_NS;
              break;
    case 's': if ( match( s, "s", "sec", "second", 0 ) )
                return NANOS;
              break;
    case 'h': if ( match( s, "h", "hr", "hour", 0 ) )
                return HOUR_NS;
              break;
    case 'n': if ( match( s, "ns", "nsec", "nanosec", "nanosecond", 0 ) )
                return 1;
              break;
    case 'u': if ( match( s, "us", "usec", 0 ) )
                return NANOS / 1000 / 1000;
              break;
  }
  return 0;
}

static bool
expires_token( const char *key,  uint64_t &exp_ns )
{
  uint64_t val = 0, ns;
  uint32_t j = 0, len = ::strlen( key );

  if ( key[ j++ ] != '+' )
    return false;
  for ( ; j < len; j++ ) {
    if ( key[ j ] < '0' || key[ j ] > '9' )
      break;
    val = val * 10 + ( key[ j ] - '0' );
  }
  if ( val > 0 ) {
    if ( (ns = time_unit( &key[ j ] )) != 0 ) {
      exp_ns = current_realtime_ns() + ( val * ns );
      return true;
    }
  }
  return false;
}

static char *
sprintf_time( uint64_t ns,  char *buf,  size_t sz )
{
  char *p = buf;
  uint32_t secs = (uint32_t) ( ns / NANOS );
  size_t i = 0;
  int n;
  buf[ 0 ] = '\0';
  buf[ --sz ] = '\0';
  if ( secs == 0 ) {
    uint32_t nano = (uint32_t) ( ns % NANOS );
    if ( nano == 0 ) {
      if ( sz >= 2 ) ::strcpy( buf, "0s" );
    }
    else {
      if ( nano >= 1000 * 1000 ) {
        n = ::snprintf( buf+i, sz-i, "%ums ", nano / ( 1000 * 1000 ) );
        nano %= 1000 * 1000; if ( (i += n) >= sz ) goto truncated;
      }
      if ( nano >= 1000 ) {
        n = ::snprintf( buf+i, sz-i, "%uus ", nano / 1000 );
        nano %= 1000; if ( (i += n) >= sz ) goto truncated;
      }
      if ( nano > 0 )
        ::snprintf( buf+i, sz-i, "%uns", nano );
    }
  }
  else {
    if ( secs >= 24 * 60 * 60 ) {
      n = ::snprintf( buf+i, sz-i, "%ud ", secs / ( 24 * 60 * 60 ) );
      secs %= 24 * 60 * 60; if ( (i += n) >= sz ) goto truncated;
    }
    if ( secs >= 60 * 60 ) {
      n = ::snprintf( buf+i, sz-i, "%uh ", secs / ( 60 * 60 ) );
      secs %= 60 * 60; if ( (i += n) >= sz ) goto truncated;
    }
    if ( secs >= 60 ) {
      n = ::snprintf( buf+i, sz-i, "%um ", secs / 60 );
      secs %= 60; if ( (i += n) >= sz ) goto truncated;
    }
    if ( secs > 0 )
      ::snprintf( buf+i, sz-i, "%us", secs );
  }
truncated:;
  return p;
}

static void
sprintf_rela_time( uint64_t ns,  uint64_t cur,  const char *what,
                   char *buf,  size_t sz )
{
  size_t i;
  for ( i = 0; (buf[ i ] = *what) != '\0'; i++ )
    what++;
  buf[ i++ ] = '=';
  if ( cur > ns ) {
    cur -= ns;
    buf[ i++ ] = '-';
  }
  else {
    cur = ns - cur;
    buf[ i++ ] = '+';
  }
  sprintf_time( cur, &buf[ i ], sz - i );
}

static void
sprintf_stamps( KeyCtx &kctx,  char *upd,  char *exp )
{
  uint64_t exp_ns, upd_ns, cur = 0;
  upd[ 0 ] = '\0';
  exp[ 0 ] = '\0';
  if ( kctx.get_stamps( exp_ns, upd_ns ) == KEY_OK ) {
    if ( upd_ns != 0 ) {
      if ( cur == 0 ) cur = current_realtime_ns();
      sprintf_rela_time( upd_ns, cur, ":upd", upd, 64 );
    }
    if ( exp_ns != 0 ) {
      if ( cur == 0 ) cur = current_realtime_ns();
      sprintf_rela_time( exp_ns, cur, ":exp", exp, 64 );
    }
  }
}

static KeyStatus
print_key_data( KeyCtx &kctx,  const char *what,  uint64_t sz )
{
  char fl[ 128 ], upd[ 64 ], exp[ 64 ];

  sprintf_stamps( kctx, upd, exp );
  if ( kctx.msg != NULL ) {
    ValueGeom &geom = kctx.geom;
    xprintf( 0, "[%lu] [h=0x%08lx:%08lx:chn=%lu:cnt=%lu:inc=%u:sz=%lu%s%s] "
            "(%s seg=%u:sz=%lu:off=%lu:cnt=%lu) %s\n",
            kctx.pos, kctx.key, kctx.key2, kctx.chains, kctx.serial -
            ( kctx.key & ValueCtr::SERIAL_MASK ), kctx.inc, sz, upd, exp,
            flags_string( kctx.entry->flags, kctx.get_type(), fl ),
            geom.segment, geom.size, geom.offset,
            geom.serial - ( kctx.key & ValueCtr::SERIAL_MASK ), what );
  }
  else {
    xprintf( 0, "[%lu] [h=%08lx:%08lx:chn=%lu:cnt=%lu:inc=%u:sz=%lu%s%s] (%s) %s\n",
            kctx.pos, kctx.key, kctx.key2, kctx.chains, kctx.serial -
            ( kctx.key & ValueCtr::SERIAL_MASK ), kctx.inc, sz, upd, exp,
            flags_string( kctx.entry->flags, kctx.get_type(), fl ), what );
  }
  return KEY_OK;
}

static void
print_rdtsc( uint64_t t1, uint64_t t2, uint64_t t3,
             const char *s1,  const char *s2 )
{
  xprintf( 0, "%lu [%s] %lu [%s]\n", t2 - t1, s1, t3 - t2, s2 );
}

static void
print_rdtsc( uint64_t t1, uint64_t t2, uint64_t t3, uint64_t t4,
             const char *s1,  const char *s2,  const char *s3 )
{
  xprintf( 0, "%lu [%s] %lu [%s] %lu [%s]\n",
           t2 - t1, s1, t3 - t2, s2, t4 - t3, s3 );
}

static void
validate_ht( HashTab *map )
{
  KeyBuf        kb, kb2;
  KeyFragment * kp;
  KeyCtx        kctx( *map, ctx_id, &kb ),
                kctx2( *map, ctx_id, &kb2 );
  WorkAlloc8k   wrk, wrk2;
  uint64_t      h1, h2;
  char          buf[ 1024 ];
  KeyStatus     status;

  for ( uint64_t pos = 0; pos < map->hdr.ht_size; pos++ ) {
    status = kctx.fetch( &wrk, pos );
    if ( status == KEY_OK && kctx.entry->test( FL_DROPPED ) == 0 ) {
      status = kctx.get_key( kp );
      uint64_t natural_pos, pos_off = 0;
      kctx.get_pos_info( natural_pos, pos_off );
      if ( status == KEY_OK )
        kb2 = *kp;
      if ( kctx.cuckoo_arity > 1 &&
           pos_off / kctx.cuckoo_buckets != kctx.inc ) {
        if ( status == KEY_OK )
          get_key_string( kb2, buf );
        else
          ::strcpy( buf, "key_part" );
        print_status( "find-buckets", buf, KEY_MAX_CHAINS );
        //kctx.get_pos_info( natural_pos, pos_off );
        xprintf( 0, "pos_off %lu natural_pos %lu\n", pos_off, natural_pos );
        print_key_data( kctx, "", 0 );
      }
      if ( status == KEY_OK ) {
        h1 = map->hdr.hash_key_seed;
        h2 = map->hdr.hash_key_seed2;
        kb2.hash( h1, h2 );
        kctx2.set_hash( h1, h2 );
      }
      else {
        kctx2.set_hash( kctx.key, kctx.key2 );
      }
      if ( (status = kctx2.find( &wrk2 )) != KEY_OK ||
           kctx2.pos != pos ) {
        if ( status == KEY_OK )
          get_key_string( kb2, buf );
        else
          ::strcpy( buf, "key_part" );
        print_status( "find-position", buf, status );
        xprintf( 0, "pos %lu != kctx.pos %lu\n", pos, kctx2.pos );
        xprintf( 0, "pos_off %lu natural_pos %lu\n", pos_off, natural_pos );
        print_key_data( kctx, "", 0 );
      }
    }
  }
}

static void
cli( void )
{
  static const uint32_t MAX_OFF = 256;
  struct KeyListData {
    uint64_t pos, jump;
    uint64_t seg_values, immed_values, no_value,
             key_count, drops, moves, busy, err_count;
    uint64_t hit[ MAX_OFF + 1 ], max_hit;
    void zero( void ) { ::memset( this, 0, sizeof( *this ) ); }
  } kld;
  KeyBuf        kb;
  KeyCtx        kctx( *map, ctx_id, &kb );
  WorkAlloc8k   wrk;
  char          buf[ 16 * 1024 ], cmd[ 16 ], key[ 8192 ], *data;
  char          fl[ 128 ], upd[ 64 ], exp[ 64 ], xbuf[ 32 ], tmp = 0;
  void        * ptr;
  FILE        * fp,
              * infp = stdin;
  uint64_t      exp_ns;
  uint64_t      sz, h, t1, t2, t3, t4, h1, h2;
  double        f;
  uint32_t      cnt, i, j, print_count;
  char          cmd_char, last_cmd = 0;
  KeyStatus     status;
  bool          inquiet = false, do_validate = false;

  ::memset( cmd, 0, sizeof( cmd ) );
  ::memset( key, 0, sizeof( key ) );
  kld.zero();

  if ( ! quiet ) {
    printf( "> " );
    fflush( stdout );
  }
  for (;;) {
    char *p, *q, *r;
    while ( fgets( buf, sizeof( buf ), infp ) == NULL ) {
      if ( infp == stdin )
        goto break_loop;
      fclose( infp );
      infp = stdin;
      if ( quiet && ! inquiet ) {
        printf( "> " );
        fflush( stdout );
      }
      quiet = inquiet;
    }
    if ( infp != stdin && ! quiet ) {
      fputs( buf, stdout );
    }
    cmd[ 0 ] = '\0'; key[ 0 ] = '\0'; data = &tmp; sz = 0;
    exp_ns = 0;
    if ( eat_token( buf, p, cmd, sizeof( cmd ) ) ) {
      if ( eat_token( p, q, key, sizeof( key ) ) ) {
        while ( *q == ' ' ) q++;
        if ( expires_token( key, exp_ns ) ) {
          if ( eat_token( q, r, key, sizeof( key ) ) ) {
            for ( q = r; *q == ' '; q++ )
              ;
          }
        }
        if ( *q > ' ' ) {
          data = q;
          sz = ::strlen( data );
          while ( sz > 0 && data[ sz - 1 ] <= ' ' )
            data[ --sz ] = '\0';
        }
      }
    }
    status = KEY_OK;
    if ( cmd[ 0 ] == '\0' )
      cmd_char = last_cmd;
    else
      cmd_char = last_cmd = cmd[ 0 ];
    switch ( cmd_char ) {
      case 'c': /* contexts */
        for ( uint32_t c = 0; c < MAX_CTX_ID; c++ ) {
          if ( c == 0 || map->ctx[ c ].ctx_pid != 0 )
            print_stats( c );
        }
        break;

      case 'd': /* drop */
      case 'p': /* put */
      case 't': /* tombstone */
      case 'u': /* update stamp */
        parse_key_string( kb, key );
        h1 = map->hdr.hash_key_seed;
        h2 = map->hdr.hash_key_seed2;
        kb.hash( h1, h2 );
        kctx.set_hash( h1, h2 );

        if ( (status = kctx.acquire( &wrk )) <= KEY_IS_NEW ) {
          if ( exp_ns != 0 )
            status = kctx.update_stamps( exp_ns, 0 );
          else
            status = kctx.clear_stamps( true, true );
          if ( status != KEY_OK )
            xprintf( 0, "stamps err: %d/%s\n", status,
                     kv_key_status_description( status ) );
          /*kctx.update_ns = current_realtime_ns();*/
          /*map->hdr.current_stamp = kctx.update_ns;*/
          if ( cmd_char == 'p' ) {
            if ( (status = kctx.resize( &ptr, sz + 1 )) == KEY_OK ) {
              ::memcpy( ptr, data, sz + 1 );
              status = print_key_data( kctx, "put", sz + 1 );
            }
          }
          else if ( cmd_char == 't' ) {
            const char *s = ( cmd_char == 'd' ? "drop" : "tomb" );
            if ( (status = print_key_data( kctx, s, 0 )) == KEY_OK ) {
              /*if ( cmd_char == 'd' )
                status = kctx.drop();
              else*/
                status = kctx.tombstone();
            }
          }
          kctx.release();
        }
        last_cmd = 0;
        if ( do_validate ) {
          validate_ht( map );
        }
        break;

      case 'a': /* acquire */
        parse_key_string( kb, key );
        h1 = map->hdr.hash_key_seed;
        h2 = map->hdr.hash_key_seed2;
        kb.hash( h1, h2 );
        kctx.set_hash( h1, h2 );

        if ( (status = kctx.acquire( &wrk )) <= KEY_IS_NEW ) {
          if ( (status = kctx.resize( &ptr, 2 )) == KEY_OK ) {
            ((uint8_t *) ptr)[ 0 ] = 'X'; ((uint8_t *) ptr)[ 1 ] = '\0';
            status = print_key_data( kctx, "acquire", 2 );
          }
        }
        break;

      case 'A': /* release acquire */
        status = print_key_data( kctx, "release", 2 );
        kctx.release();
        break;

      case 'h': /* hex */
      case 'i': /* int */
      case 'g': /* get */
      case 'f': { /* fetch */
        bool do_int = false, do_get = false;
        char action = cmd_char;
        parse_key_string( kb, key );
        if ( cmd_char == 'f' )
          action = cmd[ 1 ];
        switch ( action ) {
          case 'h': break;
          case 'i': do_int = true; break;
          default:
          case 'g': do_get = true; break;
        }
        t1 = get_rdtscp();
        h1 = map->hdr.hash_key_seed;
        h2 = map->hdr.hash_key_seed2;
        kb.hash( h1, h2 );
        kctx.set_hash( h1, h2 );
        t2 = get_rdtscp();
        cnt = 0;
        do {
          if ( cmd_char != 'f' )
            status = kctx.find( &wrk );
          else {
            uint64_t pos;
            ::memcpy( &pos, kb.u.buf, sizeof( pos ) );
            status = kctx.fetch( &wrk, pos );
          }
          if ( status == KEY_OK ) {
            t3 = get_rdtscp();
            if ( (status = kctx.value( &data, sz )) == KEY_OK ) {
              t4 = get_rdtscp();
              print_rdtsc( t1, t2, t3, t4, "hash", "find", "value" );
              bool not_printed = false;
              status = print_key_data( kctx, "get", sz );
              if ( do_int ) {
                if ( sz == 8 )
                  xprintf( 0, "->%lu\n", *(uint64_t *) (void *) data );
                else if ( sz == 4 )
                  xprintf( 0, "->%u\n", *(uint32_t *) (void *) data );
                else if ( sz == 2 )
                  xprintf( 0, "->%u\n", *(uint16_t *) (void *) data );
                else if ( sz == 1 )
                  xprintf( 0, "->%u\n", *(uint8_t *) (void *) data );
                else
                  not_printed = true;
              }
              else if ( do_get && sz < 16 * 1024 )
                xprintf( 0, "->\"%.*s\"\n", (int) sz, (char *) data );
              else
                not_printed = true;
              if ( not_printed ) {
                xprintf( 0, "->\n" );
                dump_hex( data, sz );
              }
            }
          }
          if ( status == KEY_NOT_FOUND ) {
            t3 = get_rdtscp();
            print_rdtsc( t1, t2, t3, "hash", "find" );
            xprintf( 0, "[%lu] [h=%lx]: \n",
                    kctx.pos, kctx.key );
            print_status( cmd, key, status );
            status = KEY_OK;
          }
        } while ( status == KEY_MUTATED && ++cnt < 100 );
        break;
      }
      case 'j': /* jump */
      case 'k': /* keys */
        if ( kld.pos == map->hdr.ht_size ) {
      case 'K': /* keys start at top */
          kld.zero();
        }
        if ( cmd_char == 'j' ) {
          uint64_t j = strtol( key, NULL, 0 );
          if ( j < kld.pos ) {
            if ( j == 0 ) {
              if ( key[ 0 ] == '\0' )
                j = map->hdr.ht_size - 24;
              else if ( ::strcmp( key, "0" ) != 0 ) {
                parse_key_string( kb, key );
                h1 = map->hdr.hash_key_seed;
                h2 = map->hdr.hash_key_seed2;
                kb.hash( h1, h2 );
                j = map->hdr.ht_mod( h1 );
              }
            }
            if ( j == 0 )
              kld.zero();
          }
          kld.jump = j;
          last_cmd = 'k';
        }
        for ( print_count = 0;
              kld.pos < map->hdr.ht_size && print_count < 25; ) {
          status = kctx.fetch( &wrk, kld.pos );
          if ( status == KEY_OK ) {
            KeyFragment *kp;
            status = kctx.get_key( kp );
            if ( status == KEY_OK || status == KEY_TOMBSTONE ||
                 status == KEY_PART_ONLY ) {
              uint64_t natural_pos, pos_off = 0;
              if ( kctx.key != DROPPED_HASH ) {
                kctx.get_pos_info( natural_pos, pos_off );
                if ( pos_off < MAX_OFF )
                  kld.hit[ pos_off ]++;
                else {
                  kld.hit[ MAX_OFF ]++;
                  if ( pos_off > kld.max_hit )
                    kld.max_hit = pos_off;
                }
              }
              else {
                pos_off = 0;
              }
              if ( kctx.entry->test( FL_DROPPED ) )
                kld.drops++;
              if ( kctx.entry->test( FL_MOVED ) )
                kld.moves++;
              if ( kctx.entry->test( FL_BUSY ) )
                kld.busy++;
              if ( kctx.entry->test( FL_SEGMENT_VALUE ) ) {
                if ( kld.pos >= kld.jump ) {
                  ValueGeom geom;
                  kctx.entry->get_value_geom( kctx.hash_entry_size, geom,
                                              map->hdr.seg_align_shift );
                  if ( kp != NULL )
                    get_key_string( *kp, key );
                  else
                    ::strcpy( key, kv_key_status_string( status ) );
                  sprintf_stamps( kctx, upd, exp );
                  xprintf( 0,
                           "[%lu]+%u.%lu %s (%s seg=%u:sz=%lu:off=%lu%s%s)\n",
                           kld.pos, kctx.inc, pos_off, key,
                           flags_string( kctx.entry->flags, kctx.get_type(), fl ),
                           geom.segment, geom.size, geom.offset, upd, exp );
                  print_count++;
                }
                kld.seg_values++;
              }
              else {
                if ( kctx.entry->test( FL_IMMEDIATE_VALUE ) )
                  kld.immed_values++;
                else
                  kld.no_value++;
                if ( kld.pos >= kld.jump ) {
                  if ( kp != NULL )
                    get_key_string( *kp, key );
                  else
                    ::strcpy( key, kv_key_status_string( status ) );
                  sprintf_stamps( kctx, upd, exp );
                  xprintf( 0, "[%lu]+%u.%lu %s (%s %s%s)\n",
                           kld.pos, kctx.inc, pos_off, key,
                      flags_string( kctx.entry->flags, kctx.get_type(), fl ),
                      upd, exp );
                  print_count++;
                }
              }
              kld.key_count++;
              status = KEY_OK;
            }
          }
          if ( kld.pos >= kld.jump && status > KEY_NOT_FOUND ) {
            xbuf[ 0 ] = '@';
            uint64_to_string( kld.pos, &xbuf[ 1 ] );
            print_status( "keys", xbuf, status );
            kld.err_count++;
            print_count++;
          }
          kld.pos++;
        }
        xprintf( 0, "%lu keys, (%lu segment, %lu immediate, %lu none, "
                 "%lu drops %lu moves %lu busy)\n",
                 kld.key_count, kld.seg_values,
                 kld.immed_values, kld.no_value, kld.drops,
                 kld.moves, kld.busy );
        if ( kld.err_count > 0 )
          xprintf( 0, "%lu errors\n", kld.err_count );
        h = kld.hit[ 0 ];
        f = (double) h * 100.0 / (double) kld.key_count;
        j = 0;
        xprintf( 0, "0:%lu(%.1f)", h, f );
        for ( i = 1; i < MAX_OFF; i++ ) {
          if ( kld.hit[ i ] > 0 ) {
            j = i;
            xprintf( 0, ", %u:%lu", i, kld.hit[ i ] );
            if ( f <= 99.9 ) {
              h += kld.hit[ i ];
              f = (double) h * 100.0 / (double) kld.key_count;
              if ( f <= 99.9 )
                xprintf( 0, "(%.1f)", f );
            }
          }
        }
        if ( kld.hit[ i ] > 0 ) {
          j = i;
          xprintf( 0, ", >=%u:%lu", i, kld.hit[ i ] );
          if ( f < 99.9 ) {
            h += kld.hit[ i ];
            f = (double) h * 100.0 / (double) kld.key_count;
            if ( f <= 99.9 )
              xprintf( 0, "(%.1f)", f );
          }
        }
        xprintf( 0, " max=%lu\n", kld.max_hit == 0 ? j : kld.max_hit );
        if ( kld.pos < map->hdr.ht_size ) {
          xprintf( 0, "pos %lu of %lu (more)\n",
                  kld.pos, map->hdr.ht_size );
        }
        status = KEY_OK;
        break;

      case 'm': /* mem */
        for ( uint32_t i = 0; i < map->hdr.nsegs; i++ )
          print_mem( i );
        break;

      case 'r': /* read file */
      case 'R':
        fp = fopen( key, "r" );
        if ( fp == NULL )
          perror( key );
        else {
          infp    = fp;
          inquiet = quiet;
          if ( ! quiet )
            quiet = ( cmd_char == 'R' );
        }
        break;

      case 's': /* stats */
        print_map_geom( map, ctx_id );
        print_stats();
        break;

      case 'v': /* validate */
        validate_ht( map );
        do_validate = ! do_validate;
        xprintf( 0, "do_validate = %s\n", do_validate ? "true" : "false" );
        break;

      case 'x': /* examine segment */
        print_seg( atoi( key ) );
        break;

      case 'z': /* play dead */
        xprintf( 0, "suspend ctx_id %u, pid %u\n", ctx_id, my_pid );
        map->ctx[ ctx_id ].ctx_pid = no_pid;
        break;

      case 'Z': /* unplay dead */
        xprintf( 0, "unsuspend ctx_id %u, pid %u\n", ctx_id, my_pid );
        map->ctx[ ctx_id ].ctx_pid = my_pid;
        break;

      case 'y': /* fix locks */
        xprintf( 0, "fix_locks\n" );
        fix_locks();
        break;

      case 'q': /* quit */
        goto break_loop;

      default:
        xprintf( 0,
        "acdfghijkmpqrstuvx:\n"
        "put [+exp] key value  ; set key to value w/optional +expires\n"
        "upd [+exp] key        ; update key +expires\n"
        "get key               ; print key value as string\n"
        "hex key               ; print hex dump of key value\n"
        "int key               ; print int value of key (sz 2^N)\n"
        "acquire key           ; acquire key and 'A key' to release\n"
        "f{get,int,hex} pos    ; print int value of ht[ pos ]\n"
        "drop key              ; drop key\n"
        "tomb key              ; tombstone key\n"
        "keys [pat]            ; list keys matching (K to start over)\n"
        "jump position         ; jump to position and list keys\n"
        "mem                   ; print segment offsets\n"
        "validate              ; validate all keys are reachable\n"
        "xamin seg#            ; dump segment data\n"
        "stats                 ; print stats\n"
        "contexts              ; print stats for all contexts\n"
        "y                     ; scan for broken locks\n"
        "z                     ; suspend pid (Z to unsuspend)\n"
        "read file             ; read input from file (R to read quietly)\n"
        "quit                  ; bye\n" );
        break;
    }
    if ( status != KEY_OK )
      print_status( cmd, key, status );
    if ( ! quiet ) {
      printf( "> " );
      fflush( stdout );
    }
  }
break_loop:;
  xprintf( 0, "bye\n" );
}

int
main( int argc, char *argv[] )
{
/*  if ( argc < 2 ) {
    fprintf( stderr, "%s map [key1 key2 ...]\n", argv[ 0 ] );
    return 1;
  }*/

  shm_attach( argc < 2 ? "sysv2m:shm.test" : argv[ 1 ] );
  if ( map == NULL )
    return 2;

  if ( argc > 2 ) {
    for ( int i = 2; i < argc; i++ )
      dump_value( argv[ i ] );
  }
  else {
    cli();
  }
  shm_close();
  return 0;
}
