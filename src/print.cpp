#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#define __STDC_FORMAT_MACROS
#include <stdint.h>
#include <stdarg.h>
#include <inttypes.h>
#include <raikv/shm_ht.h>
#include <raikv/util.h>

using namespace rai;
using namespace kv;

#ifndef _MSC_VER
#define gcc_printf_format __attribute__((format(printf,3,4)))
#else
#define gcc_printf_format
#endif
static size_t xnprintf( char *&b,  size_t &sz,  const char *format, ... ) gcc_printf_format;

static size_t
xnprintf( char *&b,  size_t &sz,  const char *format, ... )
{
  va_list args;
  size_t  x;
  if ( sz == 0 ) return 0;
  va_start( args, format );
  x = vsnprintf( b, sz, format, args );
  va_end( args );
  if ( x >= sz )
    x = sz - 1;
  b   = &b[ x ];
  sz -= x;
  return x;
}

const char *
rai::kv::get_kv_version( void ) noexcept
{
  return kv_stringify( KV_VER );
}

const char *
kv_get_version( void )
{
  return get_kv_version();
}

static char default_buf[ 4 * 1024 ]; /* should be about 3k */
char *
rai::kv::print_map_geom( HashTab *map,  uint32_t ctx_id,  char *buf,
                         size_t buflen ) noexcept
{
  if ( buf == NULL ) {
    buf = default_buf;
    buflen = sizeof( default_buf );
  }
  char *b  = buf;
  size_t sz = buflen;
  xnprintf( b, sz, "kv_version:           %s\n", get_kv_version() );
  xnprintf( b, sz, "map_sig:              %s\n", map->hdr.sig );
  xnprintf( b, sz, "map_name:             %s\n", map->hdr.name );
  xnprintf( b, sz, "map_size:             %" PRIu64 " (%.3fMB) (config)\n",
            map->hdr.map_size,
            (double) map->hdr.map_size / ( 1024.0 * 1024.0 ) );
  char sbuf[ 64 ];
  xnprintf( b, sz, "created:              %s\n",
            timestamp( map->hdr.create_stamp, 0, sbuf, sizeof( sbuf ) ) );
  xnprintf( b, sz, "create_stamp:         0x%" PRIx64 "\n",
            map->hdr.create_stamp );
  xnprintf( b, sz, "max_value_size:       %u (config)\n",
            map->hdr.max_value_size );
  xnprintf( b, sz, "immed_value_size:     %u (calc)\n",
            map->hdr.max_immed_value_size );
  xnprintf( b, sz, "segment_value_size:   %" PRIu64 " (calc)\n",
            map->hdr.max_segment_value_size );
  xnprintf( b, sz, "hash_entry_size:      %u (config)\n",
            map->hdr.hash_entry_size );
  xnprintf( b, sz, "ht_size:              %" PRIu64 " entries (total-size %.3fMB) (calc)\n",
          map->hdr.ht_size,
	  (double) ( map->hdr.ht_size *
		     map->hdr.hash_entry_size ) / ( 1024.0 * 1024.0 ) );
  xnprintf( b, sz, "last_entry_count:     %" PRIu64 "\n", map->hdr.last_entry_count );
  xnprintf( b, sz, "ht_mod:               ( ( hash & 0x%" PRIx64 " ) * 0x%" PRIx64 " ) >> %u (%.9f)\n",
          map->hdr.ht_mod_mask, map->hdr.ht_mod_fraction,
          map->hdr.ht_mod_shift,
          (double) map->hdr.ht_size / (double) map->hdr.ht_mod_mask );
  xnprintf( b, sz, "default_hash:         " KV_DEFAULT_HASH_STR "\n" );
  xnprintf( b, sz, "cuckoo_arity+buckets: %u+%u (config%s)\n",
            map->hdr.cuckoo_arity, map->hdr.cuckoo_buckets,
         ( map->hdr.cuckoo_buckets <= 1 ? " == linear probe" : " == cuckoo" ) );
  xnprintf( b, sz, "seg_size:             %" PRIu64 " (total-size %.3fMB) (calc)\n",
          map->hdr.seg_size(),
	  (double) ( map->hdr.seg_size() *
		     map->hdr.nsegs ) / ( 1024.0 * 1024.0 ) );
  xnprintf( b, sz, "nsegs:                %u (calc)\n", map->hdr.nsegs );
  xnprintf( b, sz, "seg_align:            %" PRIu64 "\n", map->hdr.seg_align() );
  xnprintf( b, sz, "seg_align_shift:      %u\n", map->hdr.seg_align_shift );
  map->update_load();
  xnprintf( b, sz, "current_time:         %s\n",
            timestamp( map->hdr.current_stamp, 3, sbuf, sizeof( sbuf ) ) );
  xnprintf( b, sz, "critical_load:        %u%%\n", map->hdr.critical_load );
  xnprintf( b, sz, "current_load:         %.3f%%\n",
            map->hdr.current_load() * 100.0 );
  xnprintf( b, sz, "ht_load:              %.3f%%\n", map->hdr.ht_load * 100.0 );
  xnprintf( b, sz, "value_load:           %.3f%%\n",
            map->hdr.value_load * 100.0 );
  xnprintf( b, sz, "load_pecent:          %u%%\n", map->hdr.load_percent );
  if ( ctx_id != MAX_CTX_ID )
    xnprintf( b, sz, "ctx_id:               %u (in use %u of %" PRIu64 " max)\n",
	    ctx_id, (uint32_t) (uint16_t) map->hdr.ctx_used, MAX_CTX_ID );
  else
    xnprintf( b, sz, "ctx_id:               (none) (in use %u of %" PRIu64 " max)\n",
	    (uint32_t) (uint16_t) map->hdr.ctx_used, MAX_CTX_ID );
  return buf;
}

extern "C"
char *
kv_print_map_geom( kv_hash_tab_t *map,  uint32_t ctx_id,  char *buf,
                   size_t buflen )
{
  return rai::kv::print_map_geom( reinterpret_cast<HashTab *>( map ), ctx_id,
                                  buf, buflen );
}

