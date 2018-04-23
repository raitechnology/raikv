#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <raikv/shm_ht.h>

using namespace rai;
using namespace kv;

void
print_map_geom( HashTab *map,  uint32_t ctx_id )
{
  char buf[ 64 ];
  printf( "version:              %s\n", map->hdr.sig );
  printf( "map_name:             %s\n", map->hdr.name );
  printf( "map_size:             %lu (%.3fMB) (config)\n", map->hdr.map_size,
    (double) map->hdr.map_size / ( 1024.0 * 1024.0 ) );
  printf( "created:              %s\n", timestamp( map->hdr.create_stamp,
                                                 0, buf, sizeof( buf ) ) );
  printf( "max_value_size:       %u (config)\n", map->hdr.max_value_size );
  printf( "immed_value_size:     %u (calc)\n", map->hdr.max_immed_value_size );
  printf( "segment_value_size:   %lu (calc)\n",
          map->hdr.max_segment_value_size );
  printf( "hash_entry_size:      %u (config)\n", map->hdr.hash_entry_size );
  printf( "ht_size:              %lu entries (total-size %.3fMB) (calc)\n",
          map->hdr.ht_size,
	  (double) ( map->hdr.ht_size *
		     map->hdr.hash_entry_size ) / ( 1024.0 * 1024.0 ) );
  printf( "last_entry_count:     %lu\n", map->hdr.last_entry_count );
  printf( "ht_mod:               ( ( hash & 0x%lx ) * 0x%lx ) >> %u (* %.9f)\n",
          map->hdr.ht_mod_mask, map->hdr.ht_mod_fraction, map->hdr.ht_mod_shift,
          (double) map->hdr.ht_size / (double) map->hdr.ht_mod_mask );
  printf( "default_hash:         " KV_DEFAULT_HASH_STR "\n" );
  printf( "cuckoo_arity+buckets: %u+%u (config%s)\n", map->hdr.cuckoo_arity,
          map->hdr.cuckoo_buckets,
         ( map->hdr.cuckoo_buckets <= 1 ? " == linear probe" : " == cuckoo" ) );
  printf( "seg_size:             %lu (total-size %.3fMB) (calc)\n",
          map->hdr.seg_size(),
	  (double) ( map->hdr.seg_size() *
		     map->hdr.nsegs ) / ( 1024.0 * 1024.0 ) );
  printf( "nsegs:                %u (calc)\n", map->hdr.nsegs );
  printf( "seg_align:            %lu\n", map->hdr.seg_align() );
  printf( "seg_align_shift:      %u\n", map->hdr.seg_align_shift );
  fflush( stdout );
  map->update_load();
  printf( "current_time:         %s\n", timestamp( map->hdr.current_stamp,
                                                 3, buf, sizeof( buf ) ) );
  printf( "critical_load:        %u%%\n", map->hdr.critical_load );
  printf( "current_load:         %.3f%%\n", map->hdr.current_load() * 100.0 );
  printf( "ht_load:              %.3f%%\n", map->hdr.ht_load * 100.0 );
  printf( "value_load:           %.3f%%\n", map->hdr.value_load * 100.0 );
  printf( "load_pecent:          %u%%\n", map->hdr.load_percent );
  if ( ctx_id != MAX_CTX_ID )
    printf( "ctx_id:               %u (in use %u of %lu max)\n",
	    ctx_id, (uint32_t) (uint16_t) map->hdr.ctx_used, MAX_CTX_ID );
  else
    printf( "ctx_id:               (none) (in use %u of %lu max)\n",
	    (uint32_t) (uint16_t) map->hdr.ctx_used, MAX_CTX_ID );
  fflush( stdout );
}

extern "C"
void
print_map_geom_c( kv_hash_tab_t *map,  uint32_t ctx_id )
{
  print_map_geom( reinterpret_cast<HashTab *>( map ), ctx_id );
}

