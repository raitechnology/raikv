#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <raikv/shm_ht.h>

using namespace rai;
using namespace kv;

int
main( void )
{
  HashTabGeom geom;
  HashTab   * map;

  geom.map_size         = sizeof( HashTab ) + 1024;
  geom.max_value_size   = 0;
  geom.hash_entry_size  = 64;
  geom.hash_value_ratio = 1;
  geom.cuckoo_buckets   = 0;
  geom.cuckoo_arity     = 0;

  if ( (map = HashTab::alloc_map( geom )) == NULL )
    return 1;
  map->hdr.ht_read_only = 1;
  fputs( print_map_geom( map, 0 ), stdout );
  return 0;
}

