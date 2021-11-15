#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

#include <raikv/radix_sort.h>
#include <raikv/shm_ht.h>

namespace rai {
namespace kv {
struct HtSortCompare {
  HashTab &ht;
  HtSortCompare( HashTab *tab ) : ht( *tab ) {}

  uint64_t key( const kv_ht_sort_t &v ) {
    return this->ht.hdr.ht_mod( v.key );
  }
  bool less( const kv_ht_sort_t &v1,  const kv_ht_sort_t &v2 ) {
    return this->ht.hdr.ht_mod( v1.key ) <
           this->ht.hdr.ht_mod( v2.key );
  }
};

struct HtSort : public RadixSort<kv_ht_sort_t, uint64_t, HtSortCompare> {
  HtSort( HtSortCompare &htcmp )
    : RadixSort<kv_ht_sort_t, uint64_t, HtSortCompare>( htcmp ) {}
};
}
}

extern "C"
void
kv_ht_radix_sort( kv_ht_sort_t *ar,  uint32_t ar_size,  kv_hash_tab_t *tab )
{
  rai::kv::HashTab *ht = reinterpret_cast<rai::kv::HashTab *>( tab );
  rai::kv::HtSortCompare cmp( ht );
  rai::kv::HtSort sort( cmp );

  sort.init( ar, ar_size, ht->hdr.ht_size, false );
  sort.sort();
}
