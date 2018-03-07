#include <string.h>
#include <stdint.h>

#include <raikv/radix_sort.h>

namespace rai {
namespace kv {
struct HtSortCompare {
  uint64_t ht_size;
  HtSortCompare( uint64_t h ) : ht_size( h ) {}

  uint64_t key( const kv_ht_sort_t &v ) {
    return v.key % this->ht_size;
  }
  bool less( const kv_ht_sort_t &v1,  const kv_ht_sort_t &v2 ) {
    return ( v1.key % this->ht_size ) < ( v2.key % this->ht_size );
  }
};

struct HtSort : public RadixSort<kv_ht_sort_t, uint64_t, HtSortCompare> {
  HtSort( HtSortCompare &htcmp ) : RadixSort( htcmp ) {}
};
}
}

extern "C"
void
kv_ht_radix_sort( kv_ht_sort_t *ar,  uint32_t ar_size,  uint64_t ht_size )
{
  rai::kv::HtSortCompare cmp( ht_size );
  rai::kv::HtSort sort( cmp );

  sort.init( ar, ar_size, ht_size, false );
  sort.sort();
}

