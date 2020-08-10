#ifndef __rai__raikv__zipf_h__
#define __rai__raikv__zipf_h__

#ifdef __cplusplus
namespace rai {
namespace kv {

/* more or less a port of ycsb/generator/ZipfianGenerator.java */
template <int zipf, int zipd, class RNG> /* zip const = zipf / zipd */
struct ZipfianGen {
  const double theta     = (double) zipf / (double) zipd;
  const double alpha     = 1.0 / ( 1.0 - theta );
  const double baseplus1 = 1.0 + pow( 0.5, theta );

  uint64_t next_val[ 16 ];
  size_t   next_cnt;
  RNG    & r;
  uint64_t item_cnt;
  double   zetan,
           eta;

  ZipfianGen( uint64_t cnt, RNG & rng )
      : next_cnt( 0 ), r( rng ), item_cnt( cnt ) {
    double zeta2_theta = zeta_static( 2 );
    this->zetan = zeta_static( cnt );
    this->eta   = ( 1 - pow( 2.0 / (double) cnt, 1 - theta ) ) /
                  ( 1 - zeta2_theta / this->zetan );
  }

  double zeta_static( uint64_t n ) {
    double sum = 0;
    for ( uint64_t i = 0; i < n; i++ )
      sum += 1.0 / pow( i + 1, theta );
    return sum;
  }

  uint64_t next( void ) {
    if ( this->next_cnt == 0 ) {
      do {
        double u  = this->r.next_double();
        double uz = u * this->zetan;
        uint64_t v;
        if ( uz < 1.0 )
          v = 0;
        else if ( uz < baseplus1 )
          v = 1;
        else
          v = (uint64_t) ( this->item_cnt *
                 pow( this->eta * u - this->eta + 1, alpha ) );
        this->next_val[ this->next_cnt++ ] = v;
      } while ( this->next_cnt < sizeof( this->next_val ) /
                                 sizeof( this->next_val[ 0 ] ) );
    }
    return this->next_val[ --this->next_cnt ];
  }
};

} /* namespace kv */
} /* namespace rai */
#endif

#endif
