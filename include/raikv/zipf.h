#ifndef __rai__raikv__zipf_h__
#define __rai__raikv__zipf_h__

#ifdef __cplusplus
namespace rai {
namespace kv {

struct ZipfianConst {
  double   theta;
  double   alpha;
  double   baseplus1;
  uint64_t item_cnt;
  double   zetan,
           eta;

  static double zeta_static( double theta,  uint64_t n ) {
    double sum = 0;
    for ( uint64_t i = 0; i < n; i++ )
      sum += 1.0 / pow( (double) ( i + 1 ), theta );
    return sum;
  }

  ZipfianConst( int zipf,  int zipd,  uint64_t cnt ) {
    this->theta     = (double) zipf / (double) zipd;
    this->alpha     = 1.0 / ( 1.0 - theta );
    this->baseplus1 = 1.0 + pow( 0.5, theta );
    this->item_cnt  = cnt;

    double zeta2_theta = zeta_static( this->theta, 2 );
    this->zetan = zeta_static( this->theta, cnt );
    this->eta   = ( 1 - pow( 2.0 / (double) cnt, 1 - this->theta ) ) /
                  ( 1 - zeta2_theta / this->zetan );
  }
};

struct ZipfianBuf {
  const ZipfianConst & zconst;
  static const size_t VAL_BUFSIZE = 16;
  uint64_t next_val[ VAL_BUFSIZE ];
  size_t   next_cnt;

  ZipfianBuf( const ZipfianConst &c ) : zconst( c ), next_cnt( 0 ) {}

  void gen( const double *rvals ) {
    do {
      double u  = *rvals++;
      double uz = u * this->zconst.zetan;
      uint64_t v;
      if ( uz < 1.0 )
        v = 0;
      else if ( uz < this->zconst.baseplus1 )
        v = 1;
      else
        v = (uint64_t) ( this->zconst.item_cnt *
          pow( this->zconst.eta * u - this->zconst.eta + 1, this->zconst.alpha ) );
      this->next_val[ this->next_cnt++ ] = v;
    } while ( this->next_cnt < VAL_BUFSIZE );
  }
};

/* more or less a port of ycsb/generator/ZipfianGenerator.java */
template <int zipf, int zipd, class RNG> /* zip const = zipf / zipd */
struct ZipfianGen {
  const ZipfianConst zconst;
  RNG        & r;
  ZipfianBuf   zip;

  ZipfianGen( uint64_t cnt, RNG & rng )
      : zconst( zipf, zipd, cnt ), r( rng ), zip( this->zconst ) {}

  uint64_t next( void ) {
    for (;;) {
      if ( this->zip.next_cnt > 0 )
        return this->zip.next_val[ --this->zip.next_cnt ];
      double rvals[ ZipfianBuf::VAL_BUFSIZE ];
      for ( size_t i = 0; i < ZipfianBuf::VAL_BUFSIZE; i++ )
        rvals[ i ] = this->r.next_double();
      this->zip.gen( rvals );
    }
  }
};

} /* namespace kv */
} /* namespace rai */
#endif

#endif
