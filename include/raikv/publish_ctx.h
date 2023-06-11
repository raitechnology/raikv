#ifndef __rai_raikv__publish_ctx_h__
#define __rai_raikv__publish_ctx_h__

#include <raikv/route_db.h>
#include <raikv/ev_publish.h>

namespace rai {
namespace kv {

template <class Route>
struct RoutePublishDataT {
  uint16_t prefix; /* prefix size (how much of subject matches) */
  uint32_t rcount, /* count of routes (fd)) */
           hash;   /* hash of prefix (is subject hash if prefix=64) */
  Route  * routes; /* routes for this hash */

  void set( uint16_t pref,  uint32_t rcnt,  uint32_t h,  Route *r ) {
    this->prefix = pref;
    this->rcount = rcnt;
    this->hash   = h;
    this->routes = r;
  }
  bool is_member( uint32_t r ) const {
    uint32_t j = bsearch_route( r, this->routes, this->rcount );
    return j < this->rcount && this->routes[ j ] == r;
  }
};

typedef RoutePublishDataT<uint32_t> RoutePublishData;
typedef RoutePublishDataT<QueueRef> RouteQueueData;

static inline uint32_t route_fd( uint32_t fd ) noexcept { return fd; }
static inline uint32_t route_fd( QueueRef &ref ) noexcept { return ref.r; }

template <class Data, class Elem>
struct RoutePublishSetT {
  RouteGroup  & rdb;
  RouteLookup & look;
  uint32_t      n, min_fd, max_fd;
  uint64_t      rpd_mask;
  Data          rpd[ MAX_RTE ];

  RoutePublishSetT( RouteGroup &db,  RouteLookup &l ) : rdb( db ), look( l ) {}

  void init( void ) {
    this->n = 0; this->rpd_mask = 0;
    this->rpd[ 0 ].rcount = 0;
  }

  void add( uint16_t pref,  uint32_t rcnt,  uint32_t h,  Elem *r,
            uint64_t mask ) {
    Data & data = this->rpd[ mask == 0 ? 0 : pref + 1 ];
    if ( this->n == 0 ) {
      this->min_fd = route_fd( r[ 0 ] );
      this->max_fd = route_fd( r[ rcnt - 1 ] );
      this->rpd_mask |= mask;
      this->n = 1;
      data.set( pref, rcnt, h, r );
      return;
    }
    this->min_fd = min_int( this->min_fd, route_fd( r[ 0 ] ) );
    this->max_fd = max_int( this->max_fd, route_fd( r[ rcnt - 1 ] ) );
    if ( mask == 0 ) {
      if ( data.rcount == 0 ) {
        data.set( SUB_RTE, rcnt, h, r );
        return;
      }
    }
    else if ( ( this->rpd_mask & mask ) == 0 ) {
      this->rpd_mask |= mask;
      data.set( pref, rcnt, h, r );
      return;
    }
    this->merge( data, pref, rcnt, r );
  }

  void merge( Data &data,  uint16_t pref,  uint32_t rcnt,  Elem *r ) {
    RouteRef ref( this->rdb.zip, pref + 48 );
    Elem * routes = (Elem *) ref.route_spc.make( ( data.rcount + rcnt ) *
                                      ( sizeof( Elem ) / sizeof( uint32_t ) ) );
    data.rcount = merge_route2( routes, data.routes, data.rcount, r, rcnt );
    data.routes = routes;
    this->look.add_ref( ref );
  }

  void add( RoutePublishSetT &qset ) {
    if ( qset.rpd[ 0 ].rcount != 0 )
      this->add( SUB_RTE, qset.rpd[ 0 ].rcount, qset.rpd[ 0 ].hash,
                 qset.rpd[ 0 ].routes, 0 );
    if ( qset.rpd_mask != 0 ) {
      BitSet64 bi( qset.rpd_mask );
      uint32_t pref;
      for ( bool b = bi.first( pref ); b; b = bi.next( pref ) ) {
        uint64_t mask = (uint64_t) 1 << pref;
        RoutePublishData & data = qset.rpd[ pref + 1 ];
        this->add( pref, data.rcount, data.hash, data.routes, mask );
      }
    }
  }

  void finish( void ) {
    if ( this->rpd_mask == 0 )
      return;
    uint32_t k = ( this->rpd[ 0 ].rcount == 0 ? 0 : 1 );
    BitSet64 bi( this->rpd_mask );
    uint32_t pref;
    for ( bool b = bi.first( pref ); b; b = bi.next( pref ) ) {
      this->rpd[ k++ ] = this->rpd[ pref + 1 ];
    }
    this->n = k;
  }
};

typedef RoutePublishSetT<RoutePublishData, uint32_t> RoutePublishSet;
typedef RoutePublishSetT<RouteQueueData, QueueRef> RouteQueueSet;

struct RoutePublishContext : public RouteLookup {
  EvPublish     & pub;
  RouteDB       & rdb;
  RoutePublishSet set;
  uint8_t         save_type;

  RoutePublishContext( RouteDB &db,  EvPublish &pub ) noexcept;
  ~RoutePublishContext() {
    this->pub.publish_type = this->save_type;
    this->RouteLookup::deref( this->rdb );
  }
  void add_queues( void ) noexcept;
  void make_qroutes( RouteGroup &db ) noexcept;
  void select_queue( QueueDB &q,  RouteQueueSet &qset,
                     RoutePublishSet &prune_set ) noexcept;
};

}
}
#endif
