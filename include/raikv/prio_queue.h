#ifndef __rai__raikv__prio_queue_h__
#define __rai__raikv__prio_queue_h__

#ifdef __cplusplus
namespace rai {
namespace kv {

template <class Elem, bool (*IS_GREATER)(Elem, Elem)>
struct PrioQueue {
  Elem * heap;
  size_t num_elems,
         max_elems;
  const size_t incrby;

  PrioQueue( size_t inc = 128 )
    : heap( 0 ), num_elems( 0 ), max_elems( 0 ), incrby( inc ) {}
  ~PrioQueue() {
    this->reset();
  }
  void reset( void ) {
    if ( this->heap != NULL )
      ::free( this->heap );
    this->heap      = NULL;
    this->num_elems = 0;
    this->max_elems = 0;
  }

  bool increase_heap( void ) {
    size_t newsz = this->max_elems + this->incrby;
    void *p = ::realloc( this->heap, newsz * sizeof( this->heap[ 0 ] ) );
    if ( p != NULL ) {
      this->heap      = (Elem *) p;
      this->max_elems = newsz;
      return true;
    }
    return false;
  }
  bool push( Elem el ) {
    size_t c = this->num_elems;
    if ( c >= this->max_elems )
      if ( ! this->increase_heap() )
        return false; /* if realloc() failed, return false */
    while ( c > 0 ) {
      size_t p = ( c + 1 ) / 2 - 1;
      if ( IS_GREATER( el, this->heap[ p ] ) ) /* find spot where el > heap[ p ] */
        break;
      this->heap[ c ] = this->heap[ p ]; /* move heap[ p ] down */
      c = p;
    }
    this->heap[ c ] = el; /* insert el */
    this->num_elems++;
    return true;
  }
  Elem pop( void ) {
    Elem el = this->heap[ 0 ]; /* remove first elem */
    if ( --this->num_elems > 0 ) {
      Elem &last = this->heap[ this->num_elems ]; /* reorder last el */
      size_t c = 1, p;
      for ( p = 0; c < this->num_elems; c = ( p + 1 ) * 2 - 1 ) {
        if ( c + 1 < this->num_elems ) {
          if ( IS_GREATER( this->heap[ c ], this->heap[ c + 1 ] ) )
            c++;
        }
        if ( IS_GREATER( this->heap[ c ], last ) ) /* found spot for last el */
          break;
        this->heap[ p ] = this->heap[ c ]; /* move heap[ c ] up */
        p = c;
      }
      this->heap[ p ] = last; /* insert last */
    }
    return el;
  }
  bool remove( Elem el ) {
    size_t p = this->num_elems;
    if ( p == 0 )
      return false;
    if ( el == this->heap[ --p ] ) { /* if last el */
      this->num_elems = p;
      return true;
    }
    while ( p > 0 ) {
      if ( el == this->heap[ --p ] ) {
        while ( p > 0 ) { /* move el to top, then pop */
          size_t c = p;
          p = ( c + 1 ) / 2 - 1;
          this->heap[ c ] = this->heap[ p ];
        }
        this->pop();
        return true;
      }
    }
    return false;
  }
  bool is_empty( void ) { return this->num_elems == 0; }
  size_t count( void ) { return this->num_elems; }
};

}
}
#endif
#endif
