#ifndef __rai_raikv__dlinklist_h__
#define __rai_raikv__dlinklist_h__

#ifdef __cplusplus
namespace rai {
namespace kv {

template<class HEAD, class LIST, int cmp( const LIST &x, const LIST &y )>
void merge_sort_list( HEAD &z,  HEAD &l ) {
  LIST * sorted = NULL, * last = NULL, * el;
  for (;;) {
    if ( z.hd == NULL ) {
      if ( last != NULL ) {
        last->next = l.hd;
        z.hd = sorted;
      }
      else
        z.hd = l.hd;
      z.tl = l.tl;
      l.hd = l.tl = NULL;
      return;
    }
    if ( l.hd == NULL ) {
      if ( last != NULL ) {
        last->next = z.hd;
        z.hd = sorted;
      }
      l.tl = NULL;
      return;
    }
    if ( cmp( *z.hd, *l.hd ) <= 0 ) {
      el = z.hd;
      z.hd = el->next;
    }
    else {
      el = l.hd;
      l.hd = el->next;
    }
    if ( last != NULL )
      last->next = el;
    else
      sorted = el;
    last = el;
  }
}

template<class HEAD, class LIST, int cmp( const LIST &x, const LIST &y )>
void sort_list( HEAD &z ) {
  if ( z.hd == NULL || z.hd->next == NULL )
    return;
  HEAD m, l;
  int max = 5;
  for (;;) {
    l.hd = l.tl = z.hd;
    z.hd = z.hd->next;
    LIST ** last = &z.hd, * next;
    int count = 0;
    for ( LIST *el = z.hd; el != NULL; el = next ) {
      next = el->next;
      if ( cmp( *el, *l.tl ) >= 0 ) { /* case where items are sorted */
        *last = next;
        l.tl->next = el;
        l.tl = el;
        count = 0;
      }
      else if ( cmp( *el, *l.hd ) <= 0 ) { /* items are rev-sorted */
        *last = next;
        el->next = l.hd;
        l.hd = el;
        count = 0;
      }
      else {
        /* if items are randomized, endpoints are near the bounds */
        if ( ++count == max )
          break;
        last = &el->next;
      }
    }
    l.tl->next = NULL;
    merge_sort_list<HEAD, LIST, cmp>( m, l );
    if ( z.hd == NULL ) {
      z.hd = m.hd;
      z.tl = m.tl;
      return;
    }
    max++; /* incr max search for bounds when the list is longer */
  }
}

template <class LIST>
struct SLinkList {
  LIST * hd, * tl;

  SLinkList() : hd( 0 ), tl( 0 ) {}
  void init( void ) {
    this->hd = this->tl = NULL;
  }
  bool is_empty( void ) const {
    return this->hd == NULL;
  }
  void push_hd( LIST *p ) {
    if ( this->hd == NULL )
      this->tl = p;
    p->next = this->hd;
    this->hd = p;
  }
  void push_tl( LIST *p ) {
    if ( this->tl == NULL )
      this->hd = p;
    else
      this->tl->next = p;
    p->next = NULL;
    this->tl = p;
  }
  LIST *pop_hd( void ) {
    LIST *p = this->hd;
    if ( (this->hd = (LIST *) p->next) == NULL )
      this->tl = NULL;
    else
      p->next = NULL;
    return p;
  }
  LIST *unlink( LIST *p ) {
    LIST * x = this->hd;
    if ( p != x ) {
      for (;;) {
        if ( p == (LIST *) x->next ) {
          x->next = p->next;
          if ( p == this->tl )
            this->tl = x;
          else
            p->next = NULL;
          return p;
        }
        x = x->next;
      }
    }
    return this->pop_hd();
  }
  template<int cmp( const LIST &x, const LIST &y )>
  void sort( void ) {
    sort_list<SLinkList<LIST>, LIST, cmp>( *this );
  }
};

template <class LIST>
struct DLinkList {
  LIST * hd, * tl;

  DLinkList() : hd( 0 ), tl( 0 ) {}
  void init( void ) {
    this->hd = this->tl = NULL;
  }
  bool is_empty( void ) const {
    return this->hd == NULL;
  }
  void push_hd( LIST *p ) {
    p->next = this->hd;
    p->back = NULL;
    if ( this->hd == NULL )
      this->tl = p;
    else
      this->hd->back = p;
    this->hd = p;
  }
  void push_tl( LIST *p ) {
    if ( this->tl == NULL )
      this->hd = p;
    else
      this->tl->next = p;
    p->back = this->tl;
    this->tl = p;
    p->next = NULL;
  }
  void push_tl( DLinkList<LIST> &l ) {
    if ( this->tl == NULL ) {
      this->hd = l.hd;
      this->tl = l.tl;
    }
    else if ( l.hd != NULL ) {
      this->tl->next = l.hd;
      l.hd->back = this->tl;
      this->tl = l.tl;
    }
    l.hd = NULL;
    l.tl = NULL;
  }
  void pop( LIST *p ) {
    if ( p->back == NULL )
      this->hd = (LIST *) p->next;
    else
      p->back->next = p->next;
    if ( p->next == NULL )
      this->tl = (LIST *) p->back;
    else
      p->next->back = p->back;
    p->next = p->back = NULL;
  }
  void insert_before( LIST *x,  LIST *piv ) {
    if ( piv == this->hd )
      this->push_hd( x );
    else if ( piv == NULL )
      this->push_tl( x );
    else {
      x->next = piv;
      piv->back->next = x;
      x->back = piv->back;
      piv->back = x;
    }
  }
  LIST *pop_hd( void ) {
    LIST *p = this->hd;
    if ( (this->hd = (LIST *) p->next) == NULL )
      this->tl = NULL;
    else {
      this->hd->back = NULL;
      p->next = NULL;
    }
    return p;
  }
  LIST *pop_tl( void ) {
    LIST *p = this->tl;
    if ( (this->tl = (LIST *) p->back) == NULL )
      this->hd = NULL;
    else {
      this->tl->next = NULL;
      p->back = NULL;
    }
    return p;
  }
  template<int cmp( const LIST &x, const LIST &y )>
  void sort( void ) {
    sort_list<DLinkList<LIST>, LIST, cmp>( *this );
    LIST * back = NULL;
    for ( LIST *p = this->hd; p != NULL; p = p->next ) {
      p->back = back;
      back = p;
    }
  }
};

}
}
#endif
#endif
