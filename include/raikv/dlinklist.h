#ifndef __rai_raikv__dlinklist_h__
#define __rai_raikv__dlinklist_h__

#ifdef __cplusplus
namespace rai {
namespace kv {

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
};

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
};

}
}
#endif
#endif
