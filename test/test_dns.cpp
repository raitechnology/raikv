#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <arpa/nameser.h>
#else
#include <raikv/win.h>
#endif
#include <raikv/ev_cares.h>
#include <ares_dns.h>

using namespace rai;
using namespace kv;

int
main( int argc, char *argv[] )
{
  EvPoll poll;
  CaresAddrInfo info( &poll );

  poll.init( 5, false );
  for ( int i = 1; i < argc; i++ ) {
    char *query = argv[ i ];

    info.get_address( query, 80, OPT_AF_INET | OPT_AF_INET6 );
    if ( ! info.done ) {
      for (;;) {
        poll.dispatch();
        if ( info.done )
          break;
        poll.wait( 100 );
      }
    }
    struct addrinfo * ai = info.addr_list;
    if ( ai != NULL ) {
      do {
        PeerAddrStr str;
        str.set_addr( ai->ai_addr );
        printf( "%.*s\n", (int) str.len(), str.buf );
      } while ( (ai = ai->ai_next) != NULL );
    }
    else {
      printf( "status %d timeouts %d\n", info.status, info.timeouts );
    }
    info.free_addr_list();
  }
  return 0;
}

#if 0
static void callback( void *arg, int status, int timeouts, uint8_t *abuf,
                      int alen );
static const uint8_t * display_question( const uint8_t *aptr,
                                         const uint8_t *abuf, int alen );
static const uint8_t * display_rr( const uint8_t *aptr, const uint8_t *abuf,
                                   int alen );
struct nv {
  const char *name;
  int value;
};

static const char *
class_name( int dnsclass )
{
  static const struct nv classes[] = {
    { "IN", C_IN }, { "CHAOS", C_CHAOS }, { "HS", C_HS }, { "ANY", C_ANY }
  };
  static const int nclasses = sizeof( classes ) / sizeof( classes[ 0 ] );
  for ( int i = 0; i < nclasses; i++ ) {
    if ( classes[ i ].value == dnsclass )
      return classes[ i ].name;
  }
  return "(unknown)";
}

static const char *
type_name( int type )
{
  static const struct nv types[] = { { "A", T_A }, { "NS", T_NS },
    { "MD", T_MD }, { "MF", T_MF }, { "CNAME", T_CNAME }, { "SOA", T_SOA },
    { "MB", T_MB }, { "MG", T_MG }, { "MR", T_MR }, { "NULL", T_NULL },
    { "WKS", T_WKS }, { "PTR", T_PTR }, { "HINFO", T_HINFO },
    { "MINFO", T_MINFO }, { "MX", T_MX }, { "TXT", T_TXT }, { "RP", T_RP },
    { "AFSDB", T_AFSDB }, { "X25", T_X25 }, { "ISDN", T_ISDN }, { "RT", T_RT },
    { "NSAP", T_NSAP }, { "NSAP_PTR", T_NSAP_PTR }, { "SIG", T_SIG },
    { "KEY", T_KEY }, { "PX", T_PX }, { "GPOS", T_GPOS }, { "AAAA", T_AAAA },
    { "LOC", T_LOC }, { "SRV", T_SRV }, { "AXFR", T_AXFR },
    { "MAILB", T_MAILB }, { "MAILA", T_MAILA }, { "NAPTR", T_NAPTR },
    { "DS", T_DS }, { "SSHFP", T_SSHFP }, { "RRSIG", T_RRSIG },
    { "NSEC", T_NSEC }, { "DNSKEY", T_DNSKEY }, { "CAA", T_CAA },
    { "URI", T_URI }, { "ANY", T_ANY } };
  static const int       ntypes  = sizeof( types ) / sizeof( types[ 0 ] );

  for ( int i = 0; i < ntypes; i++ ) {
    if ( types[ i ].value == type )
      return types[ i ].name;
  }
  return "(unknown)";
}

int
main( int argc, char *argv[] )
{
  ares_channel channel;
  int          i, optmask = ARES_OPT_FLAGS, dnsclass = C_IN, type = T_A;
  int          status, nfds, count;
  struct ares_options
               options;
  fd_set       read_fds, write_fds;
  struct timeval *tvp, tv;

  status = ares_init_options( &channel, &options, optmask );
  if ( status != ARES_SUCCESS ) {
    fprintf( stderr, "ares_init_options: %s\n", ares_strerror( status ) );
    return 1;
  }

  for ( i = 1; i < argc; i++ ) {
    char *query = argv[ i ];

    ares_query( channel, query, dnsclass, type, callback,
                i < argc - 1 ? (void *) query : NULL );
  }

  /* Wait for all queries to complete. */
  for ( ;; ) {
    FD_ZERO( &read_fds );
    FD_ZERO( &write_fds );
    nfds = ares_fds( channel, &read_fds, &write_fds );
    if ( nfds == 0 )
      break;
    tvp   = ares_timeout( channel, NULL, &tv );
    count = select( nfds, &read_fds, &write_fds, NULL, tvp );
    if ( count < 0 && ( status = errno ) != EINVAL ) {
      printf( "select fail: %d", status );
      return 1;
    }
    ares_process( channel, &read_fds, &write_fds );
  }
  ares_destroy( channel );
  ares_library_cleanup();

  return 0;
}

static void
callback( void *arg, int status, int timeouts, uint8_t *abuf, int alen )
{
  char        *name = (char *) arg;
  int          id, qr, opcode, aa, tc, rd, ra, rcode;
  unsigned int qdcount, ancount, nscount, arcount, i;
  const uint8_t *aptr;

  (void) timeouts;

  /* Display the query name if given. */
  if ( name )
    printf( "Answer for query %s:\n", name );

  /* Display an error message if there was an error, but only stop if
   * we actually didn't get an answer buffer.
   */
  if ( status != ARES_SUCCESS ) {
    printf( "%s\n", ares_strerror( status ) );
    if ( !abuf )
      return;
  }

  /* Won't happen, but check anyway, for safety. */
  if ( alen < HFIXEDSZ )
    return;

  /* Parse the answer header. */
  id      = DNS_HEADER_QID( abuf );
  qr      = DNS_HEADER_QR( abuf );
  opcode  = DNS_HEADER_OPCODE( abuf );
  aa      = DNS_HEADER_AA( abuf );
  tc      = DNS_HEADER_TC( abuf );
  rd      = DNS_HEADER_RD( abuf );
  ra      = DNS_HEADER_RA( abuf );
  rcode   = DNS_HEADER_RCODE( abuf );
  qdcount = DNS_HEADER_QDCOUNT( abuf );
  ancount = DNS_HEADER_ANCOUNT( abuf );
  nscount = DNS_HEADER_NSCOUNT( abuf );
  arcount = DNS_HEADER_ARCOUNT( abuf );

  /* Display the answer header. */
  printf( "id: %d\n", id );
  printf( "flags: %s%s%s%s%s\n", qr ? "qr " : "", aa ? "aa " : "",
          tc ? "tc " : "", rd ? "rd " : "", ra ? "ra " : "" );
  printf( "opcode: %d\n", opcode /*opcodes[ opcode ]*/ );
  printf( "rcode: %d\n", rcode /*rcodes[ rcode ]*/ );

  /* Display the questions. */
  printf( "Questions:\n" );
  aptr = abuf + HFIXEDSZ;
  for ( i = 0; i < qdcount; i++ ) {
    aptr = display_question( aptr, abuf, alen );
    if ( aptr == NULL )
      return;
  }

  /* Display the answers. */
  printf( "Answers:\n" );
  for ( i = 0; i < ancount; i++ ) {
    aptr = display_rr( aptr, abuf, alen );
    if ( aptr == NULL )
      return;
  }

  /* Display the NS records. */
  printf( "NS records:\n" );
  for ( i = 0; i < nscount; i++ ) {
    aptr = display_rr( aptr, abuf, alen );
    if ( aptr == NULL )
      return;
  }

  /* Display the additional records. */
  printf( "Additional records:\n" );
  for ( i = 0; i < arcount; i++ ) {
    aptr = display_rr( aptr, abuf, alen );
    if ( aptr == NULL )
      return;
  }
}

static const uint8_t *
display_question( const uint8_t *aptr, const uint8_t *abuf,
                  int alen )
{
  char *name;
  int   type, dnsclass, status;
  long  len;

  /* Parse the question name. */
  status = ares_expand_name( aptr, abuf, alen, &name, &len );
  if ( status != ARES_SUCCESS )
    return NULL;
  aptr += len;

  /* Make sure there's enough data after the name for the fixed part
   * of the question.
   */
  if ( aptr + QFIXEDSZ > abuf + alen ) {
    ares_free_string( name );
    return NULL;
  }

  /* Parse the question type and class. */
  type     = DNS_QUESTION_TYPE( aptr );
  dnsclass = DNS_QUESTION_CLASS( aptr );
  aptr += QFIXEDSZ;

  /* Display the question, in a format sort of similar to how we will
   * display RRs.
   */
  printf( "\t%-15s.\t", name );
  if ( dnsclass != C_IN )
    printf( "\t%s", class_name( dnsclass ) );
  printf( "\t%s\n", type_name( type ) );
  ares_free_string( name );
  return aptr;
}

static const uint8_t *
display_rr( const uint8_t *aptr, const uint8_t *abuf, int alen )
{
  const uint8_t *p;
  int  type, dnsclass, ttl, dlen, status, i;
  long len;
  int  vlen;
  char addr[ 46 ];
  union {
    uint8_t *as_uchar;
    char     *as_char;
  } name;

  /* Parse the RR name. */
  status = ares_expand_name( aptr, abuf, alen, &name.as_char, &len );
  if ( status != ARES_SUCCESS )
    return NULL;
  aptr += len;

  /* Make sure there is enough data after the RR name for the fixed
   * part of the RR.
   */
  if ( aptr + RRFIXEDSZ > abuf + alen ) {
    ares_free_string( name.as_char );
    return NULL;
  }

  /* Parse the fixed part of the RR, and advance to the RR data
   * field. */
  type     = DNS_RR_TYPE( aptr );
  dnsclass = DNS_RR_CLASS( aptr );
  ttl      = DNS_RR_TTL( aptr );
  dlen     = DNS_RR_LEN( aptr );
  aptr += RRFIXEDSZ;
  if ( aptr + dlen > abuf + alen ) {
    ares_free_string( name.as_char );
    return NULL;
  }

  /* Display the RR name, class, and type. */
  printf( "\t%-15s.\t%d", name.as_char, ttl );
  if ( dnsclass != C_IN )
    printf( "\t%s", class_name( dnsclass ) );
  printf( "\t%s", type_name( type ) );
  ares_free_string( name.as_char );

  /* Display the RR data.  Don't touch aptr. */
  switch ( type ) {
    case T_CNAME:
    case T_MB:
    case T_MD:
    case T_MF:
    case T_MG:
    case T_MR:
    case T_NS:
    case T_PTR:
      /* For these types, the RR data is just a domain name. */
      status = ares_expand_name( aptr, abuf, alen, &name.as_char, &len );
      if ( status != ARES_SUCCESS )
        return NULL;
      printf( "\t%s.", name.as_char );
      ares_free_string( name.as_char );
      break;

    case T_HINFO:
      /* The RR data is two length-counted character strings. */
      p   = aptr;
      len = *p;
      if ( p + len + 1 > aptr + dlen )
        return NULL;
      status = ares_expand_string( p, abuf, alen, &name.as_uchar, &len );
      if ( status != ARES_SUCCESS )
        return NULL;
      printf( "\t%s", name.as_char );
      ares_free_string( name.as_char );
      p += len;
      len = *p;
      if ( p + len + 1 > aptr + dlen )
        return NULL;
      status = ares_expand_string( p, abuf, alen, &name.as_uchar, &len );
      if ( status != ARES_SUCCESS )
        return NULL;
      printf( "\t%s", name.as_char );
      ares_free_string( name.as_char );
      break;

    case T_MINFO:
      /* The RR data is two domain names. */
      p      = aptr;
      status = ares_expand_name( p, abuf, alen, &name.as_char, &len );
      if ( status != ARES_SUCCESS )
        return NULL;
      printf( "\t%s.", name.as_char );
      ares_free_string( name.as_char );
      p += len;
      status = ares_expand_name( p, abuf, alen, &name.as_char, &len );
      if ( status != ARES_SUCCESS )
        return NULL;
      printf( "\t%s.", name.as_char );
      ares_free_string( name.as_char );
      break;

    case T_MX:
      /* The RR data is two bytes giving a preference ordering, and
       * then a domain name.
       */
      if ( dlen < 2 )
        return NULL;
      printf( "\t%d", (int) DNS__16BIT( aptr ) );
      status = ares_expand_name( aptr + 2, abuf, alen, &name.as_char, &len );
      if ( status != ARES_SUCCESS )
        return NULL;
      printf( "\t%s.", name.as_char );
      ares_free_string( name.as_char );
      break;

    case T_SOA:
      /* The RR data is two domain names and then five four-byte
       * numbers giving the serial number and some timeouts.
       */
      p      = aptr;
      status = ares_expand_name( p, abuf, alen, &name.as_char, &len );
      if ( status != ARES_SUCCESS )
        return NULL;
      printf( "\t%s.\n", name.as_char );
      ares_free_string( name.as_char );
      p += len;
      status = ares_expand_name( p, abuf, alen, &name.as_char, &len );
      if ( status != ARES_SUCCESS )
        return NULL;
      printf( "\t\t\t\t\t\t%s.\n", name.as_char );
      ares_free_string( name.as_char );
      p += len;
      if ( p + 20 > aptr + dlen )
        return NULL;
      printf( "\t\t\t\t\t\t( %u %u %u %u %u )", DNS__32BIT( p ),
              DNS__32BIT( p + 4 ), DNS__32BIT( p + 8 ), DNS__32BIT( p + 12 ),
              DNS__32BIT( p + 16 ) );
      break;

    case T_TXT:
      /* The RR data is one or more length-counted character
       * strings. */
      p = aptr;
      while ( p < aptr + dlen ) {
        len = *p;
        if ( p + len + 1 > aptr + dlen )
          return NULL;
        status = ares_expand_string( p, abuf, alen, &name.as_uchar, &len );
        if ( status != ARES_SUCCESS )
          return NULL;
        printf( "\t%s", name.as_char );
        ares_free_string( name.as_char );
        p += len;
      }
      break;

    case T_CAA:

      p = aptr;

      /* Flags */
      printf( " %u", (int) *p );
      p += 1;

      /* Remainder of record */
      vlen = (int) dlen - ( (char) *p ) - 2;

      /* The Property identifier, one of:
          - "issue",
          - "iodef", or
          - "issuewild" */
      status = ares_expand_string( p, abuf, alen, &name.as_uchar, &len );
      if ( status != ARES_SUCCESS )
        return NULL;
      printf( " %s", name.as_char );
      ares_free_string( name.as_char );
      p += len;

      if ( p + vlen > abuf + alen )
        return NULL;

      /* A sequence of octets representing the Property Value */
      printf( " %.*s", vlen, p );
      break;

    case T_A:
      /* The RR data is a four-byte Internet address. */
      if ( dlen != 4 )
        return NULL;
      printf( "\t%s", ares_inet_ntop( AF_INET, aptr, addr, sizeof( addr ) ) );
      break;

    case T_AAAA:
      /* The RR data is a 16-byte IPv6 address. */
      if ( dlen != 16 )
        return NULL;
      printf( "\t%s", ares_inet_ntop( AF_INET6, aptr, addr, sizeof( addr ) ) );
      break;

    case T_WKS:
      /* Not implemented yet */
      break;

    case T_SRV:
      /* The RR data is three two-byte numbers representing the
       * priority, weight, and port, followed by a domain name.
       */

      printf( "\t%d", (int) DNS__16BIT( aptr ) );
      printf( " %d", (int) DNS__16BIT( aptr + 2 ) );
      printf( " %d", (int) DNS__16BIT( aptr + 4 ) );

      status = ares_expand_name( aptr + 6, abuf, alen, &name.as_char, &len );
      if ( status != ARES_SUCCESS )
        return NULL;
      printf( "\t%s.", name.as_char );
      ares_free_string( name.as_char );
      break;

    case T_URI:
      /* The RR data is two two-byte numbers representing the
       * priority and weight, followed by a target.
       */

      printf( "\t%d ", (int) DNS__16BIT( aptr ) );
      printf( "%d \t\t", (int) DNS__16BIT( aptr + 2 ) );
      p = aptr + 4;
      for ( i = 0; i < dlen - 4; ++i )
        printf( "%c", p[ i ] );
      break;

    case T_NAPTR:

      printf( "\t%d", (int) DNS__16BIT( aptr ) );      /* order */
      printf( " %d\n", (int) DNS__16BIT( aptr + 2 ) ); /* preference */

      p      = aptr + 4;
      status = ares_expand_string( p, abuf, alen, &name.as_uchar, &len );
      if ( status != ARES_SUCCESS )
        return NULL;
      printf( "\t\t\t\t\t\t%s\n", name.as_char );
      ares_free_string( name.as_char );
      p += len;

      status = ares_expand_string( p, abuf, alen, &name.as_uchar, &len );
      if ( status != ARES_SUCCESS )
        return NULL;
      printf( "\t\t\t\t\t\t%s\n", name.as_char );
      ares_free_string( name.as_char );
      p += len;

      status = ares_expand_string( p, abuf, alen, &name.as_uchar, &len );
      if ( status != ARES_SUCCESS )
        return NULL;
      printf( "\t\t\t\t\t\t%s\n", name.as_char );
      ares_free_string( name.as_char );
      p += len;

      status = ares_expand_name( p, abuf, alen, &name.as_char, &len );
      if ( status != ARES_SUCCESS )
        return NULL;
      printf( "\t\t\t\t\t\t%s", name.as_char );
      ares_free_string( name.as_char );
      break;

    case T_DS:
    case T_SSHFP:
    case T_RRSIG:
    case T_NSEC:
    case T_DNSKEY: printf( "\t[RR type parsing unavailable]" ); break;

    default: printf( "\t[Unknown RR; cannot parse]" ); break;
  }
  printf( "\n" );

  return aptr + dlen;
}
#endif
