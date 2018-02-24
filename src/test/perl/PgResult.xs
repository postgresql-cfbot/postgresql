/**********************************************************************
 * PgResult
 *
 * Simple client interface for perl
 *
 *    src/test/perl/PgResult.xs
 *
 **********************************************************************/
#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "ppport.h"

/* conflicts with the same symbol defined by postgres_fe.h */
#undef _

#include "libpq-fe.h"

PGresult *getpgresult(SV *ressvrv);

PGresult *
getpgresult(SV *ressvrv)
{
	SV *resivsv = SvRV(ressvrv);

	if (!sv_isobject(ressvrv) || !sv_isa(ressvrv, "PgResult"))
		croak("unexpected parameter");

	return (PGresult *) SvIV(resivsv);
}


MODULE = PgResult		PACKAGE = PgResult
PROTOTYPES: ENABLE

=pod

=item $client->resultStatus()

Get the result status of the command.
=cut

int
resultStatus(result)
  CODE:
	PGresult *res = getpgresult(ST(0));

	/* believing the reuslt */
	RETVAL = PQresultStatus(res);

  OUTPUT:
	RETVAL


=pod

=item $client->ntuples()

Get the number of rows in the query result.
=cut

int
ntuples(result)
  CODE:
	PGresult *res = getpgresult(ST(0));

	RETVAL = PQntuples(res);

  OUTPUT:
	RETVAL

=pod

=item $client->nfields()

Get the number of columns in each row of the query result.
=cut

int
nfields(result)
  CODE:
	PGresult *res = getpgresult(ST(0));

	RETVAL = PQnfields(res);

  OUTPUT:
	RETVAL

=pod

=item $client->getvalue()

Returns a single field value of one row of a PgResult.
Row nad column numbers start at 0.
=cut

char *
getvalue(result, tup_num, field_num)
	int tup_num;
	int field_num;
  CODE:
	PGresult *res = getpgresult(ST(0));

	RETVAL = PQgetvalue(res, tup_num, field_num);

  OUTPUT:
	RETVAL

=pod

=item $client->getvalue()

Returns the actual length of a field value in bytes.
Row nad column numbers start at 0.
=cut

int
getlength(result, tup_num, field_num)
	int tup_num;
	int field_num;
  CODE:
	PGresult *res = getpgresult(ST(0));

	RETVAL = PQgetlength(res, tup_num, field_num);

  OUTPUT:
	RETVAL

=item $client->getisnull()

Tests a field for a null value.
Row nad column numbers start at 0.
=cut

int
getisnull(result, tup_num, field_num)
	int tup_num;
	int field_num;
  CODE:
	PGresult *res = getpgresult(ST(0));

	RETVAL = PQgetisnull(res, tup_num, field_num);

  OUTPUT:
	RETVAL

void
DESTROY(result)
  CODE:
	PGresult *res = getpgresult(ST(0));
	PQclear(res);
