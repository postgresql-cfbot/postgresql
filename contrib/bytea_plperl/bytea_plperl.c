/*
 * contrib/bytea_plperl/bytea_plperl.c
 */

#include "postgres.h"

#include "fmgr.h"
#include "plperl.h"
#include "varatt.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(bytea_to_plperl);
PG_FUNCTION_INFO_V1(plperl_to_bytea);

Datum
bytea_to_plperl(PG_FUNCTION_ARGS)
{
	dTHX;
	bytea *in = PG_GETARG_BYTEA_PP(0);
	return PointerGetDatum(newSVpvn_flags( (char *) VARDATA_ANY(in), VARSIZE_ANY_EXHDR(in), 0 ));
}

Datum
plperl_to_bytea(PG_FUNCTION_ARGS)
{
	dTHX;
	bytea *result;
	STRLEN len;
	SV *in = (SV *) PG_GETARG_POINTER(0);
	char *ptr = SvPVbyte(in, len);
	result = palloc(VARHDRSZ + len );
	SET_VARSIZE(result, VARHDRSZ + len );
	memcpy(VARDATA_ANY(result), ptr,len );
	PG_RETURN_BYTEA_P(result);
}
