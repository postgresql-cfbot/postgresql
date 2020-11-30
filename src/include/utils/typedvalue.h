/*-------------------------------------------------------------------------
 *
 * typedvalue.h
 *	  Declarations for typedvalue data type support.
 *
 * Copyright (c) 1996-2020, PostgreSQL Global Development Group
 *
 * src/include/utils/typedvalue.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __TYPEDVALUE_H__
#define __TYPEDVALUE_H__

typedef struct
{
	int32		vl_len;			/* varlena header */
	Oid			typid;
	bool		typbyval;
	int16		typlen;
	char		data[FLEXIBLE_ARRAY_MEMBER];
} TypedValueData;

typedef TypedValueData *TypedValue;

extern Datum makeTypedValue(Datum value, Oid typid, int16 typlen, bool typbyval);

#endif
