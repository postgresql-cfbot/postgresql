/*-------------------------------------------------------------------------
 *
 * pg_auth_password.h
 *	  definition of the "authorization identifier" system catalog (pg_auth_password)
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * src/include/catalog/pg_auth_password.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_AUTH_PASSWORD_H
#define PG_AUTH_PASSWORD_H

#include "catalog/genbki.h"
#include "catalog/pg_auth_password_d.h"

/* ----------------
 *		pg_auth_password definition.  cpp turns this into
 *		typedef struct FormData_pg_auth_password
 * ----------------
 */
CATALOG(pg_auth_password,4551,AuthPasswordRelationId) BKI_SHARED_RELATION BKI_ROWTYPE_OID(4552,AuthPasswordRelation_Rowtype_Id) BKI_SCHEMA_MACRO
{
	Oid			roleid BKI_LOOKUP(pg_authid);	/* ID of a role */
	NameData    name;            /* name of password for multiple password support */

#ifdef CATALOG_VARLEN                /* variable-length fields start here */
	text            password BKI_FORCE_NOT_NULL;        /* password */
	timestamptz     expiration BKI_FORCE_NULL;	        /* password expiration time, if any */
#endif
} FormData_pg_auth_password;

/* ----------------
 *		Form_pg_auth_password corresponds to a pointer to a tuple with
 *		the format of pg_auth_password relation.
 * ----------------
 */

DECLARE_TOAST(pg_auth_password, 4175, 4176);
#define PgAuthPasswordToastTable 4175
#define PgAuthPasswordToastIndex 4176

typedef FormData_pg_auth_password *Form_pg_auth_password;

DECLARE_UNIQUE_INDEX_PKEY(pg_auth_password_roleoid_index, 4553, AuthPasswordRoleOidIndexId, on pg_auth_password using btree(roleid oid_ops, name name_ops));

#endif							/* PG_AUTH_PASSWORD_H */
