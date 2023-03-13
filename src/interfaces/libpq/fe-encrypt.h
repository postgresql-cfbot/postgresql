/*-------------------------------------------------------------------------
 *
 * fe-encrypt.h
 *
 * client-side column encryption support
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/interfaces/libpq/fe-encrypt.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef FE_ENCRYPT_H
#define FE_ENCRYPT_H

#include "libpq-fe.h"
#include "libpq-int.h"

extern unsigned char *decrypt_cek_from_file(PGconn *conn, const char *cmkfilename, int cmkalg,
											int fromlen, const unsigned char *from,
											int *tolen);

extern unsigned char *decrypt_value(PGresult *res, const PGCEK *cek, int cekalg,
									const unsigned char *input, int inputlen,
									const char **errmsgp);

extern unsigned char *encrypt_value(PGconn *conn, const PGCEK *cek, int cekalg,
									const unsigned char *value, int *nbytesp, bool enc_det);

#endif							/* FE_ENCRYPT_H */
