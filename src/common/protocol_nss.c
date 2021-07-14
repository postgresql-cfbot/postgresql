/*-------------------------------------------------------------------------
 *
 * protocol_nss.c
 *	  NSS functionality shared between frontend and backend for working
 *	  with protocols
 *
 * This should only be used if code is compiled with NSS support.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		  src/common/protocol_nss.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "common/nss.h"

/*
 * ssl_protocol_version_to_string
 *			Translate NSS TLS version to string
 */
char *
ssl_protocol_version_to_string(int v)
{
	switch (v)
	{
		/* SSL v2 and v3 are not supported */
		case SSL_LIBRARY_VERSION_2:
		case SSL_LIBRARY_VERSION_3_0:
			Assert(false);
			break;

		case SSL_LIBRARY_VERSION_TLS_1_0:
			return pstrdup("TLSv1.0");
#ifdef SSL_LIBRARY_VERSION_TLS_1_1
		case SSL_LIBRARY_VERSION_TLS_1_1:
			return pstrdup("TLSv1.1");
#endif
#ifdef SSL_LIBRARY_VERSION_TLS_1_2
		case SSL_LIBRARY_VERSION_TLS_1_2:
			return pstrdup("TLSv1.2");
#endif
#ifdef SSL_LIBRARY_VERSION_TLS_1_3
		case SSL_LIBRARY_VERSION_TLS_1_3:
			return pstrdup("TLSv1.3");
#endif
	}

	return pstrdup("unknown");
}

