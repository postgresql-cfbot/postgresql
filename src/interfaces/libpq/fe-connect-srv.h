/*-------------------------------------------------------------------------
 *
 * fe-connect-srv.h
 *	  DNS SRV record lookup for libpq service discovery.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/interfaces/libpq/fe-connect-srv.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FE_CONNECT_SRV_H
#define FE_CONNECT_SRV_H

#include "libpq-int.h"

/*
 * pqLookupSRVHosts
 *
 * Resolve _postgresql._tcp.<conn->srvhost> SRV records and store the
 * result in conn->pghost and conn->pgport as comma-separated strings,
 * suitable for consumption by pqConnectOptions2().
 *
 * Returns true on success, false on error (error message set in conn).
 */
extern bool pqLookupSRVHosts(PGconn *conn);

#endif							/* FE_CONNECT_SRV_H */
