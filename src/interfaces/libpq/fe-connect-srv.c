/*-------------------------------------------------------------------------
 *
 * fe-connect-srv.c
 *	  DNS SRV record lookup for libpq service discovery.
 *
 * When a connection string specifies "srvhost=cluster.example.com" (or the
 * equivalent URI "postgresql+srv://cluster.example.com/"), libpq resolves
 * _postgresql._tcp.<srvhost> SRV records, sorts them by priority (ascending)
 * then weight (descending) per RFC 2782, and expands the result into the
 * conn->pghost and conn->pgport fields before pqConnectOptions2() builds the
 * per-host connection array.  All existing multi-host machinery (failover,
 * target_session_attrs, load_balance_hosts) then works unchanged.
 *
 * Platform support:
 *   - POSIX systems with res_query(3): Linux (glibc), macOS, *BSD, Solaris.
 *     Guarded by HAVE_RES_QUERY which is set by configure / meson.
 *   - Windows: DnsQuery() from windns.h, linked with Dnsapi.lib.
 *   - All other platforms: srvhost is rejected at connection time with an
 *     informative error message.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/interfaces/libpq/fe-connect-srv.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "common/int.h"
#include "fe-connect-srv.h"
#include "libpq-int.h"
#include "pqexpbuffer.h"

/* ----------
 * Internal SRV record representation.
 * ----------
 */
typedef struct
{
	uint16_t	priority;
	uint16_t	weight;
	uint16_t	port;
	char		target[NI_MAXHOST];
} SRVRecord;

/* Sort: lower priority first; higher weight first within same priority. */
static int
compareSRVRecords(const void *a, const void *b)
{
	const SRVRecord *sa = (const SRVRecord *) a;
	const SRVRecord *sb = (const SRVRecord *) b;

	if (sa->priority != sb->priority)
		return pg_cmp_u16(sa->priority, sb->priority);
	return pg_cmp_u16(sb->weight, sa->weight);
}

/*
 * Build conn->pghost and conn->pgport as comma-separated strings from a
 * sorted SRVRecord array.  Returns true on success; on OOM sets the
 * connection error and returns false.
 */
static bool
srv_build_host_port(PGconn *conn, SRVRecord *records, int nrecords)
{
	PQExpBufferData hostbuf;
	PQExpBufferData portbuf;

	initPQExpBuffer(&hostbuf);
	initPQExpBuffer(&portbuf);

	for (int i = 0; i < nrecords; i++)
	{
		if (i > 0)
		{
			appendPQExpBufferChar(&hostbuf, ',');
			appendPQExpBufferChar(&portbuf, ',');
		}
		appendPQExpBufferStr(&hostbuf, records[i].target);
		appendPQExpBuffer(&portbuf, "%u", records[i].port);
	}

	if (PQExpBufferDataBroken(hostbuf) || PQExpBufferDataBroken(portbuf))
	{
		termPQExpBuffer(&hostbuf);
		termPQExpBuffer(&portbuf);
		libpq_append_conn_error(conn, "out of memory");
		return false;
	}

	/*
	 * Replace conn->pghost and conn->pgport with the SRV-derived values.
	 * The originals were either NULL or empty (caller verified), but free
	 * them defensively before overwriting.
	 */
	free(conn->pghost);
	conn->pghost = strdup(hostbuf.data);
	free(conn->pgport);
	conn->pgport = strdup(portbuf.data);

	termPQExpBuffer(&hostbuf);
	termPQExpBuffer(&portbuf);

	if (!conn->pghost || !conn->pgport)
	{
		libpq_append_conn_error(conn, "out of memory");
		return false;
	}

	return true;
}


/* ====================================================================
 * Platform-specific implementations
 * ==================================================================== */

#ifdef WIN32

/*
 * Windows implementation using DnsQuery() from windns.h.
 * Link with -ldnsapi (Dnsapi.lib).
 */
#include <windns.h>

bool
pqLookupSRVHosts(PGconn *conn)
{
	char		qname[NI_MAXHOST + 30];
	PDNS_RECORD pDnsRecord = NULL;
	PDNS_RECORD pRec;
	SRVRecord  *records = NULL;
	int			nrecords = 0;
	int			maxrecords = 0;
	DNS_STATUS	status;
	bool		ok;

	snprintf(qname, sizeof(qname), "_postgresql._tcp.%s", conn->srvhost);

	status = DnsQuery_A(qname, DNS_TYPE_SRV, DNS_QUERY_STANDARD,
						NULL, &pDnsRecord, NULL);
	if (status != ERROR_SUCCESS)
	{
		libpq_append_conn_error(conn,
								"DNS SRV lookup failed for \"%s\": error code %lu",
								qname, (unsigned long) status);
		return false;
	}

	/* Collect SRV records from the linked list */
	for (pRec = pDnsRecord; pRec != NULL; pRec = pRec->pNext)
	{
		SRVRecord  *tmp;
		SRVRecord  *rec;
		size_t		tlen;

		if (pRec->wType != DNS_TYPE_SRV)
			continue;

		if (nrecords >= maxrecords)
		{
			maxrecords = maxrecords ? maxrecords * 2 : 4;
			tmp = realloc(records, maxrecords * sizeof(SRVRecord));
			if (!tmp)
			{
				DnsRecordListFree(pDnsRecord, DnsFreeRecordList);
				free(records);
				libpq_append_conn_error(conn, "out of memory");
				return false;
			}
			records = tmp;
		}

		rec = &records[nrecords];
		rec->priority = pRec->Data.SRV.wPriority;
		rec->weight = pRec->Data.SRV.wWeight;
		rec->port = pRec->Data.SRV.wPort;

		/*
		 * DnsQuery_A returns pNameTarget as a plain ANSI string (PSTR);
		 * no wide-char conversion needed.
		 */
		strlcpy(rec->target, pRec->Data.SRV.pNameTarget, sizeof(rec->target));

		/* Strip trailing dot from FQDN */
		tlen = strlen(rec->target);
		if (tlen > 0 && rec->target[tlen - 1] == '.')
			rec->target[tlen - 1] = '\0';

		nrecords++;
	}

	DnsRecordListFree(pDnsRecord, DnsFreeRecordList);

	if (nrecords == 0)
	{
		libpq_append_conn_error(conn, "no SRV records found for \"%s\"", qname);
		free(records);
		return false;
	}

	qsort(records, nrecords, sizeof(SRVRecord), compareSRVRecords);

	ok = srv_build_host_port(conn, records, nrecords);

	free(records);
	return ok;
}

#elif defined(HAVE_RES_QUERY)

/*
 * POSIX implementation using res_query() and manual DNS wire-format parsing.
 *
 * HAVE_RES_QUERY is set by configure (AC_SEARCH_LIBS) and meson
 * (cc.has_function / find_library) when res_query() is available.
 *
 * We avoid depending on ns_initparse() / ns_parserr() since those symbols
 * are not available on all platforms (e.g. musl libc).  Instead we parse
 * the wire format directly, using only dn_expand() for name decompression
 * (which is universally available wherever res_query() is).
 */

#include <arpa/nameser.h>
#include <resolv.h>
#include <netdb.h>

/* DNS wire-format constants (from RFC 1035 / RFC 2782) */
#ifndef T_SRV
#define T_SRV		33		/* RFC 2782: Service locator */
#endif
#ifndef C_IN
#define C_IN		1		/* the Internet */
#endif
#ifndef HFIXEDSZ
#define HFIXEDSZ	12		/* DNS message header size */
#endif

/*
 * Skip a (possibly compressed) DNS domain name starting at cp.
 * Returns bytes consumed, or -1 on error.
 */
static int
srv_skip_dname(const unsigned char *eom, const unsigned char *cp)
{
	const unsigned char *orig = cp;

	while (cp < eom)
	{
		int			n = *cp & 0xFF;

		if (n == 0)
			return (int) ((cp - orig) + 1);	/* root label */
		if ((n & 0xC0) == 0xC0)
			return (int) ((cp - orig) + 2);	/* 2-byte pointer */
		if ((n & 0xC0) != 0)
			return -1;			/* unknown label type */
		cp += n + 1;
	}
	return -1;					/* truncated */
}

bool
pqLookupSRVHosts(PGconn *conn)
{
	char		qname[NI_MAXHOST + 30];
	unsigned char answer[4096];
	int			len;
	const unsigned char *cp;
	const unsigned char *eom;
	int			qdcount,
				ancount;
	SRVRecord  *records = NULL;
	int			nrecords = 0;
	int			maxrecords = 0;
	bool		retval = false;

	snprintf(qname, sizeof(qname), "_postgresql._tcp.%s", conn->srvhost);

	len = res_query(qname, C_IN, T_SRV, answer, sizeof(answer));
	if (len < 0)
	{
		/*
		 * h_errno is set by res_query on failure.  HOST_NOT_FOUND and
		 * NO_DATA both indicate that the SRV RRset simply doesn't exist.
		 */
		const char *reason;

		switch (h_errno)
		{
			case HOST_NOT_FOUND:
				reason = "host not found";
				break;
			case NO_DATA:
				reason = "no SRV records published";
				break;
			case TRY_AGAIN:
				reason = "temporary DNS failure, try again";
				break;
			default:
				reason = hstrerror(h_errno);
				break;
		}
		libpq_append_conn_error(conn,
								"DNS SRV lookup failed for \"%s\": %s",
								qname, reason);
		return false;
	}

	if (len < HFIXEDSZ)
	{
		libpq_append_conn_error(conn,
								"DNS response too short for \"%s\"", qname);
		return false;
	}

	eom = answer + len;

	/*
	 * Parse the DNS response header (RFC 1035 §4.1.1):
	 *   bytes 4-5: QDCOUNT, bytes 6-7: ANCOUNT
	 */
	qdcount = (answer[4] << 8) | answer[5];
	ancount = (answer[6] << 8) | answer[7];
	cp = answer + HFIXEDSZ;

	/* Skip the question section */
	for (int i = 0; i < qdcount; i++)
	{
		int			n = srv_skip_dname(eom, cp);

		if (n < 0)
			goto parse_error;
		cp += n + 4;			/* name + QTYPE + QCLASS */
		if (cp > eom)
			goto parse_error;
	}

	/* Parse the answer section */
	for (int i = 0; i < ancount; i++)
	{
		int			n;
		uint16_t	rtype,
					rdlen;
		char		target[NI_MAXHOST];
		size_t		tlen;

		n = srv_skip_dname(eom, cp);
		if (n < 0)
			goto parse_error;
		cp += n;

		/* Need TYPE(2) + CLASS(2) + TTL(4) + RDLENGTH(2) = 10 bytes */
		if (cp + 10 > eom)
			goto parse_error;

		rtype = (cp[0] << 8) | cp[1];
		rdlen = (cp[8] << 8) | cp[9];
		cp += 10;

		if (cp + rdlen > eom)
			goto parse_error;

		if (rtype == T_SRV)
		{
			/* SRV RDATA: priority(2) weight(2) port(2) target(variable) */
			if (rdlen < 7)
				goto parse_error;

			n = dn_expand(answer, eom, cp + 6, target, sizeof(target));
			if (n < 0)
				goto parse_error;

			/* Strip trailing dot from FQDN */
			tlen = strlen(target);
			if (tlen > 0 && target[tlen - 1] == '.')
				target[tlen - 1] = '\0';

			if (nrecords >= maxrecords)
			{
				SRVRecord  *tmp;

				maxrecords = maxrecords ? maxrecords * 2 : 4;
				tmp = realloc(records, maxrecords * sizeof(SRVRecord));
				if (!tmp)
				{
					libpq_append_conn_error(conn, "out of memory");
					goto cleanup;
				}
				records = tmp;
			}

			records[nrecords].priority = (cp[0] << 8) | cp[1];
			records[nrecords].weight = (cp[2] << 8) | cp[3];
			records[nrecords].port = (cp[4] << 8) | cp[5];
			strlcpy(records[nrecords].target, target,
					sizeof(records[nrecords].target));
			nrecords++;
		}

		cp += rdlen;
	}

	if (nrecords == 0)
	{
		libpq_append_conn_error(conn,
								"no SRV records found for \"%s\"", qname);
		goto cleanup;
	}

	qsort(records, nrecords, sizeof(SRVRecord), compareSRVRecords);

	retval = srv_build_host_port(conn, records, nrecords);
	goto cleanup;

parse_error:
	libpq_append_conn_error(conn, "malformed DNS response for \"%s\"", qname);

cleanup:
	free(records);
	return retval;
}

#else							/* no SRV support on this platform */

bool
pqLookupSRVHosts(PGconn *conn)
{
	libpq_append_conn_error(conn,
							"\"srvhost\" is not supported on this platform "
							"(DNS SRV lookup requires res_query)");
	return false;
}

#endif							/* platform selection */
