/*-------------------------------------------------------------------------
 *
 *	libpq-trace.c
 *	  functions for libpq protocol tracing
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/interfaces/libpq/libpq-trace.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <limits.h>
#include <time.h>

#ifdef WIN32
#include "win32.h"
#else
#include <unistd.h>
#include <sys/time.h>
#endif

#include "libpq-fe.h"
#include "libpq-int.h"
#include "pgtime.h"
#include "port/pg_bswap.h"

void
PQtrace(PGconn *conn, FILE *debug_port)
{
	if (conn == NULL)
		return;
	PQuntrace(conn);
	if (debug_port == NULL)
		return;

	setvbuf(debug_port, NULL, _IOLBF, 0);
	conn->Pfdebug = debug_port;
	conn->traceFlags = 0;
}

void
PQuntrace(PGconn *conn)
{
	if (conn == NULL)
		return;
	if (conn->Pfdebug)
	{
		fflush(conn->Pfdebug);
		conn->Pfdebug = NULL;
	}

	conn->traceFlags = 0;
}

void
PQtraceSetFlags(PGconn *conn, int flags)
{
	if (conn == NULL)
		return;
	/* If PQtrace() failed, do nothing. */
	if (conn->Pfdebug == NULL)
		return;
	conn->traceFlags = flags;
}

/*
 * Print the current time, with microseconds, into a caller-supplied
 * buffer.
 * Cribbed from setup_formatted_log_time, but much simpler.
 */
static void
pqTraceFormatTimestamp(char *timestr, size_t ts_len)
{
	struct timeval tval;
	pg_time_t	stamp_time;

	gettimeofday(&tval, NULL);
	stamp_time = (pg_time_t) tval.tv_sec;

	strftime(timestr, ts_len,
			 "%Y-%m-%d %H:%M:%S",
			 localtime(&stamp_time));
	/* append microseconds */
	sprintf(timestr + strlen(timestr), ".%06d", (int) (tval.tv_usec));
}

/*
 *   pqTraceOutputByte1: output 1 char message to the log
 */
static void
pqTraceOutputByte1(FILE *pfdebug, const char *data, int *cursor)
{
	const char *v = data + *cursor;

	/*
	 * Show non-printable data in hex format, including the
	 * terminating \0 that completes ErrorResponse and NoticeResponse
	 * messages.
	 */
	if (!isprint(*v))
		fprintf(pfdebug, " \\x%02x", *v);
	else
		fprintf(pfdebug, " %c", *v);
	*cursor += 1;
}

/*
 *   pqTraceOutputInt16: output a 2-byte integer message to the log
 */
static int
pqTraceOutputInt16(FILE *pfdebug, const char *data, int *cursor)
{
	uint16		tmp;
	int			result;

	memcpy(&tmp, data + *cursor , 2);
	*cursor += 2;
	result = (int) pg_ntoh16(tmp);
	fprintf(pfdebug, " %d", result);

	return result;
}

/*
 *   pqTraceOutputInt32: output a 4-byte integer message to the log
 */
static int
pqTraceOutputInt32(FILE *pfdebug, const char *data, int *cursor)
{
	int			result;

	memcpy(&result, data + *cursor, 4);
	*cursor += 4;
	result = (int) pg_ntoh32(result);
	fprintf(pfdebug, " %d", result);

	return result;
}

/*
 *   pqTraceOutputString: output a string message to the log
 */
static void
pqTraceOutputString(FILE *pfdebug, const char *data, int *cursor)
{
	int	len;

	len = fprintf(pfdebug, " \"%s\"", data + *cursor);

	/*
	 * This is null-terminated string. So add 1 after subtracting 3
	 * which is the double quotes and space length from len.
	 */
	*cursor += (len - 3 + 1);
}

/*
 * pqTraceOutputNchar: output a string of exactly len bytes message to the log
 */
static void
pqTraceOutputNchar(FILE *pfdebug, int len, const char *data, int *cursor)
{
	int			i,
				next;			/* first char not yet printed */
	const char	*v = data + *cursor;

	fprintf(pfdebug, " \'");

	for (next = i = 0; i < len; ++i)
	{
		if (isprint(v[i]))
			continue;
		else
		{
			fwrite(v + next, 1, i - next, pfdebug);
			fprintf(pfdebug, "\\x%02x", v[i]);
			next = i + 1;
		}
	}
	if (next < len)
		fwrite(v + next, 1, len - next, pfdebug);

	fprintf(pfdebug, "\'");
	*cursor += len;
}

/*
 * Output functions by protocol message type
 */

/* Authentication */
static void
pqTraceOutputR(FILE *f, const char *message, int *cursor)
{
	fprintf(f, "Authentication\t");
	pqTraceOutputInt32(f, message, cursor);
}

/* BackendKeyData */
static void
pqTraceOutputK(FILE *f, int traceFlags, const char *message, int *cursor)
{
	fprintf(f, "BackendKeyData\t");
	if (traceFlags && PQTRACE_REGRESS_MODE)
		*cursor += 4;
	else
		pqTraceOutputInt32(f, message, cursor);

	if (traceFlags && PQTRACE_REGRESS_MODE)
		*cursor += 4;
	else
		pqTraceOutputInt32(f, message, cursor);
}

/* Bind */
static void
pqTraceOutputB(FILE *f, const char *message, int *cursor)
{
	int nparams;

	fprintf(f, "Bind\t");
	pqTraceOutputString(f, message, cursor);
	pqTraceOutputString(f, message, cursor);
	nparams = pqTraceOutputInt16(f, message, cursor);

	for (int i = 0; i < nparams; i++)
		pqTraceOutputInt16(f, message, cursor);

	nparams = pqTraceOutputInt16(f, message, cursor);

	for (int i = 0; i < nparams; i++)
	{
		int nbytes;

		nbytes = pqTraceOutputInt32(f, message, cursor);
		if (nbytes == -1)
			continue;
		pqTraceOutputNchar(f, nbytes, message, cursor);
	}

	nparams = pqTraceOutputInt16(f, message, cursor);
	for (int i = 0; i < nparams; i++)
		pqTraceOutputInt16(f, message, cursor);
}

/* Close(F) or CommandComplete(B) */
static void
pqTraceOutputC(FILE *f, bool toServer, const char *message, int *cursor)
{
	if (toServer)
	{
		fprintf(f, "Close\t");
		pqTraceOutputByte1(f, message, cursor);
		pqTraceOutputString(f, message, cursor);
	}
	else
	{
		fprintf(f, "CommandComplete\t");
		pqTraceOutputString(f, message, cursor);
	}
}

/* CopyFail */
static void
pqTraceOutputf(FILE *f, const char *message, int *cursor)
{
	fprintf(f, "CopyFail\t");
	pqTraceOutputString(f, message, cursor);
}

/* CopyInResponse */
static void
pqTraceOutputG(FILE *f, const char *message, int *cursor)
{
	int	nfields;

	fprintf(f, "CopyInResponse\t");
	pqTraceOutputByte1(f, message, cursor);
	nfields = pqTraceOutputInt16(f, message, cursor);

	for (int i = 0; i < nfields; i++)
		pqTraceOutputInt16(f, message, cursor);
}

/* CopyOutResponse */
static void
pqTraceOutputH(FILE *f, const char *message, int *cursor)
{
	int	nfields;

	fprintf(f, "CopyOutResponse\t");
	pqTraceOutputByte1(f, message, cursor);
	nfields = pqTraceOutputInt16(f, message, cursor);

	for (int i = 0; i < nfields; i++)
		pqTraceOutputInt16(f, message, cursor);
}

/* CopyBothResponse */
static void
pqTraceOutputW(FILE *f, const char *message, int *cursor, int length)
{
	fprintf(f, "CopyBothResponse\t");
	pqTraceOutputByte1(f, message, cursor);

	while (length > *cursor)
		pqTraceOutputInt16(f, message, cursor);
}

/* Describe(F) or DataRow(B) */
static void
pqTraceOutputD(FILE *f, bool toServer, const char *message, int *cursor)
{
	if (toServer)
	{
		fprintf(f, "Describe\t");
		pqTraceOutputByte1(f, message, cursor);
		pqTraceOutputString(f, message, cursor);
	}
	else
	{
		int		nfields;
		int		len;
		int		i;

		fprintf(f, "DataRow\t");
		nfields = pqTraceOutputInt16(f, message, cursor);
		for (i = 0; i < nfields; i++)
		{
			len = pqTraceOutputInt32(f, message, cursor);
			if (len == -1)
				continue;
			pqTraceOutputNchar(f, len, message, cursor);
		}
	}
}

/* Execute(F) or ErrorResponse(B) */
static void
pqTraceOutputE(FILE *f, int traceFlags, bool toServer, const char *message, int *cursor, int length)
{
	if (toServer)
	{
		fprintf(f, "Execute\t");
		pqTraceOutputString(f, message, cursor);
		pqTraceOutputInt32(f, message, cursor);
	}
	else
	{
		fprintf(f, "ErrorResponse\t");
		for (;;)
		{
			pqTraceOutputByte1(f, message, cursor);
			if (message[*cursor - 1] == '\0')
				break;
			if (message[*cursor - 1] == 'L' && traceFlags && PQTRACE_REGRESS_MODE)
				*cursor += strlen(message + *cursor) + 1;
			else
				pqTraceOutputString(f, message, cursor);
		}
	}
}

/* FunctionCall */
static void
pqTraceOutputF(FILE *f, int traceFlags, const char *message, int *cursor)
{
	int nfields;
	int nbytes;

	fprintf(f, "FunctionCall\t");
	if (traceFlags && PQTRACE_REGRESS_MODE)
		*cursor += 4;
	else
		pqTraceOutputInt32(f, message, cursor);
	nfields = pqTraceOutputInt16(f, message, cursor);

	for (int i = 0; i < nfields; i++)
		pqTraceOutputInt16(f, message, cursor);

	nfields = pqTraceOutputInt16(f, message, cursor);

	for (int i = 0; i < nfields; i++)
	{
		nbytes = pqTraceOutputInt32(f, message, cursor);
		if (nbytes == -1)
			continue;
		pqTraceOutputNchar(f, nbytes, message, cursor);
	}

	pqTraceOutputInt16(f, message, cursor);
}

/* FunctionCallResponse */
static void
pqTraceOutputV(FILE *f, const char *message, int *cursor)
{
	int		len;

	fprintf(f, "FunctionCallResponse\t");
	len = pqTraceOutputInt32(f, message, cursor);
	if (len != -1)
		pqTraceOutputNchar(f, len, message, cursor);
}

/* NegotiateProtocolVersion */
static void
pqTraceOutputv(FILE *f, const char *message, int *cursor)
{
	fprintf(f, "NegotiateProtocolVersion\t");
	pqTraceOutputInt32(f, message, cursor);
	pqTraceOutputInt32(f, message, cursor);
}

/* NoticeResponse */
static void
pqTraceOutputN(FILE *f, int traceFlags, const char *message, int *cursor, int length)
{
	fprintf(f, "NoticeResponse\t");
	for (;;)
	{
		pqTraceOutputByte1(f, message, cursor);
		if (message[*cursor - 1] == '\0')
			break;
		if (message[*cursor - 1] == 'L' && traceFlags && PQTRACE_REGRESS_MODE)
			*cursor += strlen(message + *cursor) + 1;
		else
			pqTraceOutputString(f, message, cursor);
	}
}

/* NotificationResponse */
static void
pqTraceOutputA(FILE *f, int traceFlags, const char *message, int *cursor)
{
	fprintf(f, "NotificationResponse\t");
	if (traceFlags && PQTRACE_REGRESS_MODE)
		*cursor += 4;
	else
		pqTraceOutputInt32(f, message, cursor);
	pqTraceOutputString(f, message, cursor);
	pqTraceOutputString(f, message, cursor);
}

/* ParameterDescription */
static void
pqTraceOutputt(FILE *f, int traceFlags, const char *message, int *cursor)
{
	int	nfields;

	fprintf(f, "ParameterDescription\t");
	nfields = pqTraceOutputInt16(f, message, cursor);

	for (int i = 0; i < nfields; i++)
	{
		if (traceFlags && PQTRACE_REGRESS_MODE)
			*cursor += 4;
		else
			pqTraceOutputInt32(f, message, cursor);
	}
}

/* ParameterStatus */
static void
pqTraceOutputS(FILE *f, const char *message, int *cursor)
{
	fprintf(f, "ParameterStatus\t");
	pqTraceOutputString(f, message, cursor);
	pqTraceOutputString(f, message, cursor);
}

/* Parse */
static void
pqTraceOutputP(FILE *f, int traceFlags, const char *message, int *cursor)
{
	int nparams;

	fprintf(f, "Parse\t");
	pqTraceOutputString(f, message, cursor);
	pqTraceOutputString(f, message, cursor);
	nparams = pqTraceOutputInt16(f, message, cursor);

	for (int i = 0; i < nparams; i++)
	{
		if (traceFlags && PQTRACE_REGRESS_MODE)
			*cursor += 4;
		else
			pqTraceOutputInt32(f, message, cursor);
	}
}

/* Query */
static void
pqTraceOutputQ(FILE *f, const char *message, int *cursor)
{
	fprintf(f, "Query\t");
	pqTraceOutputString(f, message, cursor);
}

/* ReadyForQuery */
static void
pqTraceOutputZ(FILE *f, const char *message, int *cursor)
{
	fprintf(f, "ReadyForQuery\t");
	pqTraceOutputByte1(f, message, cursor);
}

/* RowDescription */
static void
pqTraceOutputT(FILE *f, int traceFlags, const char *message, int *cursor)
{
	int nfields;

	fprintf(f, "RowDescription\t");
	nfields = pqTraceOutputInt16(f, message, cursor);

	for (int i = 0; i < nfields; i++)
	{
		pqTraceOutputString(f, message, cursor);
		if (traceFlags && PQTRACE_REGRESS_MODE)
			*cursor += 4;
		else
			pqTraceOutputInt32(f, message, cursor);
		pqTraceOutputInt16(f, message, cursor);
		if (traceFlags && PQTRACE_REGRESS_MODE)
			*cursor += 4;
		else
		pqTraceOutputInt32(f, message, cursor);
		pqTraceOutputInt16(f, message, cursor);
		pqTraceOutputInt32(f, message, cursor);
		pqTraceOutputInt16(f, message, cursor);
	}
}

void
pqTraceOutputMessage(PGconn *conn, const char *message, bool toServer)
{
	char		timestr[128];
	char 		id;
	int			length;
	char	   *prefix = toServer ? "F" : "B";
	int			LogCursor = 0;

	if ((conn->traceFlags & PQTRACE_REGRESS_MODE) == 0)
	{
		pqTraceFormatTimestamp(timestr, sizeof(timestr));
		fprintf(conn->Pfdebug, "%s\t", timestr);
	}

	id = message[LogCursor++];

	memcpy(&length, message + LogCursor, 4);
	length = (int) pg_ntoh32(length);
	LogCursor += 4;

	fprintf(conn->Pfdebug, "%s\t%d\t", prefix, length);

	switch(id)
	{
		case 'R':	/* Authentication */
			pqTraceOutputR(conn->Pfdebug, message, &LogCursor);
			break;
		case 'K':	/* secret key data from the backend */
			pqTraceOutputK(conn->Pfdebug, conn->traceFlags, message, &LogCursor);
			break;
		case 'B':	/* Bind */
			pqTraceOutputB(conn->Pfdebug, message, &LogCursor);
			break;
		case 'C':	/* Close(F) or Command Complete(B) */
			pqTraceOutputC(conn->Pfdebug, toServer, message, &LogCursor);
			break;
		case 'd':	/* Copy Data */
			/* Drop COPY data to reduce the overhead of logging. */
			break;
		case 'f':	/* Copy Fail */
			pqTraceOutputf(conn->Pfdebug, message, &LogCursor);
			break;
		case 'G':	/* Start Copy In */
			pqTraceOutputG(conn->Pfdebug, message, &LogCursor);
			break;
		case 'H':	/* Flush(F) or Start Copy Out(B) */
			if (!toServer)
				pqTraceOutputH(conn->Pfdebug, message, &LogCursor);
			else
				fprintf(conn->Pfdebug, "Flush");
			break;
		case 'W':	/* Start Copy Both */
			pqTraceOutputW(conn->Pfdebug, message, &LogCursor, length);
			break;
		case 'D':	/* Describe(F) or Data Row(B) */
			pqTraceOutputD(conn->Pfdebug, toServer, message, &LogCursor);
			break;
		case 'E':	/* Execute(F) or Error Response(B) */
			pqTraceOutputE(conn->Pfdebug, conn->traceFlags, toServer, message, &LogCursor, length);
			break;
		case 'F':	/* Function Call */
			pqTraceOutputF(conn->Pfdebug, conn->traceFlags, message, &LogCursor);
			break;
		case 'V':	/* Function Call response */
			pqTraceOutputV(conn->Pfdebug, message, &LogCursor);
			break;
		case 'v':	/* Negotiate Protocol Version */
			pqTraceOutputv(conn->Pfdebug, message, &LogCursor);
			break;
		case 'N':	/* Notice Response */
			pqTraceOutputN(conn->Pfdebug, conn->traceFlags, message, &LogCursor, length);
			break;
		case 'A':	/* Notification Response */
			pqTraceOutputA(conn->Pfdebug, conn->traceFlags, message, &LogCursor);
			break;
		case 't':	/* Parameter Description */
			pqTraceOutputt(conn->Pfdebug, conn->traceFlags, message, &LogCursor);
			break;
		case 'S':	/* Parameter Status(B) or Sync(F) */
			if (!toServer)
				pqTraceOutputS(conn->Pfdebug, message, &LogCursor);
			else
				fprintf(conn->Pfdebug, "Sync");
			break;
		case 'P':	/* Parse */
			pqTraceOutputP(conn->Pfdebug, conn->traceFlags, message, &LogCursor);
			break;
		case 'Q':	/* Query */
			pqTraceOutputQ(conn->Pfdebug, message, &LogCursor);
			break;
		case 'Z':	/* Ready For Query */
			pqTraceOutputZ(conn->Pfdebug, message, &LogCursor);
			break;
		case 'T':	/* Row Description */
			pqTraceOutputT(conn->Pfdebug, conn->traceFlags, message, &LogCursor);
			break;
		case '2':	/* Bind Complete */
			fprintf(conn->Pfdebug, "BindComplete");
			/* No message content */
			break;
		case '3':	/* Close Complete */
			fprintf(conn->Pfdebug, "CloseComplete");
			/* No message content */
			break;
		case 'c':	/* Copy Done */
			fprintf(conn->Pfdebug, "CopyDone");
			/* No message content */
			break;
		case 'I':	/* Empty Query Response */
			fprintf(conn->Pfdebug, "EmptyQueryResponse");
			/* No message content */
			break;
		case 'n':	/* No Data */
			fprintf(conn->Pfdebug, "NoData");
			/* No message content */
			break;
		case '1':	/* Parse Complete */
			fprintf(conn->Pfdebug, "ParseComplete");
			/* No message content */
			break;
		case 's':	/* Portal Suspended */
			fprintf(conn->Pfdebug, "PortalSuspended");
			/* No message content */
			break;
		case 'X':	/* Terminate */
			fprintf(conn->Pfdebug, "Terminate");
			/* No message content */
			break;
		default:
			fprintf(conn->Pfdebug, "Unknown message: %02x", id);
			break;
	}

	fputc('\n', conn->Pfdebug);

	/*
	 * Verify the printing routine did it right.  Note that the one-byte
	 * message identifier is not included in the length, but our cursor
	 * does include it.
	 */
	if (LogCursor - 1 != length)
		fprintf(conn->Pfdebug,
				"mismatched message length: consumed %d, expected %d\n",
				LogCursor - 1, length);
}

void
pqTraceOutputNoTypeByteMessage(PGconn *conn, const char *message)
{
	char		timestr[128];
	int			length;
	int			LogCursor = 0;

	if ((conn->traceFlags & PQTRACE_REGRESS_MODE) == 0)
		pqTraceFormatTimestamp(timestr, sizeof(timestr));
	else
		timestr[0] = '\0';

	memcpy(&length, message + LogCursor , 4);
	length = (int) pg_ntoh32(length);
	LogCursor += 4;

	fprintf(conn->Pfdebug, "%s\t>\t%d\t", timestr, length);

	switch(length)
	{
		case 16:	/* CancelRequest */
			fprintf(conn->Pfdebug, "CancelRequest\t");
			pqTraceOutputInt32(conn->Pfdebug, message, &LogCursor);
			pqTraceOutputInt32(conn->Pfdebug, message, &LogCursor);
			pqTraceOutputInt32(conn->Pfdebug, message, &LogCursor);
			break;
		case 8 :	/* GSSENCRequest or SSLRequest */
			/* These messages do not reach here. */
		default:
			fprintf(conn->Pfdebug, "Unknown message: length is %d", length);
			break;
	}

	fputc('\n', conn->Pfdebug);
}
