/*-------------------------------------------------------------------------
 *
 *	 FILE
 *		fe-misc.c
 *
 *	 DESCRIPTION
 *		 miscellaneous useful functions
 *
 * The communication routines here are analogous to the ones in
 * backend/libpq/pqcomm.c and backend/libpq/pqformat.c, but operate
 * in the considerably different environment of the frontend libpq.
 * In particular, we work with a bare nonblock-mode socket, rather than
 * a stdio stream, so that we can avoid unwanted blocking of the application.
 *
 * XXX: MOVE DEBUG PRINTOUT TO HIGHER LEVEL.  As is, block and restart
 * will cause repeat printouts.
 *
 * We must speak the same transmitted data representations as the backend
 * routines.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/interfaces/libpq/fe-misc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <signal.h>
#include <time.h>

#ifdef WIN32
#include "win32.h"
#else
#include <unistd.h>
#include <sys/time.h>
#endif

#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

#include "libpq-fe.h"
#include "libpq-int.h"
#include "mb/pg_wchar.h"
#include "pg_config_paths.h"
#include "pgtime.h"
#include "port/pg_bswap.h"

/* Log message source */
typedef enum
{
	MSGDIR_FROM_BACKEND,
	MSGDIR_FROM_FRONTEND
} PGCommSource;

/* Messages from backend */
typedef enum PGLogState
{
	LOG_FIRST_BYTE,				/* logging the first byte identifying the
								 * protocol message type */
	LOG_LENGTH,					/* logging protocol message length */
	LOG_CONTENTS				/* logging protocol message contents */
} PGLogState;

typedef struct pqBackendMessage
{
	PGLogState	state;			/* state of logging message state machine */
	int			length;			/* protocol message length */
	char		command;		/* first one byte of protocol message */
} pqBackendMessage;

/* Messages from frontend */
typedef enum
{
	LOG_BYTE1,
	LOG_STRING,
	LOG_NCHAR,
	LOG_INT16,
	LOG_INT32
} PGLogMsgDataType;

typedef struct pqFrontendMessageField
{
	PGLogMsgDataType type;
	int			offset_in_buffer;
	int			length;
} pqFrontendMessageField;

typedef struct pqFrontendMessage
{
	char		msg_type;
	int			num_fields;		/* array used size */
	int			max_fields;		/* array allocated size */
	pqFrontendMessageField fields[FLEXIBLE_ARRAY_MEMBER];
} pqFrontendMessage;
#define DEF_FE_MSGFIELDS 256	/* initial fields allocation quantum */
#define INT_MAX	(2048/2)		/* maximum array size */

/*
 * protocol message types:
 *
 * protocol_message_type_b[]: message types sent by a backend
 * protocol_message_type_f[]: message types sent by a frontend
 */
static const char *const protocol_message_type_b[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /* \x00 ... \x0f */
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /* \x10 ... \x1f */
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /* \x20 ... \x2f */
	0,							/* 0 */
	"ParseComplete",			/* 1 */
	"BindComplete",				/* 2 */
	"CloseComplete",			/* 3 */
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /* \x34 ... \x3f */
	0,							/* @ */
	"NotificationResponse",		/* A */
	0,							/* B */
	"CommandComplete",			/* C */
	"DataRow",					/* D */
	"ErrorResponse",			/* E */
	0,							/* F */
	"CopyInResponse",			/* G */
	"CopyOutResponse",			/* H */
	"EmptyQueryResponse",		/* I */
	0,							/* J */
	"BackendKeyData",			/* K */
	0,							/* L */
	0,							/* M */
	"NoticeResponse",			/* N */
	0,							/* O */
	0,							/* P */
	0,							/* Q */
	"Authentication",			/* R */
	"ParameterStatus",			/* S */
	"RowDescription",			/* T */
	0,							/* U */
	"FunctionCallResponse",		/* V */
	"CopyBothResponse",			/* W */
	0,							/* X */
	0,							/* Y */
	"ReadyForQuery",			/* Z */
	0, 0, 0, 0, 0,				/* \x5b ... \x5f */
	0,							/* ` */
	0,							/* a */
	0,							/* b */
	"CopyDone",					/* c */
	"CopyData",					/* d */
	0, 0, 0, 0, 0, 0, 0, 0,	0,	/* \x65 ... \0x6d */
	"NoData",					/* n */
	0,							/* o */
	0,							/* p */
	0,							/* q */
	0,							/* r */
	"PortalSuspended",			/* s */
	"ParameterDescription",		/* t */
	0,							/* u */
	"NegotiateProtocolVersion", /* v */
};

static const char *const protocol_message_type_f[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /* \x00 ... \x0f */
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /* \x10 ... \x1f */
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /* \x20 ... \x2f */
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /* \x30 ... \x3f */
	0,							/* @ */
	0,							/* A */
	"Bind",						/* B */
	"Close",					/* C */
	"Describe",					/* D */
	"Execute",					/* E */
	"FunctionCall",				/* F */
	0,							/* G */
	"Flush",					/* H */
	0, 0, 0, 0, 0, 0, 0,		/* I ... O */
	"Parse",					/* P */
	"Query",					/* Q */
	0,							/* R */
	"Sync",						/* S */
	0, 0, 0, 0,					/* T ... W */
	"Terminate",				/* X */
	0, 0, 0, 0, 0, 0, 0,		/* \x59 ... \x5f */
	0,							/* ` */
	0,							/* a */
	0,							/* b */
	"CopyDone",					/* c */
	"CopyData",					/* d */
	0,							/* e */
	"CopyFail",					/* f */
	0, 0, 0, 0, 0, 0, 0, 0, 0,	/* g ... o */
	"AuthenticationResponse",	/* p */
};

static int	pqPutMsgBytes(const void *buf, size_t len, PGconn *conn);
static int	pqSendSome(PGconn *conn, int len);
static int	pqSocketCheck(PGconn *conn, int forRead, int forWrite,
						  time_t end_time);
static int	pqSocketPoll(int sock, int forRead, int forWrite, time_t end_time);
static void pqStoreFrontendMsg(PGconn *conn, PGLogMsgDataType type, int length);
static void pqStoreFeMsgStart(PGconn *conn, char type);
static void pqLogFrontendMsg(PGconn *conn, int msgLen);
static void pqTraceMaybeBreakLine(int size, PGconn *conn);
static void pqLogMessageByte1(PGconn *conn, char v);
static void pqLogMessageInt(PGconn *conn, int v, int length);
static void pqLogMessageString(PGconn *conn, const char *v, int length,
							   PGCommSource commsource);
static void pqLogBinaryMsg(PGconn *conn, const char *v, int length,
						   PGCommSource commsource);
static void pqLogMessagenchar(PGconn *conn, const char *v, int length,
							  PGCommSource commsource);

/*
 * PQlibVersion: return the libpq version number
 */
int
PQlibVersion(void)
{
	return PG_VERSION_NUM;
}


/*
 * pqGetc: get 1 character from the connection
 *
 *	All these routines return 0 on success, EOF on error.
 *	Note that for the Get routines, EOF only means there is not enough
 *	data in the buffer, not that there is necessarily a hard error.
 */
int
pqGetc(char *result, PGconn *conn)
{
	if (conn->inCursor >= conn->inEnd)
		return EOF;

	*result = conn->inBuffer[conn->inCursor++];

	if (conn->Pfdebug)
		pqLogMessageByte1(conn, *result);

	return 0;
}


/*
 * pqPutc: write 1 char to the current message
 */
int
pqPutc(char c, PGconn *conn)
{
	if (pqPutMsgBytes(&c, 1, conn))
		return EOF;

	if (conn->Pfdebug)
		pqStoreFrontendMsg(conn, LOG_BYTE1, 1);

	return 0;
}


/*
 * pqGets[_append]:
 * get a null-terminated string from the connection,
 * and store it in an expansible PQExpBuffer.
 * If we run out of memory, all of the string is still read,
 * but the excess characters are silently discarded.
 */
static int
pqGets_internal(PQExpBuffer buf, PGconn *conn, bool resetbuffer)
{
	/* Copy conn data to locals for faster search loop */
	char	   *inBuffer = conn->inBuffer;
	int			inCursor = conn->inCursor;
	int			inEnd = conn->inEnd;
	int			slen;

	while (inCursor < inEnd && inBuffer[inCursor])
		inCursor++;

	if (inCursor >= inEnd)
		return EOF;

	slen = inCursor - conn->inCursor;

	if (resetbuffer)
		resetPQExpBuffer(buf);

	appendBinaryPQExpBuffer(buf, inBuffer + conn->inCursor, slen);

	conn->inCursor = ++inCursor;

	if (conn->Pfdebug)
		pqLogMessageString(conn, buf->data, buf->len + 1, MSGDIR_FROM_BACKEND);

	return 0;
}

int
pqGets(PQExpBuffer buf, PGconn *conn)
{
	return pqGets_internal(buf, conn, true);
}

int
pqGets_append(PQExpBuffer buf, PGconn *conn)
{
	return pqGets_internal(buf, conn, false);
}

/*
 * pqPuts: write a null-terminated string to the current message
 */
int
pqPuts(const char *s, PGconn *conn)
{
	int			length = strlen(s) + 1;

	if (pqPutMsgBytes(s, length, conn))
		return EOF;

	if (conn->Pfdebug)
		pqStoreFrontendMsg(conn, LOG_STRING, length);

	return 0;
}

/*
 * pqGetnchar:
 *	get a string of exactly len bytes in buffer s, no null termination
 */
int
pqGetnchar(char *s, size_t len, PGconn *conn)
{
	if (len > (size_t) (conn->inEnd - conn->inCursor))
		return EOF;

	memcpy(s, conn->inBuffer + conn->inCursor, len);
	/* no terminating null */

	conn->inCursor += len;

	if (conn->Pfdebug)
		pqLogMessagenchar(conn, s, len, MSGDIR_FROM_BACKEND);

	return 0;
}

/*
 * pqSkipnchar:
 *	skip over len bytes in input buffer.
 *
 * Note: this is primarily useful for its debug output, which should
 * be exactly the same as for pqGetnchar.  We assume the data in question
 * will actually be used, but just isn't getting copied anywhere as yet.
 */
int
pqSkipnchar(size_t len, PGconn *conn)
{
	if (len > (size_t) (conn->inEnd - conn->inCursor))
		return EOF;

	if (conn->Pfdebug)
		pqLogMessagenchar(conn, conn->inBuffer + conn->inCursor, len,
						  MSGDIR_FROM_BACKEND);
	conn->inCursor += len;

	return 0;
}

/*
 * pqPutnchar:
 *	write exactly len bytes to the current message
 */
int
pqPutnchar(const char *s, size_t len, PGconn *conn)
{
	if (pqPutMsgBytes(s, len, conn))
		return EOF;

	if (conn->Pfdebug)
		pqStoreFrontendMsg(conn, LOG_NCHAR, len);

	return 0;
}

/*
 * pqGetInt
 *	read a 2 or 4 byte integer and convert from network byte order
 *	to local byte order
 */
int
pqGetInt(int *result, size_t bytes, PGconn *conn)
{
	uint16		tmp2;
	uint32		tmp4;

	switch (bytes)
	{
		case 2:
			if (conn->inCursor + 2 > conn->inEnd)
				return EOF;
			memcpy(&tmp2, conn->inBuffer + conn->inCursor, 2);
			conn->inCursor += 2;
			*result = (int) pg_ntoh16(tmp2);
			break;
		case 4:
			if (conn->inCursor + 4 > conn->inEnd)
				return EOF;
			memcpy(&tmp4, conn->inBuffer + conn->inCursor, 4);
			conn->inCursor += 4;
			*result = (int) pg_ntoh32(tmp4);
			break;
		default:
			pqInternalNotice(&conn->noticeHooks,
							 "integer of size %lu not supported by pqGetInt",
							 (unsigned long) bytes);
			return EOF;
	}

	if (conn->Pfdebug)
		pqLogMessageInt(conn, *result, (unsigned int) bytes);

	return 0;
}

/*
 * pqPutInt
 * write an integer of 2 or 4 bytes, converting from host byte order
 * to network byte order.
 */
int
pqPutInt(int value, size_t bytes, PGconn *conn)
{
	uint16		tmp2;
	uint32		tmp4;
	PGLogMsgDataType type;

	switch (bytes)
	{
		case 2:
			type = LOG_INT16;
			tmp2 = pg_hton16((uint16) value);
			if (pqPutMsgBytes((const char *) &tmp2, 2, conn))
				return EOF;
			break;
		case 4:
			type = LOG_INT32;
			tmp4 = pg_hton32((uint32) value);
			if (pqPutMsgBytes((const char *) &tmp4, 4, conn))
				return EOF;
			break;
		default:
			pqInternalNotice(&conn->noticeHooks,
							 "integer of size %lu not supported by pqPutInt",
							 (unsigned long) bytes);
			return EOF;
	}

	if (conn->Pfdebug)
		pqStoreFrontendMsg(conn, type, (unsigned int) bytes);

	return 0;
}

/*
 * Make sure conn's output buffer can hold bytes_needed bytes (caller must
 * include already-stored data into the value!)
 *
 * Returns 0 on success, EOF if failed to enlarge buffer
 */
int
pqCheckOutBufferSpace(size_t bytes_needed, PGconn *conn)
{
	int			newsize = conn->outBufSize;
	char	   *newbuf;

	/* Quick exit if we have enough space */
	if (bytes_needed <= (size_t) newsize)
		return 0;

	/*
	 * If we need to enlarge the buffer, we first try to double it in size; if
	 * that doesn't work, enlarge in multiples of 8K.  This avoids thrashing
	 * the malloc pool by repeated small enlargements.
	 *
	 * Note: tests for newsize > 0 are to catch integer overflow.
	 */
	do
	{
		newsize *= 2;
	} while (newsize > 0 && bytes_needed > (size_t) newsize);

	if (newsize > 0 && bytes_needed <= (size_t) newsize)
	{
		newbuf = realloc(conn->outBuffer, newsize);
		if (newbuf)
		{
			/* realloc succeeded */
			conn->outBuffer = newbuf;
			conn->outBufSize = newsize;
			return 0;
		}
	}

	newsize = conn->outBufSize;
	do
	{
		newsize += 8192;
	} while (newsize > 0 && bytes_needed > (size_t) newsize);

	if (newsize > 0 && bytes_needed <= (size_t) newsize)
	{
		newbuf = realloc(conn->outBuffer, newsize);
		if (newbuf)
		{
			/* realloc succeeded */
			conn->outBuffer = newbuf;
			conn->outBufSize = newsize;
			return 0;
		}
	}

	/* realloc failed. Probably out of memory */
	appendPQExpBufferStr(&conn->errorMessage,
						 "cannot allocate memory for output buffer\n");
	return EOF;
}

/*
 * Make sure conn's input buffer can hold bytes_needed bytes (caller must
 * include already-stored data into the value!)
 *
 * Returns 0 on success, EOF if failed to enlarge buffer
 */
int
pqCheckInBufferSpace(size_t bytes_needed, PGconn *conn)
{
	int			newsize = conn->inBufSize;
	char	   *newbuf;

	/* Quick exit if we have enough space */
	if (bytes_needed <= (size_t) newsize)
		return 0;

	/*
	 * Before concluding that we need to enlarge the buffer, left-justify
	 * whatever is in it and recheck.  The caller's value of bytes_needed
	 * includes any data to the left of inStart, but we can delete that in
	 * preference to enlarging the buffer.  It's slightly ugly to have this
	 * function do this, but it's better than making callers worry about it.
	 */
	bytes_needed -= conn->inStart;

	if (conn->inStart < conn->inEnd)
	{
		if (conn->inStart > 0)
		{
			memmove(conn->inBuffer, conn->inBuffer + conn->inStart,
					conn->inEnd - conn->inStart);
			conn->inEnd -= conn->inStart;
			conn->inCursor -= conn->inStart;
			conn->inStart = 0;
		}
	}
	else
	{
		/* buffer is logically empty, reset it */
		conn->inStart = conn->inCursor = conn->inEnd = 0;
	}

	/* Recheck whether we have enough space */
	if (bytes_needed <= (size_t) newsize)
		return 0;

	/*
	 * If we need to enlarge the buffer, we first try to double it in size; if
	 * that doesn't work, enlarge in multiples of 8K.  This avoids thrashing
	 * the malloc pool by repeated small enlargements.
	 *
	 * Note: tests for newsize > 0 are to catch integer overflow.
	 */
	do
	{
		newsize *= 2;
	} while (newsize > 0 && bytes_needed > (size_t) newsize);

	if (newsize > 0 && bytes_needed <= (size_t) newsize)
	{
		newbuf = realloc(conn->inBuffer, newsize);
		if (newbuf)
		{
			/* realloc succeeded */
			conn->inBuffer = newbuf;
			conn->inBufSize = newsize;
			return 0;
		}
	}

	newsize = conn->inBufSize;
	do
	{
		newsize += 8192;
	} while (newsize > 0 && bytes_needed > (size_t) newsize);

	if (newsize > 0 && bytes_needed <= (size_t) newsize)
	{
		newbuf = realloc(conn->inBuffer, newsize);
		if (newbuf)
		{
			/* realloc succeeded */
			conn->inBuffer = newbuf;
			conn->inBufSize = newsize;
			return 0;
		}
	}

	/* realloc failed. Probably out of memory */
	appendPQExpBuffer(&conn->errorMessage,
					  "cannot allocate memory for input buffer\n");
	return EOF;
}

/*
 * pqPutMsgStart: begin construction of a message to the server
 *
 * msg_type is the message type byte, or 0 for a message without type byte
 * (only startup messages have no type byte)
 *
 * force_len forces the message to have a length word; otherwise, we add
 * a length word if protocol 3.
 *
 * Returns 0 on success, EOF on error
 *
 * The idea here is that we construct the message in conn->outBuffer,
 * beginning just past any data already in outBuffer (ie, at
 * outBuffer+outCount).  We enlarge the buffer as needed to hold the message.
 * When the message is complete, we fill in the length word (if needed) and
 * then advance outCount past the message, making it eligible to send.
 *
 * The state variable conn->outMsgStart points to the incomplete message's
 * length word: it is either outCount or outCount+1 depending on whether
 * there is a type byte.  If we are sending a message without length word
 * (pre protocol 3.0 only), then outMsgStart is -1.  The state variable
 * conn->outMsgEnd is the end of the data collected so far.
 */
int
pqPutMsgStart(char msg_type, bool force_len, PGconn *conn)
{
	int			lenPos;
	int			endPos;

	/* allow room for message type byte */
	if (msg_type)
		endPos = conn->outCount + 1;
	else
		endPos = conn->outCount;

	/* do we want a length word? */
	if (force_len || PG_PROTOCOL_MAJOR(conn->pversion) >= 3)
	{
		lenPos = endPos;
		/* allow room for message length */
		endPos += 4;
	}
	else
		lenPos = -1;

	/* make sure there is room for message header */
	if (pqCheckOutBufferSpace(endPos, conn))
		return EOF;
	/* okay, save the message type byte if any */
	if (msg_type)
		conn->outBuffer[conn->outCount] = msg_type;
	/* set up the message pointers */
	conn->outMsgStart = lenPos;
	conn->outMsgEnd = endPos;
	/* length word, if needed, will be filled in by pqPutMsgEnd */

	if (conn->Pfdebug)
		pqStoreFeMsgStart(conn, msg_type);

	return 0;
}

/*
 * pqPutMsgBytes: add bytes to a partially-constructed message
 *
 * Returns 0 on success, EOF on error
 */
static int
pqPutMsgBytes(const void *buf, size_t len, PGconn *conn)
{
	/* make sure there is room for it */
	if (pqCheckOutBufferSpace(conn->outMsgEnd + len, conn))
		return EOF;
	/* okay, save the data */
	memcpy(conn->outBuffer + conn->outMsgEnd, buf, len);
	conn->outMsgEnd += len;
	/* no Pfdebug call here, caller should do it */
	return 0;
}

/*
 * pqPutMsgEnd: finish constructing a message and possibly send it
 *
 * Returns 0 on success, EOF on error
 *
 * We don't actually send anything here unless we've accumulated at least
 * 8K worth of data (the typical size of a pipe buffer on Unix systems).
 * This avoids sending small partial packets.  The caller must use pqFlush
 * when it's important to flush all the data out to the server.
 */
int
pqPutMsgEnd(PGconn *conn)
{
	/* Fill in length word if needed */
	if (conn->outMsgStart >= 0)
	{
		uint32		msgLen = conn->outMsgEnd - conn->outMsgStart;

		if (conn->Pfdebug)
			pqLogFrontendMsg(conn, msgLen);

		msgLen = pg_hton32(msgLen);
		memcpy(conn->outBuffer + conn->outMsgStart, &msgLen, 4);
	}

	/* Make message eligible to send */
	conn->outCount = conn->outMsgEnd;

	if (conn->outCount >= 8192)
	{
		int			toSend = conn->outCount - (conn->outCount % 8192);

		if (pqSendSome(conn, toSend) < 0)
			return EOF;
		/* in nonblock mode, don't complain if unable to send it all */
	}

	return 0;
}

/* -------------------------
 * FE/BE trace support
 *
 * We accumulate frontend message pieces in an array as the libpq code writes
 * them, and log the complete message when pqLogFrontendMsg is called.
 * For backend, we print the pieces as soon as we receive them from the server.
 * -------------------------
 */

/*
 * Set up state so that we can trace. NB -- this might be called multiple
 * times in a process; make sure it's idempotent.
 */
bool
pqTraceInit(PGconn *conn, int flags)
{
	conn->traceFlags = flags;

	if (conn->be_msg == NULL)
	{
		conn->be_msg = malloc(sizeof(pqBackendMessage));
		if (conn->be_msg == NULL)
			return false;
	}

	if (conn->fe_msg == NULL)
	{
		conn->fe_msg = malloc(offsetof(pqFrontendMessage, fields) +
							  DEF_FE_MSGFIELDS * sizeof(pqFrontendMessageField));
		if (conn->fe_msg == NULL)
		{
			free(conn->be_msg);
			/* NULL out for the case that fe_msg malloc fails */
			conn->be_msg = NULL;
			return false;
		}
		conn->fe_msg->max_fields = DEF_FE_MSGFIELDS;
	}

	conn->fe_msg->num_fields = 0;
	conn->be_msg->state = LOG_FIRST_BYTE;
	conn->be_msg->length = 0;
	conn->be_msg->command = '\0';

	return true;
}

/*
 * Deallocate FE/BE message tracking memory.  We only do this because
 * FE message can grow arbitrarily large, and skip it in the initial state,
 * because it's likely pointless.
 */
void
pqTraceUninit(PGconn *conn)
{
	if (conn->fe_msg &&
		conn->fe_msg->num_fields != DEF_FE_MSGFIELDS)
	{
		free(conn->fe_msg);
		conn->fe_msg = NULL;
	}
	if (conn->be_msg)
	{
		free(conn->be_msg);
		conn->be_msg = NULL;
	}
}

/*
 * pqGetProtocolMsgType:
 *		Get a protocol type from first byte identifier
 */
static const char *
pqGetProtocolMsgType(unsigned char c, PGCommSource commsource)
{
	if (commsource == MSGDIR_FROM_BACKEND &&
		c < lengthof(protocol_message_type_b) &&
		protocol_message_type_b[c] != NULL)
		return protocol_message_type_b[c];

	if (commsource == MSGDIR_FROM_FRONTEND &&
		c < lengthof(protocol_message_type_f) &&
		protocol_message_type_f[c] != NULL)
		return protocol_message_type_f[c];

	return "UnknownMessage";
}

/* pqTraceResetBeMsg: Initialize backend message */
static void
pqTraceResetBeMsg(PGconn *conn)
{
	conn->be_msg->state = LOG_FIRST_BYTE;
	conn->be_msg->length = 0;
}

/* pqLogInvalidProtocol: Print that the protocol message is invalid */
static void
pqLogInvalidProtocol(PGconn *conn, PGCommSource commsource)
{
	fprintf(conn->Pfdebug,
			"%s\t:::Invalid Protocol\n",
			commsource == MSGDIR_FROM_BACKEND ? "<" : ">");
	conn->be_msg->state = LOG_FIRST_BYTE;
}

/*
 * pqTraceMaybeBreakLine:
 *		Check whether the backend message is complete. If so, print a line
 *		break and reset the buffer.
 */
static void
pqTraceMaybeBreakLine(int size, PGconn *conn)
{
	conn->be_msg->length -= size;
	if (conn->be_msg->length <= 0)
	{
		fprintf(conn->Pfdebug, "\n");
		pqTraceResetBeMsg(conn);
	}
	else
		fprintf(conn->Pfdebug, " ");
}

/*
 * pqTraceForcelyBreakLine:
 * 		If message is not completed, print a line break and reset.
 */
void
pqTraceForcelyBreakLine(int size, PGconn *conn)
{
	fprintf(conn->Pfdebug, "\n");
	pqTraceResetBeMsg(conn);
}

static void
pqStoreFeMsgStart(PGconn *conn, char type)
{
	if (PG_PROTOCOL_MAJOR(conn->pversion) >= 3)
		conn->fe_msg->msg_type = type;
}

/*
 * pqStoreFrontendMsg
 *		Keep track of a from-frontend message that was just written to the
 *		output buffer.
 *
 * Frontend messages are constructed piece by piece, and the message length
 * is determined at the end, but sent to the server first; so for tracing
 * purposes we store everything in memory and print to the trace file when
 * the message is complete.
 */
static void
pqStoreFrontendMsg(PGconn *conn, PGLogMsgDataType type, int length)
{
	/* realloc if we've exceeded available space */
	if (conn->fe_msg->num_fields >= conn->fe_msg->max_fields)
	{
		if (conn->fe_msg->max_fields > INT_MAX)
		{
			fprintf(conn->Pfdebug, "abandoning trace: field message overflow\n");
			PQuntrace(conn);
		}
		conn->fe_msg =
			realloc(conn->fe_msg,
					sizeof(pqFrontendMessage) +
					2 * conn->fe_msg->max_fields * sizeof(pqFrontendMessageField));
		if (conn->fe_msg == NULL)
		{
			fprintf(conn->Pfdebug, "abandoning trace: out of memory\n");
			PQuntrace(conn);
		}
		conn->fe_msg->max_fields *= 2;
	}

	conn->fe_msg->fields[conn->fe_msg->num_fields].type = type;
	conn->fe_msg->fields[conn->fe_msg->num_fields].offset_in_buffer = conn->outMsgEnd - length;
	conn->fe_msg->fields[conn->fe_msg->num_fields].length = length;
	conn->fe_msg->num_fields++;
}

/*
 * Print the current time, with milliseconds, into a caller-supplied
 * buffer.  Used for PQtrace() purposes.
 * Cribbed from setup_formatted_log_time, but much simpler.
 */
static char *
pqLogFormatTimestamp(char *timestr)
{
	struct timeval tval;
	pg_time_t	stamp_time;

	gettimeofday(&tval, NULL);
	stamp_time = (pg_time_t) tval.tv_sec;

#define FORMATTED_TS_LEN 128
	strftime(timestr, FORMATTED_TS_LEN,
				"%Y-%m-%d %H:%M:%S",
				localtime(&stamp_time));
	/* append milliseconds */
	sprintf(timestr + strlen(timestr), ".%03d", (int) (tval.tv_usec / 1000));

	return timestr;
}

/*
 * pqLogFrontendMsg
 *		Print accumulated frontend message pieces to the trace file.
 */
static void
pqLogFrontendMsg(PGconn *conn, int msgLen)
{
	if ((conn->traceFlags & PQTRACE_SUPPRESS_TIMESTAMPS) == 0)
	{
		char		timestr[128];

		fprintf(conn->Pfdebug, "%s\t>\t%s\t%d",
				pqLogFormatTimestamp(timestr),
				pqGetProtocolMsgType(conn->fe_msg->msg_type,
									 MSGDIR_FROM_FRONTEND),
				msgLen);
	}
	else
		fprintf(conn->Pfdebug, ">\t%s\t%d",
				pqGetProtocolMsgType(conn->fe_msg->msg_type,
									 MSGDIR_FROM_FRONTEND),
				msgLen);

	for (int i = 0; i < conn->fe_msg->num_fields; i++)
	{
		int			message_addr;
		int			length;
		char		v;

		message_addr = conn->fe_msg->fields[i].offset_in_buffer;
		length = conn->fe_msg->fields[i].length;

		fprintf(conn->Pfdebug, " ");

		switch (conn->fe_msg->fields[i].type)
		{
			case LOG_BYTE1:
				v = *(conn->outBuffer + message_addr);

				if (isprint(v))
					fprintf(conn->Pfdebug, "%c", v);
				else
					fprintf(conn->Pfdebug, "\\x%02x", v);
				break;

			case LOG_STRING:
				pqLogMessageString(conn, conn->outBuffer + message_addr,
								   length, MSGDIR_FROM_FRONTEND);
				break;

			case LOG_NCHAR:
				pqLogMessagenchar(conn, conn->outBuffer + message_addr,
								  length, MSGDIR_FROM_FRONTEND);
				break;

			case LOG_INT16:
				{
					uint16		result16;

					memcpy(&result16, conn->outBuffer + message_addr, length);
					result16 = pg_ntoh16(result16);
					fprintf(conn->Pfdebug, "#%d", result16);
					break;
				}

			case LOG_INT32:
				{
					uint32		result32;

					memcpy(&result32, conn->outBuffer + message_addr, length);
					result32 = pg_ntoh32(result32);
					fprintf(conn->Pfdebug, "%d", result32);
					break;
				}
		}
	}
	conn->fe_msg->num_fields = 0;

	fprintf(conn->Pfdebug, "\n");
}

/*
 * pqLogMessageByte1: output 1 char from-backend message to the log
 */
static void
pqLogMessageByte1(PGconn *conn, char v)
{
	switch (conn->be_msg->state)
	{
		case LOG_FIRST_BYTE:
			if ((conn->traceFlags & PQTRACE_SUPPRESS_TIMESTAMPS) == 0)
			{
				char		timestr[128];

				fprintf(conn->Pfdebug, "%s\t<\t",
						pqLogFormatTimestamp(timestr));
			}
			else
				fprintf(conn->Pfdebug, "<\t");

			/*
				* True type of message tagged '\0' is known when next 4 bytes
				* is checked. So we delay printing message type to
				* pqLogMessageInt()
				*/
			if (v != '\0')
				fprintf(conn->Pfdebug, "%s\t",
						pqGetProtocolMsgType((unsigned char) v,
												MSGDIR_FROM_BACKEND));
			/* Next, log the message length */
			conn->be_msg->state = LOG_LENGTH;
			conn->be_msg->command = v;
			break;

		case LOG_CONTENTS:

			/*
				* Show non-printable data in hex format, including the
				* terminating \0 that completes ErrorResponse and
				* NoticeResponse messages.
				*/
			if (!isprint(v))
				fprintf(conn->Pfdebug, "\\x%02x", v);
			else
				fprintf(conn->Pfdebug, "%c", v);
			pqTraceMaybeBreakLine(1, conn);
			break;

		default:
			pqLogInvalidProtocol(conn, MSGDIR_FROM_BACKEND);
			break;
	}
}

/*
 * pqLogMessageInt: output a 2- or 4-byte integer from-backend msg to the log
 */
static void
pqLogMessageInt(PGconn *conn, int v, int length)
{
	char	   *prefix = length == 4 ? "" : "#";

	switch (conn->be_msg->state)
	{
		case LOG_LENGTH:
			if (conn->be_msg->command == '\0')
			{
				char	   *message_type;

				/*
					* We delayed printing message type for special messages;
					* they are complete now, so print them.
					*/
				if (conn->fe_msg->num_fields > 0)
				{
					int			message_addr;
					uint32		result32;
					int			result;

					message_addr = conn->fe_msg->fields[0].offset_in_buffer;
					memcpy(&result32, conn->outBuffer + message_addr, 4);
					result = (int) pg_ntoh32(result32);

					if (result == NEGOTIATE_SSL_CODE)
						message_type = "SSLRequest";
					else if (result == NEGOTIATE_GSS_CODE)
						message_type = "GSSRequest";
					else
						message_type = "StartupMessage";
				}
				else
					message_type = "UnknownMessage";
				fprintf(conn->Pfdebug, "%s ", message_type);
			}
			fprintf(conn->Pfdebug, "%d", v);
			conn->be_msg->length = v - length;
			/* Next, log the message contents */
			conn->be_msg->state = LOG_CONTENTS;
			pqTraceMaybeBreakLine(0, conn);
			break;

		case LOG_CONTENTS:
			fprintf(conn->Pfdebug, "%s%d", prefix, v);
			pqTraceMaybeBreakLine(length, conn);
			break;

		default:
			pqLogInvalidProtocol(conn, MSGDIR_FROM_BACKEND);
			break;
	}
}


/*
 * pqLogMessageString: output a null-terminated string to the log
 */
static void
pqLogMessageString(PGconn *conn, const char *v, int length, PGCommSource source)
{
	if (source == MSGDIR_FROM_BACKEND && conn->be_msg->state != LOG_CONTENTS)
	{
		pqLogInvalidProtocol(conn, MSGDIR_FROM_BACKEND);
		return;	/* XXX ??? */
	}

	fprintf(conn->Pfdebug, "\"%s\"", v);
	if (source == MSGDIR_FROM_BACKEND)
		pqTraceMaybeBreakLine(length, conn);
}

/*
 * pqLogBinaryMsg: output a string possibly consisting of non-printable
 * characters. Hex representation is used for such chars; others are
 * printed normally.
 */
static void
pqLogBinaryMsg(PGconn *conn, const char *v, int length, PGCommSource source)
{
	int			i,
				pin;

	if (source == MSGDIR_FROM_BACKEND && conn->be_msg->state != LOG_CONTENTS)
	{
		pqLogInvalidProtocol(conn, MSGDIR_FROM_BACKEND);
		return;	/* XXX ??? */
	}

	for (pin = i = 0; i < length; ++i)
	{
		if (isprint(v[i]))
			continue;
		else
		{
			fwrite(v + pin, 1, i - pin, conn->Pfdebug);
			fprintf(conn->Pfdebug, "\\x%02x", v[i]);
			pin = i + 1;
		}
	}
	if (pin < length)
		fwrite(v + pin, 1, length - pin, conn->Pfdebug);
}

/*
 * pqLogMessagenchar: output a string of exactly len bytes message to the log
 */
static void
pqLogMessagenchar(PGconn *conn, const char *v, int len, PGCommSource commsource)
{
	fprintf(conn->Pfdebug, "\'");
	pqLogBinaryMsg(conn, v, len, commsource);
	fprintf(conn->Pfdebug, "\'");
	pqTraceMaybeBreakLine(len, conn);
}

/* ----------
 * pqReadData: read more data, if any is available
 * Possible return values:
 *	 1: successfully loaded at least one more byte
 *	 0: no data is presently available, but no error detected
 *	-1: error detected (including EOF = connection closure);
 *		conn->errorMessage set
 * NOTE: callers must not assume that pointers or indexes into conn->inBuffer
 * remain valid across this call!
 * ----------
 */
int
pqReadData(PGconn *conn)
{
	int			someread = 0;
	int			nread;

	if (conn->sock == PGINVALID_SOCKET)
	{
		appendPQExpBufferStr(&conn->errorMessage,
							 libpq_gettext("connection not open\n"));
		return -1;
	}

	/* Left-justify any data in the buffer to make room */
	if (conn->inStart < conn->inEnd)
	{
		if (conn->inStart > 0)
		{
			memmove(conn->inBuffer, conn->inBuffer + conn->inStart,
					conn->inEnd - conn->inStart);
			conn->inEnd -= conn->inStart;
			conn->inCursor -= conn->inStart;
			conn->inStart = 0;
		}
	}
	else
	{
		/* buffer is logically empty, reset it */
		conn->inStart = conn->inCursor = conn->inEnd = 0;
	}

	/*
	 * If the buffer is fairly full, enlarge it. We need to be able to enlarge
	 * the buffer in case a single message exceeds the initial buffer size. We
	 * enlarge before filling the buffer entirely so as to avoid asking the
	 * kernel for a partial packet. The magic constant here should be large
	 * enough for a TCP packet or Unix pipe bufferload.  8K is the usual pipe
	 * buffer size, so...
	 */
	if (conn->inBufSize - conn->inEnd < 8192)
	{
		if (pqCheckInBufferSpace(conn->inEnd + (size_t) 8192, conn))
		{
			/*
			 * We don't insist that the enlarge worked, but we need some room
			 */
			if (conn->inBufSize - conn->inEnd < 100)
				return -1;		/* errorMessage already set */
		}
	}

	/* OK, try to read some data */
retry3:
	nread = pqsecure_read(conn, conn->inBuffer + conn->inEnd,
						  conn->inBufSize - conn->inEnd);
	if (nread < 0)
	{
		switch (SOCK_ERRNO)
		{
			case EINTR:
				goto retry3;

				/* Some systems return EAGAIN/EWOULDBLOCK for no data */
#ifdef EAGAIN
			case EAGAIN:
				return someread;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
			case EWOULDBLOCK:
				return someread;
#endif

				/* We might get ECONNRESET etc here if connection failed */
			case ALL_CONNECTION_FAILURE_ERRNOS:
				goto definitelyFailed;

			default:
				/* pqsecure_read set the error message for us */
				return -1;
		}
	}
	if (nread > 0)
	{
		conn->inEnd += nread;

		/*
		 * Hack to deal with the fact that some kernels will only give us back
		 * 1 packet per recv() call, even if we asked for more and there is
		 * more available.  If it looks like we are reading a long message,
		 * loop back to recv() again immediately, until we run out of data or
		 * buffer space.  Without this, the block-and-restart behavior of
		 * libpq's higher levels leads to O(N^2) performance on long messages.
		 *
		 * Since we left-justified the data above, conn->inEnd gives the
		 * amount of data already read in the current message.  We consider
		 * the message "long" once we have acquired 32k ...
		 */
		if (conn->inEnd > 32768 &&
			(conn->inBufSize - conn->inEnd) >= 8192)
		{
			someread = 1;
			goto retry3;
		}
		return 1;
	}

	if (someread)
		return 1;				/* got a zero read after successful tries */

	/*
	 * A return value of 0 could mean just that no data is now available, or
	 * it could mean EOF --- that is, the server has closed the connection.
	 * Since we have the socket in nonblock mode, the only way to tell the
	 * difference is to see if select() is saying that the file is ready.
	 * Grumble.  Fortunately, we don't expect this path to be taken much,
	 * since in normal practice we should not be trying to read data unless
	 * the file selected for reading already.
	 *
	 * In SSL mode it's even worse: SSL_read() could say WANT_READ and then
	 * data could arrive before we make the pqReadReady() test, but the second
	 * SSL_read() could still say WANT_READ because the data received was not
	 * a complete SSL record.  So we must play dumb and assume there is more
	 * data, relying on the SSL layer to detect true EOF.
	 */

#ifdef USE_SSL
	if (conn->ssl_in_use)
		return 0;
#endif

	switch (pqReadReady(conn))
	{
		case 0:
			/* definitely no data available */
			return 0;
		case 1:
			/* ready for read */
			break;
		default:
			/* we override pqReadReady's message with something more useful */
			goto definitelyEOF;
	}

	/*
	 * Still not sure that it's EOF, because some data could have just
	 * arrived.
	 */
retry4:
	nread = pqsecure_read(conn, conn->inBuffer + conn->inEnd,
						  conn->inBufSize - conn->inEnd);
	if (nread < 0)
	{
		switch (SOCK_ERRNO)
		{
			case EINTR:
				goto retry4;

				/* Some systems return EAGAIN/EWOULDBLOCK for no data */
#ifdef EAGAIN
			case EAGAIN:
				return 0;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
			case EWOULDBLOCK:
				return 0;
#endif

				/* We might get ECONNRESET etc here if connection failed */
			case ALL_CONNECTION_FAILURE_ERRNOS:
				goto definitelyFailed;

			default:
				/* pqsecure_read set the error message for us */
				return -1;
		}
	}
	if (nread > 0)
	{
		conn->inEnd += nread;
		return 1;
	}

	/*
	 * OK, we are getting a zero read even though select() says ready. This
	 * means the connection has been closed.  Cope.
	 */
definitelyEOF:
	appendPQExpBufferStr(&conn->errorMessage,
						 libpq_gettext("server closed the connection unexpectedly\n"
									   "\tThis probably means the server terminated abnormally\n"
									   "\tbefore or while processing the request.\n"));

	/* Come here if lower-level code already set a suitable errorMessage */
definitelyFailed:
	/* Do *not* drop any already-read data; caller still wants it */
	pqDropConnection(conn, false);
	conn->status = CONNECTION_BAD;	/* No more connection to backend */
	return -1;
}

/*
 * pqSendSome: send data waiting in the output buffer.
 *
 * len is how much to try to send (typically equal to outCount, but may
 * be less).
 *
 * Return 0 on success, -1 on failure and 1 when not all data could be sent
 * because the socket would block and the connection is non-blocking.
 *
 * Note that this is also responsible for consuming data from the socket
 * (putting it in conn->inBuffer) in any situation where we can't send
 * all the specified data immediately.
 *
 * Upon write failure, conn->write_failed is set and the error message is
 * saved in conn->write_err_msg, but we clear the output buffer and return
 * zero anyway; this is because callers should soldier on until it's possible
 * to read from the server and check for an error message.  write_err_msg
 * should be reported only when we are unable to obtain a server error first.
 * (Thus, a -1 result is returned only for an internal *read* failure.)
 */
static int
pqSendSome(PGconn *conn, int len)
{
	char	   *ptr = conn->outBuffer;
	int			remaining = conn->outCount;
	int			oldmsglen = conn->errorMessage.len;
	int			result = 0;

	/*
	 * If we already had a write failure, we will never again try to send data
	 * on that connection.  Even if the kernel would let us, we've probably
	 * lost message boundary sync with the server.  conn->write_failed
	 * therefore persists until the connection is reset, and we just discard
	 * all data presented to be written.  However, as long as we still have a
	 * valid socket, we should continue to absorb data from the backend, so
	 * that we can collect any final error messages.
	 */
	if (conn->write_failed)
	{
		/* conn->write_err_msg should be set up already */
		conn->outCount = 0;
		/* Absorb input data if any, and detect socket closure */
		if (conn->sock != PGINVALID_SOCKET)
		{
			if (pqReadData(conn) < 0)
				return -1;
		}
		return 0;
	}

	if (conn->sock == PGINVALID_SOCKET)
	{
		conn->write_failed = true;
		/* Insert error message into conn->write_err_msg, if possible */
		/* (strdup failure is OK, we'll cope later) */
		conn->write_err_msg = strdup(libpq_gettext("connection not open\n"));
		/* Discard queued data; no chance it'll ever be sent */
		conn->outCount = 0;
		return 0;
	}

	/* while there's still data to send */
	while (len > 0)
	{
		int			sent;

#ifndef WIN32
		sent = pqsecure_write(conn, ptr, len);
#else

		/*
		 * Windows can fail on large sends, per KB article Q201213. The
		 * failure-point appears to be different in different versions of
		 * Windows, but 64k should always be safe.
		 */
		sent = pqsecure_write(conn, ptr, Min(len, 65536));
#endif

		if (sent < 0)
		{
			/* Anything except EAGAIN/EWOULDBLOCK/EINTR is trouble */
			switch (SOCK_ERRNO)
			{
#ifdef EAGAIN
				case EAGAIN:
					break;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
				case EWOULDBLOCK:
					break;
#endif
				case EINTR:
					continue;

				default:
					/* pqsecure_write set the error message for us */
					conn->write_failed = true;

					/*
					 * Transfer error message to conn->write_err_msg, if
					 * possible (strdup failure is OK, we'll cope later).
					 *
					 * We only want to transfer whatever has been appended to
					 * conn->errorMessage since we entered this routine.
					 */
					if (!PQExpBufferBroken(&conn->errorMessage))
					{
						conn->write_err_msg = strdup(conn->errorMessage.data +
													 oldmsglen);
						conn->errorMessage.len = oldmsglen;
						conn->errorMessage.data[oldmsglen] = '\0';
					}

					/* Discard queued data; no chance it'll ever be sent */
					conn->outCount = 0;

					/* Absorb input data if any, and detect socket closure */
					if (conn->sock != PGINVALID_SOCKET)
					{
						if (pqReadData(conn) < 0)
							return -1;
					}
					return 0;
			}
		}
		else
		{
			ptr += sent;
			len -= sent;
			remaining -= sent;
		}

		if (len > 0)
		{
			/*
			 * We didn't send it all, wait till we can send more.
			 *
			 * There are scenarios in which we can't send data because the
			 * communications channel is full, but we cannot expect the server
			 * to clear the channel eventually because it's blocked trying to
			 * send data to us.  (This can happen when we are sending a large
			 * amount of COPY data, and the server has generated lots of
			 * NOTICE responses.)  To avoid a deadlock situation, we must be
			 * prepared to accept and buffer incoming data before we try
			 * again.  Furthermore, it is possible that such incoming data
			 * might not arrive until after we've gone to sleep.  Therefore,
			 * we wait for either read ready or write ready.
			 *
			 * In non-blocking mode, we don't wait here directly, but return 1
			 * to indicate that data is still pending.  The caller should wait
			 * for both read and write ready conditions, and call
			 * PQconsumeInput() on read ready, but just in case it doesn't, we
			 * call pqReadData() ourselves before returning.  That's not
			 * enough if the data has not arrived yet, but it's the best we
			 * can do, and works pretty well in practice.  (The documentation
			 * used to say that you only need to wait for write-ready, so
			 * there are still plenty of applications like that out there.)
			 *
			 * Note that errors here don't result in write_failed becoming
			 * set.
			 */
			if (pqReadData(conn) < 0)
			{
				result = -1;	/* error message already set up */
				break;
			}

			if (pqIsnonblocking(conn))
			{
				result = 1;
				break;
			}

			if (pqWait(true, true, conn))
			{
				result = -1;
				break;
			}
		}
	}

	/* shift the remaining contents of the buffer */
	if (remaining > 0)
		memmove(conn->outBuffer, ptr, remaining);
	conn->outCount = remaining;

	return result;
}


/*
 * pqFlush: send any data waiting in the output buffer
 *
 * Return 0 on success, -1 on failure and 1 when not all data could be sent
 * because the socket would block and the connection is non-blocking.
 * (See pqSendSome comments about how failure should be handled.)
 */
int
pqFlush(PGconn *conn)
{
	if (conn->outCount > 0)
	{
		/* XXX comment */
		if (conn->Pfdebug)
		{
			pqLogFrontendMsg(conn, -1);
			fflush(conn->Pfdebug);
		}
		return pqSendSome(conn, conn->outCount);
	}

	return 0;
}


/*
 * pqWait: wait until we can read or write the connection socket
 *
 * JAB: If SSL enabled and used and forRead, buffered bytes short-circuit the
 * call to select().
 *
 * We also stop waiting and return if the kernel flags an exception condition
 * on the socket.  The actual error condition will be detected and reported
 * when the caller tries to read or write the socket.
 */
int
pqWait(int forRead, int forWrite, PGconn *conn)
{
	return pqWaitTimed(forRead, forWrite, conn, (time_t) -1);
}

/*
 * pqWaitTimed: wait, but not past finish_time.
 *
 * finish_time = ((time_t) -1) disables the wait limit.
 *
 * Returns -1 on failure, 0 if the socket is readable/writable, 1 if it timed out.
 */
int
pqWaitTimed(int forRead, int forWrite, PGconn *conn, time_t finish_time)
{
	int			result;

	result = pqSocketCheck(conn, forRead, forWrite, finish_time);

	if (result < 0)
		return -1;				/* errorMessage is already set */

	if (result == 0)
	{
		appendPQExpBufferStr(&conn->errorMessage,
							 libpq_gettext("timeout expired\n"));
		return 1;
	}

	return 0;
}

/*
 * pqReadReady: is select() saying the file is ready to read?
 * Returns -1 on failure, 0 if not ready, 1 if ready.
 */
int
pqReadReady(PGconn *conn)
{
	return pqSocketCheck(conn, 1, 0, (time_t) 0);
}

/*
 * pqWriteReady: is select() saying the file is ready to write?
 * Returns -1 on failure, 0 if not ready, 1 if ready.
 */
int
pqWriteReady(PGconn *conn)
{
	return pqSocketCheck(conn, 0, 1, (time_t) 0);
}

/*
 * Checks a socket, using poll or select, for data to be read, written,
 * or both.  Returns >0 if one or more conditions are met, 0 if it timed
 * out, -1 if an error occurred.
 *
 * If SSL is in use, the SSL buffer is checked prior to checking the socket
 * for read data directly.
 */
static int
pqSocketCheck(PGconn *conn, int forRead, int forWrite, time_t end_time)
{
	int			result;

	if (!conn)
		return -1;
	if (conn->sock == PGINVALID_SOCKET)
	{
		appendPQExpBufferStr(&conn->errorMessage,
							 libpq_gettext("invalid socket\n"));
		return -1;
	}

#ifdef USE_SSL
	/* Check for SSL library buffering read bytes */
	if (forRead && conn->ssl_in_use && pgtls_read_pending(conn))
	{
		/* short-circuit the select */
		return 1;
	}
#endif

	/* We will retry as long as we get EINTR */
	do
		result = pqSocketPoll(conn->sock, forRead, forWrite, end_time);
	while (result < 0 && SOCK_ERRNO == EINTR);

	if (result < 0)
	{
		char		sebuf[PG_STRERROR_R_BUFLEN];

		appendPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("select() failed: %s\n"),
						  SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
	}

	return result;
}


/*
 * Check a file descriptor for read and/or write data, possibly waiting.
 * If neither forRead nor forWrite are set, immediately return a timeout
 * condition (without waiting).  Return >0 if condition is met, 0
 * if a timeout occurred, -1 if an error or interrupt occurred.
 *
 * Timeout is infinite if end_time is -1.  Timeout is immediate (no blocking)
 * if end_time is 0 (or indeed, any time before now).
 */
static int
pqSocketPoll(int sock, int forRead, int forWrite, time_t end_time)
{
	/* We use poll(2) if available, otherwise select(2) */
#ifdef HAVE_POLL
	struct pollfd input_fd;
	int			timeout_ms;

	if (!forRead && !forWrite)
		return 0;

	input_fd.fd = sock;
	input_fd.events = POLLERR;
	input_fd.revents = 0;

	if (forRead)
		input_fd.events |= POLLIN;
	if (forWrite)
		input_fd.events |= POLLOUT;

	/* Compute appropriate timeout interval */
	if (end_time == ((time_t) -1))
		timeout_ms = -1;
	else
	{
		time_t		now = time(NULL);

		if (end_time > now)
			timeout_ms = (end_time - now) * 1000;
		else
			timeout_ms = 0;
	}

	return poll(&input_fd, 1, timeout_ms);
#else							/* !HAVE_POLL */

	fd_set		input_mask;
	fd_set		output_mask;
	fd_set		except_mask;
	struct timeval timeout;
	struct timeval *ptr_timeout;

	if (!forRead && !forWrite)
		return 0;

	FD_ZERO(&input_mask);
	FD_ZERO(&output_mask);
	FD_ZERO(&except_mask);
	if (forRead)
		FD_SET(sock, &input_mask);

	if (forWrite)
		FD_SET(sock, &output_mask);
	FD_SET(sock, &except_mask);

	/* Compute appropriate timeout interval */
	if (end_time == ((time_t) -1))
		ptr_timeout = NULL;
	else
	{
		time_t		now = time(NULL);

		if (end_time > now)
			timeout.tv_sec = end_time - now;
		else
			timeout.tv_sec = 0;
		timeout.tv_usec = 0;
		ptr_timeout = &timeout;
	}

	return select(sock + 1, &input_mask, &output_mask,
				  &except_mask, ptr_timeout);
#endif							/* HAVE_POLL */
}


/*
 * A couple of "miscellaneous" multibyte related functions. They used
 * to be in fe-print.c but that file is doomed.
 */

/*
 * returns the byte length of the character beginning at s, using the
 * specified encoding.
 */
int
PQmblen(const char *s, int encoding)
{
	return pg_encoding_mblen(encoding, s);
}

/*
 * returns the display length of the character beginning at s, using the
 * specified encoding.
 */
int
PQdsplen(const char *s, int encoding)
{
	return pg_encoding_dsplen(encoding, s);
}

/*
 * Get encoding id from environment variable PGCLIENTENCODING.
 */
int
PQenv2encoding(void)
{
	char	   *str;
	int			encoding = PG_SQL_ASCII;

	str = getenv("PGCLIENTENCODING");
	if (str && *str != '\0')
	{
		encoding = pg_char_to_encoding(str);
		if (encoding < 0)
			encoding = PG_SQL_ASCII;
	}
	return encoding;
}


#ifdef ENABLE_NLS

static void
libpq_binddomain(void)
{
	static bool already_bound = false;

	if (!already_bound)
	{
		/* bindtextdomain() does not preserve errno */
#ifdef WIN32
		int			save_errno = GetLastError();
#else
		int			save_errno = errno;
#endif
		const char *ldir;

		already_bound = true;
		/* No relocatable lookup here because the binary could be anywhere */
		ldir = getenv("PGLOCALEDIR");
		if (!ldir)
			ldir = LOCALEDIR;
		bindtextdomain(PG_TEXTDOMAIN("libpq"), ldir);
#ifdef WIN32
		SetLastError(save_errno);
#else
		errno = save_errno;
#endif
	}
}

char *
libpq_gettext(const char *msgid)
{
	libpq_binddomain();
	return dgettext(PG_TEXTDOMAIN("libpq"), msgid);
}

char *
libpq_ngettext(const char *msgid, const char *msgid_plural, unsigned long n)
{
	libpq_binddomain();
	return dngettext(PG_TEXTDOMAIN("libpq"), msgid, msgid_plural, n);
}

#endif							/* ENABLE_NLS */
