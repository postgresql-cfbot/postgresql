/*-------------------------------------------------------------------------
 *
 * fe-secure-gssapi.c
 *   The front-end (client) encryption support for GSSAPI
 *
 * Portions Copyright (c) 2016-2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *  src/interfaces/libpq/fe-secure-gssapi.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "libpq-fe.h"
#include "libpq-int.h"
#include "fe-gssapi-common.h"

/*
 * Require encryption support, as well as mutual authentication and
 * tamperproofing measures.
 */
#define GSS_REQUIRED_FLAGS GSS_C_MUTUAL_FLAG | GSS_C_REPLAY_FLAG | \
	GSS_C_SEQUENCE_FLAG | GSS_C_CONF_FLAG | GSS_C_INTEG_FLAG

static ssize_t
send_buffered_data(PGconn *conn, size_t len)
{
	ssize_t ret = pqsecure_raw_write(conn,
									 conn->gwritebuf.data + conn->gwritecurs,
									 conn->gwritebuf.len - conn->gwritecurs);
	if (ret < 0)
		return ret;

	conn->gwritecurs += ret;

	if (conn->gwritecurs == conn->gwritebuf.len)
	{
		/* entire request has now been written */
		resetPQExpBuffer(&conn->gwritebuf);
		conn->gwritecurs = 0;
		return len;
	}

	/* need to be called again */
	errno = EWOULDBLOCK;
	return -1;
}

ssize_t
pg_GSS_write(PGconn *conn, void *ptr, size_t len)
{
	OM_uint32 major, minor;
	gss_buffer_desc input, output = GSS_C_EMPTY_BUFFER;
	ssize_t ret = -1;
	int conf = 0;
	uint32 netlen;

	if (conn->gwritebuf.len != 0)
		return send_buffered_data(conn, len);

	/* encrypt the message */
	input.value = ptr;
	input.length = len;

	major = gss_wrap(&minor, conn->gctx, 1, GSS_C_QOP_DEFAULT,
					 &input, &conf, &output);
	if (major != GSS_S_COMPLETE)
	{
		pg_GSS_error(libpq_gettext("GSSAPI wrap error"), conn, major, minor);
		goto cleanup;
	}
	else if (conf == 0)
	{
		printfPQExpBuffer(&conn->errorMessage, libpq_gettext(
							  "GSSAPI did not provide confidentiality\n"));
		goto cleanup;
	}

	/* 4 network-order bytes of length, then payload */
	netlen = htonl(output.length);
	appendBinaryPQExpBuffer(&conn->gwritebuf, (char *)&netlen, 4);
	appendBinaryPQExpBuffer(&conn->gwritebuf, output.value, output.length);

	ret = send_buffered_data(conn, len);
cleanup:
	if (output.value != NULL)
		gss_release_buffer(&minor, &output);
	return ret;
}

static ssize_t
read_from_buffer(PGconn *conn, void *ptr, size_t len)
{
	ssize_t ret = 0;

	/* check for available data */
	if (conn->gcursor < conn->gbuf.len)
	{
		/* clamp length */
		if (len > conn->gbuf.len - conn->gcursor)
			len = conn->gbuf.len - conn->gcursor;

		memcpy(ptr, conn->gbuf.data + conn->gcursor, len);
		conn->gcursor += len;
		ret = len;
	}

	/* reset buffer if all data has been read */
	if (conn->gcursor == conn->gbuf.len)
	{
		conn->gcursor = 0;
		resetPQExpBuffer(&conn->gbuf);
	}

	return ret;
}

static ssize_t
load_packet_length(PGconn *conn)
{
	ssize_t ret;
	if (conn->gbuf.len < 4)
	{
		ret = enlargePQExpBuffer(&conn->gbuf, 4 - conn->gbuf.len);
		if (ret != 1)
		{
			printfPQExpBuffer(&conn->errorMessage, libpq_gettext(
								  "Failed to fit packet length in buffer\n"));
			return -1;
		}

		ret = pqsecure_raw_read(conn, conn->gbuf.data + conn->gbuf.len,
								4 - conn->gbuf.len);
		if (ret < 0)
			return ret;

		/* update buffer state */
		conn->gbuf.len += ret;
		conn->gbuf.data[conn->gbuf.len] = '\0';
		if (conn->gbuf.len < 4)
		{
			/* partial read from pqsecure_raw_read() */
			errno = EWOULDBLOCK;
			return -1;
		}
	}
	return 0;
}

static ssize_t
load_packet(PGconn *conn, size_t len)
{
	ssize_t ret;

	ret = enlargePQExpBuffer(&conn->gbuf, len - conn->gbuf.len + 4);
	if (ret != 1)
	{
		printfPQExpBuffer(&conn->errorMessage, libpq_gettext(
							  "GSSAPI encrypted packet length %ld too big\n"),
						  len);
		return -1;
	}

	/* load any missing parts of the packet */
	if (conn->gbuf.len - 4 < len)
	{
		ret = pqsecure_raw_read(conn, conn->gbuf.data + conn->gbuf.len,
								len - conn->gbuf.len + 4);
		if (ret < 0)
			return ret;

		/* update buffer state */
		conn->gbuf.len += ret;
		conn->gbuf.data[conn->gbuf.len] = '\0';
		if (conn->gbuf.len - 4 < len)
		{
			/* partial read from pqsecure_raw_read() */
			errno = EWOULDBLOCK;
			return -1;
		}
	}
	return 0;
}

ssize_t
pg_GSS_read(PGconn *conn, void *ptr, size_t len)
{
	OM_uint32 major, minor;
	gss_buffer_desc input = GSS_C_EMPTY_BUFFER, output = GSS_C_EMPTY_BUFFER;
	ssize_t ret = 0;
	int conf = 0;

	/* handle any buffered data */
	if (conn->gcursor != 0)
		return read_from_buffer(conn, ptr, len);

	/* load in the packet length, if not yet loaded */
	ret = load_packet_length(conn);
	if (ret < 0)
		return ret;

	input.length = ntohl(*(uint32 *)conn->gbuf.data);
	ret = load_packet(conn, input.length);
	if (ret < 0)
		return ret;

	/* decrypt the packet */
	input.value = conn->gbuf.data + 4;

	major = gss_unwrap(&minor, conn->gctx, &input, &output, &conf, NULL);
	if (major != GSS_S_COMPLETE)
	{
		pg_GSS_error(libpq_gettext("GSSAPI unwrap error"), conn,
					 major, minor);
		ret = -1;
		goto cleanup;
	}
	else if (conf == 0)
	{
		printfPQExpBuffer(&conn->errorMessage, libpq_gettext(
							  "GSSAPI did not provide confidentiality\n"));
		ret = -1;
		goto cleanup;
	}

	/* load decrypted packet into our buffer */
	conn->gcursor = 0;
	resetPQExpBuffer(&conn->gbuf);
	ret = enlargePQExpBuffer(&conn->gbuf, output.length);
	if (ret != 1)
	{
		printfPQExpBuffer(&conn->errorMessage, libpq_gettext(
							  "GSSAPI decrypted packet length %ld too big\n"),
						  output.length);
		ret = -1;
		goto cleanup;
	}

	memcpy(conn->gbuf.data, output.value, output.length);
	conn->gbuf.len = output.length;
	conn->gbuf.data[conn->gbuf.len] = '\0';

	ret = read_from_buffer(conn, ptr, len);
cleanup:
	if (output.value != NULL)
		gss_release_buffer(&minor, &output);
	return ret;
}

PostgresPollingStatusType
pqsecure_open_gss(PGconn *conn)
{
	ssize_t ret;
	OM_uint32 major, minor;
	uint32 netlen;
	gss_buffer_desc input = GSS_C_EMPTY_BUFFER, output = GSS_C_EMPTY_BUFFER;

	/* Send out any data we might have */
	if (conn->gwritebuf.len != 0)
	{
		ret = send_buffered_data(conn, 1);
		if (ret < 0 && errno == EWOULDBLOCK)
			return PGRES_POLLING_WRITING;
		else if (ret == 1)
			/* sent all data */
			return PGRES_POLLING_READING;
		return PGRES_POLLING_FAILED;
	}

	/* Client sends first, and sending creates a context */
	if (conn->gctx)
	{
		/* Process any incoming data we might have */
		ret = load_packet_length(conn);
		if (ret < 0 && errno == EWOULDBLOCK)
			return PGRES_POLLING_READING;
		else if (ret < 0)
			return PGRES_POLLING_FAILED;

		if (conn->gbuf.data[0] == 'E')
		{
			/*
			 * We can taken an error here, and it's my least favorite thing.
			 * How long can error messages be?  That's a good question, but
			 * backend's pg_gss_error() caps them at 256.  Do a single read
			 * for that and call it a day.  Cope with this here rather than in
			 * load_packet since expandPQExpBuffer will destroy any data in
			 * the buffer on failure.
			 */
			load_packet(conn, 256);
			printfPQExpBuffer(&conn->errorMessage,
							  libpq_gettext("Server error: %s"),
							  conn->gbuf.data + 1);
			return PGRES_POLLING_FAILED;
		}

		input.length = ntohl(*(uint32 *)conn->gbuf.data);
		ret = load_packet(conn, input.length);
		if (ret < 0 && errno == EWOULDBLOCK)
			return PGRES_POLLING_READING;
		else if (ret < 0)
			return PGRES_POLLING_FAILED;

		input.value = conn->gbuf.data + 4;
	}

	ret = pg_GSS_load_servicename(conn);
	if (ret != STATUS_OK)
		return PGRES_POLLING_FAILED;

	major = gss_init_sec_context(&minor, conn->gcred, &conn->gctx,
								 conn->gtarg_nam, GSS_C_NO_OID,
								 GSS_REQUIRED_FLAGS, 0, 0, &input, NULL,
								 &output, NULL, NULL);
	resetPQExpBuffer(&conn->gbuf);
	if (GSS_ERROR(major))
	{
		pg_GSS_error(libpq_gettext("GSSAPI context establishment error"),
					 conn, major, minor);
		return PGRES_POLLING_FAILED;
	}
	else if (output.length == 0)
	{
		/*
		 * We're done - hooray!  Kind of gross, but we need to disable SSL
		 * here so that we don't accidentally tunnel one over the other.
		 */
#ifdef USE_SSL
		conn->allow_ssl_try = false;
#endif
		gss_release_cred(&minor, &conn->gcred);
		conn->gcred = GSS_C_NO_CREDENTIAL;
		conn->gssenc = true;
		return PGRES_POLLING_OK;
	}

	/* Queue the token for writing */
	netlen = htonl(output.length);
	appendBinaryPQExpBuffer(&conn->gwritebuf, (char *)&netlen, 4);
	appendBinaryPQExpBuffer(&conn->gwritebuf, output.value, output.length);
	gss_release_buffer(&minor, &output);
	return PGRES_POLLING_WRITING;
}
