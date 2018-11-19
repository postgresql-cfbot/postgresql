/*-------------------------------------------------------------------------
 *
 * be-secure-gssapi.c
 *  GSSAPI encryption support
 *
 * Portions Copyright (c) 2018-2018, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *  src/backend/libpq/be-secure-gssapi.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "be-gssapi-common.h"

#include "libpq/auth.h"
#include "libpq/libpq.h"
#include "libpq/libpq-be.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "pgstat.h"

#include <unistd.h>

static ssize_t
send_buffered_data(Port *port, size_t len)
{
	ssize_t ret = secure_raw_write(
		port,
		port->gss->writebuf.data + port->gss->writebuf.cursor,
		port->gss->writebuf.len - port->gss->writebuf.cursor);
	if (ret < 0)
		return ret;

	/* update and possibly clear buffer state */
	port->gss->writebuf.cursor += ret;

	if (port->gss->writebuf.cursor == port->gss->writebuf.len)
	{
		/* entire request has now been written */
		resetStringInfo(&port->gss->writebuf);
		return len;
	}

	/* need to be called again */
	errno = EWOULDBLOCK;
	return -1;
}

ssize_t
be_gssapi_write(Port *port, void *ptr, size_t len)
{
	OM_uint32 major, minor;
	gss_buffer_desc input, output;
	ssize_t ret = -1;
	int conf = 0;
	uint32 netlen;
	pg_gssinfo *gss = port->gss;

	if (gss->writebuf.len != 0)
		return send_buffered_data(port, len);

	/* encrypt the message */
	output.value = NULL;
	output.length = 0;
	input.value = ptr;
	input.length = len;

	major = gss_wrap(&minor, gss->ctx, 1, GSS_C_QOP_DEFAULT,
					 &input, &conf, &output);
	if (major != GSS_S_COMPLETE)
	{
		pg_GSS_error(ERROR, gettext_noop("GSSAPI wrap error"), major, minor);
		goto cleanup;
	} else if (conf == 0)
	{
		ereport(FATAL, (errmsg("GSSAPI did not provide confidentiality")));
		goto cleanup;
	}

	/* 4 network-order length bytes, then payload */
	netlen = htonl(output.length);
	appendBinaryStringInfo(&gss->writebuf, (char *)&netlen, 4);
	appendBinaryStringInfo(&gss->writebuf, output.value, output.length);

	ret = send_buffered_data(port, len);
cleanup:
	if (output.value != NULL)
		gss_release_buffer(&minor, &output);
	return ret;
}

static ssize_t
read_from_buffer(pg_gssinfo *gss, void *ptr, size_t len)
{
	ssize_t ret = 0;

	/* load up any available data */
	if (gss->buf.len > 4 && gss->buf.cursor < gss->buf.len)
	{
		/* clamp length */
		if (len > gss->buf.len - gss->buf.cursor)
			len = gss->buf.len - gss->buf.cursor;

		memcpy(ptr, gss->buf.data + gss->buf.cursor, len);
		gss->buf.cursor += len;
		ret = len;
	}

	/* reset buffer if all data has been read */
	if (gss->buf.cursor == gss->buf.len)
		resetStringInfo(&gss->buf);

	return ret;
}

static ssize_t
load_packetlen(Port *port)
{
	pg_gssinfo *gss = port->gss;
	ssize_t ret;

	if (gss->buf.len < 4)
	{
		enlargeStringInfo(&gss->buf, 4 - gss->buf.len);
		ret = secure_raw_read(port, gss->buf.data + gss->buf.len,
							  4 - gss->buf.len);
		if (ret < 0)
			return ret;

		/* update buffer state */
		gss->buf.len += ret;
		gss->buf.data[gss->buf.len] = '\0';
		if (gss->buf.len < 4)
		{
			/* partial read from secure_raw_read() */
			errno = EWOULDBLOCK;
			return -1;
		}
	}
	return 0;
}

static ssize_t
load_packet(Port *port, size_t len)
{
	ssize_t ret;
	pg_gssinfo *gss = port->gss;

	enlargeStringInfo(&gss->buf, len - gss->buf.len + 4);

	ret = secure_raw_read(port, gss->buf.data + gss->buf.len,
						  len - gss->buf.len + 4);
	if (ret < 0)
		return ret;

	/* update buffer state */
	gss->buf.len += ret;
	gss->buf.data[gss->buf.len] = '\0';
	if (gss->buf.len - 4 < len)
	{
		/* partial read from secure_raw_read() */
		errno = EWOULDBLOCK;
		return -1;
	}
	return 0;
}

ssize_t
be_gssapi_read(Port *port, void *ptr, size_t len)
{
	OM_uint32 major, minor;
	gss_buffer_desc input, output;
	ssize_t ret;
	int conf = 0;
	pg_gssinfo *gss = port->gss;

	if (gss->buf.cursor > 0)
		return read_from_buffer(gss, ptr, len);

	/* load length if not present */
	ret = load_packetlen(port);
	if (ret != 0)
		return ret;

	input.length = ntohl(*(uint32*)gss->buf.data);
	ret = load_packet(port, input.length);
	if (ret != 0)
		return ret;

	/* decrypt the packet */
	output.value = NULL;
	output.length = 0;
	input.value = gss->buf.data + 4;

	major = gss_unwrap(&minor, gss->ctx, &input, &output, &conf, NULL);
	if (major != GSS_S_COMPLETE)
	{
		pg_GSS_error(ERROR, gettext_noop("GSSAPI unwrap error"),
					 major, minor);
		ret = -1;
		goto cleanup;
	}
	else if (conf == 0)
	{
		ereport(FATAL, (errmsg("GSSAPI did not provide confidentiality")));
		ret = -1;
		goto cleanup;
	}

	/* put the decrypted packet in the buffer */
	resetStringInfo(&gss->buf);
	enlargeStringInfo(&gss->buf, output.length);

	memcpy(gss->buf.data, output.value, output.length);
	gss->buf.len = output.length;
	gss->buf.data[gss->buf.len] = '\0';

	ret = read_from_buffer(gss, ptr, len);
cleanup:
	if (output.value != NULL)
		gss_release_buffer(&minor, &output);
	return ret;
}

ssize_t
secure_open_gssapi(Port *port)
{
	pg_gssinfo *gss = port->gss;
	bool complete_next = false;

	/*
	 * Use the configured keytab, if there is one.  Unfortunately, Heimdal
	 * doesn't support the cred store extensions, so use the env var.
	 */
	if (pg_krb_server_keyfile != NULL && strlen(pg_krb_server_keyfile) > 0)
		setenv("KRB5_KTNAME", pg_krb_server_keyfile, 1);

	while (true)
	{
		OM_uint32 major, minor;
		size_t ret;
		gss_buffer_desc input, output = GSS_C_EMPTY_BUFFER;

		/* Handle any outgoing data */
		if (gss->writebuf.len != 0)
		{
			ret = send_buffered_data(port, 1);
			if (ret != 1)
			{
				WaitLatchOrSocket(MyLatch, WL_SOCKET_WRITEABLE, port->sock, 0,
								  WAIT_EVENT_GSS_OPEN_SERVER);
				continue;
			}
		}

		if (complete_next)
			break;

		/* Load incoming data */
		ret = load_packetlen(port);
		if (ret != 0)
		{
			WaitLatchOrSocket(MyLatch, WL_SOCKET_READABLE, port->sock, 0,
							  WAIT_EVENT_GSS_OPEN_SERVER);
			continue;
		}

		input.length = ntohl(*(uint32*)gss->buf.data);
		ret = load_packet(port, input.length);
		if (ret != 0)
		{
			WaitLatchOrSocket(MyLatch, WL_SOCKET_READABLE, port->sock, 0,
							  WAIT_EVENT_GSS_OPEN_SERVER);
			continue;
		}
		input.value = gss->buf.data + 4;

		/* Process incoming data.  (The client sends first.) */
		major = gss_accept_sec_context(&minor, &port->gss->ctx,
									   GSS_C_NO_CREDENTIAL, &input,
									   GSS_C_NO_CHANNEL_BINDINGS,
									   &port->gss->name, NULL, &output, NULL,
									   NULL, NULL);
		resetStringInfo(&gss->buf);
		if (GSS_ERROR(major))
		{
			pg_GSS_error(ERROR, gettext_noop("GSSAPI context error"),
						 major, minor);
			gss_release_buffer(&minor, &output);
			return -1;
		}
		else if (!(major & GSS_S_CONTINUE_NEEDED))
		{
			/*
			 * rfc2744 technically permits context negotiation to be complete
			 * both with and without a packet to be sent.
			 */
			complete_next = true;
		}

		if (output.length != 0)
		{
			/* Queue packet for writing */
			uint32 netlen = htonl(output.length);
			appendBinaryStringInfo(&gss->writebuf, (char *)&netlen, 4);
			appendBinaryStringInfo(&gss->writebuf,
								   output.value, output.length);
			gss_release_buffer(&minor, &output);
			continue;
		}

		/* We're done - woohoo! */
		break;
	}
	port->gss->enc = true;
	return 0;
}
