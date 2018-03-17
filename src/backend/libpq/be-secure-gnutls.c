/*-------------------------------------------------------------------------
 *
 * be-secure-gnutls.c
 *	  functions for GnuTLS support in the backend.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/libpq/be-secure-gnutls.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <signal.h>
#include <fcntl.h>
#include <ctype.h>
#include <sys/socket.h>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#include <arpa/inet.h>
#endif
#include <gnutls/x509.h>
#include <gnutls/pkcs11.h>

#include "libpq/libpq.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/latch.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"


static ssize_t my_sock_read(gnutls_transport_ptr_t h, void *buf, size_t size);
static ssize_t my_sock_write(gnutls_transport_ptr_t h, const void *buf, size_t size);
static int	get_peer_certificate(gnutls_session_t ssl, gnutls_x509_crt_t * peer);
static bool load_dh_file(gnutls_dh_params_t dh, char *filename, bool isServerStart);
static bool load_dh_buffer(gnutls_dh_params_t dh, const char *, size_t, bool isServerStart);
#ifdef HAVE_GNUTLS_PKCS11_SET_PIN_FUNCTION
static int pin_function(void *userdata, int attempt,
			 const char *token_url,
			 const char *token_label,
			 unsigned int flags,
			 char *pin, size_t pin_max);
static int dummy_pin_function(void *userdata, int attempt,
				   const char *token_url,
				   const char *token_label,
				   unsigned int flags,
				   char *pin, size_t pin_max);
#endif
static int	verify_cb(gnutls_session_t ssl);
static bool initialize_dh(gnutls_dh_params_t *dh_params, bool isServerStart);

static gnutls_certificate_credentials_t tls_credentials = NULL;
static gnutls_dh_params_t tls_dh_params = NULL;
static gnutls_priority_t tls_priority = NULL;
static bool tls_initialized = false;
static bool pin_function_called = false;


/* ------------------------------------------------------------ */
/*						 Public interface						*/
/* ------------------------------------------------------------ */

int
be_tls_init(bool isServerStart)
{
	gnutls_certificate_credentials_t credentials = NULL;
	gnutls_priority_t priority = NULL;
	gnutls_dh_params_t dh_params = NULL;
	int			ret;
	const char *err_pos;

	/* This stuff need be done only once. */
	if (!tls_initialized)
	{
		gnutls_global_init();
		tls_initialized = true;
	}

	ret = gnutls_certificate_allocate_credentials(&credentials);
	if (ret < 0)
	{
		ereport(isServerStart ? FATAL : LOG,
				(errmsg("could not create SSL credentials: %s",
						gnutls_strerror(ret))));
		goto error;
	}

	/*
	 * Set PIN (passphrase) callback.  Note that there is no default for this
	 * in GnuTLS.
	 *
	 * If reloading, substitute a dummy callback, because we don't want to
	 * prompt for a passphrase in an already-running server.
	 */
#ifdef HAVE_GNUTLS_PKCS11_SET_PIN_FUNCTION
	if (isServerStart)
		gnutls_pkcs11_set_pin_function(pin_function, NULL);
	else
		gnutls_pkcs11_set_pin_function(dummy_pin_function, NULL);
#endif

	/*
	 * Load and verify server's certificate and private key
	 */
	if (!check_ssl_key_file_permissions(ssl_key_file, isServerStart))
		goto error;

	pin_function_called = false;

	ret = gnutls_certificate_set_x509_key_file(credentials, ssl_cert_file, ssl_key_file, GNUTLS_X509_FMT_PEM);
	if (ret < 0)
	{
		if (pin_function_called)
			ereport(isServerStart ? FATAL : LOG,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("private key file \"%s\" cannot be reloaded because it requires a passphrase",
							ssl_key_file)));
		else
			ereport(isServerStart ? FATAL : LOG,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("could not load server certificate \"%s\" or key file \"%s\": %s",
							ssl_cert_file, ssl_key_file, gnutls_strerror(ret))));
		goto error;
	}

	/* set up ephemeral DH keys */
	if (!initialize_dh(&dh_params, isServerStart))
		goto error;

	gnutls_certificate_set_dh_params(credentials, dh_params);

	/* set up the allowed cipher list */
	ret = gnutls_priority_init(&priority, gnutls_priority, &err_pos);
	if (ret < 0)
	{
		if (ret == GNUTLS_E_INVALID_REQUEST)
			ereport(isServerStart ? FATAL : LOG,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("could not set the cipher list: syntax error at %s", err_pos)));
		else
			ereport(isServerStart ? FATAL : LOG,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("could not set the cipher list: %s", gnutls_strerror(ret))));
		goto error;
	}

	/*
	 * Load CA store, so we can verify client certificates if needed.
	 */
	if (ssl_ca_file[0])
	{
		ret = gnutls_certificate_set_x509_trust_file(credentials, ssl_ca_file, GNUTLS_X509_FMT_PEM);
		if (ret < 0)
		{
			ereport(isServerStart ? FATAL : LOG,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("could not load root certificate file \"%s\": %s",
							ssl_ca_file, gnutls_strerror(ret))));
			goto error;
		}

		gnutls_certificate_set_verify_function(credentials, verify_cb);
	}

	/*
	 * Load the Certificate Revocation List (CRL).
	 */
	if (ssl_crl_file[0])
	{
		ret = gnutls_certificate_set_x509_crl_file(credentials, ssl_crl_file, GNUTLS_X509_FMT_PEM);
		if (ret < 0)
		{
			ereport(isServerStart ? FATAL : LOG,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("could not load SSL certificate revocation list file \"%s\": %s",
							ssl_crl_file, gnutls_strerror(ret))));
			goto error;
		}
	}

	/*
	 * Success!  Replace any existing credentials.
	 */
	if (tls_credentials)
		gnutls_certificate_free_credentials(tls_credentials);
	if (tls_priority)
		gnutls_priority_deinit(tls_priority);
	if (tls_dh_params)
		gnutls_dh_params_deinit(tls_dh_params);

	tls_credentials = credentials;
	tls_priority = priority;
	tls_dh_params = dh_params;

	/*
	 * Set flag to remember whether CA store has been loaded.
	 */
	if (ssl_ca_file[0])
		ssl_loaded_verify_locations = true;
	else
		ssl_loaded_verify_locations = false;

	return 0;

error:
	if (credentials)
		gnutls_certificate_free_credentials(credentials);
	if (priority)
		gnutls_priority_deinit(priority);
	if (dh_params)
		gnutls_dh_params_deinit(dh_params);
	return -1;
}

void
be_tls_destroy(void)
{
	if (tls_credentials)
		gnutls_certificate_free_credentials(tls_credentials);
	if (tls_priority)
		gnutls_priority_deinit(tls_priority);
	if (tls_dh_params)
		gnutls_dh_params_deinit(tls_dh_params);
	tls_credentials = NULL;
	tls_priority = NULL;
	tls_dh_params = NULL;
	ssl_loaded_verify_locations = false;
}

int
be_tls_open_server(Port *port)
{
	int			ret;

	Assert(!port->ssl);
	Assert(!port->peer);

	if (!tls_credentials || !tls_priority)
	{
		ereport(COMMERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("could not initialize SSL connection: SSL context not set up")));
		return -1;
	}

	ret = gnutls_init(&port->ssl, GNUTLS_SERVER);
	if (ret < 0)
	{
		ereport(COMMERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("could not initialize SSL connection: %s",
						gnutls_strerror(ret))));
		return -1;
	}

	gnutls_transport_set_ptr(port->ssl, port);
	gnutls_transport_set_pull_function(port->ssl, my_sock_read);
	gnutls_transport_set_push_function(port->ssl, my_sock_write);

	ret = gnutls_priority_set(port->ssl, tls_priority);
	if (ret < 0)
	{
		ereport(COMMERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("could not initialize SSL connection: %s",
						gnutls_strerror(ret))));
		return -1;
	}

	ret = gnutls_credentials_set(port->ssl, GNUTLS_CRD_CERTIFICATE, tls_credentials);
	if (ret < 0)
	{
		ereport(COMMERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("could not initialize SSL connection: %s",
						gnutls_strerror(ret))));
		return -1;
	}

	if (ssl_loaded_verify_locations)
		gnutls_certificate_server_set_request(port->ssl, GNUTLS_CERT_REQUEST);

	port->ssl_in_use = true;

	do
	{
		ret = gnutls_handshake(port->ssl);
	}
	while (ret < 0 && gnutls_error_is_fatal(ret) == 0);

	if (ret < 0)
	{
		ereport(COMMERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("could not accept SSL connection: %s",
						gnutls_strerror(ret))));
		return -1;
	}

	/* Get client certificate, if available. */
	ret = get_peer_certificate(port->ssl, &port->peer);
	if (ret < 0 && ret != GNUTLS_E_NO_CERTIFICATE_FOUND)
	{
		ereport(COMMERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("could not load peer certificates: %s",
						gnutls_strerror(ret))));
	}

	/* and extract the Common Name from it. */
	port->peer_cn = NULL;
	port->peer_cert_valid = false;
	if (port->peer != NULL)
	{
		size_t		len = 0;

		gnutls_x509_crt_get_dn_by_oid(port->peer,
									  GNUTLS_OID_X520_COMMON_NAME,
									  0, 0, NULL, &len);

		if (len > 0)
		{
			char	   *peer_cn;

			peer_cn = MemoryContextAlloc(TopMemoryContext, len);

			ret = gnutls_x509_crt_get_dn_by_oid(port->peer,
												GNUTLS_OID_X520_COMMON_NAME,
												0, 0, peer_cn, &len);

			if (ret != 0)
			{
				/* shouldn't happen */
				pfree(peer_cn);
				return -1;
			}

			/*
			 * Reject embedded NULLs in certificate common name to prevent
			 * attacks like CVE-2009-4034.
			 */
			if (len != strlen(peer_cn))
			{
				ereport(COMMERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("SSL certificate's common name contains embedded null")));
				pfree(peer_cn);
				return -1;
			}


			if (ret == 0)
				port->peer_cn = peer_cn;
			else
				pfree(peer_cn);

		}

		port->peer_cert_valid = true;
	}

	return 0;
}

void
be_tls_close(Port *port)
{
	if (port->ssl)
	{
		gnutls_bye(port->ssl, GNUTLS_SHUT_RDWR);
		gnutls_deinit(port->ssl);
		port->ssl = NULL;
		port->ssl_in_use = false;
	}

	if (port->peer)
	{
		gnutls_x509_crt_deinit(port->peer);
		port->peer = NULL;
	}

	if (port->peer_cn)
	{
		pfree(port->peer_cn);
		port->peer_cn = NULL;
	}
}

ssize_t
be_tls_read(Port *port, void *ptr, size_t len, int *waitfor)
{
	ssize_t		n;

	n = gnutls_record_recv(port->ssl, ptr, len);

	if (n > 0)
		return n;

	switch (n)
	{
		case 0:

			/*
			 * the SSL connnection was closed, leave it to the caller to
			 * ereport it
			 */
			errno = ECONNRESET;
			n = -1;
			break;
		case GNUTLS_E_AGAIN:
		case GNUTLS_E_INTERRUPTED:
			*waitfor = WL_SOCKET_READABLE;
			errno = EWOULDBLOCK;
			n = -1;
			break;
		default:
			ereport(COMMERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("SSL error: %s",
							gnutls_strerror(n))));
			errno = ECONNRESET;
			n = -1;
			break;
	}

	return n;
}

ssize_t
be_tls_write(Port *port, void *ptr, size_t len, int *waitfor)
{
	ssize_t		n;

	n = gnutls_record_send(port->ssl, ptr, len);

	if (n >= 0)
		return n;

	switch (n)
	{
		case GNUTLS_E_AGAIN:
		case GNUTLS_E_INTERRUPTED:
			*waitfor = WL_SOCKET_WRITEABLE;
			errno = EWOULDBLOCK;
			n = -1;
			break;
		default:
			ereport(COMMERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("SSL error: %s",
							gnutls_strerror(n))));
			errno = ECONNRESET;
			n = -1;
			break;
	}

	return n;
}

/* ------------------------------------------------------------ */
/*						Internal functions						*/
/* ------------------------------------------------------------ */

/*
 * Private substitute transport layer: this does the sending and receiving
 * using send() and recv() instead. This is so that we can enable and disable
 * interrupts just while calling recv(). We cannot have interrupts occurring
 * while the bulk of GnuTLS runs, because it uses malloc() and possibly other
 * non-reentrant libc facilities. We also need to call send() and recv()
 * directly so it gets passed through the socket/signals layer on Win32.
 */

static ssize_t
my_sock_read(gnutls_transport_ptr_t port, void *buf, size_t size)
{
	return secure_raw_read((Port *) port, buf, size);
}

static ssize_t
my_sock_write(gnutls_transport_ptr_t port, const void *buf, size_t size)
{
	return secure_raw_write((Port *) port, buf, size);
}

#if !HAVE_DECL_GNUTLS_X509_CRT_LIST_SORT
/*
 * GnuTLS versions before 3.4.0 do not support sorting incorrectly sorted
 * certificate chains, so we skip doing so in these earlier versions.
 */
#define GNUTLS_X509_CRT_LIST_SORT 0
#endif

/*
 *	Get peer certificate from a session
 *
 *	Returns GNUTLS_E_NO_CERTIFICATE_FOUND when not x509 certifcate was found.
 */
static int
get_peer_certificate(gnutls_session_t ssl, gnutls_x509_crt_t * peer)
{
	if (gnutls_certificate_type_get(ssl) == GNUTLS_CRT_X509)
	{
		unsigned int n;
		int			ret;
		gnutls_datum_t const *raw_certs;
		gnutls_x509_crt_t *certs;

		raw_certs = gnutls_certificate_get_peers(ssl, &n);

		if (n == 0)
			return GNUTLS_E_NO_CERTIFICATE_FOUND;

		certs = palloc(n * sizeof(gnutls_x509_crt_t));

		ret = gnutls_x509_crt_list_import(certs, &n, raw_certs,
										  GNUTLS_X509_FMT_DER,
										  GNUTLS_X509_CRT_LIST_SORT);

		if (ret >= 1)
		{
			unsigned int i;

			for (i = 1; i < ret; i++)
				gnutls_x509_crt_deinit(certs[i]);

			*peer = certs[0];

			ret = GNUTLS_E_SUCCESS;
		}
		else if (ret == 0)
			ret = GNUTLS_E_NO_CERTIFICATE_FOUND;

		pfree(certs);

		return ret;
	}

	return GNUTLS_E_NO_CERTIFICATE_FOUND;
}

#define MAX_DH_FILE_SIZE 10240

/*
 *	Load precomputed DH parameters.
 *
 *	To prevent "downgrade" attacks, we perform a number of checks
 *	to verify that the DBA-generated DH parameters file contains
 *	what we expect it to contain.
 */
static bool
load_dh_file(gnutls_dh_params_t dh_params, char *filename, bool isServerStart)
{
	FILE	   *fp;
	char		buffer[MAX_DH_FILE_SIZE];
	gnutls_datum_t datum = {(unsigned char *) buffer};
	int			ret;

	/* attempt to open file.  It's not an error if it doesn't exist. */
	if ((fp = AllocateFile(filename, "r")) == NULL)
	{
		ereport(isServerStart ? FATAL : LOG,
				(errcode_for_file_access(),
				 errmsg("could not open DH parameters file \"%s\": %m",
						filename)));
		return false;
	}

	datum.size = fread(buffer, sizeof(buffer[0]), sizeof(buffer), fp);

	FreeFile(fp);

	if (datum.size < 0)
	{
		ereport(isServerStart ? FATAL : LOG,
				(errcode_for_file_access(),
				 errmsg("could not load DH parameters file: %s",
						gnutls_strerror(ret))));
		return false;
	}

	ret = gnutls_dh_params_import_pkcs3(dh_params, &datum, GNUTLS_X509_FMT_PEM);

	if (ret < 0)
	{
		ereport(isServerStart ? FATAL : LOG,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("could not load DH parameters file: %s",
						gnutls_strerror(ret))));
		return false;
	}

	return true;
}

/*
 *	Load hardcoded DH parameters.
 *
 *	To prevent problems if the DH parameters files don't even
 *	exist, we can load DH parameters hardcoded into this file.
 */
static bool
load_dh_buffer(gnutls_dh_params_t dh_params, const char *buffer, size_t len, bool isServerStart)
{
	gnutls_datum_t datum = {(unsigned char *) buffer, len};
	int			ret;

	ret = gnutls_dh_params_import_pkcs3(dh_params, &datum, GNUTLS_X509_FMT_PEM);

	if (ret < 0)
	{
		ereport(isServerStart ? FATAL : LOG,
				(errmsg_internal("DH load buffer: %s", gnutls_strerror(ret))));
		return false;
	}

	return true;
}

#ifdef HAVE_GNUTLS_PKCS11_SET_PIN_FUNCTION
/*
 * PIN callback
 */
static int
pin_function(void *userdata, int attempt,
			 const char *token_url,
			 const char *token_label,
			 unsigned int flags,
			 char *pin, size_t pin_max)
{
	simple_prompt(token_url, pin, pin_max, false);
	return 0;
}

/*
 * Dummy PIN callback during server reload
 *
 * The standard callback is no good during a postmaster SIGHUP cycle, not to
 * mention SSL context reload in an EXEC_BACKEND postmaster child.  So
 * override it with this dummy function that just returns an error,
 * guaranteeing failure.
 */
static int
dummy_pin_function(void *userdata, int attempt,
				   const char *token_url,
				   const char *token_label,
				   unsigned int flags,
				   char *pin, size_t pin_max)
{
	/* Set flag to change the error message we'll report */
	pin_function_called = true;
	return -1;
}
#endif

/*
 *	Certificate verification callback
 *
 *	This callback is where we verify the identity of the client.
 */
static int
verify_cb(gnutls_session_t ssl)
{
	unsigned int status;
	int			ret;

	ret = gnutls_certificate_verify_peers2(ssl, &status);

	if (ret == GNUTLS_E_NO_CERTIFICATE_FOUND)
		return 0;
	else if (ret < 0)
		return ret;

	return status;
}

/*
 * Set DH parameters for generating ephemeral DH keys.  The
 * DH parameters can take a long time to compute, so they must be
 * precomputed.
 *
 * Since few sites will bother to create a parameter file, we also
 * also provide a fallback to the parameters provided by the
 * OpenSSL project.
 *
 * These values can be static (once loaded or computed) since the
 * OpenSSL library can efficiently generate random keys from the
 * information provided.
 */
static bool
initialize_dh(gnutls_dh_params_t *dh_params, bool isServerStart)
{
	bool		loaded = false;
	int			ret;

	ret = gnutls_dh_params_init(dh_params);
	if (ret < 0)
	{
		ereport(isServerStart ? FATAL : LOG,
				(errmsg_internal("DH init error: %s",
								 gnutls_strerror(ret))));
		return false;
	}

	if (ssl_dh_params_file[0])
		loaded = load_dh_file(*dh_params, ssl_dh_params_file, isServerStart);
	if (!loaded)
		loaded = load_dh_buffer(*dh_params, FILE_DH2048, sizeof(FILE_DH2048), isServerStart);
	if (!loaded)
	{
		ereport(isServerStart ? FATAL : LOG,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 (errmsg("DH: could not load DH parameters"))));
		return false;
	}

	return true;
}

int
be_tls_get_cipher_bits(Port *port)
{
	if (port->ssl)
		return gnutls_cipher_get_key_size(gnutls_cipher_get(port->ssl)) * 8;
	else
		return 0;
}

bool
be_tls_get_compression(Port *port)
{
	if (port->ssl)
	{
		gnutls_compression_method_t comp = gnutls_compression_get(port->ssl);

		return comp != GNUTLS_COMP_UNKNOWN && comp != GNUTLS_COMP_NULL;
	}
	else
		return false;
}

const char *
be_tls_get_version(Port *port)
{
	if (port->ssl)
		return gnutls_protocol_get_name(gnutls_protocol_get_version(port->ssl));
	else
		return NULL;
}

const char *
be_tls_get_cipher(Port *port)
{
	if (port->ssl)
		return gnutls_cipher_get_name(gnutls_cipher_get(port->ssl));
	else
		return NULL;
}

void
be_tls_get_peerdn_name(Port *port, char *ptr, size_t len)
{
	if (port->peer)
	{
		int			ret;

		ret = gnutls_x509_crt_get_dn_by_oid(port->peer,
											GNUTLS_OID_X520_COMMON_NAME,
											0, 0, ptr, &len);

		if (ret != 0)
			ptr[0] = '\0';
	}
	else
		ptr[0] = '\0';
}

char *
be_tls_get_peer_finished(Port *port, size_t *len)
{
	gnutls_datum_t cb;
	int			ret;
	char	   *result;

	ret = gnutls_session_channel_binding(port->ssl, GNUTLS_CB_TLS_UNIQUE, &cb);

	if (ret < 0)
		ereport(FATAL,
				(errmsg("could not get SSL channel binding data: %s",
						gnutls_strerror(ret))));

	result = palloc(cb.size);
	memcpy(result, cb.data, cb.size);
	*len = cb.size;

	return result;
}

/*
 * Stub function for SCRAM channel binding type tls-server-end-point,
 * currently not supported with GnuTLS.
 */
char *
be_tls_get_certificate_hash(Port *port, size_t *len)
{
	ereport(ERROR,
			(errcode(ERRCODE_PROTOCOL_VIOLATION),
			 errmsg("channel binding type \"tls-server-end-point\" is not supported by this build")));
	return NULL;
}
