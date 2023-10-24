/*-------------------------------------------------------------------------
 *
 * crypt.c
 *	  Functions for dealing with encrypted passwords stored in
 *	  pg_authid's rolpassword and rolsecondpassword.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/libpq/crypt.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "catalog/pg_authid.h"
#include "common/md5.h"
#include "common/scram-common.h"
#include "libpq/crypt.h"
#include "libpq/scram.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"


/*
 * Fetch valid stored passwords for a user, for authentication.
 *
 * On error, returns NULL, and stores a palloc'd string describing the reason,
 * for the postmaster log, in *logdetail.  The error reason should *not* be
 * sent to the client, to avoid giving away user information!
 */
char	  **
get_role_passwords(const char *role, const char **logdetail, int *num_passwords)
{
	TimestampTz vuntil = 0;
	TimestampTz second_vuntil = 0;
	TimestampTz current_ts;
	HeapTuple	roleTup;
	Datum		datum;
	Datum		second_datum;
	bool		vuntil_isnull;
	bool		second_vuntil_isnull;
	bool		password_isnull;
	bool		second_password_isnull;
	char	   *shadow_pass = NULL;
	char	   *second_shadow_pass = NULL;

	*num_passwords = 0;

	/* Get role info from pg_authid */
	roleTup = SearchSysCache1(AUTHNAME, PointerGetDatum(role));
	if (!HeapTupleIsValid(roleTup))
	{
		*logdetail = psprintf(_("Role \"%s\" does not exist."),
							  role);
		return NULL;			/* no such user */
	}

	datum = SysCacheGetAttr(AUTHNAME, roleTup,
							Anum_pg_authid_rolpassword,
							&password_isnull);
	second_datum = SysCacheGetAttr(AUTHNAME, roleTup,
								   Anum_pg_authid_rolsecondpassword,
								   &second_password_isnull);
	if (password_isnull && second_password_isnull)
	{
		ReleaseSysCache(roleTup);
		*logdetail = psprintf(_("User \"%s\" has no password assigned."),
							  role);
		return NULL;			/* user has no password */
	}

	if (!password_isnull)
		shadow_pass = TextDatumGetCString(datum);
	if (!second_password_isnull)
		second_shadow_pass = TextDatumGetCString(second_datum);

	datum = SysCacheGetAttr(AUTHNAME, roleTup,
							Anum_pg_authid_rolvaliduntil, &vuntil_isnull);
	second_datum = SysCacheGetAttr(AUTHNAME, roleTup,
								   Anum_pg_authid_rolsecondvaliduntil,
								   &second_vuntil_isnull);
	if (!vuntil_isnull)
		vuntil = DatumGetTimestampTz(datum);
	if (!second_vuntil_isnull)
		second_vuntil = DatumGetTimestampTz(second_datum);

	ReleaseSysCache(roleTup);

	/*
	 * Password OK, but check to be sure we are not past rolvaliduntil
	 */
	current_ts = GetCurrentTimestamp();
	*num_passwords = (!password_isnull &&
					  (vuntil_isnull || vuntil >= current_ts))
		+ (!second_password_isnull &&
		   (second_vuntil_isnull || second_vuntil >= current_ts));

	if (*num_passwords >= 1)
	{
		int			i = 0;
		char	  **passwords = palloc(sizeof(char *) * (*num_passwords));

		if (!password_isnull && (vuntil_isnull || vuntil >= current_ts))
		{
			passwords[i] = shadow_pass;
			i++;
		}

		if (!second_password_isnull &&
			(second_vuntil_isnull || second_vuntil >= current_ts))
		{
			passwords[i] = second_shadow_pass;
			i++;
		}

		return passwords;
	}
	else
	{
		*logdetail = psprintf(_("User \"%s\" has an expired password."),
							  role);
		return NULL;
	}
}

/*
 * What kind of a password type is 'shadow_pass'?
 */
PasswordType
get_password_type(const char *shadow_pass)
{
	char	   *encoded_salt;
	int			iterations;
	int			key_length = 0;
	pg_cryptohash_type hash_type;
	uint8		stored_key[SCRAM_MAX_KEY_LEN];
	uint8		server_key[SCRAM_MAX_KEY_LEN];

	if (strncmp(shadow_pass, "md5", 3) == 0 &&
		strlen(shadow_pass) == MD5_PASSWD_LEN &&
		strspn(shadow_pass + 3, MD5_PASSWD_CHARSET) == MD5_PASSWD_LEN - 3)
		return PASSWORD_TYPE_MD5;
	if (parse_scram_secret(shadow_pass, &iterations, &hash_type, &key_length,
						   &encoded_salt, stored_key, server_key))
		return PASSWORD_TYPE_SCRAM_SHA_256;
	return PASSWORD_TYPE_PLAINTEXT;
}

/*
 * Given a user-supplied password, convert it into a secret of
 * 'target_type' kind.
 *
 * If the password is already in encrypted form, we cannot reverse the
 * hash, so it is stored as it is regardless of the requested type.
 */
char *
encrypt_password(PasswordType target_type, const char *salt,
				 const char *password)
{
	PasswordType guessed_type = get_password_type(password);
	char	   *encrypted_password;
	const char *errstr = NULL;

	if (guessed_type != PASSWORD_TYPE_PLAINTEXT)
	{
		/*
		 * Cannot convert an already-encrypted password from one format to
		 * another, so return it as it is.
		 */
		return pstrdup(password);
	}

	switch (target_type)
	{
		case PASSWORD_TYPE_MD5:
			encrypted_password = palloc(MD5_PASSWD_LEN + 1);

			if (!pg_md5_encrypt(password, salt, strlen(salt),
								encrypted_password, &errstr))
				elog(ERROR, "password encryption failed: %s", errstr);
			return encrypted_password;

		case PASSWORD_TYPE_SCRAM_SHA_256:
			return pg_be_scram_build_secret(password, salt);

		case PASSWORD_TYPE_PLAINTEXT:
			elog(ERROR, "cannot encrypt password with 'plaintext'");
	}

	/*
	 * This shouldn't happen, because the above switch statements should
	 * handle every combination of source and target password types.
	 */
	elog(ERROR, "cannot encrypt password to requested type");
	return NULL;				/* keep compiler quiet */
}

/*
 * Check MD5 authentication response, and return STATUS_OK or STATUS_ERROR.
 *
 * 'shadow_pass' is the user's correct password or password hash, as stored
 * in pg_authid's rolpassword or rolsecondpassword.
 * 'client_pass' is the response given by the remote user to the MD5 challenge.
 * 'md5_salt' is the salt used in the MD5 authentication challenge.
 *
 * In the error case, save a string at *logdetail that will be sent to the
 * postmaster log (but not the client).
 */
int
md5_crypt_verify(const char *role, const char *shadow_pass,
				 const char *client_pass,
				 const char *md5_salt, int md5_salt_len,
				 const char **logdetail)
{
	int			retval;
	char		crypt_pwd[MD5_PASSWD_LEN + 1];
	const char *errstr = NULL;

	Assert(md5_salt_len > 0);

	if (get_password_type(shadow_pass) != PASSWORD_TYPE_MD5)
	{
		/* incompatible password hash format. */
		*logdetail = psprintf(_("User \"%s\" has a password that cannot be used with MD5 authentication."),
							  role);
		return STATUS_ERROR;
	}

	/*
	 * Compute the correct answer for the MD5 challenge.
	 */
	/* stored password already encrypted, only do salt */
	if (!pg_md5_encrypt(shadow_pass + strlen("md5"),
						md5_salt, md5_salt_len,
						crypt_pwd, &errstr))
	{
		*logdetail = errstr;
		return STATUS_ERROR;
	}

	if (strcmp(client_pass, crypt_pwd) == 0)
		retval = STATUS_OK;
	else
	{
		*logdetail = psprintf(_("Password does not match for user \"%s\"."),
							  role);
		retval = STATUS_ERROR;
	}

	return retval;
}

/*
 * Check given password for given user, and return STATUS_OK or STATUS_ERROR.
 *
 * 'shadow_pass' is the user's correct password hash, as stored in
 * pg_authid's rolpassword or rolsecondpassword.
 *
 * 'client_pass' is the password given by the remote user.
 *
 * In the error case, store a string at *logdetail that will be sent to the
 * postmaster log (but not the client).
 */
int
plain_crypt_verify(const char *role, const char *shadow_pass,
				   const char *client_pass,
				   const char **logdetail)
{
	char		crypt_client_pass[MD5_PASSWD_LEN + 1];
	const char *errstr = NULL;

	/*
	 * Client sent password in plaintext.  If we have an MD5 hash stored, hash
	 * the password the client sent, and compare the hashes.  Otherwise
	 * compare the plaintext passwords directly.
	 */
	switch (get_password_type(shadow_pass))
	{
		case PASSWORD_TYPE_SCRAM_SHA_256:
			if (scram_verify_plain_password(role,
											client_pass,
											shadow_pass))
			{
				return STATUS_OK;
			}
			else
			{
				*logdetail = psprintf(_("Password does not match for user \"%s\"."),
									  role);
				return STATUS_ERROR;
			}
			break;

		case PASSWORD_TYPE_MD5:
			if (!pg_md5_encrypt(client_pass,
								role,
								strlen(role),
								crypt_client_pass,
								&errstr))
			{
				*logdetail = errstr;
				return STATUS_ERROR;
			}
			if (strcmp(crypt_client_pass, shadow_pass) == 0)
				return STATUS_OK;
			else
			{
				*logdetail = psprintf(_("Password does not match for user \"%s\"."),
									  role);
				return STATUS_ERROR;
			}
			break;

		case PASSWORD_TYPE_PLAINTEXT:

			/*
			 * We never store passwords in plaintext, so this shouldn't
			 * happen.
			 */
			break;
	}

	/*
	 * This shouldn't happen.  Plain "password" authentication is possible
	 * with any kind of stored password hash.
	 */
	*logdetail = psprintf(_("Password of user \"%s\" is in unrecognized format."),
						  role);
	return STATUS_ERROR;
}
