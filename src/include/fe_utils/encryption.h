/*-------------------------------------------------------------------------
 *
 * encryption.h
 *	  Client code to support full cluster encryption.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/include/fe_utils/encryption.h
 *
 *-------------------------------------------------------------------------
 */

/* Executable to retrieve the encryption key. */
extern char *encryption_key_command;

extern void init_kdf(void);
extern void write_kdf_file(char *dir);
extern void read_kdf_file(char *dir);
extern void derive_key_from_password(unsigned char *encryption_key,
									 const char *password, int len);
extern void run_encryption_key_command(unsigned char *encryption_key,
									   char *data_dir);
extern void read_encryption_key_fe(FILE *f);
extern bool send_key_to_postmaster(const char *host, const char *port,
								   const unsigned char *encryption_Key);
