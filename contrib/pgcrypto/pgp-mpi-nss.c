/*-------------------------------------------------------------------------
 *
 * pgp-mpi-nss.c
 *	  Wrapper for using NSS as a backend for pgcrypto PGP
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/pgcrypto/pgp-mpi-nss.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#ifdef USE_NSS

#include "pgp.h"
#include "px.h"

/*
 * TODO: There is no exported BIGNUM library in NSS mapping to the OpenSSL
 * counterpart so for now this isn't supported when using NSS as a backend.
 */
int
pgp_elgamal_encrypt(PGP_PubKey *pk, PGP_MPI *_m,
					PGP_MPI **c1_p, PGP_MPI **c2_p)
{
	return PXE_PGP_UNSUPPORTED_PUBALGO;
}

int
pgp_elgamal_decrypt(PGP_PubKey *pk, PGP_MPI *_c1, PGP_MPI *_c2,
					PGP_MPI **msg_p)
{
	return PXE_PGP_UNSUPPORTED_PUBALGO;
}

int
pgp_rsa_encrypt(PGP_PubKey *pk, PGP_MPI *_m, PGP_MPI **c_p)
{
	return PXE_PGP_UNSUPPORTED_PUBALGO;
}

int
pgp_rsa_decrypt(PGP_PubKey *pk, PGP_MPI *_c, PGP_MPI **m_p)
{
	return PXE_PGP_UNSUPPORTED_PUBALGO;
}

#endif							/* USE_NSS */
