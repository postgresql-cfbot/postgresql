/*-------------------------------------------------------------------------
 *
 * compression.h
 *	  Interface to libpq/compression.c
 *
 *
 * Copyright (c) 2023, PostgreSQL Global Development Group
 *
 * src/include/libpq/compression.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LIBPQ_COMPRESSION_H
#define LIBPQ_COMPRESSION_H

#include "postgres.h"
#include "libpq-be.h"
#include "common/compression.h"

extern PGDLLIMPORT char *libpq_compress_algorithms;
extern PGDLLIMPORT pg_compress_specification libpq_compressors[COMPRESSION_ALGORITHM_COUNT];
extern PGDLLIMPORT size_t libpq_n_compressors;

/*
 * Enbles compression processing on the given port.
 * val is the value of the _pq_.libpq_compression startup packet parameter
 */
extern void configure_libpq_compression(Port *port, const char *val);

#endif							/* LIBPQ_COMPRESSION_H */
