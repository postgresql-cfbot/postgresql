/*-------------------------------------------------------------------------
*
 * compression.c
 *	  Functions and variables to support backend configuration of libpq
 *
 *
 * Copyright (c) 2023, PostgreSQL Global Development Group
 *
 * src/backend/libpq/compression.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "libpq/libpq-be.h"
#include "libpq/compression.h"
#include "utils/guc_hooks.h"

/* GUC variable containing the allowed compression algorithms list (separated by semicolon) */
char	   *libpq_compress_algorithms = "on";
pg_compress_specification libpq_compressors[COMPRESSION_ALGORITHM_COUNT];
size_t		libpq_n_compressors = 0;

bool
check_libpq_compression(char **newval, void **extra, GucSource source)
{
	pg_compress_specification compressors[COMPRESSION_ALGORITHM_COUNT];
	size_t		n_compressors;
	char	   *serialized_compressors;

	if (zpq_parse_compression_setting(*newval, compressors, &n_compressors) == -1)
	{
		GUC_check_errdetail("Cannot parse the libpq_compression setting.");
		return false;
	}

	if (n_compressors > 0)
	{
		guc_free(*newval);
		serialized_compressors = zpq_serialize_compressors(compressors, n_compressors);
		*newval = guc_strdup(ERROR, serialized_compressors);
		pfree(serialized_compressors);
	}
	else
	{
		guc_free(*newval);
		*newval = guc_strdup(ERROR, "");
	}
	return true;
}

void
assign_libpq_compression(const char *newval, void *extra)
{
	if (strlen(newval) == 0)
	{
		libpq_n_compressors = 0;
		return;
	}
	zpq_parse_compression_setting(newval, libpq_compressors, &libpq_n_compressors);
}

void
configure_libpq_compression(Port *port, const char *newval)
{
	pg_compress_specification fe_compressors[COMPRESSION_ALGORITHM_COUNT];
	size_t		n_fe_compressors;

	Assert(!port->zpq_stream);

	if (libpq_n_compressors == 0)
	{
		return;
	}

	/* Init compression */
	port->zpq_stream = zpq_create(libpq_compressors, libpq_n_compressors, MyProcPort->io_stream);
	if (!port->zpq_stream)
	{
		ereport(FATAL,
				errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("failed to initialize the compression stream"));
	}

	if (strlen(newval) == 0)
	{
		return;
	}

	if (!zpq_deserialize_compressors(newval, fe_compressors, &n_fe_compressors))
	{
		ereport(FATAL,
				errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("Cannot parse the _pq_.libpq_compression setting."));
	}

	if (MyProcPort && MyProcPort->zpq_stream)
	{
		pg_compress_algorithm algorithms[COMPRESSION_ALGORITHM_COUNT];
		size_t		n_algorithms = 0;

		if (n_fe_compressors == 0)
		{
			return;
		}

		/*
		 * Intersect client and server compressors to determine the final list
		 * of the supported compressors. O(N^2) is negligible because of a
		 * small number of the compression methods.
		 */
		for (size_t i = 0; i < libpq_n_compressors; i++)
		{
			for (size_t j = 0; j < n_fe_compressors; j++)
			{
				if (libpq_compressors[i].algorithm == fe_compressors[j].algorithm && libpq_compressors[i].compress && fe_compressors[j].decompress)
				{
					algorithms[n_algorithms] = libpq_compressors[i].algorithm;
					n_algorithms += 1;
					break;
				}
			}
		}

		zpq_enable_compression(MyProcPort->zpq_stream, algorithms, n_algorithms);
	}
}
