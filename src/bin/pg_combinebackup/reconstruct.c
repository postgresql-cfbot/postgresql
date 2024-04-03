/*-------------------------------------------------------------------------
 *
 * reconstruct.c
 *		Reconstruct full file from incremental file and backup chain.
 *
 * Copyright (c) 2017-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/bin/pg_combinebackup/reconstruct.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <unistd.h>

#include "backup/basebackup_incremental.h"
#include "common/file_perm.h"
#include "common/logging.h"
#include "copy_file.h"
#include "lib/stringinfo.h"
#include "reconstruct.h"
#include "storage/block.h"

/*
 * An rfile stores the data that we need in order to be able to use some file
 * on disk for reconstruction. For any given output file, we create one rfile
 * per backup that we need to consult when we constructing that output file.
 *
 * If we find a full version of the file in the backup chain, then only
 * filename and fd are initialized; the remaining fields are 0 or NULL.
 * For an incremental file, header_length, num_blocks, relative_block_numbers,
 * and truncation_block_length are also set.
 *
 * num_blocks_read and highest_offset_read always start out as 0.
 */
typedef struct rfile
{
	char	   *filename;
	int			fd;
	size_t		header_length;
	unsigned	num_blocks;
	BlockNumber *relative_block_numbers;
	unsigned	truncation_block_length;
	unsigned	num_blocks_read;
	off_t		highest_offset_read;
} rfile;

static void debug_reconstruction(int n_source,
								 rfile **sources,
								 bool dry_run);
static unsigned find_reconstructed_block_length(rfile *s);
static rfile *make_incremental_rfile(char *filename);
static rfile *make_rfile(char *filename, bool missing_ok);
static void write_reconstructed_file(char *input_filename,
									 char *output_filename,
									 unsigned block_length,
									 rfile **sourcemap,
									 off_t *offsetmap,
									 pg_checksum_context *checksum_ctx,
									 bool debug,
									 bool dry_run,
									 CopyMethod copy_method,
									 int prefetch_target);
static void read_bytes(rfile *rf, void *buffer, unsigned length);
static void write_blocks(int wfd, char *output_filename,
						 uint8 *buffer, int nblocks,
						 pg_checksum_context *checksum_ctx);
static void read_blocks(rfile *s, off_t off, uint8 *buffer, int nblocks);

/*
 * state of the asynchronous prefetcher
 */
typedef struct prefetch_state
{
	unsigned	next_block;			/* block to prefetch next */
	int			prefetch_distance;	/* current distance (<= target) */
	int			prefetch_target;	/* target distance */

	/* prefetch statistics - number of requests and prefetched blocks */
	unsigned	prefetch_count;
	unsigned	prefetch_blocks;
} prefetch_state;

static void prefetch_init(prefetch_state *state,
						  int prefetch_target);
static void prefetch_blocks(prefetch_state *state,
							unsigned block,
							unsigned block_length,
							rfile **sourcemap,
							off_t *offsetmap);

/*
 * Reconstruct a full file from an incremental file and a chain of prior
 * backups.
 *
 * input_filename should be the path to the incremental file, and
 * output_filename should be the path where the reconstructed file is to be
 * written.
 *
 * relative_path should be the relative path to the directory containing this
 * file. bare_file_name should be the name of the file within that directory,
 * without "INCREMENTAL.".
 *
 * n_prior_backups is the number of prior backups, and prior_backup_dirs is
 * an array of pathnames where those backups can be found.
 */
void
reconstruct_from_incremental_file(char *input_filename,
								  char *output_filename,
								  char *relative_path,
								  char *bare_file_name,
								  int n_prior_backups,
								  char **prior_backup_dirs,
								  manifest_data **manifests,
								  char *manifest_path,
								  pg_checksum_type checksum_type,
								  int *checksum_length,
								  uint8 **checksum_payload,
								  bool debug,
								  bool dry_run,
								  CopyMethod copy_method,
								  int prefetch_target)
{
	rfile	  **source;
	rfile	   *latest_source = NULL;
	rfile	  **sourcemap;
	off_t	   *offsetmap;
	unsigned	block_length;
	unsigned	i;
	unsigned	sidx = n_prior_backups;
	bool		full_copy_possible = true;
	int			copy_source_index = -1;
	rfile	   *copy_source = NULL;
	pg_checksum_context checksum_ctx;

	/*
	 * Every block must come either from the latest version of the file or
	 * from one of the prior backups.
	 */
	source = pg_malloc0(sizeof(rfile *) * (1 + n_prior_backups));

	/*
	 * Use the information from the latest incremental file to figure out how
	 * long the reconstructed file should be.
	 */
	latest_source = make_incremental_rfile(input_filename);
	source[n_prior_backups] = latest_source;
	block_length = find_reconstructed_block_length(latest_source);

	/*
	 * For each block in the output file, we need to know from which file we
	 * need to obtain it and at what offset in that file it's stored.
	 * sourcemap gives us the first of these things, and offsetmap the latter.
	 */
	sourcemap = pg_malloc0(sizeof(rfile *) * block_length);
	offsetmap = pg_malloc0(sizeof(off_t) * block_length);

	/*
	 * Every block that is present in the newest incremental file should be
	 * sourced from that file. If it precedes the truncation_block_length,
	 * it's a block that we would otherwise have had to find in an older
	 * backup and thus reduces the number of blocks remaining to be found by
	 * one; otherwise, it's an extra block that needs to be included in the
	 * output but would not have needed to be found in an older backup if it
	 * had not been present.
	 */
	for (i = 0; i < latest_source->num_blocks; ++i)
	{
		BlockNumber b = latest_source->relative_block_numbers[i];

		Assert(b < block_length);
		sourcemap[b] = latest_source;
		offsetmap[b] = latest_source->header_length + (i * BLCKSZ);

		/*
		 * A full copy of a file from an earlier backup is only possible if no
		 * blocks are needed from any later incremental file.
		 */
		full_copy_possible = false;
	}

	while (1)
	{
		char		source_filename[MAXPGPATH];
		rfile	   *s;

		/*
		 * Move to the next backup in the chain. If there are no more, then
		 * we're done.
		 */
		if (sidx == 0)
			break;
		--sidx;

		/*
		 * Look for the full file in the previous backup. If not found, then
		 * look for an incremental file instead.
		 */
		snprintf(source_filename, MAXPGPATH, "%s/%s/%s",
				 prior_backup_dirs[sidx], relative_path, bare_file_name);
		if ((s = make_rfile(source_filename, true)) == NULL)
		{
			snprintf(source_filename, MAXPGPATH, "%s/%s/INCREMENTAL.%s",
					 prior_backup_dirs[sidx], relative_path, bare_file_name);
			s = make_incremental_rfile(source_filename);
		}
		source[sidx] = s;

		/*
		 * If s->header_length == 0, then this is a full file; otherwise, it's
		 * an incremental file.
		 */
		if (s->header_length == 0)
		{
			struct stat sb;
			BlockNumber b;
			BlockNumber blocklength;

			/* We need to know the length of the file. */
			if (fstat(s->fd, &sb) < 0)
				pg_fatal("could not stat \"%s\": %m", s->filename);

			/*
			 * Since we found a full file, source all blocks from it that
			 * exist in the file.
			 *
			 * Note that there may be blocks that don't exist either in this
			 * file or in any incremental file but that precede
			 * truncation_block_length. These are, presumably, zero-filled
			 * blocks that result from the server extending the file but
			 * taking no action on those blocks that generated any WAL.
			 *
			 * Sadly, we have no way of validating that this is really what
			 * happened, and neither does the server. From it's perspective,
			 * an unmodified block that contains data looks exactly the same
			 * as a zero-filled block that never had any data: either way,
			 * it's not mentioned in any WAL summary and the server has no
			 * reason to read it. From our perspective, all we know is that
			 * nobody had a reason to back up the block. That certainly means
			 * that the block didn't exist at the time of the full backup, but
			 * the supposition that it was all zeroes at the time of every
			 * later backup is one that we can't validate.
			 */
			blocklength = sb.st_size / BLCKSZ;
			for (b = 0; b < latest_source->truncation_block_length; ++b)
			{
				if (sourcemap[b] == NULL && b < blocklength)
				{
					sourcemap[b] = s;
					offsetmap[b] = b * BLCKSZ;
				}
			}

			/*
			 * If a full copy looks possible, check whether the resulting file
			 * should be exactly as long as the source file is. If so, a full
			 * copy is acceptable, otherwise not.
			 */
			if (full_copy_possible)
			{
				uint64		expected_length;

				expected_length =
					(uint64) latest_source->truncation_block_length;
				expected_length *= BLCKSZ;
				if (expected_length == sb.st_size)
				{
					copy_source = s;
					copy_source_index = sidx;
				}
			}

			/* We don't need to consider any further sources. */
			break;
		}

		/*
		 * Since we found another incremental file, source all blocks from it
		 * that we need but don't yet have.
		 */
		for (i = 0; i < s->num_blocks; ++i)
		{
			BlockNumber b = s->relative_block_numbers[i];

			if (b < latest_source->truncation_block_length &&
				sourcemap[b] == NULL)
			{
				sourcemap[b] = s;
				offsetmap[b] = s->header_length + (i * BLCKSZ);

				/*
				 * A full copy of a file from an earlier backup is only
				 * possible if no blocks are needed from any later incremental
				 * file.
				 */
				full_copy_possible = false;
			}
		}
	}

	/*
	 * If a checksum of the required type already exists in the
	 * backup_manifest for the relevant input directory, we can save some work
	 * by reusing that checksum instead of computing a new one.
	 */
	if (copy_source_index >= 0 && manifests[copy_source_index] != NULL &&
		checksum_type != CHECKSUM_TYPE_NONE)
	{
		manifest_file *mfile;

		mfile = manifest_files_lookup(manifests[copy_source_index]->files,
									  manifest_path);
		if (mfile == NULL)
		{
			char	   *path = psprintf("%s/backup_manifest",
										prior_backup_dirs[copy_source_index]);

			/*
			 * The directory is out of sync with the backup_manifest, so emit
			 * a warning.
			 */
			/*- translator: the first %s is a backup manifest file, the second is a file absent therein */
			pg_log_warning("\"%s\" contains no entry for \"%s\"",
						   path,
						   manifest_path);
			pfree(path);
		}
		else if (mfile->checksum_type == checksum_type)
		{
			*checksum_length = mfile->checksum_length;
			*checksum_payload = pg_malloc(*checksum_length);
			memcpy(*checksum_payload, mfile->checksum_payload,
				   *checksum_length);
			checksum_type = CHECKSUM_TYPE_NONE;
		}
	}

	/* Prepare for checksum calculation, if required. */
	pg_checksum_init(&checksum_ctx, checksum_type);

	/*
	 * If the full file can be created by copying a file from an older backup
	 * in the chain without needing to overwrite any blocks or truncate the
	 * result, then forget about performing reconstruction and just copy that
	 * file in its entirety.
	 *
	 * Otherwise, reconstruct.
	 */
	if (copy_source != NULL)
		copy_file(copy_source->filename, output_filename,
				  &checksum_ctx, dry_run, copy_method);
	else
	{
		write_reconstructed_file(input_filename, output_filename,
								 block_length, sourcemap, offsetmap,
								 &checksum_ctx, debug, dry_run,
								 copy_method, prefetch_target);
		debug_reconstruction(n_prior_backups + 1, source, dry_run);
	}

	/* Save results of checksum calculation. */
	if (checksum_type != CHECKSUM_TYPE_NONE)
	{
		*checksum_payload = pg_malloc(PG_CHECKSUM_MAX_LENGTH);
		*checksum_length = pg_checksum_final(&checksum_ctx,
											 *checksum_payload);
	}

	/*
	 * Close files and release memory.
	 */
	for (i = 0; i <= n_prior_backups; ++i)
	{
		rfile	   *s = source[i];

		if (s == NULL)
			continue;
		if (close(s->fd) != 0)
			pg_fatal("could not close \"%s\": %m", s->filename);
		if (s->relative_block_numbers != NULL)
			pfree(s->relative_block_numbers);
		pg_free(s->filename);
	}
	pfree(sourcemap);
	pfree(offsetmap);
	pfree(source);
}

/*
 * Perform post-reconstruction logging and sanity checks.
 */
static void
debug_reconstruction(int n_source, rfile **sources, bool dry_run)
{
	unsigned	i;

	for (i = 0; i < n_source; ++i)
	{
		rfile	   *s = sources[i];

		/* Ignore source if not used. */
		if (s == NULL)
			continue;

		/* If no data is needed from this file, we can ignore it. */
		if (s->num_blocks_read == 0)
			continue;

		/* Debug logging. */
		if (dry_run)
			pg_log_debug("would have read %u blocks from \"%s\"",
						 s->num_blocks_read, s->filename);
		else
			pg_log_debug("read %u blocks from \"%s\"",
						 s->num_blocks_read, s->filename);

		/*
		 * In dry-run mode, we don't actually try to read data from the file,
		 * but we do try to verify that the file is long enough that we could
		 * have read the data if we'd tried.
		 *
		 * If this fails, then it means that a non-dry-run attempt would fail,
		 * complaining of not being able to read the required bytes from the
		 * file.
		 */
		if (dry_run)
		{
			struct stat sb;

			if (fstat(s->fd, &sb) < 0)
				pg_fatal("could not stat \"%s\": %m", s->filename);
			if (sb.st_size < s->highest_offset_read)
				pg_fatal("file \"%s\" is too short: expected %llu, found %llu",
						 s->filename,
						 (unsigned long long) s->highest_offset_read,
						 (unsigned long long) sb.st_size);
		}
	}
}

/*
 * When we perform reconstruction using an incremental file, the output file
 * should be at least as long as the truncation_block_length. Any blocks
 * present in the incremental file increase the output length as far as is
 * necessary to include those blocks.
 */
static unsigned
find_reconstructed_block_length(rfile *s)
{
	unsigned	block_length = s->truncation_block_length;
	unsigned	i;

	for (i = 0; i < s->num_blocks; ++i)
		if (s->relative_block_numbers[i] >= block_length)
			block_length = s->relative_block_numbers[i] + 1;

	return block_length;
}

/*
 * Initialize an incremental rfile, reading the header so that we know which
 * blocks it contains.
 */
static rfile *
make_incremental_rfile(char *filename)
{
	rfile	   *rf;
	unsigned	magic;

	rf = make_rfile(filename, false);

	/* Read and validate magic number. */
	read_bytes(rf, &magic, sizeof(magic));
	if (magic != INCREMENTAL_MAGIC)
		pg_fatal("file \"%s\" has bad incremental magic number (0x%x not 0x%x)",
				 filename, magic, INCREMENTAL_MAGIC);

	/* Read block count. */
	read_bytes(rf, &rf->num_blocks, sizeof(rf->num_blocks));
	if (rf->num_blocks > RELSEG_SIZE)
		pg_fatal("file \"%s\" has block count %u in excess of segment size %u",
				 filename, rf->num_blocks, RELSEG_SIZE);

	/* Read truncation block length. */
	read_bytes(rf, &rf->truncation_block_length,
			   sizeof(rf->truncation_block_length));
	if (rf->truncation_block_length > RELSEG_SIZE)
		pg_fatal("file \"%s\" has truncation block length %u in excess of segment size %u",
				 filename, rf->truncation_block_length, RELSEG_SIZE);

	/* Read block numbers if there are any. */
	if (rf->num_blocks > 0)
	{
		rf->relative_block_numbers =
			pg_malloc0(sizeof(BlockNumber) * rf->num_blocks);
		read_bytes(rf, rf->relative_block_numbers,
				   sizeof(BlockNumber) * rf->num_blocks);
	}

	/* Remember length of header. */
	rf->header_length = sizeof(magic) + sizeof(rf->num_blocks) +
		sizeof(rf->truncation_block_length) +
		sizeof(BlockNumber) * rf->num_blocks;

	/*
	 * Round header length to a multiple of BLCKSZ, so that blocks contents
	 * are properly aligned. Only do this when the file actually has data for
	 * some blocks.
	 */
	if ((rf->num_blocks > 0) && ((rf->header_length % BLCKSZ) != 0))
		rf->header_length += (BLCKSZ - (rf->header_length % BLCKSZ));

	return rf;
}

/*
 * Allocate and perform basic initialization of an rfile.
 */
static rfile *
make_rfile(char *filename, bool missing_ok)
{
	rfile	   *rf;

	rf = pg_malloc0(sizeof(rfile));
	rf->filename = pstrdup(filename);
	if ((rf->fd = open(filename, O_RDONLY | PG_BINARY, 0)) < 0)
	{
		if (missing_ok && errno == ENOENT)
		{
			pg_free(rf);
			return NULL;
		}
		pg_fatal("could not open file \"%s\": %m", filename);
	}

	return rf;
}

/*
 * Read the indicated number of bytes from an rfile into the buffer.
 */
static void
read_bytes(rfile *rf, void *buffer, unsigned length)
{
	int			rb = read(rf->fd, buffer, length);

	if (rb != length)
	{
		if (rb < 0)
			pg_fatal("could not read file \"%s\": %m", rf->filename);
		else
			pg_fatal("could not read file \"%s\": read only %d of %u bytes",
					 rf->filename, rb, length);
	}
}

/*
 * Write out a reconstructed file.
 */
static void
write_reconstructed_file(char *input_filename,
						 char *output_filename,
						 unsigned block_length,
						 rfile **sourcemap,
						 off_t *offsetmap,
						 pg_checksum_context *checksum_ctx,
						 bool debug,
						 bool dry_run,
						 CopyMethod copy_method,
						 int prefetch_target)
{
	int			wfd = -1;
	unsigned	next_idx;
	unsigned	zero_blocks = 0;
	prefetch_state	prefetch;

	/* initialize the block prefetcher */
	prefetch_init(&prefetch, prefetch_target);

	/* Debugging output. */
	if (debug)
	{
		StringInfoData debug_buf;
		unsigned	start_of_range = 0;
		unsigned	current_block = 0;

		/* Basic information about the output file to be produced. */
		if (dry_run)
			pg_log_debug("would reconstruct \"%s\" (%u blocks, checksum %s)",
						 output_filename, block_length,
						 pg_checksum_type_name(checksum_ctx->type));
		else
			pg_log_debug("reconstructing \"%s\" (%u blocks, checksum %s)",
						 output_filename, block_length,
						 pg_checksum_type_name(checksum_ctx->type));

		/* Print out the plan for reconstructing this file. */
		initStringInfo(&debug_buf);
		while (current_block < block_length)
		{
			rfile	   *s = sourcemap[current_block];

			/* Extend range, if possible. */
			if (current_block + 1 < block_length &&
				s == sourcemap[current_block + 1])
			{
				++current_block;
				continue;
			}

			/* Add details about this range. */
			if (s == NULL)
			{
				if (current_block == start_of_range)
					appendStringInfo(&debug_buf, " %u:zero", current_block);
				else
					appendStringInfo(&debug_buf, " %u-%u:zero",
									 start_of_range, current_block);
			}
			else
			{
				if (current_block == start_of_range)
					appendStringInfo(&debug_buf, " %u:%s@" UINT64_FORMAT,
									 current_block, s->filename,
									 (uint64) offsetmap[current_block]);
				else
					appendStringInfo(&debug_buf, " %u-%u:%s@" UINT64_FORMAT,
									 start_of_range, current_block,
									 s->filename,
									 (uint64) offsetmap[current_block]);
			}

			/* Begin new range. */
			start_of_range = ++current_block;

			/* If the output is very long or we are done, dump it now. */
			if (current_block == block_length || debug_buf.len > 1024)
			{
				pg_log_debug("reconstruction plan:%s", debug_buf.data);
				resetStringInfo(&debug_buf);
			}
		}

		/* Free memory. */
		pfree(debug_buf.data);
	}

	/* Open the output file, except in dry_run mode. */
	if (!dry_run &&
		(wfd = open(output_filename,
					O_RDWR | PG_BINARY | O_CREAT | O_EXCL,
					pg_file_create_mode)) < 0)
		pg_fatal("could not open file \"%s\": %m", output_filename);

	/* Read and write the blocks as required. */
	next_idx = 0;
	while (next_idx < block_length)
	{
#define	BLOCK_COUNT(first, last)	((last) - (first) + 1)
#define	BATCH_SIZE		128			/* 1MB */
		uint8		buffer[BATCH_SIZE * BLCKSZ];
		int			first_idx = next_idx;
		int			last_idx = next_idx;
		rfile	   *s = sourcemap[first_idx];
		int			wb;
		int			nblocks;

		/*
		 * Determine the range of blocks coming from the same source file,
		 * but not more than BLOCK_COUNT (1MB) at a time. The range starts
		 * at first_idx, ends with last_idx (both are inclusive).
		 */
		while ((last_idx + 1 < block_length) &&			/* valid block */
			   (sourcemap[last_idx+1] == s) &&			/* same file */
			   (BLOCK_COUNT(first_idx, last_idx) < BATCH_SIZE))	/* 1MB */
			last_idx += 1;

		/* Calculate batch size, set start of the next loop. */
		nblocks = BLOCK_COUNT(first_idx, last_idx);
		next_idx += nblocks;

		Assert(nblocks <= BATCH_SIZE);
		Assert(next_idx == (last_idx + 1));

		/* Update accounting information. */
		if (s == NULL)
			zero_blocks += nblocks;
		else
		{
			s->num_blocks_read += nblocks;
			s->highest_offset_read = Max(s->highest_offset_read,
										 offsetmap[last_idx] + BLCKSZ);
		}

		/* Skip the rest of this in dry-run mode. */
		if (dry_run)
			continue;

		/* do prefetching if enabled */
		prefetch_blocks(&prefetch, last_idx, block_length, sourcemap, offsetmap);

		/* Read or zero-fill the block as appropriate. */
		if (s == NULL)
		{
			/*
			 * New block not mentioned in the WAL summary. Should have been an
			 * uninitialized block, so just zero-fill it.
			 */
			memset(buffer, 0, BLCKSZ);

			/* Write out the block(s), update checksum if needed. */
			write_blocks(wfd, output_filename, buffer, nblocks, checksum_ctx);

			continue;
		}

		/* now copy the blocks using either read/write or copy_file_range */
		if (copy_method != COPY_METHOD_COPY_FILE_RANGE)
		{
			/*
			 * Read the batch of blocks from the correct source file, and
			 * then write them out, possibly with checksum update.
			 */
			read_blocks(s, offsetmap[first_idx], buffer, nblocks);
			write_blocks(wfd, output_filename, buffer, nblocks, checksum_ctx);
		}
		else	/* use copy_file_range */
		{
			/* copy_file_range modifies the passed offset, so make a copy */
			off_t	off = offsetmap[first_idx];
			size_t	nwritten = 0;

			/*
			 * Retry until we've written all the bytes (the offset is updated
			 * by copy_file_range, and so is the wfd file offset).
			 */
			do
			{
				wb = copy_file_range(s->fd, &off, wfd, NULL, (BLCKSZ * nblocks) - nwritten, 0);

				if (wb < 0)
					pg_fatal("error while copying file range from \"%s\" to \"%s\": %m",
							 input_filename, output_filename);

				nwritten += wb;

			} while ((nblocks * BLCKSZ) > nwritten);

			/* when checksum calculation not needed, we're done */
			if (checksum_ctx->type == CHECKSUM_TYPE_NONE)
				continue;

			/* read the block(s) and update the checksum */
			read_blocks(s, offsetmap[first_idx], buffer, nblocks);

			/* Update the checksum computation. */
			if (pg_checksum_update(checksum_ctx, buffer, (nblocks * BLCKSZ)) < 0)
				pg_fatal("could not update checksum of file \"%s\"",
						 output_filename);
		}
	}

	/* Debugging output. */
	if (zero_blocks > 0)
	{
		if (dry_run)
			pg_log_debug("would have zero-filled %u blocks", zero_blocks);
		else
			pg_log_debug("zero-filled %u blocks", zero_blocks);
	}

	if (prefetch.prefetch_blocks > 0)
	{
		/* print how many blocks we prefetched / skipped */
		pg_log_debug("prefetch requests %u blocks %u (%u skipped)",
					 prefetch.prefetch_count, prefetch.prefetch_blocks,
					 (block_length - prefetch.prefetch_blocks));
	}

	/* Close the output file. */
	if (wfd >= 0 && close(wfd) != 0)
		pg_fatal("could not close \"%s\": %m", output_filename);
}

static void
write_blocks(int fd, char *output_filename,
			 uint8 *buffer, int nblocks, pg_checksum_context *checksum_ctx)
{
	int	wb;

	if ((wb = write(fd, buffer, nblocks * BLCKSZ)) != (nblocks * BLCKSZ))
	{
		if (wb < 0)
			pg_fatal("could not write file \"%s\": %m", output_filename);
		else
			pg_fatal("could not write file \"%s\": wrote only %d of %d bytes",
					 output_filename, wb, (nblocks * BLCKSZ));
	}

	/* Update the checksum computation. */
	if (pg_checksum_update(checksum_ctx, buffer, (nblocks * BLCKSZ)) < 0)
		pg_fatal("could not update checksum of file \"%s\"",
				 output_filename);
}

static void
read_blocks(rfile *s, off_t off, uint8 *buffer, int nblocks)
{
	int			rb;

	/* Read the block from the correct source, except if dry-run. */
	rb = pg_pread(s->fd, buffer, (nblocks * BLCKSZ), off);
	if (rb != (nblocks * BLCKSZ))
	{
		if (rb < 0)
			pg_fatal("could not read file \"%s\": %m", s->filename);
		else
			pg_fatal("could not read file \"%s\": read only %d of %d bytes at offset %llu",
					 s->filename, rb, (nblocks * BLCKSZ),
					 (unsigned long long) off);
	}
}

/*
 * prefetch_init
 *		Initializes state of the prefetcher.
 *
 * Initialize state of the prefetcher, to start with the first block and maximum
 * prefetch distance (prefetch_target=0 means prefetching disabled). The actual
 * prefetch distance will gradually increase, until it reaches the target.
 */
static void
prefetch_init(prefetch_state *state, int prefetch_target)
{
	Assert(prefetch_target >= 0);

	state->next_block = 0;

	/* XXX Disables the gradual ramp-up, but we're reading data in batches and
	 * we probably need to cover the whole next batch at once. */
	state->prefetch_distance = prefetch_target;
	state->prefetch_target = prefetch_target;

	state->prefetch_count = 0;
	state->prefetch_blocks = 0;
}

/*
 * prefetch_blocks
 *		Perform asynchronous prefetching of blocks to be reconstructed next.
 *
 * Initiates asynchronous prefetch of to be reconstructed blocks.
 *
 * current_block - The block to be reconstructed in the current loop, right
 * after the prefetching. This means we're potentially prefetching blocks
 * in the range [current_block+1, current_block+current_dinstance], with
 * both values inclusive.
 *
 * block_length - Number of blocks to reconstruct, also length of sourcemap
 * and offsetmap arrays.
 */
static void
prefetch_blocks(prefetch_state *state, unsigned current_block,
				unsigned block_length, rfile **sourcemap, off_t* offsetmap)
{
#ifdef USE_PREFETCH
	/* end of prefetch range (first block to not prefetch) */
	unsigned	max_block;

	/* bail out if prefetching not enabled */
	if (state->prefetch_target == 0)
		return;

	/* bail out if we've already prefetched the last block */
	if (state->next_block == block_length)
		return;

	/* gradually increase prefetch distance until the target */
	state->prefetch_distance = Min(state->prefetch_distance + 1,
								   state->prefetch_target);

	/*
	 * Where should we start prefetching? We don't want to prefetch blocks
	 * that we've already prefetched, that's pointless. And we also don't
	 * want to prefetch the block we're just about to read. This can't
	 * overflow because we know (current_block < block_length).
	 */
	state->next_block = Max(state->next_block, current_block + 1);

	/*
	 * How far to prefetch? Calculate the first block to not prefetch, i.e.
	 * right after [current_block + current_distance]. It's possible the
	 * second part overflows and wraps around, but in that case we just
	 * don't prefetch a couple pages at the end.
	 *
	 * XXX But this also shouldn't be possible, thanks to the check of
	 * (next_block == block_length) at the very beginning.
	 */
	max_block = Min(block_length, current_block + state->prefetch_distance + 1);

	while (state->next_block < max_block)
	{
		/* range to prefetch in this round */
		int			nblocks = 0;
		unsigned	block = state->next_block;

		rfile  *f = sourcemap[block];
		off_t	off = offsetmap[block];

		/* find the last block in this prefetch range */
		while ((block + nblocks < max_block) &&
			   (sourcemap[block + nblocks] == f))
			nblocks++;

		Assert(nblocks <= state->prefetch_distance);

		/* remember how far we prefetched, even for f=NULL */
		state->next_block = block + nblocks;

		if (f == NULL)
			continue;

		state->prefetch_blocks += nblocks;
		state->prefetch_count += 1;

		/* We ignore errors because this is only a hint.*/
		(void) posix_fadvise(f->fd, off, (nblocks * BLCKSZ), POSIX_FADV_WILLNEED);
	}

	Assert(state->next_block <= block_length);
#endif
}
