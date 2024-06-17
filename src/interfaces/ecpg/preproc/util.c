/* src/interfaces/ecpg/preproc/util.c */

#include "postgres_fe.h"

#include <unistd.h>

#include "preproc_extern.h"

static void vmmerror(int error_code, enum errortype type, const char *error, va_list ap) pg_attribute_printf(3, 0);


/*
 * Handle preprocessor errors and warnings
 */
static void
vmmerror(int error_code, enum errortype type, const char *error, va_list ap)
{
	/* localize the error message string */
	error = _(error);

	fprintf(stderr, "%s:%d: ", input_filename, base_yylineno);

	switch (type)
	{
		case ET_WARNING:
			fprintf(stderr, _("WARNING: "));
			break;
		case ET_ERROR:
			fprintf(stderr, _("ERROR: "));
			break;
	}

	vfprintf(stderr, error, ap);

	fprintf(stderr, "\n");

	/* If appropriate, set error code to be inspected by ecpg.c */
	switch (type)
	{
		case ET_WARNING:
			break;
		case ET_ERROR:
			ret_value = error_code;
			break;
	}
}

/* Report an error or warning */
void
mmerror(int error_code, enum errortype type, const char *error,...)
{
	va_list		ap;

	va_start(ap, error);
	vmmerror(error_code, type, error, ap);
	va_end(ap);
}

/* Report an error and abandon execution */
void
mmfatal(int error_code, const char *error,...)
{
	va_list		ap;

	va_start(ap, error);
	vmmerror(error_code, ET_ERROR, error, ap);
	va_end(ap);

	if (base_yyin)
		fclose(base_yyin);
	if (base_yyout)
		fclose(base_yyout);

	if (strcmp(output_filename, "-") != 0 && unlink(output_filename) != 0)
		fprintf(stderr, _("could not remove output file \"%s\"\n"), output_filename);
	exit(error_code);
}

/*
 * Basic memory management support
 */

/* malloc + error check */
void *
mm_alloc(size_t size)
{
	void	   *ptr = malloc(size);

	if (ptr == NULL)
		mmfatal(OUT_OF_MEMORY, "out of memory");

	return ptr;
}

/* strdup + error check */
char *
mm_strdup(const char *string)
{
	char	   *new = strdup(string);

	if (new == NULL)
		mmfatal(OUT_OF_MEMORY, "out of memory");

	return new;
}

/*
 * "Local" (or "location"?) memory management support
 *
 * These functions manage memory that is only needed for a short time
 * (processing of one input statement) within the ecpg grammar.
 * Data allocated with these is not meant to be freed separately;
 * rather it's freed by calling reclaim_local_storage() at the end
 * of each statement cycle.
 */

typedef struct loc_chunk
{
	struct loc_chunk *next;		/* list link */
	unsigned int chunk_used;	/* index of first unused byte in data[] */
	unsigned int chunk_avail;	/* # bytes still available in data[] */
	char		data[FLEXIBLE_ARRAY_MEMBER];	/* actual storage */
} loc_chunk;

#define LOC_CHUNK_OVERHEAD	MAXALIGN(offsetof(loc_chunk, data))
#define LOC_CHUNK_MIN_SIZE	8192

/* Head of list of loc_chunks */
static loc_chunk *loc_chunks = NULL;

/*
 * Allocate local space of the requested size.
 *
 * Exits on OOM.
 */
void *
loc_alloc(size_t size)
{
	void	   *result;
	loc_chunk  *cur_chunk = loc_chunks;

	/* Ensure all allocations are adequately aligned */
	size = MAXALIGN(size);

	/* Need a new chunk? */
	if (cur_chunk == NULL || size > cur_chunk->chunk_avail)
	{
		size_t		chunk_size = Max(size, LOC_CHUNK_MIN_SIZE);

		cur_chunk = mm_alloc(chunk_size + LOC_CHUNK_OVERHEAD);
		/* Depending on alignment rules, we could waste a bit here */
		cur_chunk->chunk_used = LOC_CHUNK_OVERHEAD - offsetof(loc_chunk, data);
		cur_chunk->chunk_avail = chunk_size;
		/* New chunk becomes the head of the list */
		cur_chunk->next = loc_chunks;
		loc_chunks = cur_chunk;
	}

	result = cur_chunk->data + cur_chunk->chunk_used;
	cur_chunk->chunk_used += size;
	cur_chunk->chunk_avail -= size;
	return result;
}

/*
 * Copy given string into local storage
 */
char *
loc_strdup(const char *string)
{
	char	   *result = loc_alloc(strlen(string) + 1);

	strcpy(result, string);
	return result;
}

/*
 * Reclaim local storage when appropriate
 */
void
reclaim_local_storage(void)
{
	loc_chunk  *cur_chunk,
			   *next_chunk;

	for (cur_chunk = loc_chunks; cur_chunk; cur_chunk = next_chunk)
	{
		next_chunk = cur_chunk->next;
		free(cur_chunk);
	}
	loc_chunks = NULL;
}
