/*-------------------------------------------------------------------------
 *
 * arrayutils.h
 *		inline C-array utility functions
 *
 * Portions Copyright (c) 2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/include/lib/arrayutils.h
 *-------------------------------------------------------------------------
 */

#ifndef ARRAYUTILS_H
#define ARRAYUTILS_H

/*
 * Remove duplicates from a pre-sorted array, according to a user-supplied
 * comparator.  Usually the array should have been sorted with qsort using the
 * same arguments.  Return the new size.
 *
 * The name reflects the fact that the arguments are compatible with the
 * standard qsort function.
 */
static inline size_t
qunique(void *array, size_t elements, size_t width,
		int (*compare)(const void *, const void *))
{
	int i, j;
	char *bytes = array;

	if (elements <= 1)
		return elements;

	for (i = 1, j = 0; i < elements; ++i)
	{
		if (compare(bytes + i * width, bytes + j * width) != 0)
			memcpy(bytes + ++j * width, bytes + i * width, width);
	}

	return j + 1;
}

/*
 * Like qunique, but takes a comparator with an extra user data argument which
 * is passed through, for compatibility with qsort_arg.
 */
static inline size_t
qunique_arg(void *array, size_t elements, size_t width,
			int (*compare)(const void *, const void *, void *),
			void *arg)
{
	int i, j;
	char *bytes = array;

	if (elements <= 1)
		return elements;

	for (i = 1, j = 0; i < elements; ++i)
	{
		if (compare(bytes + i * width, bytes + j * width, arg) != 0)
			memcpy(bytes + ++j * width, bytes + i * width, width);
	}

	return j + 1;
}

#endif
