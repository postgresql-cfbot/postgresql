/*-------------------------------------------------------------------------
 *
 * sort_template.h
 *
 *	  A template for a sort algorithm that supports varying degrees of
 *	  specialization.
 *
 * Copyright (c) 2021-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1992-1994, Regents of the University of California
 *
 * Usage notes:
 *
 *	  To generate functions specialized for a type, the following parameter
 *	  macros should be #define'd before this file is included.
 *
 *	  - ST_SORT - the name of a sort function to be generated
 *	  - ST_ELEMENT_TYPE - type of the referenced elements
 *	  - ST_DECLARE - if defined the functions and types are declared
 *	  - ST_DEFINE - if defined the functions and types are defined
 *	  - ST_SCOPE - scope (e.g. extern, static inline) for functions
 *	  - ST_CHECK_FOR_INTERRUPTS - if defined the sort is interruptible
 *
 *	  Instead of ST_ELEMENT_TYPE, ST_ELEMENT_TYPE_VOID can be defined.  Then
 *	  the generated functions will automatically gain an "element_size"
 *	  parameter.  This allows us to generate a traditional qsort function.
 *
 *	  One of the following macros must be defined, to show how to compare
 *	  elements.  The first two options are arbitrary expressions depending
 *	  on whether an extra pass-through argument is desired, and the third
 *	  option should be defined if the sort function should receive a
 *	  function pointer at runtime.
 *
 *	  - ST_COMPARE(a, b) - a simple comparison expression
 *	  - ST_COMPARE(a, b, arg) - variant that takes an extra argument
 *	  - ST_COMPARE_RUNTIME_POINTER - sort function takes a function pointer
 *
 *	  To say that the comparator and therefore also sort function should
 *	  receive an extra pass-through argument, specify the type of the
 *	  argument.
 *
 *	  - ST_COMPARE_ARG_TYPE - type of extra argument
 *
 *	  The prototype of the generated sort function is:
 *
 *	  void ST_SORT(ST_ELEMENT_TYPE *data, size_t n,
 *				   [size_t element_size,]
 *				   [ST_SORT_compare_function compare,]
 *				   [ST_COMPARE_ARG_TYPE *arg]);
 *
 *	  ST_SORT_compare_function is a function pointer of the following type:
 *
 *	  int (*)(const ST_ELEMENT_TYPE *a, const ST_ELEMENT_TYPE *b,
 *			  [ST_COMPARE_ARG_TYPE *arg])
 *
 * HISTORY
 *
 *	  Modifications from vanilla NetBSD source:
 *	  - Add do ... while() macro fix
 *	  - Remove __inline, _DIAGASSERTs, __P
 *	  - Remove ill-considered "swap_cnt" switch to insertion sort, in favor
 *		of a simple check for presorted input.
 *	  - Take care to recurse on the smaller partition, to bound stack usage
 *	  - Convert into a header that can generate specialized functions
 *
 * IDENTIFICATION
 *		src/include/lib/sort_template.h
 *
 *-------------------------------------------------------------------------
 */

/*	  $NetBSD: qsort.c,v 1.13 2003/08/07 16:43:42 agc Exp $   */

/*-
 * Copyright (c) 1992, 1993
 *	  The Regents of the University of California.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *	  notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *	  notice, this list of conditions and the following disclaimer in the
 *	  documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *	  may be used to endorse or promote products derived from this software
 *	  without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/*
 * Qsort routine based on J. L. Bentley and M. D. McIlroy,
 * "Engineering a sort function",
 * Software--Practice and Experience 23 (1993) 1249-1265.
 *
 * We have modified their original by adding a check for already-sorted
 * input, which seems to be a win per discussions on pgsql-hackers around
 * 2006-03-21.
 *
 * Also, we recurse on the smaller partition and iterate on the larger one,
 * which ensures we cannot recurse more than log(N) levels (since the
 * partition recursed to is surely no more than half of the input).  Bentley
 * and McIlroy explicitly rejected doing this on the grounds that it's "not
 * worth the effort", but we have seen crashes in the field due to stack
 * overrun, so that judgment seems wrong.
 */

#define ST_MAKE_PREFIX(a) CppConcat(a,_)
#define ST_MAKE_NAME(a,b) ST_MAKE_NAME_(ST_MAKE_PREFIX(a),b)
#define ST_MAKE_NAME_(a,b) CppConcat(a,b)

/*
 * If the element type is void, we'll also need an element_size argument
 * because we don't know the size.
 */
#ifdef ST_ELEMENT_TYPE_VOID
#define ST_ELEMENT_TYPE void
#define ST_SORT_PROTO_ELEMENT_SIZE , size_t element_size
#define ST_SORT_INVOKE_ELEMENT_SIZE , element_size
#else
#define ST_SORT_PROTO_ELEMENT_SIZE
#define ST_SORT_INVOKE_ELEMENT_SIZE
#endif

/*
 * If the user wants to be able to pass in compare functions at runtime,
 * we'll need to make that an argument of the sort functions.
 */
#ifdef ST_COMPARE_RUNTIME_POINTER
/*
 * The type of the comparator function pointer that ST_SORT will take, unless
 * you've already declared a type name manually and want to use that instead of
 * having a new one defined.
 */
#ifndef ST_COMPARATOR_TYPE_NAME
#define ST_COMPARATOR_TYPE_NAME ST_MAKE_NAME(ST_SORT, compare_function)
#endif
#define ST_COMPARE compare
#ifndef ST_COMPARE_ARG_TYPE
#define ST_SORT_PROTO_COMPARE , ST_COMPARATOR_TYPE_NAME compare
#define ST_SORT_INVOKE_COMPARE , compare
#else
#define ST_SORT_PROTO_COMPARE , ST_COMPARATOR_TYPE_NAME compare
#define ST_SORT_INVOKE_COMPARE , compare
#endif
#else
#define ST_SORT_PROTO_COMPARE
#define ST_SORT_INVOKE_COMPARE
#endif

/*
 * If the user wants to use a compare function or expression that takes an
 * extra argument, we'll need to make that an argument of the sort and compare
 * functions.
 */
#ifdef ST_COMPARE_ARG_TYPE
#define ST_SORT_PROTO_ARG , ST_COMPARE_ARG_TYPE *arg
#define ST_SORT_INVOKE_ARG , arg
#else
#define ST_SORT_PROTO_ARG
#define ST_SORT_INVOKE_ARG
#endif

/* Default input size threshold to control algorithm choice. */
#ifndef ST_INSERTION_SORT_THRESHOLD
#define ST_INSERTION_SORT_THRESHOLD 12
#endif

#ifdef ST_DECLARE

#ifdef ST_COMPARE_RUNTIME_POINTER
typedef int (*ST_COMPARATOR_TYPE_NAME) (const ST_ELEMENT_TYPE *,
										const ST_ELEMENT_TYPE * ST_SORT_PROTO_ARG);
#endif

/* Declare the sort function.  Note optional arguments at end. */
ST_SCOPE void ST_SORT(ST_ELEMENT_TYPE * first, size_t n
					  ST_SORT_PROTO_ELEMENT_SIZE
					  ST_SORT_PROTO_COMPARE
					  ST_SORT_PROTO_ARG);

#endif

#ifdef ST_DEFINE

/* sort private helper functions */
#define ST_SORT_INTERNAL ST_MAKE_NAME(ST_SORT, internal)
#define ST_SWAP ST_MAKE_NAME(ST_SORT, swap)
#define ST_SWAPN ST_MAKE_NAME(ST_SORT, swapn)

/* Users expecting to run very large sorts may need them to be interruptible. */
#ifdef ST_CHECK_FOR_INTERRUPTS
#define DO_CHECK_FOR_INTERRUPTS() CHECK_FOR_INTERRUPTS()
#else
#define DO_CHECK_FOR_INTERRUPTS()
#endif

/*
 * Create wrapper macros that know how to invoke compare and sort with the
 * right arguments.
 */
#ifdef ST_COMPARE_RUNTIME_POINTER
#define DO_COMPARE(a_, b_) ST_COMPARE((a_), (b_) ST_SORT_INVOKE_ARG)
#elif defined(ST_COMPARE_ARG_TYPE)
#define DO_COMPARE(a_, b_) ST_COMPARE((a_), (b_), arg)
#else
#define DO_COMPARE(a_, b_) ST_COMPARE((a_), (b_))
#endif
#define DO_SORT(a_, n_)													\
	ST_SORT_INTERNAL((a_), (n_)													\
			ST_SORT_INVOKE_ELEMENT_SIZE									\
			ST_SORT_INVOKE_COMPARE										\
			ST_SORT_INVOKE_ARG)

/*
 * If we're working with void pointers, we'll use pointer arithmetic based on
 * uint8, and use the runtime element_size to step through the array and swap
 * elements.  Otherwise we'll work with ST_ELEMENT_TYPE.
 */
#ifndef ST_ELEMENT_TYPE_VOID
#define ST_POINTER_TYPE ST_ELEMENT_TYPE
#define ST_POINTER_STEP 1
#define DO_SWAPN(a_, b_, n_) ST_SWAPN((a_), (b_), (n_))
#define DO_SWAP(a_, b_) ST_SWAP((a_), (b_))
#else
#define ST_POINTER_TYPE uint8
#define ST_POINTER_STEP element_size
#define DO_SWAPN(a_, b_, n_) ST_SWAPN((a_), (b_), (n_))
#define DO_SWAP(a_, b_) DO_SWAPN((a_), (b_), element_size)
#endif

static inline void
ST_SWAP(ST_POINTER_TYPE * a, ST_POINTER_TYPE * b)
{
	ST_POINTER_TYPE tmp = *a;

	*a = *b;
	*b = tmp;
}

static inline void
ST_SWAPN(ST_POINTER_TYPE * a, ST_POINTER_TYPE * b, size_t n)
{
	for (size_t i = 0; i < n; ++i)
		ST_SWAP(&a[i], &b[i]);
}

/*
 * Workhorse for ST_SORT
 */
static void
ST_SORT_INTERNAL(ST_POINTER_TYPE * data, size_t n
		ST_SORT_PROTO_ELEMENT_SIZE
		ST_SORT_PROTO_COMPARE
		ST_SORT_PROTO_ARG)
{
	ST_POINTER_TYPE *a = data,
				*left,
				*right,
				*e1,
				*e2,
				*e3,
				*e4,
				*e5,
				*pivot1,
				*pivot2,
				*less,
				*great,
				*k,
			   *pa,
			   *pb,
			   *pc,
			   *pd,
			   *pl,
			   *pm,
			   *pn;
	size_t		d1,
				d2,
				d3,
				seventh;
	int			r,
				presorted;
	bool		unique_hint = true;

loop:
	DO_CHECK_FOR_INTERRUPTS();
	if (n < ST_INSERTION_SORT_THRESHOLD)
	{
		for (pm = a + ST_POINTER_STEP; pm < a + n * ST_POINTER_STEP;
			 pm += ST_POINTER_STEP)
			for (pl = pm; pl > a && DO_COMPARE(pl - ST_POINTER_STEP, pl) > 0;
				 pl -= ST_POINTER_STEP)
				DO_SWAP(pl, pl - ST_POINTER_STEP);
		return;
	}
	presorted = 1;
	for (pm = a + ST_POINTER_STEP; pm < a + n * ST_POINTER_STEP;
		 pm += ST_POINTER_STEP)
	{
		DO_CHECK_FOR_INTERRUPTS();
		if (DO_COMPARE(pm - ST_POINTER_STEP, pm) > 0)
		{
			presorted = 0;
			break;
		}
	}
	if (presorted)
		return;

	left = a;
	right = a + (n - 1) * ST_POINTER_STEP;

#ifdef QSORT_DEBUG
	elog(NOTICE, "start:");

	for (ST_POINTER_TYPE *i = left; i <= right; i += ST_POINTER_STEP)
	{
			elog(NOTICE, "%d ", *i);
	}
#endif

	/* select five pivot candidates spaced equally around the midpoint */
	seventh = n / 7 * ST_POINTER_STEP;
	e3 = a + (n / 2) * ST_POINTER_STEP;
	e2 = e3 - seventh;
	e1 = e2 - seventh;
	e4 = e3 + seventh;
	e5 = e4 + seventh;

	/* do insertion sort on pivot candidates */
	for (pm = e2; pm <= e5; pm += seventh)
		for (pl = pm; pl > e1 && (r = DO_COMPARE(pl - seventh, pl)) >= 0;
			 pl -= seventh)
			{
				if (r == 0)
				{
					/* found two equal candidates */
					unique_hint = false;
					break;
				}
				else
					DO_SWAP(pl, pl - seventh);
			}

	/*
	 * If the pivot candidates were all unique, use dual-pivot quicksort,
	 * otherwise use B&M quicksort since it is faster on inputs with many
	 * duplicates.
	 */
	if (unique_hint)
	{
		DO_SWAP(e2, left);
		DO_SWAP(e4, right);

		pivot1 = left;
		pivot2 = right;
		less = left + ST_POINTER_STEP;
		great = right - ST_POINTER_STEP;

		/*
		 * dual-pivot partitioning
		 *
		 *   left part           center part                   right part
		 * +--------------------------------------------------------------+
		 * |  < pivot1  |  pivot1 <= && <= pivot2  |    ?    |  > pivot2  |
		 * +--------------------------------------------------------------+
		 *               ^                          ^       ^
		 *               |                          |       |
		 *              less                        k     great
		 *
		 * Invariants:
		 *
		 *              all in (left, less)   < pivot1
		 *    pivot1 <= all in [less, k)     <= pivot2
		 *              all in (great, right) > pivot2
		 *
		 * k points to the first element in the "?" part
		 */
		for (k = less; k <= great; k += ST_POINTER_STEP)
		{
			if (DO_COMPARE(k, pivot1) < 0)
			{
				if (k > less)
					DO_SWAP(k, less);
				less += ST_POINTER_STEP;
			}
			else if (DO_COMPARE(k, pivot2) > 0)
			{
				while (k < great && DO_COMPARE(great, pivot2) > 0)
				{
					great-= ST_POINTER_STEP;
					DO_CHECK_FOR_INTERRUPTS();
				}

				DO_SWAP(k, great);
				great-= ST_POINTER_STEP;

				if (DO_COMPARE(k, pivot1) < 0)
				{
					if (k > less)
						DO_SWAP(k, less);
					less += ST_POINTER_STEP;
				}
			}
			DO_CHECK_FOR_INTERRUPTS();
		}

		DO_SWAP(less - ST_POINTER_STEP, pivot1);
		DO_SWAP(great + ST_POINTER_STEP, pivot2);

		d1 = (less - ST_POINTER_STEP) - left;
		d2 = (great + ST_POINTER_STEP) - less;
		d3 = right - (great + ST_POINTER_STEP);

		/* recurse on shorter subarrays to save stack space */
		if (d1 > d2)
		{
			/* recurse on d2 */
			DO_SORT(less, d2 / ST_POINTER_STEP);
			if (d1 > d3)
			{
				/* recurse on d3 */
				DO_SORT(great + 2 * ST_POINTER_STEP, d3 / ST_POINTER_STEP);
				/* iterate on d1 */
				/* DO_SORT(left, d1 / ST_POINTER_STEP) */
				n = d1 / ST_POINTER_STEP;
				goto loop;
			}
			else
			{
				goto recurse_d1;
			}
		}
		else
		{
recurse_d1:
			/* recurse on d1 */
			DO_SORT(left, d1 / ST_POINTER_STEP);
			if (d2 > d3)
			{
				/* recurse on d3 */
				DO_SORT(great + 2 * ST_POINTER_STEP, d3 / ST_POINTER_STEP);
				/* iterate on d2 */
				/* DO_SORT(less, d2 / ST_POINTER_STEP) */
				a = less;
				n = d2 / ST_POINTER_STEP;
				goto loop;
			}
			else
			{
				/* recurse on d2 */
				DO_SORT(less, d2 / ST_POINTER_STEP);
				/* iterate on d3 */
				/* DO_SORT(great + 2 * ST_POINTER_STEP, d3 / ST_POINTER_STEP) */
				a = great + 2 * ST_POINTER_STEP;
				n = d3 / ST_POINTER_STEP;
				goto loop;
			}
		}
	}
	else
	/* classic B&M quicksort with a single pivot */
	// WIP: clean up variables to match dual pivot more closely
	{
	/* use median of five for the pivot */
	DO_SWAP(a, e3);
	pa = pb = a + ST_POINTER_STEP;
	pc = pd = a + (n - 1) * ST_POINTER_STEP;
	for (;;)
	{
		while (pb <= pc && (r = DO_COMPARE(pb, a)) <= 0)
		{
			if (r == 0)
			{
				DO_SWAP(pa, pb);
				pa += ST_POINTER_STEP;
			}
			pb += ST_POINTER_STEP;
			DO_CHECK_FOR_INTERRUPTS();
		}
		while (pb <= pc && (r = DO_COMPARE(pc, a)) >= 0)
		{
			if (r == 0)
			{
				DO_SWAP(pc, pd);
				pd -= ST_POINTER_STEP;
			}
			pc -= ST_POINTER_STEP;
			DO_CHECK_FOR_INTERRUPTS();
		}
		if (pb > pc)
			break;
		DO_SWAP(pb, pc);
		pb += ST_POINTER_STEP;
		pc -= ST_POINTER_STEP;
	}
	pn = a + n * ST_POINTER_STEP;
	d1 = Min(pa - a, pb - pa);
	DO_SWAPN(a, pb - d1, d1);
	d1 = Min(pd - pc, pn - pd - ST_POINTER_STEP);
	DO_SWAPN(pb, pn - d1, d1);
	d1 = pb - pa;
	d2 = pd - pc;
	if (d1 <= d2)
	{
		/* Recurse on left partition, then iterate on right partition */
		if (d1 > ST_POINTER_STEP)
			DO_SORT(a, d1 / ST_POINTER_STEP);
		if (d2 > ST_POINTER_STEP)
		{
			/* Iterate rather than recurse to save stack space */
			/* DO_SORT(pn - d2, d2 / ST_POINTER_STEP) */
			a = pn - d2;
			n = d2 / ST_POINTER_STEP;
			goto loop;
		}
	}
	else
	{
		/* Recurse on right partition, then iterate on left partition */
		if (d2 > ST_POINTER_STEP)
			DO_SORT(pn - d2, d2 / ST_POINTER_STEP);
		if (d1 > ST_POINTER_STEP)
		{
			/* Iterate rather than recurse to save stack space */
			/* DO_SORT(a, d1 / ST_POINTER_STEP) */
			n = d1 / ST_POINTER_STEP;
			goto loop;
		}
	}
	}
}

/*
 * Sort an array.
 */
ST_SCOPE void
ST_SORT(ST_ELEMENT_TYPE * data, size_t n
		ST_SORT_PROTO_ELEMENT_SIZE
		ST_SORT_PROTO_COMPARE
		ST_SORT_PROTO_ARG)
{
	ST_POINTER_TYPE *begin = (ST_POINTER_TYPE *) data;

	DO_SORT(begin, n);

#ifdef USE_ASSERT_CHECKING
	/* WIP: verify the sorting worked */
	for (ST_POINTER_TYPE *pm = begin + ST_POINTER_STEP; pm < begin + n * ST_POINTER_STEP;
		 pm += ST_POINTER_STEP)
	{
		if (DO_COMPARE(pm, pm - ST_POINTER_STEP) < 0)
			Assert(false);
	}
#endif							/* USE_ASSERT_CHECKING */
}
#endif

#undef DO_CHECK_FOR_INTERRUPTS
#undef DO_COMPARE
#undef DO_SORT
#undef DO_SWAP
#undef DO_SWAPN
#undef ST_CHECK_FOR_INTERRUPTS
#undef ST_COMPARATOR_TYPE_NAME
#undef ST_COMPARE
#undef ST_COMPARE_ARG_TYPE
#undef ST_COMPARE_RUNTIME_POINTER
#undef ST_ELEMENT_TYPE
#undef ST_ELEMENT_TYPE_VOID
#undef ST_INSERTION_SORT_THRESHOLD
#undef ST_MAKE_NAME
#undef ST_MAKE_NAME_
#undef ST_MAKE_PREFIX
#undef ST_POINTER_STEP
#undef ST_POINTER_TYPE
#undef ST_SCOPE
#undef ST_SORT
#undef ST_SORT_INTERNAL
#undef ST_SORT_INVOKE_ARG
#undef ST_SORT_INVOKE_COMPARE
#undef ST_SORT_INVOKE_ELEMENT_SIZE
#undef ST_SORT_PROTO_ARG
#undef ST_SORT_PROTO_COMPARE
#undef ST_SORT_PROTO_ELEMENT_SIZE
#undef ST_SWAP
#undef ST_SWAPN
