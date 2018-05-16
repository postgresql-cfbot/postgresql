/*	$NetBSD: qsort.c,v 1.13 2003/08/07 16:43:42 agc Exp $	*/

/*-
 * Copyright (c) 1992, 1993
 *	The Regents of the University of California.  All rights reserved.
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
 * We have modified their original by adding a check for already-sorted input,
 * which seems to be a win per discussions on pgsql-hackers around 2006-03-21.
 *
 * Also, we recurse on the smaller partition and iterate on the larger one,
 * which ensures we cannot recurse more than log(N) levels (since the
 * partition recursed to is surely no more than half of the input).  Bentley
 * and McIlroy explicitly rejected doing this on the grounds that it's "not
 * worth the effort", but we have seen crashes in the field due to stack
 * overrun, so that judgment seems wrong.
 */


/*
 * Template parameters are:
 * QS_SUFFIX - name suffix.
 * QS_TYPE - array element type.
 * QS_CMP  - Function used to compare elements, should be defined
 *           before inclusion, or passed using QS_EXTRAPARAMS/QS_EXTRAARGS.
 *           Default is `cmp_##QS_SUFFIX`
 * QS_EXTRAPARAMS - extra parameters consumed by qsort and insertion sort.
 * QS_EXTRAARGS - extra arguments passed to qsort and insertion sort.
 * QS_CMPARGS - extra arguments passed to cmp function.
 *
 * if QS_EXTRAPARAMS, QS_EXTRAARGS and QS_CMPARGS are all undefined, then
 * QS_CMP assummed to be predefined and accepting only elements to compare
 * (ie no extra parameters).
 *
 * QS_DECLARE - if defined function prototypes and type declarations are
 *		        generated
 * QS_DEFINE - if defined function definitions are generated
 * QS_SCOPE - in which scope (e.g. extern, static) do function declarations reside
 *
 * QS_CHECK_FOR_INTERRUPTS - if defined, then CHECK_FOR_INTERRUPTS is called
 *			periodically.
 * QS_CHECK_PRESORTED - if defined, check for presorted array is included.
 */

#ifndef QS_SUFFIX
#error "QS_SUFFIX should be defined"
#endif
#ifndef QS_TYPE
#error "QS_TYPE should be defined"
#endif

/* helpers */
#define QS_MAKE_NAME_(a, b) CppConcat(a, b)
#define QS_MAKE_NAME(a) QS_MAKE_NAME_(a, QS_SUFFIX)

#define QS_SWAPMANY QS_MAKE_NAME(qsswapmany_)
#define QS_SWAPONE QS_MAKE_NAME(qsswapone_)
#define QS_MED3 QS_MAKE_NAME(qsmed3_)
#define QS_INSERTION_SORT QS_MAKE_NAME(insertion_sort_)
#define QS_QSORT QS_MAKE_NAME(qsort_)
#define QS_QSORT_IMPL QS_MAKE_NAME(qsort_impl_)
#ifndef QS_CMP
#define QS_CMP QS_MAKE_NAME(cmp_)
#endif

#if !defined(QS_EXTRAPARAMS) && !defined(QS_EXTRAARGS) && !defined(QS_CMPARGS)
#  define QS_EXTRAPARAMS
#  define QS_EXTRAARGS
#  define QS_CMPARGS
#else
#  ifndef QS_EXTRAPARAMS
#  error "QS_EXTRAPARAMS should be defined"
#  endif
#  ifndef QS_EXTRAARGS
#  error "QS_EXTRAARGS should be defined"
#  endif
#  ifndef QS_CMPARGS
#  error "QS_CMPARGS should be defined"
#  endif
#endif

/* generate forward declarations necessary to use the hash table */
#ifdef QS_DECLARE
QS_SCOPE void QS_INSERTION_SORT(QS_TYPE *a, QS_TYPE *b QS_EXTRAPARAMS);
QS_SCOPE void QS_QSORT(QS_TYPE *a, QS_TYPE *b QS_EXTRAPARAMS);
#endif

#ifdef QS_DEFINE
static inline void
QS_SWAPONE(QS_TYPE *a, QS_TYPE *b)
{
	QS_TYPE 	t = *a;
	*a = *b;
	*b = t;
}

static void
QS_SWAPMANY(QS_TYPE *a, QS_TYPE *b, size_t n)
{
	for (;n > 0; n--, a++, b++)
		QS_SWAPONE(a, b);
}

#ifndef QS_SKIP_MED3
static QS_TYPE*
QS_MED3(QS_TYPE *a, QS_TYPE *b, QS_TYPE *c QS_EXTRAPARAMS)
{
	return QS_CMP(a, b QS_CMPARGS) < 0 ?
		(QS_CMP(b, c QS_CMPARGS) < 0 ? b :
			(QS_CMP(a, c QS_CMPARGS) < 0 ? c : a))
		: (QS_CMP(b, c QS_CMPARGS) > 0 ? b :
			(QS_CMP(a, c QS_CMPARGS) < 0 ? a : c));
}
#endif

QS_SCOPE void
QS_INSERTION_SORT(QS_TYPE *a, size_t n QS_EXTRAPARAMS)
{
	QS_TYPE *pm, *pl;
	for (pm = a + 1; pm < a + n; pm++)
		for (pl = pm; pl > a && QS_CMP(pl - 1, pl QS_CMPARGS) > 0; pl--)
			QS_SWAPONE(pl, pl - 1);
}

#ifdef QS_CHECK_FOR_INTERRUPTS
#define DO_CHECK_FOR_INTERRUPTS() CHECK_FOR_INTERRUPTS()
#else
#define DO_CHECK_FOR_INTERRUPTS()
#endif

QS_SCOPE void
QS_QSORT(QS_TYPE *a, size_t n QS_EXTRAPARAMS)
{
	QS_TYPE *pa,
			*pb,
			*pc,
			*pd,
			*pm,
			*pn;
	size_t	d1,
			d2;
	int		r;

loop:
	DO_CHECK_FOR_INTERRUPTS();
	if (n < 7)
	{
		QS_INSERTION_SORT(a, n QS_EXTRAARGS);
		return;
	}

#ifdef QS_CHECK_PRESORTED
	{
		int presorted = 1;
		for (pm = a + 1; pm < a + n; pm++)
		{
			DO_CHECK_FOR_INTERRUPTS();
			if (QS_CMP(pm - 1, pm QS_CMPARGS) > 0)
			{
				presorted = 0;
				break;
			}
		}
		if (presorted)
			return;
	}
#endif
	pm = a + (n / 2);
#ifndef QS_SKIP_MED3
	if (n > 7)
	{
		QS_TYPE *pl = a;
		pn = a + (n - 1);
#ifndef QS_SKIP_MED_OF_MED
		if (n > 40)
		{
			size_t		d = (n / 8);

			pl = QS_MED3(pl, pl + d, pl + 2 * d QS_EXTRAARGS);
			pm = QS_MED3(pm - d, pm, pm + d QS_EXTRAARGS);
			pn = QS_MED3(pn - 2 * d, pn - d, pn QS_EXTRAARGS);
		}
#endif
		pm = QS_MED3(pl, pm, pn QS_EXTRAARGS);
	}
#endif
	QS_SWAPONE(a, pm);
	pa = pb = a + 1;
	pc = pd = a + (n - 1);
	for (;;)
	{
		while (pb <= pc && (r = QS_CMP(pb, a QS_CMPARGS)) <= 0)
		{
			if (r == 0)
			{
				QS_SWAPONE(pa, pb);
				pa++;
			}
			pb++;
			DO_CHECK_FOR_INTERRUPTS();
		}
		while (pb <= pc && (r = QS_CMP(pc, a QS_CMPARGS)) >= 0)
		{
			if (r == 0)
			{
				QS_SWAPONE(pc, pd);
				pd--;
			}
			pc--;
			DO_CHECK_FOR_INTERRUPTS();
		}
		if (pb > pc)
			break;
		QS_SWAPONE(pb, pc);
		pb++;
		pc--;
	}
	pn = a + n;
	d1 = Min(pa - a, pb - pa);
	if (d1 > 0) QS_SWAPMANY(a, pb - d1, d1);
	d1 = Min(pd - pc, pn - pd - 1);
	if (d1 > 0) QS_SWAPMANY(pb, pn - d1, d1);
	d1 = pb - pa;
	d2 = pd - pc;
	if (d1 <= d2)
	{
		/* Recurse on left partition, then iterate on right partition */
		if (d1 > 1)
			QS_QSORT(a, d1 QS_EXTRAARGS);
		if (d2 > 1)
		{
			/* Iterate rather than recurse to save stack space */
			/* QS_QSORT(pn - d2, d2 EXTRAARGS); */
			a = pn - d2;
			n = d2;
			goto loop;
		}
	}
	else
	{
		/* Recurse on right partition, then iterate on left partition */
		if (d2 > 1)
			QS_QSORT(pn - d2, d2 QS_EXTRAARGS);
		if (d1 > 1)
		{
			/* Iterate rather than recurse to save stack space */
			/* QS_QSORT(a, d1 EXTRAARGS); */
			n = d1;
			goto loop;
		}
	}
}
#endif

#undef QS_SUFFIX
#undef QS_TYPE
#ifdef QS_DECLARE
#undef QS_DECLARE
#endif
#ifdef QS_DEFINE
#undef QS_DEFINE
#endif
#ifdef QS_SCOPE
#undef QS_SCOPE
#endif
#ifdef QS_CHECK_FOR_INTERRUPTS
#undef QS_CHECK_FOR_INTERRUPTS
#endif
#ifdef QS_CHECK_PRESORTED
#undef QS_CHECK_PRESORTED
#endif
#ifdef QS_SKIP_MED3
#undef QS_SKIP_MED3
#endif
#ifdef QS_SKIP_MED_OF_MED
#undef QS_SKIP_MED_OF_MED
#endif
#undef DO_CHECK_FOR_INTERRUPTS
#undef QS_MAKE_NAME
#undef QS_SWAPMANY
#undef QS_SWAPONE
#undef QS_MED3
#undef QS_INSERTION_SORT
#undef QS_QSORT
#undef QS_QSORT_IMPL
#undef QS_CMP
#undef QS_EXTRAPARAMS
#undef QS_EXTRAARGS
#undef QS_CMPARGS
