/*-------------------------------------------------------------------------
 *
 * spgproc.c
 *	  Common procedures for SP-GiST.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/backend/access/spgist/spgproc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <math.h>

#include "access/spgist_private.h"
#include "utils/builtins.h"
#include "utils/geo_decls.h"

/* Point-box distance in the assumption that box is aligned by axis */
static double
point_box_distance(Point *point, BOX *box)
{
	double		dx,
				dy;

	if (isnan(point->x) || isnan(box->low.x) ||
		isnan(point->y) || isnan(box->low.y))
		return get_float8_nan();

	if (point->x < box->low.x)
		dx = box->low.x - point->x;
	else if (point->x > box->high.x)
		dx = point->x - box->high.x;
	else
		dx = 0.0;

	if (point->y < box->low.y)
		dy = box->low.y - point->y;
	else if (point->y > box->high.y)
		dy = point->y - box->high.y;
	else
		dy = 0.0;

	return HYPOT(dx, dy);
}

void
spg_point_distance(Datum to, int norderbys, ScanKey orderbyKeys,
				   double **distances, bool isLeaf)
{
	double	   *distance;
	int			sk_num;

	distance = *distances = (double *) palloc(norderbys * sizeof(double));

	for (sk_num = 0; sk_num < norderbys; ++sk_num, ++orderbyKeys, ++distance)
	{
		Point	   *point = DatumGetPointP(orderbyKeys->sk_argument);

		*distance = isLeaf ? point_dt(point, DatumGetPointP(to))
						   : point_box_distance(point, DatumGetBoxP(to));
	}
}
