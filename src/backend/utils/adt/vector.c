/*-------------------------------------------------------------------------
 *
 * vector.c
 *      Support functions for float8 arrays treated as vectors.
 *
 * For the purposes of this file, a "vector" is a one-dimensional float8
 * array with no NULL values and at least one element.  The values in such
 * arrays are treated as Cartesian coordinates in n-dimensional Euclidean
 * space, where "n" is the cardinality of the array.  For more information,
 * see https://en.wikipedia.org/wiki/Cartesian_coordinate_system.
 *
 * Copyright (c) 2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/backend/utils/adt/vector.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <float.h>

#ifdef USE_OPENBLAS
#include "cblas.h"
#endif

#include "catalog/pg_type.h"
#include "common/int.h"
#include "common/pg_prng.h"
#include "funcapi.h"
#include "utils/array.h"
#include "utils/float.h"
#include "utils/fmgrprotos.h"

static inline int float8_array_is_vector_internal(ArrayType *v, bool error);
static inline float8 euclidean_norm_internal(int n, const float8 *a);
static inline float8 squared_euclidean_distance_internal(int n, const float8 *a, const float8 *b);
static inline float8 euclidean_distance_internal(int n, const float8 *a, const float8 *b);

/*
 * Checks that an array is a vector (as described at the top of this file) and
 * returns its cardinality.  If "error" is true, this function ERRORs if the
 * array is not a vector.  If "error" is false, this function returns -1 if the
 * array is not a vector.
 */
static inline int
float8_array_is_vector_internal(ArrayType *v, bool error)
{
	int			n = ArrayGetNItems(ARR_NDIM(v), ARR_DIMS(v));

	if (unlikely(n == 0))
	{
		if (error)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("vectors must have at least one element")));
		return -1;
	}

	if (unlikely(ARR_NDIM(v) != 1))
	{
		if (error)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("vectors must be one-dimensional arrays")));
		return -1;
	}

	if (unlikely(ARR_HASNULL(v) && array_contains_nulls(v)))
	{
		if (error)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("vectors must not contain nulls")));
		return -1;
	}

	return n;
}

/*
 * SQL-callable version of float8_array_is_vector_internal().  Unlike the
 * internal version, this does not return the array's cardinality.
 */
Datum
float8_array_is_vector(PG_FUNCTION_ARGS)
{
	ArrayType  *v = PG_GETARG_ARRAYTYPE_P(0);
	bool		r = (float8_array_is_vector_internal(v, false) != -1);

	PG_RETURN_BOOL(r);
}

/*
 * Workhorse for euclidean_norm() and normalize_vector().
 */
static inline float8
euclidean_norm_internal(int n, const float8 *a)
{
	float8		en = 0.0;

#ifdef USE_OPENBLAS
	en = cblas_dnrm2(n, a, 1);
#else
	for (int i = 0; i < n; i++)
		en = float8_pl(en, float8_mul(a[i], a[i]));

	en = float8_sqrt(en);
#endif

	return en;
}

/*
 * Returns the Euclidean norm of a vector.  For more information, see
 * https://en.wikipedia.org/wiki/Norm_(mathematics)#Euclidean_norm
 */
Datum
euclidean_norm(PG_FUNCTION_ARGS)
{
	ArrayType  *a = PG_GETARG_ARRAYTYPE_P(0);
	int			an = float8_array_is_vector_internal(a, true);
	float8	   *ad = (float8 *) ARR_DATA_PTR(a);

	PG_RETURN_FLOAT8(euclidean_norm_internal(an, ad));
}

/*
 * Returns a normalized version of the given vector.  For more information, see
 * https://en.wikipedia.org/wiki/Unit_vector.
 */
Datum
normalize_vector(PG_FUNCTION_ARGS)
{
	ArrayType  *a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *ua;
	int			an = float8_array_is_vector_internal(a, true);
	float8	   *ad = (float8 *) ARR_DATA_PTR(a);
	float8		en = euclidean_norm_internal(an, ad);
	Datum	   *ud = palloc(an * sizeof(Datum));

	for (int i = 0; i < an; i++)
		ud[i] = Float8GetDatumFast(float8_div(ad[i], en));

	ua = construct_array(ud, an, FLOAT8OID, sizeof(float8),
						 FLOAT8PASSBYVAL, TYPALIGN_DOUBLE);

	PG_RETURN_ARRAYTYPE_P(ua);
}

/*
 * Returns the dot product of two vectors.  For more information, see
 * https://en.wikipedia.org/wiki/Dot_product.
 */
Datum
dot_product(PG_FUNCTION_ARGS)
{
	ArrayType  *a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *b = PG_GETARG_ARRAYTYPE_P(1);
	int			an = float8_array_is_vector_internal(a, true);
	int			bn = float8_array_is_vector_internal(b, true);
	float8	   *ad = (float8 *) ARR_DATA_PTR(a);
	float8	   *bd = (float8 *) ARR_DATA_PTR(b);
	float8		dp = 0.0;

	if (an != bn)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("vectors must have the same number of elements")));

#ifdef USE_OPENBLAS
	dp = cblas_ddot(an, ad, 1, bd, 1);
#else
	for (int i = 0; i < an; i++)
		dp = float8_pl(dp, float8_mul(ad[i], bd[i]));
#endif

	PG_RETURN_FLOAT8(dp);
}

/*
 * Workhorse for squared_euclidean_distance() and
 * euclidean_distance_internal().
 */
static inline float8
squared_euclidean_distance_internal(int n, const float8 *a, const float8 *b)
{
	float8		sed = 0.0;

	for (int i = 0; i < n; i++)
	{
		float8		d = float8_mi(a[i], b[i]);

		sed = float8_pl(sed, float8_mul(d, d));
	}

	return sed;
}

/*
 * Returns the squared Euclidean distance between two vectors.  For more
 * information, see
 * https://en.wikipedia.org/wiki/Euclidean_distance#Squared_Euclidean_distance.
 */
Datum
squared_euclidean_distance(PG_FUNCTION_ARGS)
{
	ArrayType  *a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *b = PG_GETARG_ARRAYTYPE_P(1);
	int			an = float8_array_is_vector_internal(a, true);
	int			bn = float8_array_is_vector_internal(b, true);
	float8	   *ad = (float8 *) ARR_DATA_PTR(a);
	float8	   *bd = (float8 *) ARR_DATA_PTR(b);
	float8		sed;

	if (an != bn)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("vectors must have the same number of elements")));

	sed = squared_euclidean_distance_internal(an, ad, bd);

	PG_RETURN_FLOAT8(sed);
}

/*
 * Workhorse for euclidean_distance().
 */
static inline float8
euclidean_distance_internal(int n, const float8 *a, const float8 *b)
{
	float8		sed = squared_euclidean_distance_internal(n, a, b);

	return float8_sqrt(sed);
}

/*
 * Returns the Euclidean distance between vectors.  For more information, see
 * https://en.wikipedia.org/wiki/Euclidean_distance.
 */
Datum
euclidean_distance(PG_FUNCTION_ARGS)
{
	ArrayType  *a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *b = PG_GETARG_ARRAYTYPE_P(1);
	int			an = float8_array_is_vector_internal(a, true);
	int			bn = float8_array_is_vector_internal(b, true);
	float8	   *ad = (float8 *) ARR_DATA_PTR(a);
	float8	   *bd = (float8 *) ARR_DATA_PTR(b);
	float8		ed;

	if (an != bn)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("vectors must have the same number of elements")));

	ed = euclidean_distance_internal(an, ad, bd);

	PG_RETURN_FLOAT8(ed);
}

/*
 * Returns the cosine distance between two vectors.  For more information, see
 * https://en.wikipedia.org/wiki/Cosine_similarity#Cosine_Distance.
 */
Datum
cosine_distance(PG_FUNCTION_ARGS)
{
	ArrayType  *a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *b = PG_GETARG_ARRAYTYPE_P(1);
	int			an = float8_array_is_vector_internal(a, true);
	int			bn = float8_array_is_vector_internal(b, true);
	float8	   *ad = (float8 *) ARR_DATA_PTR(a);
	float8	   *bd = (float8 *) ARR_DATA_PTR(b);
	float8		dp = 0.0;
	float8		aen = 0.0;
	float8		ben = 0.0;
	float8		cd;

	if (an != bn)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("vectors must have the same number of elements")));

	for (int i = 0; i < an; i++)
	{
		dp = float8_pl(dp, float8_mul(ad[i], bd[i]));
		aen = float8_pl(aen, float8_mul(ad[i], ad[i]));
		ben = float8_pl(ben, float8_mul(bd[i], bd[i]));
	}

	cd = float8_mi(1.0, float8_div(dp, float8_sqrt(float8_mul(aen, ben))));

	PG_RETURN_FLOAT8(cd);
}

/*
 * Returns the taxicab distance between two vectors.  For more information, see
 * https://en.wikipedia.org/wiki/Taxicab_geometry.
 */
Datum
taxicab_distance(PG_FUNCTION_ARGS)
{
	ArrayType  *a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *b = PG_GETARG_ARRAYTYPE_P(1);
	int			an = float8_array_is_vector_internal(a, true);
	int			bn = float8_array_is_vector_internal(b, true);
	float8	   *ad = (float8 *) ARR_DATA_PTR(a);
	float8	   *bd = (float8 *) ARR_DATA_PTR(b);
	float8		td = 0.0;

	if (an != bn)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("vectors must have the same number of elements")));

	for (int i = 0; i < an; i++)
		td = float8_pl(td, fabs(float8_mi(ad[i], bd[i])));

	PG_RETURN_FLOAT8(td);
}

/*
 * Returns the Chebyshev distance between two vectors.  For more information,
 * see https://en.wikipedia.org/wiki/Chebyshev_distance.
 */
Datum
chebyshev_distance(PG_FUNCTION_ARGS)
{
	ArrayType  *a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *b = PG_GETARG_ARRAYTYPE_P(1);
	int			an = float8_array_is_vector_internal(a, true);
	int			bn = float8_array_is_vector_internal(b, true);
	float8	   *ad = (float8 *) ARR_DATA_PTR(a);
	float8	   *bd = (float8 *) ARR_DATA_PTR(b);
	float8		cd = 0.0;

	if (an != bn)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("vectors must have the same number of elements")));

	for (int i = 0; i < an; i++)
		cd = float8_max(cd, fabs(float8_mi(ad[i], bd[i])));

	PG_RETURN_FLOAT8(cd);
}

/*
 * Returns a standard unit vector with the nth element set to 1.  All other
 * elements are set to 0.  For more information, see
 * https://en.wikipedia.org/wiki/Standard_basis.
 */
Datum
standard_unit_vector(PG_FUNCTION_ARGS)
{
	int			d = PG_GETARG_INT32(0);
	int			n = PG_GETARG_INT32(1);
	Datum	   *ud = palloc(d * sizeof(Datum));
	ArrayType  *ua;

	if (d <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("standard unit vectors must have at least one element")));

	if (n == 0 || n > d)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot set nonexistent element to one")));

	for (int i = 0; i < d; i++)
		ud[i] = Float8GetDatumFast(0.0);

	ud[n - 1] = Float8GetDatumFast(1.0);

	ua = construct_array(ud, d, FLOAT8OID, sizeof(float8),
						 FLOAT8PASSBYVAL, TYPALIGN_DOUBLE);

	PG_RETURN_ARRAYTYPE_P(ua);
}

/*
 * Returns centers generated via the kmeans++ algorithm.  For more information,
 * see https://theory.stanford.edu/~sergei/papers/kMeansPP-soda.pdf.
 *
 * XXX: Accelerate using triange inequality (see
 * https://cseweb.ucsd.edu/~elkan/kmeansicml03.pdf).
 */
Datum
kmeans(PG_FUNCTION_ARGS)
{
	int			ncenters = PG_GETARG_INT32(0);
	ArrayType  *a = PG_GETARG_ARRAYTYPE_P(1);
	int			iterations = PG_GETARG_INT32(2);
	int			nsamples = ARR_NDIM(a) > 1 ? ARR_DIMS(a)[0] : ARR_NDIM(a);
	int			dimensions;
	int		   *clusters = palloc(nsamples * sizeof(int));
	int		   *pops = palloc(ncenters * sizeof(int));
	float8	   **centers = palloc(ncenters * sizeof(float8 *));
	float8	   **vectors = palloc(nsamples * sizeof(float8 *));
	float8	   *weights = palloc(nsamples * sizeof(float8));
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Datum	   *cdata;
	pg_prng_state rstate;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	if (ncenters < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("must request at least one center")));

	if (ncenters > nsamples)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("number of sample vectors must be greater than or equal to the number of requested centers")));

	if (ARR_HASNULL(a) && array_contains_nulls(a))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("vectors must not contain nulls")));

	if (iterations < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("must request at least one iteration")));

	dimensions = ARR_NDIM(a) > 1 ? ARR_DIMS(a)[1] : ARR_DIMS(a)[0];
	for (int i = 0; i < nsamples; i++)
	{
		int			r = -1;

		if (pg_mul_s32_overflow(i, dimensions, &r))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("sample vectors too large")));

		vectors[i] = ((float8 *) ARR_DATA_PTR(a)) + r;
	}

	/* choose initial center randomly */
	pg_prng_seed(&rstate, 0x5eed);
	centers[0] = vectors[(int) float8_mul(pg_prng_double(&rstate), nsamples)];

	for (int i = 0; i < nsamples; i++)
		weights[i] = DBL_MAX;

	/* calculate initial values for the other centers */
	for (int i = 1; i < ncenters; i++)
	{
		float8		sum = 0.0;
		float8		wi;

		for (int j = 0; j < nsamples; j++)
		{
			float8		distance = euclidean_distance_internal(dimensions,
															   centers[i - 1],
															   vectors[j]);

			weights[j] = float8_min(distance, weights[j]);
			sum = float8_pl(sum, weights[j]);
		}

		wi = float8_mul(pg_prng_double(&rstate), sum);
		for (int j = 0; j < nsamples; j++)
		{
			wi = float8_mi(wi, weights[j]);
			if (float8_le(wi, 0))
			{
				centers[i] = vectors[j];
				break;
			}
		}
	}

	/* XXX: exit early if converged */
	for (int i = 0; i < iterations; i++)
	{
		/* assign each sample to a center */
		for (int j = 0; j < nsamples; j++)
		{
			float8		min_distance = DBL_MAX;

			for (int k = 0; k < ncenters; k++)
			{
				float8		distance = euclidean_distance_internal(dimensions,
																   centers[k],
																   vectors[j]);

				if (float8_lt(distance, min_distance))
				{
					min_distance = distance;
					clusters[j] = k;
				}
			}
		}

		/* clear centers */
		for (int j = 0; j < ncenters; j++)
		{
			if (i == 0)
				centers[j] = palloc0(dimensions * sizeof(float8));
			else
				memset(centers[j], 0, dimensions * sizeof(float8));

			pops[j] = 0;
		}

		/* set each center to average of all vectors in cluster */
		for (int j = 0; j < nsamples; j++)
		{
			int			cluster = clusters[j];
			float8	   *center = centers[cluster];

			for (int k = 0; k < dimensions; k++)
				center[k] = float8_pl(center[k], vectors[j][k]);

			pops[cluster]++;
		}

		for (int j = 0; j < ncenters; j++)
		{
			for (int k = 0; k < dimensions; k++)
				centers[j][k] = float8_div(centers[j][k], pops[j]);
		}
	}

	cdata = palloc(dimensions * sizeof(Datum));
	for (int i = 0; i < ncenters; i++)
	{
		Datum		values[1];
		bool		nulls[1];

		for (int j = 0; j < dimensions; j++)
			cdata[j] = Float8GetDatumFast(centers[i][j]);

		values[0] = PointerGetDatum(construct_array(cdata, dimensions,
													FLOAT8OID, sizeof(float8),
													FLOAT8PASSBYVAL,
													TYPALIGN_DOUBLE));
		nulls[0] = false;

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum) 0;
}

/*
 * Returns the index of the closest vector.
 */
Datum
closest_vector(PG_FUNCTION_ARGS)
{
	ArrayType  *a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *v = PG_GETARG_ARRAYTYPE_P(1);
	int			alen = ARR_NDIM(a) > 1 ? ARR_DIMS(a)[0] : ARR_NDIM(a);
	int			dimensions = float8_array_is_vector_internal(v, true);
	float8		min_dist = DBL_MAX;
	float8	   *vd = (float8 *) ARR_DATA_PTR(v);
	int			idx = -1;

	if (alen == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("must provide at least one candidate vector")));

	if (ARR_HASNULL(a) && array_contains_nulls(a))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("vectors must not contain nulls")));

	if ((ARR_NDIM(a) > 1 ? ARR_DIMS(a)[1] : ARR_DIMS(a)[0]) != dimensions)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("vectors must have the same number of elements")));

	for (int i = 0; i < alen; i++)
	{
		float8	   *c;
		float8		d;
		int			r = -1;

		if (pg_mul_s32_overflow(i, dimensions, &r))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("candidate vectors too large")));

		c = ((float8 *) ARR_DATA_PTR(a)) + r;
		d = euclidean_distance_internal(dimensions, c, vd);

		if (r == -1 || float8_lt(d, min_dist))
		{
			min_dist = d;
			idx = i;
		}
	}

	PG_RETURN_INT32(idx);
}
