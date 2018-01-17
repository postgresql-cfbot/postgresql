/*-------------------------------------------------------------------------
 *
 * pg_aggregate.h
 *	  definition of the system "aggregate" relation (pg_aggregate)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_aggregate.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_AGGREGATE_H
#define PG_AGGREGATE_H

#include "catalog/genbki.h"

/* ----------------------------------------------------------------
 *		pg_aggregate definition.
 *
 *		cpp turns this into typedef struct FormData_pg_aggregate
 *
 *	aggfnoid			pg_proc OID of the aggregate itself
 *	aggkind				aggregate kind, see AGGKIND_ categories below
 *	aggnumdirectargs	number of arguments that are "direct" arguments
 *	aggtransfn			transition function
 *	aggfinalfn			final function (0 if none)
 *	aggcombinefn		combine function (0 if none)
 *	aggserialfn			function to convert transtype to bytea (0 if none)
 *	aggdeserialfn		function to convert bytea to transtype (0 if none)
 *	aggmtransfn			forward function for moving-aggregate mode (0 if none)
 *	aggminvtransfn		inverse function for moving-aggregate mode (0 if none)
 *	aggmfinalfn			final function for moving-aggregate mode (0 if none)
 *	aggfinalextra		true to pass extra dummy arguments to aggfinalfn
 *	aggmfinalextra		true to pass extra dummy arguments to aggmfinalfn
 *	aggfinalmodify		tells whether aggfinalfn modifies transition state
 *	aggmfinalmodify		tells whether aggmfinalfn modifies transition state
 *	aggsortop			associated sort operator (0 if none)
 *	aggtranstype		type of aggregate's transition (state) data
 *	aggtransspace		estimated size of state data (0 for default estimate)
 *	aggmtranstype		type of moving-aggregate state data (0 if none)
 *	aggmtransspace		estimated size of moving-agg state (0 for default est)
 *	agginitval			initial value for transition state (can be NULL)
 *	aggminitval			initial value for moving-agg state (can be NULL)
 * ----------------------------------------------------------------
 */
#define AggregateRelationId  2600

CATALOG(pg_aggregate,2600) BKI_WITHOUT_OIDS
{
	regproc		aggfnoid;
	char		aggkind BKI_DEFAULT(n);
	int16		aggnumdirectargs BKI_DEFAULT(0);
	regproc		aggtransfn;
	regproc		aggfinalfn BKI_DEFAULT(-);
	regproc		aggcombinefn BKI_DEFAULT(-);
	regproc		aggserialfn BKI_DEFAULT(-);
	regproc		aggdeserialfn BKI_DEFAULT(-);
	regproc		aggmtransfn BKI_DEFAULT(-);
	regproc		aggminvtransfn BKI_DEFAULT(-);
	regproc		aggmfinalfn BKI_DEFAULT(-);
	bool		aggfinalextra BKI_DEFAULT(f);
	bool		aggmfinalextra BKI_DEFAULT(f);
	char		aggfinalmodify BKI_DEFAULT(r);
	char		aggmfinalmodify BKI_DEFAULT(r);
	Oid			aggsortop BKI_DEFAULT(0);
	Oid			aggtranstype;
	int32		aggtransspace BKI_DEFAULT(0);
	Oid			aggmtranstype BKI_DEFAULT(0);
	int32		aggmtransspace BKI_DEFAULT(0);

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		agginitval BKI_DEFAULT(_null_);
	text		aggminitval BKI_DEFAULT(_null_);
#endif
} FormData_pg_aggregate;

DECLARE_UNIQUE_INDEX(pg_aggregate_fnoid_index, 2650, on pg_aggregate using btree(aggfnoid oid_ops));
#define AggregateFnoidIndexId  2650

/* ----------------
 *		Form_pg_aggregate corresponds to a pointer to a tuple with
 *		the format of pg_aggregate relation.
 * ----------------
 */
typedef FormData_pg_aggregate *Form_pg_aggregate;

/* ----------------
 *		compiler constants for pg_aggregate
 * ----------------
 */

#define Natts_pg_aggregate					22
#define Anum_pg_aggregate_aggfnoid			1
#define Anum_pg_aggregate_aggkind			2
#define Anum_pg_aggregate_aggnumdirectargs	3
#define Anum_pg_aggregate_aggtransfn		4
#define Anum_pg_aggregate_aggfinalfn		5
#define Anum_pg_aggregate_aggcombinefn		6
#define Anum_pg_aggregate_aggserialfn		7
#define Anum_pg_aggregate_aggdeserialfn		8
#define Anum_pg_aggregate_aggmtransfn		9
#define Anum_pg_aggregate_aggminvtransfn	10
#define Anum_pg_aggregate_aggmfinalfn		11
#define Anum_pg_aggregate_aggfinalextra		12
#define Anum_pg_aggregate_aggmfinalextra	13
#define Anum_pg_aggregate_aggfinalmodify	14
#define Anum_pg_aggregate_aggmfinalmodify	15
#define Anum_pg_aggregate_aggsortop			16
#define Anum_pg_aggregate_aggtranstype		17
#define Anum_pg_aggregate_aggtransspace		18
#define Anum_pg_aggregate_aggmtranstype		19
#define Anum_pg_aggregate_aggmtransspace	20
#define Anum_pg_aggregate_agginitval		21
#define Anum_pg_aggregate_aggminitval		22

/*
 * Symbolic values for aggkind column.  We distinguish normal aggregates
 * from ordered-set aggregates (which have two sets of arguments, namely
 * direct and aggregated arguments) and from hypothetical-set aggregates
 * (which are a subclass of ordered-set aggregates in which the last
 * direct arguments have to match up in number and datatypes with the
 * aggregated arguments).
 */
#define AGGKIND_NORMAL			'n'
#define AGGKIND_ORDERED_SET		'o'
#define AGGKIND_HYPOTHETICAL	'h'

/* Use this macro to test for "ordered-set agg including hypothetical case" */
#define AGGKIND_IS_ORDERED_SET(kind)  ((kind) != AGGKIND_NORMAL)

/*
 * Symbolic values for aggfinalmodify and aggmfinalmodify columns.
 * Preferably, finalfns do not modify the transition state value at all,
 * but in some cases that would cost too much performance.  We distinguish
 * "pure read only" and "trashes it arbitrarily" cases, as well as the
 * intermediate case where multiple finalfn calls are allowed but the
 * transfn cannot be applied anymore after the first finalfn call.
 */
#define AGGMODIFY_READ_ONLY			'r'
#define AGGMODIFY_SHARABLE			's'
#define AGGMODIFY_READ_WRITE		'w'

#endif							/* PG_AGGREGATE_H */
