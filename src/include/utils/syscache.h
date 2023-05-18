/*-------------------------------------------------------------------------
 *
 * syscache.h
 *	  System catalog cache definitions.
 *
 * See also lsyscache.h, which provides convenience routines for
 * common cache-lookup operations.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/syscache.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SYSCACHE_H
#define SYSCACHE_H

#include "access/attnum.h"
#include "access/htup.h"
/* we intentionally do not include utils/catcache.h here */

/*
 *		SysCache identifiers.
 *
 *		The order of these identifiers must match the order
 *		of the entries in the array cacheinfo[] in syscache.c.
 *		Keep them in alphabetical order (renumbering only costs a
 *		backend rebuild).
 */

enum SysCacheIdentifier
{
	AGGFNOID = 0,
	AMNAME,
	AMOID,
	AMOPOPID,
	AMOPSTRATEGY,
	AMPROCNUM,
	ATTNAME,
	ATTNUM,
	AUTHMEMMEMROLE,
	AUTHMEMROLEMEM,
	AUTHNAME,
	AUTHOID,
	CASTSOURCETARGET,
	CLAAMNAMENSP,
	CLAOID,
	COLLNAMEENCNSP,
	COLLOID,
	CONDEFAULT,
	CONNAMENSP,
	CONSTROID,
	CONVOID,
	DATABASEOID,
	DEFACLROLENSPOBJ,
	ENUMOID,
	ENUMTYPOIDNAME,
	EVENTTRIGGERNAME,
	EVENTTRIGGEROID,
	FOREIGNDATAWRAPPERNAME,
	FOREIGNDATAWRAPPEROID,
	FOREIGNSERVERNAME,
	FOREIGNSERVEROID,
	FOREIGNTABLEREL,
	INDEXRELID,
	LANGNAME,
	LANGOID,
	NAMESPACENAME,
	NAMESPACEOID,
	OPERNAMENSP,
	OPEROID,
	OPFAMILYAMNAMENSP,
	OPFAMILYOID,
	PARAMETERACLNAME,
	PARAMETERACLOID,
	PARTRELID,
	PROCNAMEARGSNSP,
	PROCOID,
	PUBLICATIONNAME,
	PUBLICATIONNAMESPACE,
	PUBLICATIONNAMESPACEMAP,
	PUBLICATIONOID,
	PUBLICATIONREL,
	PUBLICATIONRELMAP,
	RANGEMULTIRANGE,
	RANGETYPE,
	RELNAMENSP,
	RELOID,
	REPLORIGIDENT,
	REPLORIGNAME,
	RULERELNAME,
	SEQRELID,
	STATEXTDATASTXOID,
	STATEXTNAMENSP,
	STATEXTOID,
	STATRELATTINH,
	SUBSCRIPTIONNAME,
	SUBSCRIPTIONOID,
	SUBSCRIPTIONRELMAP,
	TABLESPACEOID,
	TRFOID,
	TRFTYPELANG,
	TSCONFIGMAP,
	TSCONFIGNAMENSP,
	TSCONFIGOID,
	TSDICTNAMENSP,
	TSDICTOID,
	TSPARSERNAMENSP,
	TSPARSEROID,
	TSTEMPLATENAMENSP,
	TSTEMPLATEOID,
	TYPENAMENSP,
	TYPEOID,
	USERMAPPINGOID,
	USERMAPPINGUSERSERVER

#define SysCacheSize (USERMAPPINGUSERSERVER + 1)
};

extern void InitCatalogCache(void);
extern void InitCatalogCachePhase2(void);

extern HeapTuple SearchSysCacheImpl(int cacheId, int nkeys,
									Datum key1, Datum key2, Datum key3, Datum key4);



extern void ReleaseSysCache(HeapTuple tuple);

/* convenience routines */
extern HeapTuple SearchSysCacheCopyImpl(int cacheId, int nkey,
										Datum key1, Datum key2, Datum key3, Datum key4);
extern bool SearchSysCacheExistsImpl(int cacheId, int nkey,
									 Datum key1, Datum key2, Datum key3, Datum key4);
extern Oid	GetSysCacheOidImpl(int cacheId, AttrNumber oidcol, int nkey,
							   Datum key1, Datum key2, Datum key3, Datum key4);

extern HeapTuple SearchSysCacheAttName(Oid relid, const char *attname);
extern HeapTuple SearchSysCacheCopyAttName(Oid relid, const char *attname);
extern bool SearchSysCacheExistsAttName(Oid relid, const char *attname);

extern HeapTuple SearchSysCacheAttNum(Oid relid, int16 attnum);
extern HeapTuple SearchSysCacheCopyAttNum(Oid relid, int16 attnum);

extern Datum SysCacheGetAttr(int cacheId, HeapTuple tup,
							 AttrNumber attributeNumber, bool *isNull);

extern Datum SysCacheGetAttrNotNull(int cacheId, HeapTuple tup,
									AttrNumber attributeNumber);

extern uint32 GetSysCacheHashValueImpl(int cacheId,
									   Datum key1, Datum key2, Datum key3, Datum key4);

/* list-search interface.  Users of this must import catcache.h too */
struct catclist;
extern struct catclist *SearchSysCacheListImpl(int cacheId, int nkeys,
											   Datum key1, Datum key2, Datum key3);

extern void SysCacheInvalidate(int cacheId, uint32 hashValue);

extern bool RelationInvalidatesSnapshotsOnly(Oid relid);
extern bool RelationHasSysCache(Oid relid);
extern bool RelationSupportsSysCache(Oid relid);

/* Macros to provide dummy 0 arguments. */
#define SYSCACHE_ARG_1(arg1, ...) arg1
#define SYSCACHE_ARG_2(arg1, arg2, ...) arg2
#define SYSCACHE_ARG_3(arg1, arg2, arg3, ...) arg3
#define SYSCACHE_ARG_4(arg1, arg2, arg3, arg4, ...) arg4
#define SYSCACHE_ARG_2_OR_0(...) SYSCACHE_ARG_2(__VA_ARGS__, 0, 0, 0)
#define SYSCACHE_ARG_3_OR_0(...) SYSCACHE_ARG_3(__VA_ARGS__, 0, 0, 0)
#define SYSCACHE_ARG_4_OR_0(...) SYSCACHE_ARG_4(__VA_ARGS__, 0, 0, 0)

/* Variadic macros to call syscache routines with variable number of keys. */
#define SearchSysCache(cacheId, ...) \
	SearchSysCacheImpl(cacheId, VA_ARGS_NARGS(__VA_ARGS__), \
					   SYSCACHE_ARG_1(__VA_ARGS__), \
					   SYSCACHE_ARG_2_OR_0(__VA_ARGS__), \
					   SYSCACHE_ARG_3_OR_0(__VA_ARGS__), \
					   SYSCACHE_ARG_4_OR_0(__VA_ARGS__))
#define SearchSysCacheList(cacheId, ...) \
	SearchSysCacheListImpl(cacheId, VA_ARGS_NARGS(__VA_ARGS__), \
						   SYSCACHE_ARG_1(__VA_ARGS__), \
						   SYSCACHE_ARG_2_OR_0(__VA_ARGS__), \
						   SYSCACHE_ARG_3_OR_0(__VA_ARGS__))
#define SearchSysCacheCopy(cacheId, ...) \
	SearchSysCacheCopyImpl(cacheId, VA_ARGS_NARGS(__VA_ARGS__), \
						   SYSCACHE_ARG_1(__VA_ARGS__), \
						   SYSCACHE_ARG_2_OR_0(__VA_ARGS__), \
						   SYSCACHE_ARG_3_OR_0(__VA_ARGS__), \
						   SYSCACHE_ARG_4_OR_0(__VA_ARGS__))
#define SearchSysCacheExists(cacheId, ...) \
	SearchSysCacheExistsImpl(cacheId, VA_ARGS_NARGS(__VA_ARGS__), \
							 SYSCACHE_ARG_1(__VA_ARGS__), \
							 SYSCACHE_ARG_2_OR_0(__VA_ARGS__), \
							 SYSCACHE_ARG_3_OR_0(__VA_ARGS__), \
							 SYSCACHE_ARG_4_OR_0(__VA_ARGS__))
#define GetSysCacheOid(cacheId, oidcol, ...) \
	GetSysCacheOidImpl(cacheId, oidcol, VA_ARGS_NARGS(__VA_ARGS__), \
					   SYSCACHE_ARG_1(__VA_ARGS__), \
					   SYSCACHE_ARG_2_OR_0(__VA_ARGS__), \
					   SYSCACHE_ARG_3_OR_0(__VA_ARGS__), \
					   SYSCACHE_ARG_4_OR_0(__VA_ARGS__))
#define GetSysCacheHashValue(cacheId, ...) \
	GetSysCacheHashValueImpl(cacheId, \
							 SYSCACHE_ARG_1(__VA_ARGS__), \
							 SYSCACHE_ARG_2_OR_0(__VA_ARGS__), \
							 SYSCACHE_ARG_3_OR_0(__VA_ARGS__), \
							 SYSCACHE_ARG_4_OR_0(__VA_ARGS__))

#define ReleaseSysCacheList(x)	ReleaseCatCacheList(x)

#endif							/* SYSCACHE_H */
