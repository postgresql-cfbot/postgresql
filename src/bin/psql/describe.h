/*
 * psql - the PostgreSQL interactive terminal
 *
 * Copyright (c) 2000-2022, PostgreSQL Global Development Group
 *
 * src/bin/psql/describe.h
 */
#ifndef DESCRIBE_H
#define DESCRIBE_H


/* \da */
extern bool describeAggregates(const char *pattern, int verbose, bool showSystem);

/* \dA */
extern bool describeAccessMethods(const char *pattern, int verbose);

/* \db */
extern bool describeTablespaces(const char *pattern, int verbose);

/* \df, \dfa, \dfn, \dft, \dfw, etc. */
extern bool describeFunctions(const char *functypes, const char *func_pattern,
							  char **arg_patterns, int num_arg_patterns,
							  int verbose, bool showSystem);

/* \dT */
extern bool describeTypes(const char *pattern, int verbose, bool showSystem);

/* \do */
extern bool describeOperators(const char *oper_pattern,
							  char **arg_patterns, int num_arg_patterns,
							  int verbose, bool showSystem);

/* \du, \dg */
extern bool describeRoles(const char *pattern, int verbose, bool showSystem);

/* \drds */
extern bool listDbRoleSettings(const char *pattern, const char *pattern2);

/* \z (or \dp) */
extern bool permissionsList(const char *pattern);

/* \ddp */
extern bool listDefaultACLs(const char *pattern);

/* \dd */
extern bool objectDescription(const char *pattern, bool showSystem);

/* \d foo */
extern bool describeTableDetails(const char *pattern, int verbose, bool showSystem);

/* \dF */
extern bool listTSConfigs(const char *pattern, int verbose);

/* \dFp */
extern bool listTSParsers(const char *pattern, int verbose);

/* \dFd */
extern bool listTSDictionaries(const char *pattern, int verbose);

/* \dFt */
extern bool listTSTemplates(const char *pattern, int verbose);

/* \l */
extern bool listAllDbs(const char *pattern, int verbose);

/* \dt, \di, \ds, \dS, etc. */
extern bool listTables(const char *tabtypes, const char *pattern, int verbose, bool showSystem);

/* \dP */
extern bool listPartitionedTables(const char *reltypes, const char *pattern, int verbose);

/* \dD */
extern bool listDomains(const char *pattern, int verbose, bool showSystem);

/* \dc */
extern bool listConversions(const char *pattern, int verbose, bool showSystem);

/* \dconfig */
extern bool describeConfigurationParameters(const char *pattern, int verbose,
											bool showSystem);

/* \dC */
extern bool listCasts(const char *pattern, int verbose);

/* \dO */
extern bool listCollations(const char *pattern, int verbose, bool showSystem);

/* \dn */
extern bool listSchemas(const char *pattern, int verbose, bool showSystem);

/* \dew */
extern bool listForeignDataWrappers(const char *pattern, int verbose);

/* \des */
extern bool listForeignServers(const char *pattern, int verbose);

/* \deu */
extern bool listUserMappings(const char *pattern, int verbose);

/* \det */
extern bool listForeignTables(const char *pattern, int verbose);

/* \dL */
extern bool listLanguages(const char *pattern, int verbose, bool showSystem);

/* \dx */
extern bool listExtensions(const char *pattern);

/* \dx+ */
extern bool listExtensionContents(const char *pattern);

/* \dX */
extern bool listExtendedStats(const char *pattern);

/* \dy */
extern bool listEventTriggers(const char *pattern, int verbose);

/* \dRp */
bool		listPublications(const char *pattern);

/* \dRp+ */
bool		describePublications(const char *pattern);

/* \dRs */
bool		describeSubscriptions(const char *pattern, int verbose);

/* \dAc */
extern bool listOperatorClasses(const char *access_method_pattern,
								const char *opclass_pattern,
								int verbose);

/* \dAf */
extern bool listOperatorFamilies(const char *access_method_pattern,
								 const char *opclass_pattern,
								 int verbose);

/* \dAo */
extern bool listOpFamilyOperators(const char *accessMethod_pattern,
								  const char *family_pattern, int verbose);

/* \dAp */
extern bool listOpFamilyFunctions(const char *access_method_pattern,
								  const char *family_pattern, int verbose);

/* \dl or \lo_list */
extern bool listLargeObjects(int verbose);

#endif							/* DESCRIBE_H */
