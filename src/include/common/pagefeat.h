/*-------------------------------------------------------------------------
 *
 * pagefeat.h
 *	  POSTGRES page feature support
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/common/pagefeat.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PAGEFEAT_H
#define PAGEFEAT_H

/* revealed for GUCs */
extern int reserved_page_size;
extern bool page_feature_extended_checksums;
extern bool page_feature_wasted_space;

/* forward declaration to avoid circular includes */
typedef Pointer Page;
typedef uint8 PageFeatureSet;

extern PageFeatureSet cluster_page_features;

#define SizeOfPageReservedSpace() reserved_page_size

/* bit offset for features flags */
typedef enum {
	PF_EXT_CHECKSUMS = 0,  /* must be first */
	PF_WASTED_SPACE,
	PF_MAX_FEATURE /* must be last */
} PageFeature;

/* Limit for total number of features we will support.  Since we are storing a
 * single status byte, we are reserving the top bit here to be set to indicate
 * for whether there are more than 7 features; used for future extensibility.
 * This should not be increased as part of normal feature development, only
 * when adding said mechanisms */

#define PF_MAX_POSSIBLE_FEATURE_CUTOFF 7

StaticAssertDecl(PF_MAX_FEATURE <= PF_MAX_POSSIBLE_FEATURE_CUTOFF,
				 "defined more features than will fit in one byte");

/* prototypes */
void SetExtendedFeatureConfigOptions(PageFeatureSet features);
uint16 PageFeatureSetCalculateSize(PageFeatureSet features);
PageFeatureSet PageFeatureSetAddFeatureByName(PageFeatureSet features, const char *feat_name);
PageFeatureSet PageFeatureSetAddFeature(PageFeatureSet features, PageFeature feature);

/* macros dealing with the current cluster's page features */
char *PageGetFeatureOffset(Page page, PageFeature feature);
#define PageFeatureSetHasFeature(fs,f) (fs&(1<<f))
#define ClusterPageFeatureInit(features) cluster_page_features = features;

#endif							/* PAGEFEAT_H */
