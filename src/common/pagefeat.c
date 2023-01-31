/*-------------------------------------------------------------------------
 *
 * pagefeat.c
 *	  POSTGRES optional page features
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/common/pagefeat.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "common/pagefeat.h"
#include "utils/guc.h"

/* global variables */
int reserved_page_size;
PageFeatureSet cluster_page_features;

/* status GUCs, display only. set by XLog startup */
bool page_feature_extended_checksums;

/*
 * A "page feature" is an optional cluster-defined additional data field that
 * is stored in the "reserved_page_size" area in the footer of a given Page.
 * These features are set at initdb time and are static for the life of the cluster.
 *
 * Page features are identified by flags, each corresponding to a blob of data
 * with a fixed length and content.  For a given cluster, these features will
 * globally exist or not, and can be queried for feature existence.  You can
 * also get the data/length for a given feature using accessors.
 */

typedef struct PageFeatureDesc
{
	uint16 length;
	char *guc_name;
} PageFeatureDesc;

/* These are the fixed widths for each feature type, indexed by feature.  This
 * is also used to lookup page features by the bootstrap process and expose
 * the state of this page feature as a readonly boolean GUC, so when adding a
 * named feature here ensure you also update the guc_tables file to add this,
 * or the attempt to set the GUC will fail. */

static PageFeatureDesc feature_descs[PF_MAX_FEATURE] = {
	/* PF_EXT_CHECKSUMS */
	{ 8, "extended_checksums" }
};


/* Return the size for a given set of feature flags */
uint16
PageFeatureSetCalculateSize(PageFeatureSet features)
{
	uint16 size = 0;
	int i;

	if (!features)
		return 0;

	for (i = 0; i < PF_MAX_FEATURE; i++)
		if (features & (1<<i))
			size += feature_descs[i].length;

	return MAXALIGN(size);
}


/* does a specific page have a feature? */
static inline bool PageHasFeature(Page page, PageFeature feature)
{
	Assert(feature >= 0 && feature < PF_MAX_FEATURE);

	return ((PageHeader) page)->pd_flags & PD_EXTENDED_FEATS && \
		((PageHeader)page)->pd_feat.features & (1<<feature);
}


/*
 * Get the page offset for the given feature given the page, flags, and
 * feature id.  Returns NULL if the feature is not enabled.
 */

char *
PageGetFeatureOffset(Page page, PageFeature feature_id)
{
	uint16 size = 0;
	int i;
	PageFeatureSet enabled_features;

	Assert(page != NULL);

	/* short circuit if page does not have extended features or is not using
	 * this specific feature */

	if (!PageHasFeature(page, feature_id))
		return (char*)0;

	enabled_features = ((PageHeader)page)->pd_feat.features;

	/* we need to find the offsets of all previous features to skip */
	for (i = PF_MAX_FEATURE; i > feature_id; i--)
		if (enabled_features & (1<<i))
			size += feature_descs[i].length;

	/* size is now the offset from the start of the reserved page space */
	return (char*)((char *)page + BLCKSZ - reserved_page_size + size);
}

/* expose the given feature flags as boolean yes/no GUCs */
void
SetExtendedFeatureConfigOptions(PageFeatureSet features)
{
#ifndef FRONTEND
	int i;

	for (i = 0; i < PF_MAX_FEATURE; i++)
		SetConfigOption(feature_descs[i].guc_name, (features & (1<<i)) ? "yes" : "no",
						PGC_INTERNAL, PGC_S_DYNAMIC_DEFAULT);
#endif
	cluster_page_features = features;
}

/* add a named feature to the feature set */
PageFeatureSet
PageFeatureSetAddFeatureByName(PageFeatureSet features, const char *feat_name)
{
	int i;

	for (i = 0; i < PF_MAX_FEATURE; i++)
		if (!strcmp(feat_name, feature_descs[i].guc_name))
			return features | (1<<i);
	return features;
}

/* add feature to the feature set by identifier */
PageFeatureSet
PageFeatureSetAddFeature(PageFeatureSet features, PageFeature feature)
{
	Assert(feature >= 0 && feature < PF_MAX_FEATURE);
	return features | (1<<feature);
}
