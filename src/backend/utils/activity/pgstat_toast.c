/* -------------------------------------------------------------------------
 *
 * pgstat_toast.c
 *	  Implementation of TOAST statistics.
 *
 * This file contains the implementation of TOAST statistics. It is kept
 * separate from pgstat.c to enforce the line between the statistics access /
 * storage implementation and the details about individual types of
 * statistics.
 *
 * Copyright (c) 2001-2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_toast.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/pgstat_internal.h"
#include "utils/timestamp.h"

/*
 * Indicates if backend has some function stats that it hasn't yet
 * sent to the collector.
 */
bool		have_toast_stats = false;

/*
 * Backends store per-toast-column info that's waiting to be sent to the collector
 * in this hash table (indexed by column's PgStat_BackendAttrIdentifier).
 */
static HTAB *pgStatToastActions = NULL;

/*
 * Report TOAST activity
 * Called by toast_helper functions.
 */
void
pgstat_report_toast_activity(Oid relid, int attr,
							bool externalized,
							bool compressed,
							int32 old_size,
							int32 new_size,
							instr_time start_time)
{
	PgStat_BackendAttrIdentifier toastattr = { relid, attr };
	PgStat_BackendToastEntry *htabent;
	instr_time	time_spent;
	bool		found;

	if (pgStatSock == PGINVALID_SOCKET || !pgstat_track_toast)
		return;

	INSTR_TIME_SET_CURRENT(time_spent);
	INSTR_TIME_SUBTRACT(time_spent, start_time);

	if (!pgStatToastActions)
	{
		/* First time through - initialize toast stat table */
		HASHCTL		hash_ctl;

		hash_ctl.keysize = sizeof(PgStat_BackendAttrIdentifier);
		hash_ctl.entrysize = sizeof(PgStat_BackendToastEntry);
		pgStatToastActions = hash_create("TOAST stat entries",
									  PGSTAT_TOAST_HASH_SIZE,
									  &hash_ctl,
									  HASH_ELEM | HASH_BLOBS);
	}

	/* Get the stats entry for this TOAST attribute, create if necessary */
	htabent = hash_search(pgStatToastActions, &toastattr,
						  HASH_ENTER, &found);
	if (!found)
	{
		MemSet(&htabent->t_counts, 0, sizeof(PgStat_ToastCounts));
	}

	/* update counters */
	if (externalized)
	{
		htabent->t_counts.t_numexternalized++;
	}
	if (compressed)
	{
		htabent->t_counts.t_numcompressed++;
		htabent->t_counts.t_size_orig+=old_size;
		if (new_size)
		{
			htabent->t_counts.t_numcompressionsuccess++;
			htabent->t_counts.t_size_compressed+=new_size;
		}
	}
	/* record time spent */
	INSTR_TIME_ADD(htabent->t_counts.t_comp_time, time_spent);

	/* indicate that we have something to send */
	have_toast_stats = true;
}

/*
 * Subroutine for pgstat_report_stat: populate and send a toast stat message
 */
void
pgstat_send_toaststats(void)
{
	static const PgStat_ToastCounts all_zeroes;

	PgStat_MsgToaststat msg;
	PgStat_BackendToastEntry *entry;
	HASH_SEQ_STATUS tstat;

	if (pgStatToastActions == NULL)
		return;

	pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_TOASTSTAT);
	msg.m_databaseid = MyDatabaseId;
	msg.m_nentries = 0;

	hash_seq_init(&tstat, pgStatToastActions);
	while ((entry = (PgStat_BackendToastEntry *) hash_seq_search(&tstat)) != NULL)
	{
		PgStat_ToastEntry *m_ent;

		/* Skip it if no counts accumulated since last time */
		if (memcmp(&entry->t_counts, &all_zeroes,
				   sizeof(PgStat_ToastCounts)) == 0)
			continue;

		/* need to convert format of time accumulators */
		m_ent = &msg.m_entry[msg.m_nentries];
		m_ent->attr = entry->attr;
		m_ent->t_numexternalized = entry->t_counts.t_numexternalized;
		m_ent->t_numcompressed = entry->t_counts.t_numcompressed;
		m_ent->t_numcompressionsuccess = entry->t_counts.t_numcompressionsuccess;
		m_ent->t_size_orig = entry->t_counts.t_size_orig;
		m_ent->t_size_compressed = entry->t_counts.t_size_compressed;
		m_ent->t_comp_time = INSTR_TIME_GET_MICROSEC(entry->t_counts.t_comp_time);

		if (++msg.m_nentries >= PGSTAT_NUM_TOASTENTRIES)
		{
			pgstat_send(&msg, offsetof(PgStat_MsgToaststat, m_entry[0]) +
						msg.m_nentries * sizeof(PgStat_ToastEntry));
			msg.m_nentries = 0;
		}

		/* reset the entry's counts */
		MemSet(&entry->t_counts, 0, sizeof(PgStat_ToastCounts));
	}

	if (msg.m_nentries > 0)
		pgstat_send(&msg, offsetof(PgStat_MsgToaststat, m_entry[0]) +
					msg.m_nentries * sizeof(PgStat_ToastEntry));

	have_toast_stats = false;
}

