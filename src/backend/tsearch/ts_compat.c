#include "postgres.h"
#include "tsearch/ts_type.h"

/*
 * Definition of old WordEntry struct in TSVector. Because of limitations
 * in size (max 1MB for lexemes), the format has changed
 */
typedef struct
{
	uint32
				haspos:1,
				len:11,
				pos:20;
}			OldWordEntry;

typedef struct
{
	uint16		npos;
	WordEntryPos pos[FLEXIBLE_ARRAY_MEMBER];
}			OldWordEntryPosVector;

#define OLDSTRPTR(x)	( (char *) &(x)->entries[x->size_] )
#define _OLDPOSVECPTR(x, e)	\
	((OldWordEntryPosVector *)(STRPTR(x) + SHORTALIGN((e)->pos + (e)->len)))
#define OLDPOSDATALEN(x,e) ( ( (e)->haspos ) ? (_OLDPOSVECPTR(x,e)->npos) : 0 )
#define OLDPOSDATAPTR(x,e) (_OLDPOSVECPTR(x,e)->pos)

/*
 * Converts tsvector with the old structure to current.
 * Can return copy of tsvector, but it has a meaning when tsvector doensn't
 * need to be converted.
 */
TSVector
tsvector_upgrade(Datum orig, bool copy)
{
	int			i,
				dataoff = 0,
				datalen = 0,
				totallen;
	TSVector	in,
				out;

	in = (TSVector) PG_DETOAST_DATUM(orig);

	/* If already in new format, return as is */
	if (in->size_ & TS_FLAG_STRETCHED)
	{
		TSVector	out;

		if (!copy)
			return in;

		out = (TSVector) palloc(VARSIZE(in));
		memcpy(out, in, VARSIZE(in));
		return out;
	}

	/*
	 * Calculate required size. We don't check any sizes here because old
	 * format was limited with 1MB
	 */
	for (i = 0; i < in->size_; i++)
	{
		OldWordEntry *entry = (OldWordEntry *) (in->entries + i);

		INCRSIZE(datalen, i, entry->len, OLDPOSDATALEN(in, entry));
	}

	totallen = CALCDATASIZE(in->size_, datalen);
	out = (TSVector) palloc0(totallen);
	SET_VARSIZE(out, totallen);
	TS_SETCOUNT(out, in->size_);

	for (i = 0; i < in->size_; i++)
	{
		OldWordEntry *entry = (OldWordEntry *) (in->entries + i);

		tsvector_addlexeme(out, i, &dataoff,
						   OLDSTRPTR(in) + entry->pos, entry->len,
						   OLDPOSDATAPTR(in, entry), OLDPOSDATALEN(in, entry));
	}

	return out;
}
