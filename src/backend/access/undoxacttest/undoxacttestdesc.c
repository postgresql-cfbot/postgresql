#include "postgres.h"

#include "access/undoxacttest.h"

void
undoxacttest_undo_desc(StringInfo buf, const WrittenUndoNode *record)
{
	const xu_undoxactest_mod *uxt_r = (const xu_undoxactest_mod *) record->n.data;

	appendStringInfo(buf, "UNDOXACTTEST reloid=%u, mod=%ld",
					 uxt_r->reloid, uxt_r->mod);
}
