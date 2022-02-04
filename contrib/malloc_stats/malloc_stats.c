#include "postgres.h"
#include "storage/ipc.h"
#include "fmgr.h"
#include "utils/builtins.h"

#include "malloc.h"

PG_MODULE_MAGIC;

void _PG_init(void);
void malloc_exit_callback(int, Datum);

bool proc_exit_setup = false;

void
malloc_exit_callback(int code, Datum arg)
{
	struct mallinfo2 m;
	m = mallinfo2();
	elog(NOTICE, "On exit malloc stats: \n"
		 		 "\tTotal memory: %ld\n"
				 "\tMemory (without mmap): %ld\n"
				 "\tMmaped memory: %ld\n"
				 "\tIn-use: %ld\n"
				 "\tFree blocks: %ld\n"
				 "\tKeep cost: %ld",
			 m.arena + m.hblkhd,
			 m.arena,
			 m.hblkhd,
			 m.uordblks,
			 m.fordblks,
			 m.keepcost);
}

void
_PG_init(void)
{
	on_proc_exit(malloc_exit_callback, 0);
}
