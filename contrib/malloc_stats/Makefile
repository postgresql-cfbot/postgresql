# contrib/malloc_stats/Makefile

MODULES = malloc_stats

PGFILEDESC = "malloc_stats - register on_proc_exit to dump malloc_stats"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/malloc_stats
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
