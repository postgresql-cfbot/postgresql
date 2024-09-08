MODULE_big = bench_sort_improvements

OBJS = \
	$(WIN32RES) \
	bench.o

EXTENSION = bench_sort_improvements

DATA = bench_sort_improvements--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/bench_sort_improvements
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
