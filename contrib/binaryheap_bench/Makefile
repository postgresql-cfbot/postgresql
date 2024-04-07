# contrib/binaryheap_bench/Makefile

MODULE_big = binaryheap_bench
OBJS = \
	$(WIN32RES) \
	xx_binaryheap.o \
	old_binaryheap.o \
	binaryheap_bench.o

EXTENSION = binaryheap_bench
DATA = binaryheap_bench--1.0.sql
PGFILEDESC = "binaryheap_bench"

REGRESS = binaryheap_bench
TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/binaryheap_bench
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
