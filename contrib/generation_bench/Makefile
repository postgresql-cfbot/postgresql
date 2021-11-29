# contrib/generation_bench/Makefile

MODULE_big = generation_bench
OBJS = generation_bench.o

EXTENSION = generation_bench
DATA = generation_bench--1.0.sql
PGFILEDESC = "generation_bench - slab context benchmarking functions"

REGRESS = generation_bench

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/generation_bench
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
