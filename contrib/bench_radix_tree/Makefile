# contrib/bench_radix_tree/Makefile

MODULE_big = bench_radix_tree
OBJS = \
	bench_radix_tree.o

EXTENSION = bench_radix_tree
DATA = bench_radix_tree--1.0.sql

REGRESS = bench_fixed_height

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/bench_radix_tree
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
