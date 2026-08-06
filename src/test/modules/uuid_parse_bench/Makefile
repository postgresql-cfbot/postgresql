# src/test/modules/uuid_parse_bench/Makefile

MODULE_big = uuid_parse_bench
OBJS = \
	$(WIN32RES) \
	uuid_parse_bench.o
PGFILEDESC = "uuid_parse_bench - microbenchmark for UUID input parsing"

EXTENSION = uuid_parse_bench
DATA = uuid_parse_bench--1.0.sql

REGRESS = uuid_parse_bench

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/uuid_parse_bench
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
