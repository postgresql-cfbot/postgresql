# src/test/modules/heap/Makefile

MODULES = test_heap
PGFILEDESC = "test_heap - regression test for heap"

EXTENSION = test_heap
DATA = test_heap--1.0.sql

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/heap
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
