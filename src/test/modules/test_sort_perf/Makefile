MODULE_big = test_sort_perf
OBJS = test_sort_perf.o
PGFILEDESC = "test"
EXTENSION = test_sort_perf
DATA = test_sort_perf--1.0.sql

first: all

test_sort_perf.o: test_sort_object_include.c test_sort_itemptr_include.c

test_sort_object_include.c: make-object-tests.sh
	./make-object-tests.sh > test_sort_object_include.c
test_sort_itemptr_include.c: make-itemptr-tests.sh
	./make-itemptr-tests.sh > test_sort_itemptr_include.c

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_sort_perf
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
