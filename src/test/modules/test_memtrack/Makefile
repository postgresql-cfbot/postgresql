# src/test/modules/test_memtrack/Makefile

MODULE_big = test_memtrack
OBJS = \
	$(WIN32RES) \
	test_memtrack.o \
	worker_pool.o
PGFILEDESC = "test_memtrack - example use of shared memory message queue"

EXTENSION = test_memtrack
DATA = test_memtrack--1.0.sql

REGRESS = test_memtrack
REGRESS_OPTS = --temp-config=$(top_srcdir)/src/test/modules/test_memtrack/test_memtrack.conf

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_memtrack
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
