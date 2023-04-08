# src/test/modules/test_wal_read_from_buffers/Makefile

MODULE_big = test_wal_read_from_buffers
OBJS = \
	$(WIN32RES) \
	test_wal_read_from_buffers.o
PGFILEDESC = "test_wal_read_from_buffers - test module to verify that WAL can be read from WAL buffers"

EXTENSION = test_wal_read_from_buffers
DATA = test_wal_read_from_buffers--1.0.sql

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_wal_read_from_buffers
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
