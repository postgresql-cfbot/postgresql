# src/test/modules/read_wal_from_buffers/Makefile

MODULE_big = read_wal_from_buffers
OBJS = \
	$(WIN32RES) \
	read_wal_from_buffers.o
PGFILEDESC = "read_wal_from_buffers - test module to read WAL from WAL buffers"

EXTENSION = read_wal_from_buffers
DATA = read_wal_from_buffers--1.0.sql

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/read_wal_from_buffers
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
