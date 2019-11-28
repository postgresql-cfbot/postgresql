# src/test/modules/test_pg_dump/Makefile

MODULES = test_pg_dump_fdw
PGFILEDESC = "test_pg_dump - Test pg_dump with extensions"

EXTENSION = test_pg_dump_fdw test_pg_dump
DATA = test_pg_dump_fdw--1.0.sql test_pg_dump--1.0.sql

REGRESS = test_pg_dump test_pg_dump_fdw
TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_pg_dump
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
