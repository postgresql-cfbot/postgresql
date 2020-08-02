# src/test/modules/test_fdwxact/Makefile

MODULE_big = test_fdwxact
OBJS = \
	$(WIN32RES) \
	test_fdwxact.o
PGFILEDESC = "test_fdwxact - test code for src/backend/access/fdwxact"

EXTENSION = test_fdwxact
DATA = test_fdwxact--1.0.sql

REGRESS_OPTS = --temp-config=$(top_srcdir)/src/test/modules/test_fdwxact/fdwxact.conf
REGRESS = test_fdwxact

NO_INSTALLCHECK = 1

TAP_TESTS =1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_fdwxact
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
