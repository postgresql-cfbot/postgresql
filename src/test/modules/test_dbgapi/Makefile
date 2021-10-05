# src/test/modules/test_dbgapi/Makefile

MODULES = test_dbgapi

EXTENSION = test_dbgapi
DATA = test_dbgapi--1.0.sql
PGFILEDESC = "test_dbgapi - test of PL/pgSQL debug API"

REGRESS = test_dbgapi

PG_CPPFLAGS = -I$(top_srcdir)/src/pl/plpgsql/src

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_dbgapi
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
