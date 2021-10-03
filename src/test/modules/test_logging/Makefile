# src/test/modules/test_logging/Makefile

MODULES = test_logging

EXTENSION = test_logging
DATA = test_logging--1.0.sql
PGFILEDESC = "test_logging - simple log hook test"

REGRESS = test_logging

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_logging
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
