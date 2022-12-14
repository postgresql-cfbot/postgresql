# src/test/modules/test_dsa/Makefile

MODULES = test_dsa

EXTENSION = test_dsa
DATA = test_dsa--1.0.sql
PGFILEDESC = "test_dsa -- tests for DSA areas"
REGRESS = test_dsa

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_dsa
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
