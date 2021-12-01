# src/test/modules/test_path/Makefile

MODULE_big = test_path
OBJS = \
	$(WIN32RES) \
	test_path.o
PGFILEDESC = "test_path - test canonicalize_path"

EXTENSION = test_path
DATA = test_path--1.0.sql

REGRESS = test_path

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_path
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
