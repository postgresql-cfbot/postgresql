# src/test/modules/test_misc/Makefile

TAP_TESTS = 1

MODULE_big = test_misc
OBJS = \
	$(WIN32RES) \
	test_misc.o
PGFILEDESC = "test_misc"

EXTENSION = test_misc
DATA = test_misc--1.0.sql

REGRESS = test_bitset

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_misc
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
