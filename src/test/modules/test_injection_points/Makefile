# src/test/modules/test_injection_points/Makefile

MODULE_big = test_injection_points
OBJS = \
	$(WIN32RES) \
	test_injection_points.o
PGFILEDESC = "test_injection_points - test injection points"

EXTENSION = test_injection_points
DATA = test_injection_points--1.0.sql
REGRESS = test_injection_points

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_injection_points
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
