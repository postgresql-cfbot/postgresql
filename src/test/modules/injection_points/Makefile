# src/test/modules/injection_points/Makefile

MODULE_big = injection_points
OBJS = \
	$(WIN32RES) \
	injection_points.o
PGFILEDESC = "injection_points - facility for injection points"

EXTENSION = injection_points
DATA = injection_points--1.0.sql
REGRESS = injection_points

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/injection_points
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
