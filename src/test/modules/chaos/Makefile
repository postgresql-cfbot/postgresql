# src/test/modules/chaos/Makefile

MODULE_big = chaos
OBJS = \
	$(WIN32RES) \
	chaos.o
PGFILEDESC = "chaos - module in which to write throwaway fault-injection hacks"

EXTENSION = chaos
DATA = chaos--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/chaos
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
