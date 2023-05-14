# src/test/modules/xid_wraparound/Makefile

MODULE_big = xid_wraparound
OBJS = \
	$(WIN32RES) \
	xid_wraparound.o
PGFILEDESC = "xid_wraparound - tests for XID wraparound"

EXTENSION = xid_wraparound
DATA = xid_wraparound--1.0.sql

# Disabled by default because these tests could take a long time,
# which typical installcheck users cannot tolerate (e.g. buildfarm
# clients).
ifdef ENABLE_XID_WRAPAROUND_TESTS
TAP_TESTS = 1
endif

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/xid_wraparound
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
