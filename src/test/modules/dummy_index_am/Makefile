# contrib/bloom/Makefile

MODULE_big = dummy_index_am
OBJS = dummy_index.o direloptions.o $(WIN32RES)

EXTENSION = dummy_index_am
DATA = dummy_index_am--1.0.sql
PGFILEDESC = "dummy index access method - needed for all kinds of tests"

REGRESS = reloptions

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/dummy_index_am
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
