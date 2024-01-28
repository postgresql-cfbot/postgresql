# src/test/modules/dummy_sequence_am/Makefile

MODULES = dummy_sequence_am

EXTENSION = dummy_sequence_am
DATA = dummy_sequence_am--1.0.sql

REGRESS = dummy_sequence

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/dummy_sequence_am
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
