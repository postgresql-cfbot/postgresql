EXTENSION = zson
MODULES = zson
DATA = zson--1.1.sql zson--1.0--1.1.sql
REGRESS = zson

PG_CPPFLAGS = -g -O2
SHLIB_LINK = # -lz -llz4

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/zson
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
