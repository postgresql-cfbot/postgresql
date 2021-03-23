# contrib/brin_bloom/Makefile

MODULE_big = brin_bloom
OBJS = \
	$(WIN32RES) \
	brin_bloom.o

EXTENSION = brin_bloom
DATA = brin_bloom--1.0.sql
PGFILEDESC = "brin_bloom - BRIN bloom operator classes"

REGRESS = brin_bloom

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/brin_bloom
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
