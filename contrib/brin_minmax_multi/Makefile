# contrib/brin_minmax_multi/Makefile

MODULE_big = brin_minmax_multi
OBJS = \
	$(WIN32RES) \
	brin_minmax_multi.o

EXTENSION = brin_minmax_multi
DATA = brin_minmax_multi--1.0.sql
PGFILEDESC = "brin_minmax_multi - BRIN minmax-multi operator classes"

REGRESS = brin_multi

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/brin_minmax_multi
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
