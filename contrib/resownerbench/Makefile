MODULE_big = resownerbench
OBJS = resownerbench.o

EXTENSION = resownerbench
DATA = resownerbench--1.0.sql
PGFILEDESC = "resownerbench - benchmark for ResourceOwners"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/resownerbench
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
