MODULE_big = catcachebench
OBJS = catcachebench.o

EXTENSION = catcachebench
DATA = catcachebench--0.0.sql
PGFILEDESC = "catcachebench - benchmark for catcache pruning feature"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/catcachebench
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
