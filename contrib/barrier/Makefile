# contrib/barrier/Makefile

MODULE_big = barrier
OBJS = \
	$(WIN32RES) \
	barrier.o

EXTENSION = barrier
DATA = barrier--1.0.sql
PGFILEDESC = "barrier - barrier test code NOT FOR COMMIT"

REGRESS = barrier

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/barrier
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
