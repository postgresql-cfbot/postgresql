# contrib/cmzlib/Makefile

MODULE_big = cmzlib
OBJS = \
	$(WIN32RES) \
	cmzlib.o

EXTENSION = cmzlib
DATA = cmzlib--1.0.sql
PGFILEDESC = "zlib compression method "

SHLIB_LINK += $(filter -lz, $(LIBS))

REGRESS = cmzlib


ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/cmzlib
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
