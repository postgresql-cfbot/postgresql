# contrib/pg_copy_json//Makefile

MODULE_big = pg_copy_json
OBJS = \
	$(WIN32RES) \
	pg_copy_json.o
PGFILEDESC = "pg_copy_json - COPY TO JSON (JavaScript Object Notation) format"

EXTENSION = pg_copy_json
DATA = pg_copy_json--1.0.sql

REGRESS = test_copy_format

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_copy_json
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
