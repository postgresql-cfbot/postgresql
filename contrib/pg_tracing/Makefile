# contrib/pg_tracing/Makefile

MODULE_big = pg_tracing
OBJS = \
	$(WIN32RES) \
	pg_tracing.o \
	pg_tracing_query_process.o \
	pg_tracing_explain.o \
	pg_tracing_parallel.o \
	pg_tracing_span.o

EXTENSION = pg_tracing
DATA = pg_tracing--1.0.sql
PGFILEDESC = "pg_tracing - Distributed tracing for postgres"

REGRESS_OPTS = --temp-config $(top_srcdir)/contrib/pg_tracing/pg_tracing.conf
REGRESS = utility select extended insert trigger sample subxact reset nested wal parallel cleanup

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_tracing
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
