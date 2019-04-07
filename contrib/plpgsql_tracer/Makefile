# contrib/plpgsql_tracer/Makefile

MODULES = plpgsql_tracer

EXTENSION = plpgsql_tracer
PGFILEDESC = "plpgsql tracer - example of PRAGMA based extension"

REGRESS = plpgsql_tracer

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/plpgsql_tracer
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

ifeq ($(PORTNAME), darwin)
override CFLAGS += -undefined dynamic_lookup
endif

override CFLAGS += -I$(top_builddir)/src/pl/plpgsql/src -Wall
