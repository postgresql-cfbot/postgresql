# contrib/pg_amcheck/Makefile

PGFILEDESC = "pg_amcheck - detects corruption within database relations"
PGAPPICON = win32

PROGRAM = pg_amcheck
OBJS = \
	$(WIN32RES) \
	pg_amcheck.o

REGRESS_OPTS += --load-extension=amcheck --load-extension=pageinspect
EXTRA_INSTALL += contrib/amcheck contrib/pageinspect

TAP_TESTS = 1

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS_INTERNAL = -L$(top_builddir)/src/fe_utils -lpgfeutils $(libpq_pgport)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_amcheck
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
