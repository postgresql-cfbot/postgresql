export PYTHON

MODULES = validator
PGFILEDESC = "validator - test OAuth validator module"

NO_INSTALLCHECK = 1

TAP_TESTS = 1

REGRESS = validator

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/oauth_validator
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
