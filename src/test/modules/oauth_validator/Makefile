#-------------------------------------------------------------------------
#
# Makefile for src/test/modules/oauth_validator
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/test/modules/oauth_validator/Makefile
#
#-------------------------------------------------------------------------

MODULES = validator
PGFILEDESC = "validator - test OAuth validator module"

NO_INSTALLCHECK = 1

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/oauth_validator
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk

export PYTHON
export with_oauth
export with_python

endif
