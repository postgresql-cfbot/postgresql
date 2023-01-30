#-------------------------------------------------------------------------
#
# Makefile for src/test/modules/test_ddl_deparse_regress
#
# Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/test/modules/test_ddl_deparse_regress/Makefile
#
#-------------------------------------------------------------------------


MODULES = test_ddl_deparse_regress
PGFILEDESC = "test_ddl_deparse_regress - regression testing for DDL deparsing"

EXTENSION = test_ddl_deparse_regress
DATA = test_ddl_deparse_regress--1.0.sql

# test_ddl_deparse must be first
REGRESS = test_ddl_deparse \
	create_extension \
	create_schema \
	create_table \
	aggregate

export REGRESS

EXTRA_INSTALL = contrib/pg_stat_statements

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_ddl_deparse_regress
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

REGRESS_SHLIB=$(abs_top_builddir)/src/test/regress/regress$(DLSUFFIX)
export REGRESS_SHLIB
