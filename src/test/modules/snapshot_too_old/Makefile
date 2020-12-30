# src/test/modules/snapshot_too_old/Makefile

MODULE_big = test_sto
OBJS = \
	$(WIN32RES) \
	test_sto.o

EXTENSION = test_sto
DATA = test_sto--1.0.sql
PGDESCFILE = "test_sto -- internal testing for snapshot too old errors"

EXTRA_INSTALL = contrib/pg_visibility contrib/old_snapshot

ISOLATION = sto_using_cursor sto_using_select sto_using_hash_index
ISOLATION_OPTS = --temp-config $(top_srcdir)/src/test/modules/snapshot_too_old/sto.conf

# Disabled because these tests require "old_snapshot_threshold" = 10, which
# typical installcheck users do not have (e.g. buildfarm clients) and also
# because it'd be dangerous on a production system.
NO_INSTALLCHECK = 1

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/snapshot_too_old
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
