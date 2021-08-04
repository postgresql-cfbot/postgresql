# src/test/modules/test_hooks/Makefile

MODULE_big = test_hooks
OBJS = \
	$(WIN32RES) \
	test_hooks.o \
	test_process_interrupts_hook.o
PGFILEDESC = "test_hooks - example use of assorted PostgreSQL hooks"

EXTENSION = test_hooks
DATA = test_hooks--1.0.sql

REGRESS = test_processinterrupt_hook
REGRESS_OPTS = --temp-config=$(top_srcdir)/src/test/modules/test_hooks/hooks.conf
# Disabled because these tests require "shared_preload_libraries=test_hooks",
# which typical installcheck users do not have (e.g. buildfarm clients).
NO_INSTALLCHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_hooks
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
