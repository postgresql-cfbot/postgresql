# src/test/modules/test_shm_mcxt/Makefile

MODULES = test_shm_mcxt
EXTENSION = test_shm_mcxt
DATA = test_shm_mcxt--1.0.sql
PGFILEDESC = "test_shm_mcxt - example use of shared memory context"

ISOLATION = concurrent_test

# enable our module in shared_preload_libraries
ISOLATION_OPTS = --temp-config $(top_srcdir)/src/test/modules/test_shm_mcxt/test_shm_mcxt.conf

# Disabled because these tests require "shared_preload_libraries=test_shm_mcxt",
# which typical installcheck users do not have (e.g. buildfarm clients).
NO_INSTALLCHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_shm_mcxt
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# But it can nonetheless be very helpful to run tests on preexisting
# installation, allow to do so, but only if requested explicitly.
installcheck-force:
	$(pg_isolation_regress_installcheck) $(ISOLATION)
