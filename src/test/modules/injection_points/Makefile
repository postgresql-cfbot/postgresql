# src/test/modules/injection_points/Makefile

MODULE_big = injection_points
OBJS = \
	$(WIN32RES) \
	injection_points.o \
	regress_injection.o
EXTENSION = injection_points
DATA = injection_points--1.0.sql
PGFILEDESC = "injection_points - facility for injection points"

REGRESS = injection_points hashagg reindex_conc vacuum
REGRESS_OPTS = --dlpath=$(top_builddir)/src/test/regress

ISOLATION = basic \
	    inplace \
	    repack \
	    repack_temporal \
	    repack_temporal_multirange \
	    repack_toast \
	    syscache-update-pruned \
	    heap_lock_update

# some isolation tests require wal_level=replica
ISOLATION_OPTS = --temp-config $(top_srcdir)/src/test/modules/injection_points/extra.conf

TAP_TESTS = 1

# The injection points are cluster-wide, so disable installcheck
NO_INSTALLCHECK = 1

# Standalone client used by the TAP tests to wait on and wake up injection
# points without a backend connection.  It is built alongside the module
# (MODULE_big already owns OBJS, so it cannot go through PGXS PROGRAM) and its
# absolute path is exported for the TAP tests.
INJ_STATE_CLIENT = injection_points_state$(X)

export enable_injection_points
export INJECTION_POINTS_STATE := $(abspath $(INJ_STATE_CLIENT))

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/injection_points
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

# XXX: This test is conditional on enable_injection_points in the
# parent Makefile, so we should never get here in the first place if
# injection points are not enabled. But the buildfarm 'misc-check'
# step doesn't pay attention to the if-condition in the parent
# Makefile. To work around that, disable running the test here too.
ifeq ($(enable_injection_points),yes)
include $(top_srcdir)/contrib/contrib-global.mk
else
check:
	@echo "injection points are disabled in this build"
endif

endif

# Build the standalone state client.  Its object is compiled by the implicit
# rule (which adds -I. so injection_points.h is found) and it links against no
# backend libraries.
all: $(INJ_STATE_CLIENT)

$(INJ_STATE_CLIENT): injection_points_state.o
	$(CC) $(CFLAGS) $< $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@

clean: clean-injection-points-state
clean-injection-points-state:
	rm -f $(INJ_STATE_CLIENT) injection_points_state.o
