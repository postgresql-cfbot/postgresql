# src/test/modules/test_session_hooks/Makefile

MODULES = test_session_hooks
PGFILEDESC = "test_session_hooks - Test session hooks with an extension"

REGRESS_OPTS = --temp-config=$(top_srcdir)/src/test/modules/test_session_hooks/session_hooks.conf

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_session_hooks
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# Disabled because these tests require shared_preload_libraries to be
# set but users unlikely have that set up.
installcheck:;

# But it can nonetheless be very helpful to run tests on preexisting
# installation, allow to do so, but only if requested explicitly.
installcheck-force: regresscheck-install-force

check: regresscheck

submake-regress:
	$(MAKE) -C $(top_builddir)/src/test/regress all

submake-test_session_hooks:
	$(MAKE) -C $(top_builddir)/src/test/modules/test_session_hooks

REGRESSCHECKS = test_session_hooks

regresscheck: | submake-regress submake-test_session_hooks temp-install
	$(pg_regress_check) $(REGRESS_OPTS) $(REGRESSCHECKS)

regresscheck-install-force: | submake-regress submake-test_session_hooks temp-install
	$(pg_regress_installcheck) $(REGRESSCHECKS)

.PHONY: submake-test_session_hooks submake-regress check \
	regresscheck regresscheck-install-force

temp-install: EXTRA_INSTALL=src/test/modules/test_session_hooks
