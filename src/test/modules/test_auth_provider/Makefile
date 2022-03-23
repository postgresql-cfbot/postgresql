# src/test/modules/test_auth_provider/Makefile

MODULE_big = test_auth_provider
OBJS = test_auth_provider.o
PGFILEDESC = "test_auth_provider - provider to test auth hooks"

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_auth_provider
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
