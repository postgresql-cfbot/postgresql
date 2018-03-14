# src/test/modules/test_subscription/Makefile

MODULE_big = test_subscription
OBJS = test_subscription.o $(WIN32RES)
PGFILEDESC = "test_subscription - module for subscription test"

EXTENSION = test_subscription
DATA = test_subscription--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_subscription
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
