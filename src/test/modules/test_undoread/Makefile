# src/test/modules/test_undoread/Makefile

MODULE_big = test_undoread
OBJS = test_undoread.o $(WIN32RES)
PGFILEDESC = "test_undoread - test code for src/backend/access/undo/undoread.c"

EXTENSION = test_undoread
DATA = test_undoread--1.0.sql

REGRESS = test_undoread

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_undoread
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
