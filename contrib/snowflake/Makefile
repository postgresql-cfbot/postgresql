# contrib/snowflake/Makefile

MODULES = snowflake

EXTENSION = snowflake
DATA = snowflake--1.0.sql

REGRESS = snowflake

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/snowflake
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
