# contrib/bytea_plperl/Makefile

MODULE_big = bytea_plperl
OBJS = \
	$(WIN32RES) \
	bytea_plperl.o
PGFILEDESC = "bytea_plperl - bytea transform for plperl"

PG_CPPFLAGS = -I$(top_srcdir)/src/pl/plperl

EXTENSION = bytea_plperlu bytea_plperl
DATA = bytea_plperlu--1.0.sql bytea_plperl--1.0.sql

REGRESS = bytea_plperl bytea_plperlu

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/bytea_plperl
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# We must link libperl explicitly
ifeq ($(PORTNAME), win32)
# these settings are the same as for plperl
override CPPFLAGS += -DPLPERL_HAVE_UID_GID -Wno-comment
# ... see silliness in plperl Makefile ...
SHLIB_LINK_INTERNAL += $(sort $(wildcard ../../src/pl/plperl/libperl*.a))
else
rpathdir = $(perl_archlibexp)/CORE
SHLIB_LINK += $(perl_embed_ldflags)
endif

# As with plperl we need to include the perl_includespec directory last.
override CPPFLAGS := $(CPPFLAGS) $(perl_embed_ccflags) $(perl_includespec)
