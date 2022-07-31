# contrib/bool_plperl/Makefile

MODULE_big = bool_plperl
OBJS = \
	$(WIN32RES) \
	bool_plperl.o
PGFILEDESC = "bool_plperl - bool transform for plperl"

PG_CPPFLAGS = -I$(top_srcdir)/src/pl/plperl

# We do not yet know whether we are building with trusted PL/Perl, untrusted
# PL/Perl, or both, so for now we assume we are just building trusted PL/Perl.
# We'll adjust these later on if this assumption was accurate.
EXTENSION = bool_plperl
DATA = bool_plperl--1.0.sql
REGRESS = bool_plperl

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/bool_plperl
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

# Since we now know whether we are building trusted PL/Perl, untrusted PL/Perl,
# or both, we should adjust the relevant variables accordingly.
ifeq ($(PERL_TRUSTED), no)
	override undefine EXTENSION
	override undefine DATA
	override undefine REGRESS
endif

ifeq ($(PERL_UNTRUSTED), yes)
	override EXTENSION += bool_plperlu
	override DATA += bool_plperlu--1.0.sql
	override REGRESS += bool_plperlu
endif
