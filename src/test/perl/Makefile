#-------------------------------------------------------------------------
#
# Makefile for src/test/perl
#
# Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/test/perl/Makefile
#
#-------------------------------------------------------------------------

subdir = src/test/perl
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

ifeq ($(enable_tap_tests),yes)

OBJS = PostgresClient.o PgResult.o PostgresClient.so PgResult.so \
	PostgresClient.c PgResult.c
XSUBPPDIR = $(shell $(PERL) -e 'use List::Util qw(first); print first { -r "$$_/ExtUtils/xsubpp" } @INC')
XSUBPPTYPEMAP = $(XSUBPPDIR)/../ExtUtils/typemap
LDFLAGS = -L$(top_builddir)/src/interfaces/libpq -lpq
ARCHLIBEXP = $(shell $(PERL) -e 'use Config; print $$Config{"archlibexp"};')
override CPPFLAGS := -fPIC -I. -I$(srcdir) -I$(CPPFLAGS) -I$(top_builddir)/src/include -I$(top_builddir)/src/interfaces/libpq -I$(ARCHLIBEXP)/CORE -I$(top_builddir)/src/pl/plperl

%.c: %.xs
	$(PERL) $(XSUBPPDIR)/ExtUtils/xsubpp -typemap $(XSUBPPTYPEMAP) $< >$@

# These files are generated from libpq-fe.h. Must be re-generated when
# definitions of constants in the file is changed. Especially,
# EXPORT_TAGS and EXPORT in PostgresClient.pm must be edited according
# to generated PostgresClient/lib/PostgresClient.pm if related symbols
# in libpq-fe.h are removed or added.
const-c.inc const-xs.inc:
	h2xs -OPb 5.8.0 -n PostgresClient $(top_builddir)/src/interfaces/libpq/libpq-fe.h
	(cd PostgresClient; $(PERL) Makefile.PL)
	cp PostgresClient/*.inc ./

PostgresClient.c PgResult.c : $(XSUBPPDEPS) const-c.inc const-xs.inc

all: PostgresClient.so PgResult.so

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(pgxsdir)/$(subdir)'

install: all installdirs
	$(INSTALL_DATA) $(srcdir)/TestLib.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/TestLib.pm'
	$(INSTALL_DATA) $(srcdir)/SimpleTee.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/SimpleTee.pm'
	$(INSTALL_DATA) $(srcdir)/RecursiveCopy.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/RecursiveCopy.pm'
	$(INSTALL_DATA) $(srcdir)/PostgresNode.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgresNode.pm'

uninstall:
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/TestLib.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/SimpleTee.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/RecursiveCopy.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgresNode.pm'

clean:
	rm -rf $(OBJS) PostgresClient

distclean: clean
	rm *.inc

endif
