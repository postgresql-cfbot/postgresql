#-------------------------------------------------------------------------
#
# Makefile for src/test/perl
#
# Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/test/perl/Makefile
#
#-------------------------------------------------------------------------

subdir = src/test/perl
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

ifeq ($(enable_tap_tests),yes)

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/Test'

install: all installdirs
	$(INSTALL_DATA) $(srcdir)/PostgreSQL/Test/Utils.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/Test/Utils.pm'
	$(INSTALL_DATA) $(srcdir)/PostgreSQL/Test/SimpleTee.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/Test/SimpleTee.pm'
	$(INSTALL_DATA) $(srcdir)/PostgreSQL/Test/RecursiveCopy.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/Test/RecursiveCopy.pm'
	$(INSTALL_DATA) $(srcdir)/PostgreSQL/Test/Kerberos.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/Test/Kerberos.pm'
	$(INSTALL_DATA) $(srcdir)/PostgreSQL/Test/Cluster.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/Test/Cluster.pm'
	$(INSTALL_DATA) $(srcdir)/PostgreSQL/Test/BackgroundPsql.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/Test/BackgroundPsql.pm'
	$(INSTALL_DATA) $(srcdir)/PostgreSQL/Test/Session.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/Test/Session.pm'
	$(INSTALL_DATA) $(srcdir)/PostgreSQL/Test/AdjustDump.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/Test/AdjustDump.pm'
	$(INSTALL_DATA) $(srcdir)/PostgreSQL/Test/AdjustUpgrade.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/Test/AdjustUpgrade.pm'
	$(INSTALL_DATA) $(srcdir)/PostgreSQL/Version.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/Version.pm'
	$(INSTALL_DATA) $(srcdir)/PostgreSQL/FindLib.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/FindLib.pm'
	$(INSTALL_DATA) $(srcdir)/PostgreSQL/PqFFI.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/PqFFI.pm'
	$(INSTALL_DATA) $(srcdir)/PostgreSQL/PqConstants.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/PqConstants.pm'
	$(INSTALL_DATA) $(srcdir)/PostgreSQL/PGTypes.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/PGTypes.pm'

uninstall:
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/Test/Utils.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/Test/SimpleTee.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/Test/RecursiveCopy.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/Test/Kerberos.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/Test/Cluster.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/Test/BackgroundPsql.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/Test/Session.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/Test/AdjustDump.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/Test/AdjustUpgrade.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/Version.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/FindLib.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/PqFFI.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/PqConstants.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/PostgreSQL/PGTypes.pm'

endif
