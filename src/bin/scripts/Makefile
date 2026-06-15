#-------------------------------------------------------------------------
#
# Makefile for src/bin/scripts
#
# Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/bin/scripts/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "PostgreSQL utility"
PGAPPICON=win32

subdir = src/bin/scripts
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

PROGRAMS = \
	clusterdb \
	createdb \
	createuser \
	dropdb \
	dropuser \
	pg_isready \
	reindexdb \
	vacuumdb

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)
LDFLAGS_INTERNAL += -L$(top_builddir)/src/fe_utils -lpgfeutils $(libpq_pgport)

all: $(PROGRAMS)

createdb: createdb.o common.o $(WIN32RES) $(libpgfeutils) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) createdb.o common.o $(WIN32RES) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)
createuser: createuser.o common.o $(WIN32RES) $(libpgfeutils) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) createuser.o common.o $(WIN32RES) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)
dropdb: dropdb.o common.o $(WIN32RES) $(libpgfeutils) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) dropdb.o common.o $(WIN32RES) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)
dropuser: dropuser.o common.o $(WIN32RES) $(libpgfeutils) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) dropuser.o common.o $(WIN32RES) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)
clusterdb: clusterdb.o common.o $(WIN32RES) $(libpgfeutils) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) clusterdb.o common.o $(WIN32RES) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)
vacuumdb: vacuumdb.o vacuuming.o common.o $(WIN32RES) $(libpgfeutils) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) vacuumdb.o vacuuming.o common.o $(WIN32RES) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)
reindexdb: reindexdb.o common.o $(WIN32RES) $(libpgfeutils) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) reindexdb.o common.o $(WIN32RES) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)
pg_isready: pg_isready.o common.o $(WIN32RES) $(libpgfeutils) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) pg_isready.o common.o $(WIN32RES) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) createdb$(X)   '$(DESTDIR)$(bindir)'/createdb$(X)
	$(INSTALL_PROGRAM) dropdb$(X)     '$(DESTDIR)$(bindir)'/dropdb$(X)
	$(INSTALL_PROGRAM) createuser$(X) '$(DESTDIR)$(bindir)'/createuser$(X)
	$(INSTALL_PROGRAM) dropuser$(X)   '$(DESTDIR)$(bindir)'/dropuser$(X)
	$(INSTALL_PROGRAM) clusterdb$(X)  '$(DESTDIR)$(bindir)'/clusterdb$(X)
	$(INSTALL_PROGRAM) vacuumdb$(X)   '$(DESTDIR)$(bindir)'/vacuumdb$(X)
	$(INSTALL_PROGRAM) reindexdb$(X)  '$(DESTDIR)$(bindir)'/reindexdb$(X)
	$(INSTALL_PROGRAM) pg_isready$(X) '$(DESTDIR)$(bindir)'/pg_isready$(X)

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f $(addprefix '$(DESTDIR)$(bindir)'/, $(addsuffix $(X), $(PROGRAMS)))

clean distclean:
	rm -f $(addsuffix $(X), $(PROGRAMS)) $(addsuffix .o, $(PROGRAMS))
	rm -f common.o vacuuming.o $(WIN32RES)
	rm -rf tmp_check

export with_icu

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
