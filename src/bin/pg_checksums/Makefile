#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_checksums
#
# Copyright (c) 1998-2019, PostgreSQL Global Development Group
#
# src/bin/pg_checksums/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "pg_checksums - verify data checksums in an offline cluster"
PGAPPICON=win32

subdir = src/bin/pg_checksums
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS= encryption.o pg_checksums.o $(WIN32RES)

override CPPFLAGS := -DFRONTEND $(CPPFLAGS)
LDFLAGS_INTERNAL += -L$(top_builddir)/src/fe_utils -lpgfeutils $(libpq_pgport)

all: pg_checksums

pg_checksums: $(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

encryption.c: % : $(top_srcdir)/src/backend/storage/file/%
	rm -f $@ && $(LN_S) $< .

install: all installdirs
	$(INSTALL_PROGRAM) pg_checksums$(X) '$(DESTDIR)$(bindir)/pg_checksums$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_checksums$(X)'

clean distclean maintainer-clean:
	rm -f pg_checksums$(X) $(OBJS)
	rm -rf tmp_check
	rm -f encryption.c

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
