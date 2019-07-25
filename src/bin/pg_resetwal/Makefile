#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_resetwal
#
# Copyright (c) 1998-2019, PostgreSQL Global Development Group
#
# src/bin/pg_resetwal/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "pg_resetwal - reset PostgreSQL WAL log"
PGAPPICON=win32

subdir = src/bin/pg_resetwal
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -DFRONTEND $(CPPFLAGS)
LDFLAGS_INTERNAL += -L$(top_builddir)/src/fe_utils -lpgfeutils $(libpq_pgport)

OBJS= pg_resetwal.o encryption.o $(WIN32RES)

all: pg_resetwal

pg_resetwal: $(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

encryption.c: % : $(top_srcdir)/src/backend/storage/file/%
	rm -f $@ && $(LN_S) $< .

install: all installdirs
	$(INSTALL_PROGRAM) pg_resetwal$(X) '$(DESTDIR)$(bindir)/pg_resetwal$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_resetwal$(X)'

clean distclean maintainer-clean:
	rm -f pg_resetwal$(X) $(OBJS) encryption.c
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
