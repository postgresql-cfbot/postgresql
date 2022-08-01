# src/bin/pg_walcleaner/Makefile

PGFILEDESC = "pg_walcleaner - deletes (optionally archives before deletion) unneeded PostgreSQL Write-Ahead Log files"
PGAPPICON = win32

subdir = src/bin/pg_walcleaner
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	$(WIN32RES) \
	pg_walcleaner.o

all: pg_walcleaner

pg_walcleaner: $(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) pg_walcleaner$(X) '$(DESTDIR)$(bindir)/pg_walcleaner$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_walcleaner$(X)'

clean distclean maintainer-clean:
	rm -f pg_walcleaner$(X) $(OBJS)
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
