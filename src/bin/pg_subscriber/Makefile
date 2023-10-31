# src/bin/pg_subscriber/Makefile

PGFILEDESC = "pg_subscriber - create a new logical replica from a standby server"
PGAPPICON=win32

subdir = src/bin/pg_subscriber
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)
LDFLAGS_INTERNAL += -L$(top_builddir)/src/fe_utils -lpgfeutils $(libpq_pgport)

OBJS = \
	$(WIN32RES) \
	pg_subscriber.o

all: pg_subscriber

pg_subscriber: $(OBJS) | submake-libpq submake-libpgport submake-libpgfeutils
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) pg_subscriber$(X) '$(DESTDIR)$(bindir)/pg_subscriber$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_subscriber$(X)'

clean distclean maintainer-clean:
	rm -f pg_subscriber$(X) $(OBJS)
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
