# src/bin/pg_keysetup/Makefile

PGFILEDESC = "pg_keytool - handle cluster encryption key"
PGAPPICON=win32

subdir = src/bin/pg_keytool
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = pg_keytool.o $(RMGRDESCOBJS) $(WIN32RES)

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)
override CPPFLAGS := -DFRONTEND $(CPPFLAGS)
LDFLAGS_INTERNAL += -L$(top_builddir)/src/fe_utils -lpgfeutils $(libpq_pgport)

all: pg_keytool

pg_keytool: $(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) pg_keytool$(X) '$(DESTDIR)$(bindir)/pg_keytool$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_keytool$(X)'

clean distclean maintainer-clean:
	rm -f pg_keytool$(X) $(OBJS) encryption.c
