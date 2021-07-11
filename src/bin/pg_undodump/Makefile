# src/bin/pg_undodump/Makefile

PGFILEDESC = "pg_undodump - decode and display UNDO log"
PGAPPICON=win32

subdir = src/bin/pg_undodump
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -DFRONTEND $(CPPFLAGS)

RMGRDESCSOURCES = dbasedesc.c smgrdesc.c
RMGRDESCOBJS = $(patsubst %.c,%.o,$(RMGRDESCSOURCES))

OBJS = \
	$(RMGRDESCOBJS) \
	$(WIN32RES) \
	pg_undodump.o \
	rmgrdesc.o \
	undoxacttestdesc.o

all: pg_undodump

pg_undodump: $(OBJS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

undoxacttestdesc.c: % : $(top_srcdir)/src/backend/access/undoxacttest/%
	rm -f $@ && $(LN_S) $< .

install: all installdirs
	$(INSTALL_PROGRAM) pg_undodump$(X) '$(DESTDIR)$(bindir)/pg_undodump$(X)'

$(RMGRDESCSOURCES): % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	rm -f $@ && $(LN_S) $< .

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_undodump$(X)'

clean distclean maintainer-clean:
	rm -f pg_undodump$(X) $(OBJS) $(RMGRDESCSOURCES) undoxacttestdesc.c
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
