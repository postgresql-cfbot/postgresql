# src/pl/stub_plpython2/Makefile

# Note that this Makefile only builds and installs a quasi-dummy
# version of plpython2.so.  The control and script files for the
# plpythonu and plpython2u extensions are installed by ../plpython;
# they are the same whether we're using real or stub plpython2.
# (We'd probably not have this separate subdirectory at all, except
# that Makefile.shlib can only build one shlib per directory.)

subdir = src/pl/stub_plpython2
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

PGFILEDESC = "PL/Python - procedural language stub for Python 2"

NAME = plpython2

OBJS = \
	$(WIN32RES) \
	stub_plpython2.o

include $(top_srcdir)/src/Makefile.shlib

all: all-lib

# Ensure parallel safety if a build is started in this directory
$(OBJS): | submake-generated-headers

install: all install-lib

installdirs: installdirs-lib

uninstall: uninstall-lib

clean distclean: clean-lib
	rm -f $(OBJS)

maintainer-clean: distclean
