#
# Makefile for libpq tests
#

subdir = src/test/modules/test_libpq
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

TAP_TESTS = 1

ifeq ($(PORTNAME), win32)
LDFLAGS += -lws2_32
endif

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)
LDFLAGS_INTERNAL += $(libpq_pgport)

PROGS = test_stored_proc_outparams$(X)

all: $(PROGS)

check: $(PROGS)
	$(prove_check)

clean distclean maintainer-clean:
	rm -f $(PROGS) *.o
