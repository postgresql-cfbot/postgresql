# src/interfaces/libpq/test/Makefile

PGFILEDESC = "libpq test program"
PGAPPICON = win32

subdir = src/interfaces/libpq/test
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

ifeq ($(PORTNAME), win32)
LDFLAGS += -lws2_32
endif

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)
LDFLAGS_INTERNAL += $(libpq_pgport)

PROGRAMS = libpq_testclient libpq_uri_regress libpq_pipeline

all: $(PROGRAMS)

$(PROGRAMS): $(WIN32RES)

clean distclean maintainer-clean:
	rm -f $(PROGRAMS) *.o
