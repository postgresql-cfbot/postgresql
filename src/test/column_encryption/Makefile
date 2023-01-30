#-------------------------------------------------------------------------
#
# Makefile for src/test/column_encryption
#
# Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/test/column_encryption/Makefile
#
#-------------------------------------------------------------------------

subdir = src/test/column_encryption
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

export OPENSSL PERL

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)
LDFLAGS_INTERNAL += -L$(top_builddir)/src/fe_utils -lpgfeutils $(libpq_pgport)

all: test_client

check: all
	$(prove_check)

installcheck:
	$(prove_installcheck)

clean distclean maintainer-clean:
	rm -f test_client.o test_client
	rm -rf tmp_check
