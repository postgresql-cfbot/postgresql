#-------------------------------------------------------------------------
#
# Makefile for src/test/cmdstats
#
# Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/test/cmdstats/Makefile
#
#-------------------------------------------------------------------------

EXTRA_INSTALL=contrib/test_decoding

subdir = src/test/cmdstats
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)

clean distclean maintainer-clean:
	rm -rf tmp_check
