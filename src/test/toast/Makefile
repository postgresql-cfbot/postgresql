#-------------------------------------------------------------------------
#
# Makefile for src/test/toast
#
# Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/test/toast/Makefile
#
#-------------------------------------------------------------------------

subdir = src/test/recovery
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

export with_lz4

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)

clean distclean maintainer-clean:
	rm -rf tmp_check
