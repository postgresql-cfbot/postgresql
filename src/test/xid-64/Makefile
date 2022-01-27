#-------------------------------------------------------------------------
#
# Makefile for src/test/xid-64
#
# Copyright (c) 2018, Postgres Professional
#
# src/test/xid-64/Makefile
#
#-------------------------------------------------------------------------

subdir = src/test/xid-64
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)

clean distclean maintainer-clean:
	rm -rf tmp_check
