#-------------------------------------------------------------------------
#
# Makefile for src/test/crypto
#
# Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
#
# src/test/crypto/Makefile
#
#-------------------------------------------------------------------------

subdir = src/test/crypto
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

export with_openssl

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)

clean distclean maintainer-clean:
	rm -rf tmp_check
