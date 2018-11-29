# src/test/default_collation/Makefile

subdir = src/test/default_collation
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

ifeq ($(with_icu),yes)
check:
	$(MAKE) -C icu check
check-utf8:
	$(MAKE) -C icu.utf8 check
	$(MAKE) -C libc.utf8 check
else
check:
	$(MAKE) -C libc check
check-utf8:
	$(MAKE) -C libc.utf8 check
endif

# We don't check libc/ if with_icu or vice versa, but we do want "make clean" to
# recurse into it. The same goes for libc.utf8/ or icu.utf8/, which we don't
# check by default.
ALWAYS_SUBDIRS = libc libc.utf8 icu icu.utf8

clean distclean maintainer-clean:
	for d in $(ALWAYS_SUBDIRS); do \
		$(MAKE) -C $$d clean || exit; \
	done
