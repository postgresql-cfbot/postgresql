#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for lib (miscellaneous stuff)
#
# IDENTIFICATION
#    src/backend/lib/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/lib
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

include Makefile.dfor

OBJS = \
	bipartite_match.o \
	bloomfilter.o \
	dshash.o \
	hyperloglog.o \
	ilist.o \
	integerset.o \
	knapsack.o \
	pairingheap.o \
	rbtree.o \
	$(OBJS_DFOR) \

CPPFLAGS := -I$(top_srcdir)/src/backend/lib $(CPPFLAGS)

include $(top_srcdir)/src/backend/common.mk
