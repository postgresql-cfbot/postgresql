#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/heap
#
# IDENTIFICATION
#    src/backend/access/heap/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/access/heap
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = heapam.o heapam_common.o heapam_storage.o hio.o \
	pruneheap.o rewriteheap.o storageam.o storageamapi.o \
	syncscan.o tuptoaster.o visibilitymap.o

include $(top_srcdir)/src/backend/common.mk
