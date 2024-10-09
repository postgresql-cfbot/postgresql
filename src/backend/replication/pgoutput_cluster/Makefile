#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for src/backend/replication/pgoutput_cluster
#
# IDENTIFICATION
#    src/backend/replication/pgoutput_cluster
#
#-------------------------------------------------------------------------

subdir = src/backend/replication/pgoutput_cluster
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	$(WIN32RES) \
	pgoutput_cluster.o
PGFILEDESC = "pgoutput_cluster - logical replication output plugin for CLUSTER command"
NAME = pgoutput_cluster

all: all-shared-lib

include $(top_srcdir)/src/Makefile.shlib

install: all installdirs install-lib

installdirs: installdirs-lib

uninstall: uninstall-lib

clean distclean: clean-lib
	rm -f $(OBJS)
