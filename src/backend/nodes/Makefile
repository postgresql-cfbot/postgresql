#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for backend/nodes
#
# IDENTIFICATION
#    src/backend/nodes/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/nodes
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	bitmapset.o \
	copyfuncs.o \
	equalfuncs.o \
	extensible.o \
	list.o \
	makefuncs.o \
	nodeFuncs.o \
	nodes.o \
	outfuncs.o \
	params.o \
	print.o \
	read.o \
	readfuncs.o \
	tidbitmap.o \
	value.o

include $(top_srcdir)/src/backend/common.mk

node_headers = \
	nodes/nodes.h \
	nodes/execnodes.h \
	nodes/plannodes.h \
	nodes/primnodes.h \
	nodes/pathnodes.h \
	nodes/memnodes.h \
	nodes/extensible.h \
	nodes/parsenodes.h \
	nodes/replnodes.h \
	commands/trigger.h \
	commands/event_trigger.h \
	foreign/fdwapi.h \
	access/amapi.h \
	access/tableam.h \
	access/tsmapi.h \
	utils/rel.h \
	nodes/supportnodes.h \
	executor/tuptable.h \
	nodes/lockoptions.h \
	access/sdir.h

node_sources = \
	executor/nodeWindowAgg.c \
	nodes/tidbitmap.c \
	utils/mmgr/aset.c \
	utils/mmgr/generation.c \
	utils/mmgr/slab.c

node_files = $(addprefix $(top_srcdir)/src/include/,$(node_headers)) $(addprefix $(top_srcdir)/src/backend/,$(node_sources))

# see also catalog/Makefile for an explanation of these make rules

all: distprep generated-header-symlinks

distprep: node-stuff-stamp

.PHONY: generated-header-symlinks

generated-header-symlinks: $(top_builddir)/src/include/nodes/header-stamp

node-stuff-stamp: gen_node_stuff.pl
	$(PERL) $< $(node_files)
	touch $@

$(top_builddir)/src/include/nodes/header-stamp: node-stuff-stamp
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	cd '$(dir $@)' && for file in nodetags.h; do \
	  rm -f $$file && $(LN_S) "$$prereqdir/$$file" . ; \
	done
	touch $@

maintainer-clean: clean
	rm -f node-stuff-stamp *funcs.inc?.c nodetags.h
