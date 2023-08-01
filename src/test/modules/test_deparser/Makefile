# src/test/modules/test_deparser/Makefile

MODULES = test_deparser
PGFILEDESC = "test_deparser - regression testing for DDL deparsing"

EXTENSION = test_deparser
DATA = test_deparser--1.0.sql

EXTRA_CLEAN = $(pg_regress_clean_files) deparse_init_schedule deparse_regress_schedule

MODULE_big = test_deparser
OBJS = \
	$(WIN32RES) \
	test_deparser.o

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_deparser
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

MINIMAL_REGRESS = test_setup \
		  create_index \
		  create_table \
		  alter_table

REGRESS_OPTS += --load-extension=test_deparser --dlpath=$(top_builddir)/src/test/regress \
		--temp-config $(top_srcdir)/src/test/modules/test_deparser/test_deparser.conf \
		--inputdir=$(top_srcdir)/src/test/regress

deparse_init: all deparse_init_schedule
	$(pg_regress_check) $(REGRESS_OPTS) --schedule=deparse_init_schedule

deparse_regress: all deparse_regress_schedule
	$(pg_regress_check) $(REGRESS_OPTS) --schedule=deparse_regress_schedule

check: all deparse_init deparse_regress

checkminimal: all
	$(pg_regress_check) $(REGRESS_OPTS) test_deparser_1 $(MINIMAL_REGRESS) copy_cmd
	$(pg_regress_check) $(REGRESS_OPTS) test_deparser_2 $(MINIMAL_REGRESS)

deparse_init_schedule:
	echo "test: test_deparser_1" > $@
	cat $(top_srcdir)/src/test/regress/parallel_schedule >> $@
	echo "test: copy_cmd" >> $@
	sed 's/\(.*\)/s\/\\b\1\\b\/\/g/' exclusion_schedule | sed -f - -i $@

deparse_regress_schedule:
	echo "test: test_deparser_2" > $@
	cat $(top_srcdir)/src/test/regress/parallel_schedule >> $@
	sed 's/\(.*\)/s\/\\b\1\\b\/\/g/' exclusion_schedule | sed -f - -i $@
