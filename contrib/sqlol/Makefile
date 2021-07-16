# contrib/sqlol/Makefile

MODULE_big = sqlol
OBJS = \
	$(WIN32RES) \
	sqlol.o sqlol_gram.o sqlol_scan.o sqlol_keywords.o
PGFILEDESC = "sqlol - Toy alternative grammar based on LOLCODE"

REGRESS = 01_sqlol

sqlol_gram.h: sqlol_gram.c
	touch $@

sqlol_gram.c: BISONFLAGS += -d
# sqlol_gram.c: BISON_CHECK_CMD = $(PERL) $(srcdir)/check_keywords.pl $< $(top_srcdir)/src/include/parser/kwlist.h


sqlol_scan.c: FLEXFLAGS = -CF -p -p
sqlol_scan.c: FLEX_NO_BACKUP=yes
sqlol_scan.c: FLEX_FIX_WARNING=yes


# Force these dependencies to be known even without dependency info built:
sqlol.o sqlol_gram.o sqlol_scan.o parser.o: sqlol_gram.h

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/sqlol
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
