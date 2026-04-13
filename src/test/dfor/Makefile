#-------------------------------------------------------------------------
# File: src/test/dfor/Makefile
#-------------------------------------------------------------------------

subdir = src/test/dfor
top_builddir = ../../..
dfor_dir := $(top_builddir)/src/backend/lib

include $(dfor_dir)/Makefile.dfor

# Ensure dependency tracking works
OBJS += $(OBJS_DFOR)

include $(top_builddir)/src/Makefile.global

# This fixes a problem in some CI jobs
DEPDIR ?= ".deps"

# Object files of vect, bitpack and dfor used by these unit-tests are in the same
# directory as this Makefile. Tests don't use ones from src/backend/lib and compile
# different ones for themselves.
$(info Use OBJS_DFOR=$(OBJS_DFOR) from current directory $(subdir). \
       They are built on sources from $(dfor_dir))

$(OBJS_DFOR): %.o: $(dfor_dir)/%.c
	@if test ! -d $(DEPDIR); then mkdir -p $(DEPDIR); fi
	$(CC) $(CFLAGS) $(CPPFLAGS) -DFRONTEND \
		-c $< \
		-MMD -MP -MF $(DEPDIR)/$(notdir $<:.c=.Po) \
		-o $@

LIBTAP_OBJS = $(top_builddir)/src/test/libtap/tap.o

TESTS= test_vect_u16 \
       test_uniqsortvect_u16 \
       test_bitpack_u16

$(TESTS:%=%.o): CPPFLAGS += -I$(top_builddir)/src/test -DFRONTEND

all: $(TESTS)

$(TESTS): %: %.o $(LIBTAP_OBJS) $(OBJS_DFOR)
	$(CC) $(CFLAGS) $(CPPFLAGS) $^ $(LDFLAGS) $(LIBS) -o $@$(X)

check-unit: $(TESTS)
	echo "# +++ Unit tests in $(subdir) +++" && \
	cd $(top_builddir)/$(subdir) && \
	   $(PROVE) $(PROVE_FLAGS) \
	    -v $(addprefix ./,$(if $(PROVE_TESTS), $(PROVE_TESTS), $(TESTS)))

check: check-unit

clean distclean:
	rm -rf $(TESTS) *.o
