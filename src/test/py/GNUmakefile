#-------------------------------------------------------------------------
#
# GNUmakefile--
#    Makefile for src/test/py
#
# src/test/py/GNUmakefile
#
# This just creates a phony check target so that we can run this test
# suite in a way that's consistent with the rest of the code base, and
# so that these tests are recognized we run `make check` from the source
# root.
#-------------------------------------------------------------------------

subdir = src/test/py
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

LIBPQ = ../../../src/interfaces/libpq
PG = ../../../src/include

.PHONY: check

all: extended

extended:
	$(CC) extended.c -I$(LIBPQ) -I$(PG) -L$(LIBPQ) $^ -lpq -o $@

check: all
	py.test -v

clean:
	rm -rf ./.pdb*
	rm -rf ./__pycache__
	rm -f extended
	rm -rf test_binary_upgrade_data_dir
