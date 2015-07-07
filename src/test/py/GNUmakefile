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

.PHONY: check
	
check: all
	py.test -v
	
clean:
	rm -rf ./.pdb*
	rm -rf ./__pycache__
