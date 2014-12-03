#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for unit tests
#
# IDENTIFICATION
#    src/test/unit/Makefile
#
#-------------------------------------------------------------------------

subdir = src/test/unit
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

unit_test = $(top_builddir)/$(subdir)/unit_tests

OBJS_TO_TEST = ../../backend/pipeline/hll.o

OBJS = $(OBJS_TO_TEST) runner.o test_hll.o test_test.o

LIBS += -lcheck -lpthread -lm -lrt

include $(top_srcdir)/src/backend/common.mk

all: unit_tests

check: all
	$(unit_test)
#	./unit_tests

unit_tests: $(OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDFLAGS_EX) $(export_dynamic) $(call expand_subsys,$^) $(LIBS) -o $@
	
clean:
	rm -rf unit_tests