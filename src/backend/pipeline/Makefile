#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for events package
#
# IDENTIFICATION
#    src/backend/pipeline/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/pipeline
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = combiner.o combinerReceiver.o cqanalyze.o cqplan.o cqproc.o cqwindow.o \
			 stream.o streambuf.o worker.o cqmatrel.o cqvacuum.o tdigest.o \
			 miscutils.o cuckoofilter.o

include $(top_srcdir)/src/backend/common.mk
