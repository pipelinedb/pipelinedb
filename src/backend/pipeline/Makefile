# Copyright (c) 2013-2015 PipelineDB
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

OBJS = combinerReceiver.o cqanalyze.o cqplan.o cqwindow.o stream.o tuplebuf.o \
			 cqmatrel.o cqvacuum.o tdigest.o miscutils.o bloom.o hll.o cmsketch.o \
			 gcs.o cont_analyze.o cont_scheduler.o cont_worker.o cont_combiner.o \
			 groupcache.o

include $(top_srcdir)/src/backend/common.mk
