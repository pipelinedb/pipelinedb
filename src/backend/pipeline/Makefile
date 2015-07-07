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

OBJS = combinerReceiver.o cqanalyze.o cont_plan.o cqwindow.o stream.o \
			 cqmatrel.o sw_vacuum.o tdigest.o miscutils.o bloom.o hll.o cmsketch.o \
			 cont_analyze.o cont_scheduler.o cont_worker.o cont_combiner.o \
			 groupcache.o streamReceiver.o tuplebuf.o update.o

include $(top_srcdir)/src/backend/common.mk
