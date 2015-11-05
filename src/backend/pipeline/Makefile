#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for events package
#
# Copyright (c) 2013-2015, PipelineDB
#
# IDENTIFICATION
#    src/backend/pipeline/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/pipeline
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = combinerReceiver.o cont_plan.o update.o stream.o \
			 cqmatrel.o sw_vacuum.o tdigest.o miscutils.o bloom.o hll.o cmsketch.o \
			 cont_analyze.o cont_scheduler.o cont_worker.o cont_combiner.o \
			 groupcache.o fss.o cont_adhoc_format.o dsm_cqueue.o stream_fdw.o \
			 adhocReceiver.o cont_adhoc.o cont_execute.o

include $(top_srcdir)/src/backend/common.mk
