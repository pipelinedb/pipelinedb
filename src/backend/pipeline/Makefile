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

OBJS = combiner_receiver.o planner.o update.o stream.o \
			 matrel.o ttl_vacuum.o tdigest.o miscutils.o bloom.o hll.o cmsketch.o \
			 analyzer.o scheduler.o worker.o combiner.o fss.o stream_fdw.o executor.o transform_receiver.o \
			 queue.o

SUBDIRS = ipc

include $(top_srcdir)/src/backend/common.mk
