# contrib/pgbench/Makefile

subdir = src/backend/pipeline/trigger
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

SRCS = $(wildcard *.c)
OBJS = $(patsubst %.c,%.o,$(SRCS))

include $(top_srcdir)/src/backend/common.mk
