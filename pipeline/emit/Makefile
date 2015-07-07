# Copyright (c) 2013-2015 PipelineDB
# pipeline/emit/Makefile

PGFILEDESC = "emit - sends raw events to the postgres backend"

PROGRAM = emit
OBJS	= emit.o

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
