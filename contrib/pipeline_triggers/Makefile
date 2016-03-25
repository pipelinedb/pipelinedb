# contrib/pgbench/Makefile

MODULE_big = pipeline_triggers

SOURCES += $(wildcard src/*.c)
OBJS = $(patsubst %.c,%.o,$(SOURCES))

EXTENSION = pipeline_triggers
DATA = pipeline_triggers--0.9.1.sql
PGFILEDESC = "pipeline_triggers - an extension for triggers on pipeline continuious views"

PG_CPPFLAGS += -Iinclude 
REGRESS = pipeline_triggers
SHLIB_LINK = $(libpq)

#ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
#else
#subdir = contrib/pipeline_triggers
#top_builddir = ../..
#include $(top_builddir)/src/Makefile.global
#include $(top_srcdir)/contrib/contrib-global.mk
#endif
