# contrib/pipeline_kafka/Makefile

MODULE_big = pipeline_kafka
OBJS = pipeline_kafka.o

EXTENSION = pipeline_kafka
DATA = pipeline_kafka--0.8.0.sql

REGRESS = pipeline_kafka

LIB_RDKAFKA_STATIC ?= /usr/lib/librdkafka.a

SHLIB_LINK += $(LIB_RDKAFKA_STATIC)
SHLIB_LINK += -lz -lpthread

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pipeline_kafka
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
