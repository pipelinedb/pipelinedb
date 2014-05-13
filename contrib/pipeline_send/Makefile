# contrib/pipeline_send/Makefile

PGFILEDESC = "pipeline_send - sends raw events to the postgres backend"

PROGRAM = pipeline_send
OBJS	= pipeline_send.o

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
