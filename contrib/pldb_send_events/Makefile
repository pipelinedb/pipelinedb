# contrib/pldb_send_events/Makefile

PGFILEDESC = "pldb_send_events - sends raw events to the postgres backend"

PROGRAM = pldb_send_events
OBJS	= pldb_send_events.o

PG_CPPFLAGS = -I$(libpq_srcdir) 
PG_LIBS = $(libpq_pgport)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
