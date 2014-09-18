# pipeline/emit/Makefile

PGFILEDESC = "read - consumes events stored in the stream buffer"

PROGRAM = read
OBJS	= read.o

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
