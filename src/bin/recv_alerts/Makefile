PGFILEDESC = "pipeline-recv-alerts - streaming alerts from the alert server"
PGAPPICON = win32

PROGRAM = pipeline-recv-alerts
OBJS	= recv_alerts.o

recv_alerts.o: options.h

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
