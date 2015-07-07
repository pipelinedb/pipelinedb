CFLAGS += -g -Wall -O2 -pedantic -Wno-long-long -I$(shell pg_config --includedir-server)
CC = gcc
MODULE_FILENAME = $(PWD)/libWFS_locks.so

ifneq ($(findstring 7.1,$(shell pg_config --version)),)
	USE_VERSION=71
else
	ifneq ($(findstring 7.2,$(VERSION)),)
		USE_VERSION=72
	else
		ifneq ($(findstring 7.3,$(VERSION)),)
			USE_VERSION=73
		else
			ifneq ($(findstring 7.4,$(VERSION)),)
				USE_VERSION=74
			else
				USE_VERSION=80
			endif
		endif
	endif
endif


all: libWFS_locks.so WFS_locks.sql

libWFS_locks.so: WFS_locks.c
	$(CC) $(CFLAGS) $(LDFLAGS) -shared -o $@ $<

WFS_locks.sql: WFS_locks.sql.in
	cpp -P -traditional-cpp -DUSE_VERSION=$(USE_VERSION) $< | sed -e 's:@MODULE_FILENAME@:$(MODULE_FILENAME):g' | grep -v '^#' > $@

clean:
	rm -f libWFS_locks.so WFS_locks.sql
