PGFILEDESC = "pipeline-recv-alerts - client to receive streaming alerts from the alert server"

subdir = src/bin/recv-alerts
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

#override CPPFLAGS := -I. -I$(srcdir) -I$(libpq_srcdir) $(CPPFLAGS)

SRCS=$(wildcard *.c)
OBJS=$(patsubst %.c,%.o,$(SRCS))

all: pipeline-recv-alerts

pipeline-recv-alerts: $(OBJS) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) $(OBJS) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) pipeline-recv-alerts$(X) '$(DESTDIR)$(bindir)/pipeline-recv-alerts$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)' '$(DESTDIR)$(datadir)'
