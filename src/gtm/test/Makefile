#----------------------------------------------------------------------------
#
# Postgres-XC GTM test makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# src/gtm/test/Makefile
#
#-----------------------------------------------------------------------------
top_build_dir=../..
include $(top_build_dir)/gtm/Makefile.global

override CPPFLAGS := -I$(top_build_dir)/gtm/client $(CPPFLAGS)

SRCS=test_serialize.c test_connect.c test_node.c test_node5.c test_txn.c test_txn4.c test_txn5.c test_repli.c test_repli2.c test_seq.c test_seq4.c test_seq5.c test_scenario.c test_startup.c test_standby.c test_common.c

PROGS=test_serialize test_connect test_txn test_txn4 test_txn5 test_repli test_repli2 test_seq test_seq4 test_seq5 test_scenario test_startup test_node test_node5 test_standby

OBJS=$(SRCS:.c=.o)
LIBS=$(top_build_dir)/gtm/client/libgtmclient.a \
  $(top_build_dir)/gtm/common/libgtm.a \
  $(top_build_dir)/gtm/libpq/libpqcomm.a

LOADLIBES=-lpthread
CFLAGS=-g -O0

all: $(PROGS)

test_serialize: test_serialize.o test_common.o $(LIBS)

test_connect: test_connect.o test_common.o $(LIBS)
test_startup: test_startup.o test_common.o $(LIBS)

test_node: test_node.o test_common.o $(LIBS)
test_node5: test_node5.o test_common.o $(LIBS)

test_txn: test_txn.o test_common.o $(LIBS)

test_txn4: test_txn4.o test_common.o $(LIBS)
test_txn5: test_txn5.o test_common.o $(LIBS)

test_repli: test_repli.o test_common.o $(LIBS)

test_standby: test_standby.o test_common.o $(LIBS)

test_repli2: test_repli2.o test_common.o $(LIBS)

test_seq: test_seq.o test_common.o $(LIBS)
test_seq4: test_seq4.o test_common.o $(LIBS)
test_seq5: test_seq5.o test_common.o $(LIBS)

test_scenario: test_scenario.o test_common.o $(LIBS)

clean:
	rm -f $(OBJS) *~
	rm -f $(PROGS)

distclean: clean

maintainer-clean: distclean
