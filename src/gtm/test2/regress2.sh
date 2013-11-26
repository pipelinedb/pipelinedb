#!/bin/sh

make

./test_startup
./test_node5
./test_txn4
./test_seq4

./test_standby

./test_txn5

