#!/bin/sh

cat /dev/null>regress.log

./test_serialize | tee -a regress.log 2>&1

./stop.sh
./start_a.sh
./test_connect 2>&1 | tee -a regress.log
./test_node 2>&1 | tee -a regress.log
./test_txn 2>&1 | tee -a regress.log
./test_seq 2>&1 | tee -a regress.log

echo ""
echo "=========== SUMMARY ============"
date
echo -n "Assert: "
grep -c ASSERT regress.log
