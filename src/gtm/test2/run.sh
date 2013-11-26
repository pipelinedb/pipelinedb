#!/bin/sh

. ./regress2.sh 2>&1 | tee regress.log

echo ""
echo "=========== SUMMARY ============"
date
echo -n "Assert: "
grep -c ASSERT regress.log
