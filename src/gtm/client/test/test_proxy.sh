#!/bin/bash
# Copyright (c) 2010-2012 Postgres-XC Development Group

GTM_SERVER_HOSTNAME=gtm
GTM_SERVER_PORT=16667

GTM_PROXY_HOSTNAMES=(coordinator1 coordinator2 coordinator3 coordinator4 coordinator5)
GTM_PROXY_PORTS=(16666 16666 16666 16666 16666)
GTM_PROXY_COUNT=${#GTM_PROXY_HOSTNAMES[*]}

PGXC_BASE=$HOME/pgsql_pgxc

GTM_SERVER_PROCESS=gtm
GTM_PROXY_PROCESS=gtm_proxy
GTM_TEST_CLIENT_PROCESS=test_txnperf

GTM_SERVER=$PGXC_BASE/src/gtm/main/$GTM_SERVER_PROCESS
GTM_PROXY=$PGXC_BASE/src/gtm/proxy/$GTM_PROXY_PROCESS
GTM_TEST_CLIENT=$PGXC_BASE/src/gtm/client/test/$GTM_TEST_CLIENT_PROCESS

GTM_SERVER_LOG_FILE=/tmp/gtmlog
GTM_SERVER_CONTROL_FILE=/tmp/gtmcontrol
GTM_PROXY_LOG_FILE=/tmp/gtmptoxylog


if [ "$#" -ne "5" ];
then
	echo "Usage: test_proxy.sh <test_gtm_proxy> <num_clients> <num_xacts> <num_stmts> <num_worker_threads>"
	exit;
fi

TEST_GTM_PROXY=$1
NUM_CLIENTS=$2
NUM_XACTS=$3
NUM_STMTS=$4
NUM_THREADS=$5


# Stop and kill any gtm server or proxy processes
#
ssh $GTM_SERVER_HOSTNAME "killall -9 $GTM_SERVER_PROCESS"

for index in ${!GTM_PROXY_HOSTNAMES[*]}
do
	ssh ${GTM_PROXY_HOSTNAMES[$index]} "killall -9 $GTM_PROXY_PROCESS" > /dev/null 2>&1
done

echo "Killed stale server and proxies - sleeping for 5 seconds"
sleep 5

# Remove any stale log and control files
#
ssh $GTM_SERVER_HOSTNAME "rm -f $GTM_SERVER_LOG_FILE $GTM_SERVER_CONTROL_FILE"
for index in ${!GTM_PROXY_HOSTNAMES[*]}
do
	ssh ${GTM_PROXY_HOSTNAMES[$index]} "rm -f ${GTM_PROXY_LOG_FILE}_$index"
done

# Create a output directoty to store all test related data
#
OUTPUT_DIR=output
dir=`date "+%F-%H-%M-%S"`
echo "Creating output directory $OUTPUT_DIR/$dir"
mkdir -p $OUTPUT_DIR/$dir


# Start the GTM server
#
echo "Starting GTM server at $GTM_SERVER_HOSTNAME on port $GTM_SERVER_PORT"
ssh $GTM_SERVER_HOSTNAME "$GTM_SERVER -h $GTM_SERVER_HOSTNAME -p $GTM_SERVER_PORT -l $GTM_SERVER_LOG_FILE&"&

echo "Sleeping for 3 seconds"
sleep 3

# Start the GTM proxy on all nodes
#
for index in ${!GTM_PROXY_HOSTNAMES[*]}
do
	echo "Starting GTM proxy at ${GTM_PROXY_HOSTNAMES[$index]} on port ${GTM_PROXY_PORTS[$index]} - $NUM_THREADS worker threads"
	ssh ${GTM_PROXY_HOSTNAMES[$index]} "$GTM_PROXY -h ${GTM_PROXY_HOSTNAMES[$index]} -p ${GTM_PROXY_PORTS[$index]} -s $GTM_SERVER_HOSTNAME -t $GTM_SERVER_PORT -n $NUM_THREADS -l ${GTM_PROXY_LOG_FILE}_$index&"&
done

echo "Sleeping for 3 seconds"
sleep 3

# Kill all clients
#
for index in ${!GTM_PROXY_HOSTNAMES[*]}
do
	ssh ${GTM_PROXY_HOSTNAMES[$index]} "killall -9 $GTM_TEST_CLIENT_PROCESS" > /dev/null 2>&1
done

echo "Killed all stale clients -- sleeping for 5 seconds"
sleep 5

# Remove any stale result files
#
for index in ${!GTM_PROXY_HOSTNAMES[*]}
do
	ssh ${GTM_PROXY_HOSTNAMES[$index]} "rm -f TEST_OUTPUT_$index TEST_OUTPUT_$index.CSV TEST_END_$index"
done

# Write out some information about the test configuration
#
if ( $TEST_GTM_PROXY -eq true );
then
	echo "Testing GTM Proxy Configuration" >> $OUTPUT_DIR/$dir/TEST_SUMMARY
	echo "" >> $OUTPUT_DIR/$dir/TEST_SUMMARY
	echo "Number of GTM Proxy Worker Threads $NUM_THREADS" >> $OUTPUT_DIR/$dir/TEST_SUMMARY
	echo "" >> $OUTPUT_DIR/$dir/TEST_SUMMARY
	echo "" >> $OUTPUT_DIR/$dir/TEST_SUMMARY
else
	echo "Testing GTM Server Configuration" >> $OUTPUT_DIR/$dir/TEST_SUMMARY
	echo "" >> $OUTPUT_DIR/$dir/TEST_SUMMARY
	echo "" >> $OUTPUT_DIR/$dir/TEST_SUMMARY
fi

# Start the stats collection scripts . Kill any stale commands and remove the old files first
#
ssh $GTM_SERVER_HOSTNAME "killall -9 vmstat" > /dev/null 2>&1
ssh $GTM_SERVER_HOSTNAME "rm -f TEST_VMSTATS_GTM" > /dev/null 2>&1
ssh $GTM_SERVER_HOSTNAME "vmstat 1 > TEST_VMSTATS_GTM&"&

for index in ${!GTM_PROXY_HOSTNAMES[*]}
do
	ssh ${GTM_PROXY_HOSTNAMES[$index]} "killall -9 vmstat" > /dev/null 2>&1
	ssh ${GTM_PROXY_HOSTNAMES[$index]} "rm -f TEST_VMSTATS_$index" > /dev/null 2>&1
	ssh ${GTM_PROXY_HOSTNAMES[$index]} "vmstat 1 > TEST_VMSTATS_$index&"&
done

# Start the clients
#
rm -f TEST_END*

echo "Starting clients"
for index in ${!GTM_PROXY_HOSTNAMES[*]}
do
	if ( $TEST_GTM_PROXY -eq true );
	then
		SERVER_HOSTNAME=${GTM_PROXY_HOSTNAMES[$index]};
		SERVER_PORT=${GTM_PROXY_PORTS[$index]};
	else
		SERVER_HOSTNAME=$GTM_SERVER_HOSTNAME;
		SERVER_PORT=$GTM_SERVER_PORT;
	fi
	ssh ${GTM_PROXY_HOSTNAMES[$index]} "$GTM_TEST_CLIENT -h $SERVER_HOSTNAME -p $SERVER_PORT -c $NUM_CLIENTS -n $NUM_XACTS -s $NUM_STMTS -i $index &"&
done

# Wait for all the clients to finish
#
while (true)
do
	all_done=true
	for index in ${!GTM_PROXY_HOSTNAMES[*]}
	do
		scp ${GTM_PROXY_HOSTNAMES[$index]}:TEST_END_$index . > /dev/null 2>&1
		if ! [ -f TEST_END_$index ];
		then
			all_done=false;
		fi;
	done

	if ( $all_done -eq true ); then break; fi
	sleep 5;
done

echo "All clients finished"

# Copy GTM server log files
#
scp $GTM_SERVER_HOSTNAME:$GTM_SERVER_LOG_FILE $OUTPUT_DIR/$dir > /dev/null 2>&1

# Copy GTM server vmstat file
scp $GTM_SERVER_HOSTNAME:TEST_VMSTATS_GTM $OUTPUT_DIR/$dir > /dev/null 2>&1
ssh $GTM_SERVER_HOSTNAME "killall -9 vmstat" > /dev/null 2>&1
ssh $GTM_SERVER_HOSTNAME "rm -f TEST_VMSTATS_GTM" > /dev/null 2>&1

# Copy GTM Proxy log file and the results
for index in ${!GTM_PROXY_HOSTNAMES[*]}
do
	scp ${GTM_PROXY_HOSTNAMES[$index]}:TEST_OUTPUT_$index $OUTPUT_DIR/$dir/ > /dev/null 2>&1
	scp ${GTM_PROXY_HOSTNAMES[$index]}:TEST_OUTPUT_$index.CSV $OUTPUT_DIR/$dir/ > /dev/null 2>&1
	scp ${GTM_PROXY_HOSTNAMES[$index]}:${GTM_PROXY_LOG_FILE}_$index $OUTPUT_DIR/$dir/ > /dev/null 2>&1
	scp ${GTM_PROXY_HOSTNAMES[$index]}:TEST_VMSTATS_$index $OUTPUT_DIR/$dir/ > /dev/null 2>&1
	ssh ${GTM_PROXY_HOSTNAMES[$index]} "killall -9 vmstat" > /dev/null 2>&1
	ssh ${GTM_PROXY_HOSTNAMES[$index]} "rm -f TEST_VMSTATS_$index" > /dev/null 2>&1
done

# Paste the result in the summary file
#
for index in ${!GTM_PROXY_HOSTNAMES[*]}
do
	cat $OUTPUT_DIR/$dir/TEST_OUTPUT_$index >> $OUTPUT_DIR/$dir/TEST_SUMMARY
done

echo "Done"
