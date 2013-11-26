#!/bin/sh

# GTM start script for test

pushd /tmp/pgxc/bin; ln -fs gtm gtm_standby; popd

./start_a.sh

echo "sleeping 3 seconds..."
sleep 3;

./start_s.sh
