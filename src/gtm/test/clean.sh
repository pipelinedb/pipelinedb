#!/bin/sh

# GTM start script for test

pushd /tmp/pgxc/bin; ln -fs gtm gtm_standby; popd

export PATH=/tmp/pgxc/bin:$PATH

# -------------------------------
# starting standby...
# -------------------------------
echo "cleaning standby..."
export DATA=/tmp/pgxc/data/gtm_standby

pushd $DATA
rm -rf gtm.control gtm.opts gtm.pid register.node
cat /dev/null > gtm.log
popd

# -------------------------------
# starting active...
# -------------------------------
echo "cleaning active..."
export DATA=/tmp/pgxc/data/gtm

pushd $DATA
rm -rf gtm.control gtm.opts gtm.pid register.node
cat /dev/null > gtm.log
popd
