#!/bin/sh

# GTM start script for test

pushd /tmp/pgxc/bin; ln -fs gtm gtm_standby; popd

export PATH=/tmp/pgxc/bin:$PATH
export DATA=/tmp/pgxc/data/gtm

# -------------------------------
# starting active...
# -------------------------------
echo "starting active..."

gtm_ctl -D ${DATA} -Z gtm stop
rm -rf ${DATA}/gtm.opts ${DATA}/gtm.pid ${DATA}/register.node

gtm_ctl -D ${DATA} -Z gtm -o "-n 101" start

# -------------------------------
# process check
# -------------------------------
echo "checking process..."
ps -aef |grep gtm
