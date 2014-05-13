#!/bin/sh

# GTM start script for test

pushd /tmp/pgxc/bin; ln -fs gtm gtm_standby; popd

export PATH=/tmp/pgxc/bin:$PATH

# -------------------------------
# starting standby...
# -------------------------------
echo "starting standby..."

export DATA=/tmp/pgxc/data/gtm_standby

gtm_ctl -D ${DATA} -Z gtm stop
rm -rf ${DATA}/gtm.opts ${DATA}/gtm.pid ${DATA}/register.node

gtm_ctl -D ${DATA} -Z gtm_standby -o "-n 102 -s -p 6667 -i 127.0.0.1 -q 6666" start

# -------------------------------
# process check
# -------------------------------
echo "checking process..."
ps -aef |grep gtm
