#!/bin/sh

# GTM start script for test

pushd /tmp/pgxc/bin; ln -fs gtm gtm_standby; popd

export PATH=/tmp/pgxc/bin:$PATH

# -------------------------------
# starting standby...
# -------------------------------
echo "stopping standby..."
export DATA=/tmp/pgxc/data/gtm_standby

gtm_ctl -D ${DATA} -Z gtm stop

# -------------------------------
# starting active...
# -------------------------------
echo "stopping active..."
export DATA=/tmp/pgxc/data/gtm

gtm_ctl -D ${DATA} -Z gtm stop

killall -9 gtm gtm_standby

# -------------------------------
# process check
# -------------------------------
echo "checking process..."
ps -aef |grep gtm
