#!/bin/sh

# GTM start script for test

pushd /tmp/pgxc/bin; ln -fs gtm gtm_standby; popd

export PATH=/tmp/pgxc/bin:$PATH

# -------------------------------
# promoting standby...
# -------------------------------
echo "promoting standby..."
export DATA=/tmp/pgxc/data/gtm_standby

gtm_ctl -D ${DATA} -Z gtm_standby promote

# -------------------------------
# process check
# -------------------------------
echo "checking process..."
ps -aef |grep gtm
