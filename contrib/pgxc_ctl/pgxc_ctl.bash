#!/bin/bash
#
# TODO
# * Backup configuration file (at update, too) to some remote site for pgxc_ctl HA feature too.
# * Write output of *_ctl, intdb and intgtm result to log files
# * Write every operation to the log.  Log file can be specified with --l file or --log file
# * Configure log level
# * Switch log		log file
# * Log option to the configuration file so that this can be failed over.
# * Log to a remote server?
# * Multiple log?
#
# Configuration file.   Configuration file can be specified as -c option of
# the command like, or PGXCCONFIG environment variable.  If both are
# not specified, the following configuration file will be used.
#
# Change in the cluster status due to failover will be added to the configuration file so that
# new master can be invoked as the master when restarted.
#
# All such addition will be tagged with proper comment and date/time info.   If you'd like to
# cancel such changes, you can remove or comment-out such additional lines.
#
#
#==========================================================================================================================
#
# Configuration Section
#
#		This section should be in the file $configFile for
#		user's configuration.
#
# Several assumptons:
# 1) configuration file will be set to data directory.
#    configuration file name is fixed to postgresql.conf
# 2) pg_hba.conf will be set to data directory.  File name is
#    fixed to pg_hba.conf
#
#================================================================
# MEMO
#
# max_connections, min_pool_size, max_pool_size --> should be configurable!
# They're not cluster specific.  So we may give a chance to include
# these specific options to be included from external files.
# They should not change by failover so they just have to be
# configured at first time only.
#===============================================================
#
#---- Configuration File
pgxcInstallDir=$HOME/pgxc
configFile=$pgxcInstallDir/pgxcConf
#---- OVERALL -----------------------------------------------------------------------------------------------------------
#
pgxcOwner=koichi		# owner of the Postgres-XC database cluster.  Here, we use this
						# both as linus user and database user.  This must be
						# the super user of each coordinator and datanode.
pgxcUser=$pgxcOwner		# OS user of Postgres-XC owner


tmpDir=/tmp					# temporary dir used in XC servers
localTmpDir=$tmpDir			# temporary dir used here locally

logOpt=y						# If you want log
logDir=$pgxcInstallDir/pgxc_ctl_log	# Directory to write pgxc_ctl logs

configBackup=y					# If you want config file backup
configBackupHost=pgxc-linker	# host to backup config file
configBackupDir=$pgxcInstallDir
configBackupFile=$configFile	# Backup file name

#---- GTM --------------------------------------------------------------------------------------------------------------

# GTM is mandatory.  You must have at least (and only) one GTM master in your Postgres-XC cluster.
# If GTM crashes and you need to reconfigure it, you can do it by pgxc_update_gtm command to update
# GTM master with others.   Of course, we provide pgxc_remove_gtm command to remove it.  This command
# will not stop the current GTM.  It is up to the operator.

#---- Overall -------
gtmName=gtm

#---- GTM Master -----------------------------------------------

#---- Overall ----
gtmMasterServer=node13
gtmMasterPort=20001
gtmMasterDir=$HOME/pgxc/nodes/gtm

#---- Configuration ---
gtmExtraConfig=none			# Will be added gtm.conf for both Master and Slave (done at initilization only)
gtmMasterSpecificExtraConfig=none	# Will be added to Master's gtm.conf (done at initialization only)

#---- GTM Slave -----------------------------------------------

# Because GTM is a key component to maintain database consistency, you may want to configure GTM slave
# for backup.

#---- Overall ------
gtmSlave=y					# Specify y if you configure GTM Slave.   Otherwise, GTM slave will not be configured and
							# all the following variables will be reset.
gtmSlaveServer=node12		# value none means GTM slave is not available.  Give none if you don't configure GTM Slave.
gtmSlavePort=20001			# Not used if you don't configure GTM slave.
gtmSlaveDir=$HOME/pgxc/nodes/gtm	# Not used if you don't configure GTM slave.
# Please note that when you have GTM failover, then there will be no slave available until you configure the slave
# again. (pgxc_add_gtm_slave function will handle it)

#---- Configuration ----
gtmSlaveSpecificExtraConfig=none # Will be added to Slave's gtm.conf (done at initialization only)

#---- GTM Proxy -------------------------------------------------------------------------------------------------------
# GTM proxy will be selected based upon which server each component runs on.
# When fails over to the slave, the slave inherits its master's gtm proxy.  It should be
# reconfigured based upon the new location.
#
# To do so, slave should be restarted.   So pg_ctl promote -> (edit postgresql.conf and recovery.conf) -> pg_ctl restart
#
# You don't have to configure GTM Proxy if you dont' configure GTM slave or you are happy if every component connects
# to GTM Master directly.  If you configure GTL slave, you must configure GTM proxy too.

#---- Shortcuts ------
gtmProxyDir=$HOME/pgxc/nodes/gtm_pxy

#---- Overall -------
gtmProxy=y				# Specify y if you conifugre at least one GTM proxy.   You may not configure gtm proxies
						# only when you dont' configure GTM slaves.
						# If you specify this value not to y, the following parameters will be set to default empty values.
						# If we find there're no valid Proxy server names (means, every servers are specified
						# as none), then gtmProxy value will be set to "n" and all the entries will be set to
						# empty values.
gtmProxyNames=(gtm_pxy1 gtm_pxy2 gtm_pxy3 gtm_pxy4)	# No used if it is not configured
gtmProxyServers=(node06 node07 node08 node09)			# Specify none if you dont' configure it.
gtmProxyPorts=(20001 20001 20001 20001)				# Not used if it is not configured.
gtmProxyDirs=($gtmProxyDir $gtmProxyDir $gtmProxyDir $gtmProxyDir)	# Not used if it is not configured.

#---- Configuration ----
gtmPxyExtraConfig=none		# Extra configuration parameter for gtm_proxy
gtmPxySpecificExtraConfig=(none none none none)

#---- Coordinators ----------------------------------------------------------------------------------------------------

#---- shortcuts ----------
coordMasterDir=$HOME/pgxc/nodes/coord
coordSlaveDir=$HOME/pgxc/nodes/coord_slave
coordArchLogDir=$HOME/pgxc/nodes/coord_archlog

#---- Overall ------------
coordNames=(coord1 coord2 coord3 coord4)		# Master and slave use the same name
coordPorts=(20004 20005 20004 20005)			# Master and slave use the same port
poolerPorts=(20010 20011 20010 20011)			# Master and slave use the same pooler port
coordPgHbaEntries=(192.168.1.0/24)				# Assumes that all the coordinator (master/slave) accepts
												# the same connection
												# This entry allows only $pgxcOwner to connect.
												# If you'd like to setup another connection, you should
												# supply these entries through files specified below.
# Note: The above parameter is extracted as "host all all 0.0.0.0/0 trust".   If you don't want
# such setups, specify the value () to this variable and suplly what you want using coordExtraPgHba
# and/or coordSpecificExtraPgHba variables.

#---- Master -------------
coordMasterServers=(node06 node07 node08 node09)		# none means this master is not available
coordMasterDirs=($coordMasterDir $coordMasterDir $coordMasterDir $coordMasterDir)
coordMaxWALsernder=5	# max_wal_senders: needed to configure slave. If zero value is specified,
						# it is expected to supply this parameter explicitly by external files
						# specified in the following.	If you don't configure slaves, leave this value to zero.
coordMaxWALSenders=($coordMaxWALsernder $coordMaxWALsernder $coordMaxWALsernder $coordMaxWALsernder)
						# max_wal_senders configuration for each coordinator.

#---- Slave -------------
coordSlave=y			# Specify y if you configure at least one coordiantor slave.  Otherwise, the following
						# configuration parameters will be set to empty values.
						# If no effective server names are found (that is, every servers are specified as none),
						# then coordSlave value will be set to n and all the following values will be set to
						# empty values.
coordSlaveServers=(node07 node08 node09 node06)			# none means this slave is not available
coordSlaveDirs=($coordSlaveDir $coordSlaveDir $coordSlaveDir $coordSlaveDir)
coordArchLogDirs=($coordArchLogDir $coordArchLogDir $coordArchLogDir $coordArchLogDir)

#---- Configuration files---
# Need these when you'd like setup specific non-default configuration 
# These files will go to corresponding files for the master.
# You may supply your bash script to setup extra config lines and extra pg_hba.conf entries 
# Or you may supply these files manually.
coordExtraConfig=none	# Extra configuration file for coordinators.  This file will be added to all the coordinators'
						# postgresql.conf
coordSpecificExraConfig=(none none none none)
coordExtraPgHba=none	# Extra entry for pg_hba.conf.  This file will be added to all the coordinators' pg_hba.conf
coordSpecificExtraPgHba=(none none none none)

#---- Datanodes -------------------------------------------------------------------------------------------------------

#---- Shortcuts --------------
datanodeMasterDir=$HOME/pgxc/nodes/dn_master
datanodeSlaveDir=$HOME/pgxc/nodes/dn_slave
datanodeArchLogDir=$HOME/pgxc/nodes/datanode_archlog

#---- Overall ---------------
#primaryDatanode=datanode1				# Primary Node.
# At present, xc has a priblem to issue ALTER NODE against the primay node.  Until it is fixed, the test will be done
# without this feature.
primaryDatanode=datanode1				# Primary Node.
datanodeNames=(datanode1 datanode2 datanode3 datanode4)
datanodePorts=(20008 20009 20008 20009)	# Master and slave use the same port!
datanodePgHbaEntries=(192.168.1.0/24)	# Assumes that all the coordinator (master/slave) accepts
										# the same connection
										# This list sets up pg_hba.conf for $pgxcOwner user.
										# If you'd like to setup other entries, supply them
										# through extra configuration files specified below.
# Note: The above parameter is extracted as "host all all 0.0.0.0/0 trust".   If you don't want
# such setups, specify the value () to this variable and suplly what you want using datanodeExtraPgHba
# and/or datanodeSpecificExtraPgHba variables.

#---- Master ----------------
datanodeMasterServers=(node06 node07 node08 node09)	# none means this master is not available.
													# This means that there should be the master but is down.
													# The cluster is not operational until the master is
													# recovered and ready to run.	
datanodeMasterDirs=($datanodeMasterDir $datanodeMasterDir $datanodeMasterDir $datanodeMasterDir)
datanodeMaxWalSender=5								# max_wal_senders: needed to configure slave. If zero value is 
													# specified, it is expected this parameter is explicitly supplied
													# by external configuration files.
													# If you don't configure slaves, leave this value zero.
datanodeMaxWalSenders=($datanodeMaxWalSender $datanodeMaxWalSender $datanodeMaxWalSender $datanodeMaxWalSender)
						# max_wal_senders configuration for each datanode

#---- Slave -----------------
datanodeSlave=y			# Specify y if you configure at least one coordiantor slave.  Otherwise, the following
						# configuration parameters will be set to empty values.
						# If no effective server names are found (that is, every servers are specified as none),
						# then datanodeSlave value will be set to n and all the following values will be set to
						# empty values.
datanodeSlaveServers=(node07 node08 node09 node06)	# value none means this slave is not available
datanodeSlaveDirs=($datanodeSlaveDir $datanodeSlaveDir $datanodeSlaveDir $datanodeSlaveDir)
datanodeArchLogDirs=( $datanodeArchLogDir $datanodeArchLogDir $datanodeArchLogDir $datanodeArchLogDir )

# ---- Configuration files ---
# You may supply your bash script to setup extra config lines and extra pg_hba.conf entries here.
# These files will go to corresponding files for the master.
# Or you may supply these files manually.
datanodeExtraConfig=none	# Extra configuration file for datanodes.  This file will be added to all the 
							# datanodes' postgresql.conf
datanodeSpecificExtraConfig=(none none none none)
datanodeExtraPgHba=none		# Extra entry for pg_hba.conf.  This file will be added to all the datanodes' postgresql.conf
datanodeSpecificExtraPgHba=(none none none none)

#
#		End of Configuration Section
#
#==========================================================================================================================

# Common variables ######################################################################
xc_prompt='PGXC$ '
interactive=n
verbose=n
progname=$0

# Create config file template
#
# If you change the structure of configuration file section, you must reflect the change in the part below.
#
function create_config_file_template
{
	cat > $configFile <<EOF
#!/bin/bash
#==========================================================================================================================
#
# Configuration Section
#
#		This section should be in the file $configFile for
#		user's configuration.
#
# Several assumptons:
# 1) configuration file will be set to data directory.
#    configuration file name is fixed to postgresql.conf
# 2) pg_hba.conf will be set to data directory.  File name is
#    fixed to pg_hba.conf
#
#================================================================
# MEMO
#
# max_connections, min_pool_size, max_pool_size --> should be configurable!
# They're not cluster specific.  So we may give a chance to include
# these specific options to be included from external files.
# They should not change by failover so they just have to be
# configured at first time only.
#===============================================================
#
#---- Configuration File
#
# If you're using non-default config file, you should specify config file each time you invoke pgxc_ctl
pgxcInstallDir=$HOME/pgxc
configFile=$pgxcInstallDir/pgxcConf
#---- OVERALL -----------------------------------------------------------------------------------------------------------
#
pgxcOwner=koichi		# owner of the Postgres-XC database cluster.  Here, we use this
						# both as linus user and database user.  This must be
						# the super user of each coordinator and datanode.
pgxcUser=$pgxcOwner		# OS user of Postgres-XC owner


tmpDir=/tmp					# temporary dir used in XC servers
localTmpDir=$tmpDir			# temporary dir used here locally

logOpt=y						# If you want log
logDir=$pgxcInstallDir/pgxc_ctl_log	# Directory to write pgxc_ctl logs

configBackup=y					# If you want config file backup
configBackupHost=pgxc-linker	# host to backup config file
configBackupFile=$configFile	# Backup file name

#---- GTM --------------------------------------------------------------------------------------------------------------

# GTM is mandatory.  You must have at least (and only) one GTM master in your Postgres-XC cluster.
# If GTM crashes and you need to reconfigure it, you can do it by pgxc_update_gtm command to update
# GTM master with others.   Of course, we provide pgxc_remove_gtm command to remove it.  This command
# will not stop the current GTM.  It is up to the operator.

#---- Overall -------
gtmName=gtm

#---- GTM Master -----------------------------------------------

#---- Overall ----
gtmMasterServer=node13
gtmMasterPort=20001
gtmMasterDir=$HOME/pgxc/nodes/gtm

#---- Configuration ---
gtmExtraConfig=none			# Will be added gtm.conf for both Master and Slave (done at initilization only)
gtmMasterSpecificExtraConfig=none	# Will be added to Master's gtm.conf (done at initialization only)

#---- GTM Slave -----------------------------------------------

# Because GTM is a key component to maintain database consistency, you may want to configure GTM slave
# for backup.

#---- Overall ------
gtmSlave=y					# Specify y if you configure GTM Slave.   Otherwise, GTM slave will not be configured and
							# all the following variables will be reset.
gtmSlaveServer=node12		# value none means GTM slave is not available.  Give none if you don't configure GTM Slave.
gtmSlavePort=20001			# Not used if you don't configure GTM slave.
gtmSlaveDir=$HOME/pgxc/nodes/gtm	# Not used if you don't configure GTM slave.
# Please note that when you have GTM failover, then there will be no slave available until you configure the slave
# again. (pgxc_add_gtm_slave function will handle it)

#---- Configuration ----
gtmSlaveSpecificExtraConfig=none # Will be added to Slave's gtm.conf (done at initialization only)

#---- GTM Proxy -------------------------------------------------------------------------------------------------------
# GTM proxy will be selected based upon which server each component runs on.
# When fails over to the slave, the slave inherits its master's gtm proxy.  It should be
# reconfigured based upon the new location.
#
# To do so, slave should be restarted.   So pg_ctl promote -> (edit postgresql.conf and recovery.conf) -> pg_ctl restart
#
# You don't have to configure GTM Proxy if you dont' configure GTM slave or you are happy if every component connects
# to GTM Master directly.  If you configure GTL slave, you must configure GTM proxy too.

#---- Shortcuts ------
gtmProxyDir=$HOME/pgxc/nodes/gtm_pxy

#---- Overall -------
gtmProxy=y				# Specify y if you conifugre at least one GTM proxy.   You may not configure gtm proxies
						# only when you dont' configure GTM slaves.
						# If you specify this value not to y, the following parameters will be set to default empty values.
						# If we find there're no valid Proxy server names (means, every servers are specified
						# as none), then gtmProxy value will be set to "n" and all the entries will be set to
						# empty values.
gtmProxyNames=(gtm_pxy1 gtm_pxy2 gtm_pxy3 gtm_pxy4)	# No used if it is not configured
gtmProxyServers=(node06 node07 node08 node09)			# Specify none if you dont' configure it.
gtmProxyPorts=(20001 20001 20001 20001)				# Not used if it is not configured.
gtmProxyDirs=($gtmProxyDir $gtmProxyDir $gtmProxyDir $gtmProxyDir)	# Not used if it is not configured.

#---- Configuration ----
gtmPxyExtraConfig=none		# Extra configuration parameter for gtm_proxy
gtmPxySpecificExtraConfig=(none none none none)

#---- Coordinators ----------------------------------------------------------------------------------------------------

#---- shortcuts ----------
coordMasterDir=$HOME/pgxc/nodes/coord
coordSlaveDir=$HOME/pgxc/nodes/coord_slave
coordArchLogDir=$HOME/pgxc/nodes/coord_archlog

#---- Overall ------------
coordNames=(coord1 coord2 coord3 coord4)		# Master and slave use the same name
coordPorts=(20004 20005 20004 20005)			# Master and slave use the same port
poolerPorts=(20010 20011 20010 20011)			# Master and slave use the same pooler port
coordPgHbaEntries=(192.168.1.0/24)				# Assumes that all the coordinator (master/slave) accepts
												# the same connection
												# This entry allows only $pgxcOwner to connect.
												# If you'd like to setup another connection, you should
												# supply these entries through files specified below.
# Note: The above parameter is extracted as "host all all 0.0.0.0/0 trust".   If you don't want
# such setups, specify the value () to this variable and suplly what you want using coordExtraPgHba
# and/or coordSpecificExtraPgHba variables.

#---- Master -------------
coordMasterServers=(node06 node07 node08 node09)		# none means this master is not available
coordMasterDirs=($coordMasterDir $coordMasterDir $coordMasterDir $coordMasterDir)
coordMaxWALsernder=5	# max_wal_senders: needed to configure slave. If zero value is specified,
						# it is expected to supply this parameter explicitly by external files
						# specified in the following.	If you don't configure slaves, leave this value to zero.
coordMaxWALSenders=($coordMaxWALsernder $coordMaxWALsernder $coordMaxWALsernder $coordMaxWALsernder)
						# max_wal_senders configuration for each coordinator.

#---- Slave -------------
coordSlave=y			# Specify y if you configure at least one coordiantor slave.  Otherwise, the following
						# configuration parameters will be set to empty values.
						# If no effective server names are found (that is, every servers are specified as none),
						# then coordSlave value will be set to n and all the following values will be set to
						# empty values.
coordSlaveServers=(node07 node08 node09 node06)			# none means this slave is not available
coordSlaveDirs=($coordSlaveDir $coordSlaveDir $coordSlaveDir $coordSlaveDir)
coordArchLogDirs=($coordArchLogDir $coordArchLogDir $coordArchLogDir $coordArchLogDir)

#---- Configuration files---
# Need these when you'd like setup specific non-default configuration 
# These files will go to corresponding files for the master.
# You may supply your bash script to setup extra config lines and extra pg_hba.conf entries 
# Or you may supply these files manually.
coordExtraConfig=none	# Extra configuration file for coordinators.  This file will be added to all the coordinators'
						# postgresql.conf
coordSpecificExraConfig=(none none none none)
coordExtraPgHba=none	# Extra entry for pg_hba.conf.  This file will be added to all the coordinators' pg_hba.conf
coordSpecificExtraPgHba=(none none none none)

#---- Datanodes -------------------------------------------------------------------------------------------------------

#---- Shortcuts --------------
datanodeMasterDir=$HOME/pgxc/nodes/dn_master
datanodeSlaveDir=$HOME/pgxc/nodes/dn_slave
datanodeArchLogDir=$HOME/pgxc/nodes/datanode_archlog

#---- Overall ---------------
#primaryDatanode=datanode1				# Primary Node.
# At present, xc has a priblem to issue ALTER NODE against the primay node.  Until it is fixed, the test will be done
# without this feature.
primaryDatanode=N/A						# Primary Node.
datanodeNames=(datanode1 datanode2 datanode3 datanode4)
datanodePorts=(20008 20009 20008 20009)	# Master and slave use the same port!
datanodePgHbaEntries=(192.168.1.0/24)	# Assumes that all the coordinator (master/slave) accepts
										# the same connection
										# This list sets up pg_hba.conf for $pgxcOwner user.
										# If you'd like to setup other entries, supply them
										# through extra configuration files specified below.
# Note: The above parameter is extracted as "host all all 0.0.0.0/0 trust".   If you don't want
# such setups, specify the value () to this variable and suplly what you want using datanodeExtraPgHba
# and/or datanodeSpecificExtraPgHba variables.

#---- Master ----------------
datanodeMasterServers=(node06 node07 node08 node09)	# none means this master is not available.
													# This means that there should be the master but is down.
													# The cluster is not operational until the master is
													# recovered and ready to run.	
datanodeMasterDirs=($datanodeMasterDir $datanodeMasterDir $datanodeMasterDir $datanodeMasterDir)
datanodeMaxWalSender=5								# max_wal_senders: needed to configure slave. If zero value is 
													# specified, it is expected this parameter is explicitly supplied
													# by external configuration files.
													# If you don't configure slaves, leave this value zero.
datanodeMaxWalSenders=($datanodeMaxWalSender $datanodeMaxWalSender $datanodeMaxWalSender $datanodeMaxWalSender)
						# max_wal_senders configuration for each datanode

#---- Slave -----------------
datanodeSlave=y			# Specify y if you configure at least one coordiantor slave.  Otherwise, the following
						# configuration parameters will be set to empty values.
						# If no effective server names are found (that is, every servers are specified as none),
						# then datanodeSlave value will be set to n and all the following values will be set to
						# empty values.
datanodeSlaveServers=(node07 node08 node09 node06)	# value none means this slave is not available
datanodeSlaveDirs=($datanodeSlaveDir $datanodeSlaveDir $datanodeSlaveDir $datanodeSlaveDir)
datanodeArchLogDirs=( $datanodeArchLogDir $datanodeArchLogDir $datanodeArchLogDir $datanodeArchLogDir )

# ---- Configuration files ---
# You may supply your bash script to setup extra config lines and extra pg_hba.conf entries here.
# These files will go to corresponding files for the master.
# Or you may supply these files manually.
datanodeExtraConfig=none	# Extra configuration file for datanodes.  This file will be added to all the 
							# datanodes' postgresql.conf
datanodeSpecificExtraConfig=(none none none none)
datanodeExtraPgHba=none		# Extra entry for pg_hba.conf.  This file will be added to all the datanodes' postgresql.conf
datanodeSpecificExtraPgHba=(none none none none)

#
#		End of Configuration Section
#
#==========================================================================================================================
EOF

	chmod +x $configFile
}
#============================================================
#
# Common functions
#
#============================================================

logfile=none;
function log_echo
{
	if [ $logOpt == "y" ]; then
		echo $* >> $logfile
	fi
}

function log_echo_with_date
{
	if [ $logOpt == "y" ]; then
		echo `date +%y%m%d,%H:%M:%S` $* >> $logfile
	fi
}

function set_log_file
{
	mkdir -p $logDir
	if [ -d $logDir ]; then
		logfile=$logDir/pgxc_ctl_$datetime.log
	else
		logOpt=n
		eecho Log directory $logDir could not found or is not a directory.
	fi
}

function change_log_file
{
	if [ $logOpt == y ]; then
		logfile=$logDir/$1
	else
		eecho Log disabled.   Cannot change log file.
	fi
}

function vecho
{
	if [ "$verbose" == y ]; then
		echo $*
	fi
	log_echo $*
}

function iecho
{
	if [ "$interactive" == y ]; then
		echo $*
	fi
}

function eecho
{
	echo $*
	log_echo $*
}

function doit
{
	vecho $*
	$*
}

function doall
{
	local i
    vecho doall target: "(" ${allServers[@]} ")"
    for (( i=0; i< ${#allServers[@]}; i++ )); do
		if [ ${allServers[$i]} != none ] && [ ${allServers[$i]} != N/A ]; then
			vecho "... ${allServers[$i]}: $* ..."
			ssh $pgxcUser@${allServers[$i]} $*
		fi
    done
}

function Doall
{
	local i
    vecho Doall target: "(" ${DoallTarget[@]} ")"
    for (( i=0; i< ${#DoallTarget[@]}; i++ )); do
		if [ ${DoallTarget[$i]} != none ] && [ ${DoallTarget[$i]} != N/A ]; then
			vecho "${DoallTarget[$i]}: $* ..."
			ssh $pgxcUser@${DoallTarget[$i]} $*
		fi
    done
}

function cpall
{
	local i
    vecho cpall target: "(" ${allServers[@]} ")"
    for (( i=0; i < ${#allServers[@]}; i++ )); do
		if [ ${allServers[$i]} != none ] && [ ${allServers[$i]} != N/A ]; then
			vecho scp -r $1 $pgxcUser@${allServers[$i]}:$2
			scp -r $1 $pgxcUser@${allServers[$i]}:$2
		fi
    done
}

function Cpall
{
	local i
    vecho Cpall target: "(" ${CpallTarget[@]} ")"
    for (( i=0; i< ${#CpallTarget[@]}; i++ )); do
		if [ ${CpallTarget[$i]} != none ] && [ ${CpallTarget[$i]} != N/A ]; then
			vecho scp -r $1 $pgxcUser@${CpallTarget[$i]}:$2
			scp -r $1 $pgxcUser@${CpallTarget[$i]}:$2
		fi
    done
}


function set_verbose
{
	if [ $# -le 0 ]; then
		echo verbose=$verbose
		return
	fi
	case $1 in
		y )
			verbose=y;;
		n )
			verbose=n;;
		on )
			verbose=y;;
		off )
			verbose=n;;
		* )
			echo Specify y/n/on/off
			return 1;;
	esac
	iecho Verbose set to $verbose
	return 0
}

function set_interactive
{
	if [ $# -le 0 ]; then
		echo interactive=$interactive
		return
	fi
	case $1 in
		y )
			interactive=y;;
		n )
			interactive=n;;
		on )
			interactive=y;;
		off )
			interactive=n;;
		* )
			echo Specify y/n/on/off
			return 1;;
	esac
	iecho Interactive set to $interactive
	return 0
}
function set_prompt
{
	if [ $# -le 0 ]; then
		echo xc_prompt=$xc_prompt
		return
	fi
	xc_prompt=$1
	iecho Prompt set to $xc_prompt
}

function readyesno
{
	if [ $# -ne 1 ];then
		echo n
		return 1
	fi
	local yesno
	read yesno
	case $yesno in
		y )
			echo y;;
		yes )
			echo y;;
		n )
			echo n;;
		no )
			echo n;;
		* )
			echo $1;;
	esac
}

#
# Backups configuration file, especially done when configuraiton file is updated due to
# failover.
#
function pgxc_backup_config_file
{
	if [ "$configBackup" == "y" ]; then
		log_echo Backing up configuration file $configFile to $configBackupHost:$configBackupFile
		doit ssh $pgxcUser@$configBackupHost mkdir -p $configBackupDir
		doit scp $configFile $pgxcUser@$configBackupHost:$configBackupFile
	fi
}
datetime=`date +%y%m%d_%H%M`
immediate="-m fast"			# option for pg_ctl stop.

allServers=()				# List of all the servers which appear in this configuation.

##############################################################################################################
#
#   FUNCTIONS 
#
##############################################################################################################

function create_config_file
{
	# The configuration file is just a copy of the above configuraiton section.   If you modify the above,
	# you should reflect it to the below too.
	cat > $configFile <<EOF
#!/bin/bash
# The context will be supplied finally...
EOF
}

#
# A couple of following functions helps to kill all the processes of specified
# coordinator or datanode.  Target coordinator or datanode are identified by
# the server and the working directory.
#
# They depend upon ps output format.   It depends upon specific operating system
# and may need rewrite.
#
function extract_pid
{
	# Caution: ps format deeply depends upon operating system.   
	# Checked for CentOS (RHEL), Ubuntu 10.4, ubuntu 12.4.
	local uid
	local pid
	local extra
	read uid pid extra
	if [ $? == 0 ]; then
		echo $pid
	else
		echo none
	fi
}

function get_postmaster_pid
{
	# arguments are server and directory
	# Caution: ps format deeply depends upon operating system.   
	# Checked for CentOS (RHEL), Ubuntu 10.4, ubuntu 12.4.
	# This assumes that grep extracts at most one line.
	ssh $pgxcUser@$1 ps -f -C postgres | grep $2 | extract_pid
}

#
# Get next xid of the node.  Used to determine what xid gtm should start with.
#
# $1: host
# $2: dir
# Result will be set to the variable nextXid, which is declared as number
#
declare -i nextXid

function get_nextXid
{
	nextXid=`ssh $pgxcUser@$1 pg_controldata $2 | grep NextXid | sed -e "s%.*/%%"`
}

function kill_all_child_parent
{
	# argument is the target node name and the PID of the parent.
	if [ $# -ne 2 ]; then
		iecho Specify nodename and parent PID
		return 1
	fi
	if [ "$2" == "" ] || [ $2 == none ] || [ $2 == N/A ]; then
		return 1
	fi
	ssh $pgxcUser@$1 kill -9 $2 `ssh $pgxcUser@$1 pgrep -P $2`
}

#----------------------------------------------
# DEBUG Aid
#----------------------------------------------

# Debug --> Should be cleaned

DEBUG=n

function set_debug
{
	if [ $# -le 0 ]; then
		iecho Specify y/n/on/off
		return 1
	fi
	case $1 in
		y )
			DEBUG=y
			;;
		n )
			DEBUG=n;;
		on )
			DEBUG=y;;
		off )
			DEBUG=n;;
		* )
			iecho Specify y/n/on/off
			return 1;;
	esac
	iecho Debug mode set to $DEBUG
	return 0
}

function funcname
{
	if [ "$DEBUG" == y ];	then
		echo '******** ' "$1() called" ' **********'
	fi
}

function decho
{
	if [ "$DEBUG" == y ]; then
		echo $*
	fi
}

function ddo
{
	if [ "$DEBUG" == y ]; then
		$*
	fi
}

# Extract the server list into ${allServers[@]}
# Check if there's no duplicate elements in ${allServers[@]}.   If not, then add the argument
# to ${allServers[@]}
function addServer
{
	local append
	local i

	append=y
	if [ "$1" == 'none' ] || [ "$i" == N/A ]; then
		return
	fi
	for((i=0; i<${#allServers[@]}; i++)); do
		if [ ${allServers[$i]} == "$1" ]; then
			append=n
			break
		fi
	done
	if [ $append == y ]; then
		allServers[$i]=$1
	fi
}

# Build unique server list
#
function makeServerList
{
	local i

	# GTM Master
	if [ $gtmMasterServer != none ]; then
		addServer $gtmMasterServer
	fi
	# GTM Slave
	if [ $gtmSlaveServer != none ]; then
		addServer $gtmSlaveServer
	fi
	# GTM Proxy
	for ((i=0; i<${#gtmProxyServers[@]};i++)); do
		if [ ${gtmProxyServers[$i]} != 'none' -a ${gtmProxyServers[$i]} != "" ]; then
			addServer ${gtmProxyServers[$i]}
		fi
	done
	# Coordinator Master
	for ((i=0; i<${#coordMasterServers[@]}; i++)); do
		if [ ${coordMasterServers[$i]} != none ]; then
			addServer ${coordMasterServers[$i]}
		fi
	done
	# Coordinator Slave
	for ((i=0; i<${#coordSlaveServers[@]}; i++)); do
		if [ ${coordSlaveServers[$i]} != none ]; then
			addServer ${coordSlaveServers[$i]}
		fi
	done
	# Datanode Master
	for ((i=0; i<${#datanodeMasterServers[@]}; i++)); do
		if [ ${datanodeMasterServers[$i]} != none ]; then
			addServer ${datanodeMasterServers[$i]}
		fi
	done
	# Datanode Slave
	for ((i=0; i<${#datanodeSlaveServers[@]}; i++)); do
		if [ ${datanodeSlaveServers[$i]} != none ] ; then
			addServer ${datanodeSlaveServers[$i]}
		fi
	done
	decho '(' ${allServers[@]} ')'
}

#### Handle Slave Configurations ###################################

# Set GTM Proxy info unconfigured.
function gtm_proxy_set_to_no
{
	local i

	gtmProxy=n
	gtmProxyNames=()
	gtmProxyServers=()
	gtmProxyPorts=()
	gtmProxyDirs=()
	gtmPxySpecificExtraConfig=()
	gtmPxyExtraConfig=""
	for ((i=0; i< ${#allServers[@]}; i++)); do
		gtmProxyNames[$i]="none"
		gtmProxyServers[$i]="none"
		gtmProxyPorts[$i]=0
		gtmProxyDirs[$i]="none"
		gtmProxySpecificExtraConfig[$i]=""
	done
}

# Set Coordinator Slave info unconfigured
function coord_slave_set_to_no
{
	local i

	coordSlave=n
	coordSlaveServers=()
	coordSlaveDirs=()
	coordArchLogDirs=()
	for ((i=0; i<${#coordMasterServers[@]}; i++)); do
		coordSlaveServers[$i]=none
		coordSlaveDirs[$i]=none
		coordArchLogDirs[$i]=none
	done
}

# Set Datanode slave info unconfigured
function datanode_slave_set_to_no
{
	local i

	datanodeSlave=n
	datanodeSlaveServers=()
	datanodeSlaveDirs=()
	datanodeSlaveArchLogDirs=()
	for ((i=0; i<${#datanodeMasterServers[@]}; i++)); do
		datanodeSlaveServers[$i]=none
		datanodeSlaveDirs[$i]=none
		datanodeSlaveArchLogDirs[$i]=none
	done
}

# Handle the case where slaves are not configured. --> Construct empty configuration for them
# We assume that all the server list has been constructed.

function handle_no_slaves
{
	local i
	local isEmpty

	# GTM slave
	if [ $gtmSlave != y ] || [ "$gtmSlaveServer" == none ] || [ "$gtmSlaveServer" == N/A ]; then
		gtmSlave=n
		gtmSlaveServer=none
		gtmSlavePort=0
		gtmSlaveDir=none
	fi

	# GTM Proxy
	if [ $gtmProxy != y ]; then
		gtm_proxy_set_to_no
	else
		isEmpty=y
		for ((i=0; i<${#gtmProxyServers[@]}; i++)); do
			if [ ${gtmProxyServers[$i]} != none ] && [ ${gtmProxyServers[$i]} != N/A ]; then
				isEmpty=n
				break
			fi
		done
		if [ "$isEmpty" == y ]; then
			gtm_proxy_set_to_no
			gtmProxy=n
		fi
	fi

	# Coordinator
	if [ $coordSlave != y ]; then
		coord_slave_set_to_no
	else
		isEmpty=y
		for ((i=0; i<${#coordSlaveServers[@]}; i++)); do
			if [ ${coordSlaveServers[$i]} != none ] && [ ${coordSlaveServers[$i]} != N/A ]; then
				isEmpty=n
				break
			fi
		done
		if [ "$isEmpty" == y ]; then
			coord_slave_set_to_no
			coordSlave=n
		fi
	fi

	# Datanode
	if [ $datanodeSlave != y ]; then
		datanode_slave_set_to_no
	else
		isEmpty=y
		for ((i=0; i<${#datanodeSlaveServers[@]}; i++)); do
			if [ ${datanodeSlaveServers[$i]} != none ] && [ ${coordSlaveServers[$I]} != N/A ]; then
				isEmpty=n
				break
			fi
		done
		if [ "$isEmpty" == y ]; then
			datanode_slave_set_to_no
			datanodeSlave=n
		fi
	fi
}



# Check if there're no duplicates in port and working directory assigment
function verifyResource
{
	local i
	local j

	# Number of array entries
	# GTM proxies
	if [ "$gtmProxy" == y ]; then
		i=${#gtmProxyNames[@]}
		if [ $i -ne ${#gtmProxyServers[@]} -o $i -ne ${#gtmProxyPorts[@]} -o $i -ne ${#gtmProxyDirs[@]} -o $i -ne ${#gtmPxySpecificExtraConfig[@]} ]; then
			echo ERROR: Invalid entry numbers in gtm proxy configuration.
			return 1
		fi
	fi
	# Coordinators
	i=${#coordNames[@]}
	if [ $i -ne ${#coordPorts[@]} -o $i -ne ${#poolerPorts[@]} -o $i -ne ${#coordSpecificExraConfig[@]} -o $i -ne ${#coordSpecificExtraPgHba[@]} ]; then
		echo ERROR: Invalid entry numbers in coordinator configuration.
		return 1
	fi
	if [ $i -ne ${#coordMasterServers[@]} -o $i -ne ${#coordMasterDirs[@]} ]; then
		echo ERROR: Invalid entry numbers in coordinator configuration.
		return 1
	fi
	if [ "$coordSlave" == y ]; then
		if [ $i -ne ${#coordSlaveServers[@]} -o $i -ne ${#coordSlaveDirs[@]} -o $i -ne ${#coordArchLogDirs[@]} -o $i -ne ${#coordMaxWALSenders[@]} ]; then
			echo ERROR: Invalid entry numbers in coordinator configuration.
			return 1
		fi
	fi
	# Datanodes
	i=${#datanodeNames[@]}
	if [ $i -ne ${#datanodePorts[@]} -o $i -ne ${#datanodeSpecificExtraConfig[@]} -o $i -ne ${#datanodeSpecificExtraPgHba[@]} ]; then
		echo ERROR: Invalid entry numbers in datanode configuration.
		return 1
	fi
	if [ $i -ne ${#datanodeMasterServers[@]} -o $i -ne ${#datanodeMasterDirs[@]} ]; then
		echo ERROR: Invalid entry numbers in datanode configuration.
		return 1
	fi
	if [ "$datanodeSlave" == y ]; then
		if [ $i -ne ${#datanodeSlaveServers[@]} -o $i -ne ${#datanodeSlaveDirs[@]} -o $i -ne ${#datanodeArchLogDirs[@]} -o $i -ne ${#datanodeMaxWalSenders[@]} ]; then
			echo ERROR: Invalid entry numbers in datanode configuration.
			return 1
		fi
	fi

	# Check if node names don't duplicate
	# GTM
	for ((i=0; i<${#gtmProxyNames[@]};i++)); do
		if [ $gtmName == ${gtmProxyNames[$i]} ]; then
			echo ERROR: GTM name duplicates one of the GTM Proxies, $gtmName
			return 1
		fi
	done
	for ((i=0; i<${#coordNames[@]}; i++)); do
		if [ $gtmName == ${coordNames[$i]} ]; then
			echo ERROR: GTM name duplicates one of the coordinators, $gtmName
			return 1
		fi
	done
	for ((i=0; i<${#datanodeNames[@]}; i++)); do
		if [ $gtmName == ${datanodeNames[$i]} ]; then
			echo ERROR: GTM name duplicates one of the datanodes, $gtmName
			return 1
		fi
	done
	# GTM Proxy
	for ((i=0; i<${#gtmProxyNames[@]}; i++)); do
		for ((j=$i+1;j<${#gtmProxyNames[@]}; j++)); do
			if [ ${gtmProxyNames[$i]} == ${gtmProxyNames[$j]} ]; then
				echo ERROR: GTM proxy name duplicates one of the other GTM proxies, ${gtmProxyNames[$i]}
				return 1
			fi
		done
		for ((j=0;j<${#coordNames[@]};j++));do
			if [ ${coordNames[$j]} == ${gtmProxyNames[$i]} ]; then
				echo ERROR: GTM proxy name duplicates one of the coordinator names, ${gtmProxyNames[$i]}
				return 1
			fi
		done
		for ((j=0;j<${#datanodeNames[@]};j++));do
			if [ ${datanodeNames[$j]} == ${gtmProxyNames[$i]} ]; then
				echo ERROR: GTM proxy name duplicates one of the datanode names, ${gtmProxyNames[$i]}
				return 1
			fi
		done
	done
	# Cordinator
	for ((i=0; i<${#coordNames[@]}; i++)); do
		for ((j=$i+1; j<${#coordNames[@]}; j++)); do
			if [ ${coordNames[$i]} == ${coordNames[$j]} ]; then
				echo ERROR: Coordinator name duplicates on of the other coordinators, ${coordNames[$i]}
				return 1
			fi
		done
		for ((j=0; j<${#datanodeNames[@]}; j++)); do
			if [ ${coordNames[$i]} == ${datanodeNames[$j]} ]
				then
				echo ERROR: Coordinator name duplicates one of the datanodes, ${coordNames[$i]}
				return 1
			fi
		done
	done
	# Datanode
	for ((i=0; i<${#datanodeNames[@]}; i++)); do
		for ((j=$i+1; j<${#datanodeNames[@]}; j++)); do
			if [ ${datanodeNames[$i]} == ${datanodeNames[$j]} ]; then
				echo ERROR: Datanode name duplicates one of the other datanodes, ${datanodeNames[$i]}
				return 1
			fi
		done
	done
	# Check if primary datanode is in datanode name list, or none
	if [ "$primaryDatanode" == "none" ] || [ "$primaryDatanode" == "N/A" ]; then
		return 0
	fi
	# Primary Datanode: is it specified?
	local xx
	xx=n
	for ((i=0;i<${#datanodeNames[@]};i++));do
		if [ "$primaryDatanode" == ${datanodeNames[$i]} ]; then
			xx=y;
			break;
		fi
	done
	if [ $xx == n ]; then
		echo ERROR: Primary datanode is not in the datanode list, $primaryDatanode
		return 1
	fi
}

function verifyNode
{
	local i
	local j
	local Ports
	local Dirs

	Ports=()
	Dirs=()

	decho $1
	# Check if there's no duplicate in port/directory assignment
	if [ $1 == $gtmMasterServer ]; then
		Ports=( ${Ports[@]} $gtmMasterPort )
		Dirs=( ${Dirs[@]} $gtmMasterDir )
	fi
	if [ $1 == $gtmSlaveServer ]; then
		Ports=( ${Ports[@]} $gtmSlavePort )
		Dirs=( ${Dirs[@]} $gtmSlaveDir )
	fi
	for ((i=0; i<${#gtmProxyServers[@]}; i++)); do
		if [ $1 == ${gtmProxyServers[$i]} ]; then
			Ports=( ${Ports[@]} ${gtmProxyPorts[$i]} )
			Dirs=( ${Dirs[@]} ${gtmProxyDirs[$i]} )
		fi
	done
	for ((i=0; i<${#coordMasterServers[@]}; i++)); do
		if [ $1 == ${coordMasterServers[$i]} ]; then
			Ports=( ${Ports[@]} ${coordPorts[$i]} )
			Ports=( ${Ports[@]} ${poolerPorts[$i]} )
			Dirs=( ${Dirs[@]} ${coordMasterDirs[$i]} )
		fi
		if [ $1 == ${coordSlaveServers[$i]} ]; then
			Ports=( ${Ports[@]} ${coordPorts[$i]} )
			Ports=( ${Ports[@]} ${poolerPorts[$i]} )
			Dirs=( ${Dirs[@]} ${coordSlaveDirs[$i]} )
		fi
	done
	for ((i=0; i<${#datanodeMasterServers[$i]}; i++)); do
		if [ $1 == ${datanodeMasterServers[$i]} ]; then
			Ports=( ${Ports[@]} ${datanodePorts[$i]} )
			Dirs=( ${Dirs[@]} ${datanodeMasterDirs[$i]} )
		fi
		if [ $1 == ${datanodeSlaveServers[$i]} ]; then
			Ports=( ${Ports[@]} ${datanodePorts[$i]} )
			Ports=( ${Ports[@]} ${datanodeSlaveDirs[$i]} )
		fi
	done
	for ((i=0; i<${#Ports[@]}; i++)); do
		for ((j=$i+1; j<${#Ports[@]}; j++)); do
			if [ ${Ports[$i]} -eq ${Ports[$j]} -a ${Ports[$i]} != none ]; then
				echo ERROR: duplicate port assignment for the server $1
				return 1
			fi
		done
	done
	for ((i=0; i<${#Dirs[@]}; i++)); do
		for ((j=$i+1; j<${#Dirs[@]}; j++)); do
			if [ ${Dirs[$i]} == ${Dirs[$j]} -a ${Dirs[$i]} != none ]; then
				echo ERROR: duplicate work directory assignment for the server $1
				return 1
			fi
		done
	done
	# We should check if GTM proxy is configured when GTM slave is configured.
	# We could do this here but it's better to do it when we configure
	# postgresql.conf of coordinator/datanode master.
}

function print_config
{
	local ii
	local jj

	echo "========= Postgres-XC configuration ========================"
	echo "=== Overall ==="
	echo Postgres-XC owner: $pgxcOwner
	echo Postgres-XC user: $pgxcUser
	echo Postgres-XC install directory: $pgxcInstallDir
	echo tmpDir: $tmpDir
	echo localTmpDir: $localTmpDir
	echo "=== Each Server ==="
	for ((ii=0;ii<${#allServers[@]};ii++)); do
		echo "=== ${allServers[$ii]} ==="
	    # GTM Master
		if [ ${allServers[$ii]} == $gtmMasterServer ]; then
			echo "GTM Master: "
			echo "    " Nodename: "'"$gtmName"'", port: $gtmMasterPort, dir: "'"$gtmMasterDir"'"
			echo "    " ExtraConfig: "'"$gtmExtraCofig"'", Specific Extra Config: "'"$gtmMasterSpecificExtraConfig"'"
		fi
	    # GTM Slave
		if [ $gtmSlave == y ]; then
			if [ ${allServers[$ii]} == $gtmSlaveServer ]; then
				echo "GTM Slave: "
				echo "    " Nodename: "'"$gtmName"'", port: $gtmSlavePort, dir: "'"$gtmSlaveDir"'"
				echo "    " ExtraConfig: "'"$gtmExtraConfig"'", Specific Extra Config: "'"$gtmSlaveSpecificExtraConfig"'"
			fi
		fi
	    # GTM Proxy
		if [ $gtmProxy == y ]; then
			for ((jj=0;jj<${#gtmProxyServers[@]};jj++)); do
				if [ ${allServers[$ii]} == ${gtmProxyServers[$jj]} ]; then
					echo "GTM Proxy:"
					echo "    " Nodename: "'"${gtmProxyNames[$jj]}"'", port: ${gtmProxyPorts[$jj]}, dir: "'"${gtmProxyDirs[$jj]}"'"
					echo "    " ExtraConfig: "'"$gtmPxyExtraConfig"'", Specific Extra Config: "'"${gtmPxySpecificExtraConfig[$jj]}"'"
				fi
			done
		fi
	    # Coordinator Master
		for ((jj=0;jj<${#coordMasterServers[@]};jj++)); do
			if [ ${allServers[$ii]} == ${coordMasterServers[$jj]} ]; then
				echo "Coordinator Master:"
				echo "    " Nodename: "'"${coordNames[$jj]}"'", port: ${coordPorts[$jj]}, pooler port: "'"${poolerPorts[$jj]}"'"
				echo "    " MaxWalSenders: ${coordMaxWalsenders[$jj]}, Dir: "'"${coordMasterDirs[$jj]}"'"
				echo "    " ExtraConfig: "'"$coordExtraConfig"'", Specific Extra Config: "'"${coordSpecificExtraConfig[$jj]}"'"
				echo "    " pg_hba entries: '(' ${coordPgHbaEntries[@]} ')'
				echo "    " Extra pg_hba: "'"$coordExraPgHba"'", Specific Extra pg_hba: "'"${coordSpecificExtraPgHba[$jj]}"'"
			fi
		done
	    # Coordinator Slave
		if [ $coordSlave == y ]; then
			for ((jj=0;jj<${#coordSlaveServers[@]};jj++)); do
				if [ ${allServers[$ii]} == ${coordSlaveServers[$jj]} ]; then
					echo "Coordinator Slave:"
					echo "    " Nodename: "'"${coordNames[$jj]}"'", port: ${coordPorts[$jj]}, pooler port: ${poolerPorts[$jj]}
					echo "    " Dir: "'"${coordSlaveDirs[$jj]}"'", Archive Log Dir: "'"${coordArchLogDirs[$jj]}"'"
				fi
			done
		fi
	    # Datanode Master
		for ((jj=0;jj<${#datanodeMasterServers[@]};jj++)); do
			if [ ${allServers[$ii]} == ${datanodeMasterServers[$jj]} ]; then
				echo "Datanode Master"
				echo "    " Nodename: "'"${datanodeNames[$jj]}"'", port: ${datanodePorts[$jj]}
				echo "    " MaxWalSenders: ${datanodeMaxWalSenders[$jj]}, Dir: "'"${datanodeMasterDirs[$jj]}
				echo "    " ExtraConfig: "'"datanodeExtraConfig"'", Specific Extra Config: \
					"'"${datanodeSpecificExtraConfig[$jj]}"'"
				echo "    " pg_hba entries: '(' ${datanodePgHbaEntries[@]} ')'
				echo "    " Extra pg_hba: "'"$datanodeExtraPgHba"'", Specific Extra pg_hba: \
					"'"${datanodeSpecificExtraPgHba[$jj]}"'"
			fi
		done
	    # Datanode Slave
		if [ $datanodeSlave == y ]; then
			for ((jj=0;jj<${#datanodeMasterServers[@]};jj++)); do
				if [ ${allServers[$ii]} == ${datanodeSlaveServers[$jj]} ]; then
					echo "Datanode Slave"
					echo "    " Nodename: "'"${datanodeNames[$jj]}"'", port: ${datanodePorts[$jj]}
					echo "    " MaxWalSenders: ${datanodeMaxWalSenders[$jj]}, Dir: "'"${datanodeSlaveDirs[$jj]}
					echo "    " ExtraConfig: "'"datanodeExtraConfig"'", Specific Extra Config: \
						"'"${datanodeSpecificExtraConfig[$jj]}"'"
					echo "    " pg_hba entries: '(' ${datanodePgHbaEntries[@]} ')'
					echo "    " Extra pg_hba: "'"$datanodeExtraPgHba"'", Specific Extra pg_hba: \
						"'"${datanodeSpecificExtraPgHba[$jj]}"'"
				fi
			done
		fi
	done
	echo "=== End of configuration ==="
}

# $1: nodename, $2: [master/slave/all]
function pgxc_ctl_monitor
{
	local fn=monitor
	local i
	if [ $# -le 0 ]; then
		eecho "$progname:$fn" specify node name
		return 2
	fi
	if [ "$1" == "none"  ] || [ "$1" == "N/A" ]; then
		eecho "$progname:$fn" invalid node name
		return 2
	fi
	if [ "$1" == "$gtmName" ]; then
		if [ "$2" == "master" ]; then
			pgxc_monitor -Z gtm -p $gtmMasterPort -h $gtmMasterServer
			if [ $? -eq 0 ]; then
				echo GTM master "("$gtmName")" running
			else
				echo GTM master "("$gtmName")" not running
			fi
			return
		elif [ "$2" == "slave" ]; then
			if [ "$gtmSlaveServer" == "none" ] || [ "$gtmSlaveServer" == "N/A" ]; then
				echo GTM slave not configured.
				return
			fi
			pgxc_monitor -Z gtm -p $gtmSlavePort -h $gtmSlaveServer
			if [ $? -eq 0 ]; then
				echo GTM slave "("$gtmName")" running
			else
				echo GTM slave "("$gtmName")" not running
			fi
			return
		elif [ "$2" == "all" ] || [ "$2" == "" ]; then
			pgxc_monitor -Z gtm -p $gtmMasterPort -h $gtmMasterServer
			if [ $? -eq 0 ]; then
				echo GTM master "("$gtmName")" running
			else
				echo GTM master "("$gtmName")" not running
			fi
			if [ "$gtmSlaveServer" == "none" ] || [ "$gtmSlaveServer" == "N/A" ]; then
				echo GTM slave not configured.
				return
			fi
			pgxc_monitor -Z gtm -p $gtmSlavePort -h $gtmSlaveServer
			if [ $? -eq 0 ]; then
				echo GTM slave "("$gtmName")" running
			else
				echo GTM slave "("$gtmName")" not running
			fi
		fi
		return
	fi
	# GTM-Proxy
	for ((i=0; i<${#gtmProxyNames[@]}; i++)); do
		if [ "$1" == "${gtmProxyNames[$i]}" ]; then
			if [ "${gtmProxyServers[$i]}" == "none" ] || [ "${gtmProxyServers[$i]}" == "N/A" ]; then
				echo GTM proxy "("${gtmProxyNames[$i]}")": not configured
				return;
			fi
			pgxc_monitor -Z gtm -p ${gtmProxyPorts[$i]} -h ${gtmProxyServers[$i]}
			if [ $? -eq 0 ]; then
				echo GTM proxy "("${gtmProxyNames[$i]}")": running
			else
				echo GTM proxy "("${gtmProxyNames[$i]}")": not running
			fi
			return
		fi
	done
	# Coordinator
	for ((i=0; i<${#coordNames[@]}; i++)); do
		if [ "$1" == "${coordNames[$i]}" ]; then
			if [ "$2" == "master" ]; then
				if [ "${coordMasterServers[$i]}" == "none" ] || [ "${coordMasterServers[$i]}" == "N/A" ]; then
					echo Coordinator master "("${coordNames[$i]}")": not configured
					return
				fi
				pgxc_monitor -Z node -p ${coordPorts[$i]} -h ${coordMasterServers[$i]} -U $pgxcOwner -d postgres
				if [ $? == 0 ]; then
					echo Coordinator master "("${coordNames[$i]}")": running
				else
					echo Coordinator master "("${coordNames[$i]}")": not running
				fi
				return
			fi
			if [ "$2" == "slave" ]; then
				if [ "${coordSlaveServers[$i]}" == "none" ] || [ "${coordSlaveServers[$i]}" == "N/A" ]; then
					echo Coordinator slave "("${coordNames[$i]}")": not configured
					return
				fi
				pgxc_monitor -Z node -p ${coordPorts[$i]} -h ${coordSlaveServers[$i]} -U $pgxcOwner -d postgres
				if [ $? == 0 ]; then
					echo Coordinator slave "("${coordNames[$i]}")": running
				else
					echo Coordinator slave "("${coordNames[$i]}")": not running
				fi
				return
			fi
			if [ "$2" == "all" ] || [ "$2" == "" ]; then
				if [ "${coordMasterServers[$i]}" == "none" ] || [ "${coordMasterServers[$i]}" == "N/A" ]; then
					echo Coordinator master "("${coordNames[$i]}")": not configured
				else
					pgxc_monitor -Z node -p ${coordPorts[$i]} -h ${coordMasterServers[$i]} -U $pgxcOwner -d postgres
					if [ $? == 0 ]; then
						echo Coordinator master "("${coordNames[$i]}")": running
					else
						echo Coordinator master "("${coordNames[$i]}")": not running
					fi
				fi
				if [ "${coordSlaveServers[$i]}" == "none" ] || [ "${coordSlaveServers[$i]}" == "N/A" ]; then
					echo Coordinator slave "("${coordNames[$i]}")": not configured
				else
					pgxc_monitor -Z node -p ${coordPorts[$i]} -h ${coordSlaveServers[$i]} -U $pgxcOwner -d postgres
					if [ $? == 0 ]; then
						echo Coordinator slave "("${coordNames[$i]}")": running
					else
						echo Coordinator slave "("${coordNames[$i]}")": not running
					fi
				fi
				return
			fi
		fi
	done
	# Datanode
	for ((i=0; i<${#datanodeNames[@]}; i++)); do
		if [ "$1" == "${datanodeNames[$i]}" ]; then
			if [ "$2" == "master" ]; then
				if [ "${datanodeMasterServers[$i]}" == "none" ] || [ "${datanodeMasterServers[$i]}" == "N/A" ]; then
					echo Datanode master "("${datanodeNames[$i]}")": not configured
					return
				fi
				pgxc_monitor -Z node -p ${datanodePorts[$i]} -h ${datanodeMasterServers[$i]} -U $pgxcOwner -d postgres
				if [ $? == 0 ]; then
					echo Datanode master "("${datanodeNames[$i]}")": running
				else
					echo Datanode master "("${datanodeNames[$i]}")": not running
				fi
				return
			fi
			if [ "$2" == "slave" ]; then
				if [ "${datanodeSlaveServers[$i]}" == "none" ] || [ "${datanodeSlaveServers[$i]}" == "N/A" ]; then
					echo Datanode slave "("${datanodeNames[$i]}")": not configured
					return
				fi
				pgxc_monitor -Z node -p ${datanodePorts[$i]} -h ${datanodeSlaveServers[$i]} -U $pgxcOwner -d postgres
				if [ $? == 0 ]; then
					echo Datanode slave "("${datanodeNames[$i]}")": running
				else
					echo Datanode slave "("${datanodeNames[$i]}")": not running
				fi
				return
			fi
			if [ "$2" == "all" ] || [ "$2" == "" ]; then
				if [ "${datanodeMasterServers[$i]}" == "none" ] || [ "${datanodeMasterServers[$i]}" == "N/A" ]; then
					echo Datanode master "("${datanodeNames[$i]}")": not configured
				else
					pgxc_monitor -Z node -p ${datanodePorts[$i]} -h ${datanodeMasterServers[$i]} -U $pgxcOwner -d postgres
					if [ $? == 0 ]; then
						echo Datanode master "("${datanodeNames[$i]}")": running
					else
						echo Datanode master "("${datanodeNames[$i]}")": not running
					fi
				fi
				if [ "${datanodeSlaveServers[$i]}" == "none" ] || [ "${datanodeSlaveServers[$i]}" == "N/A" ]; then
					echo Datanode slave "("${datanodeNames[$i]}")": not configured
				else
					pgxc_monitor -Z node -p ${datanodePorts[$i]} -h ${datanodeSlaveServers[$i]} -U $pgxcOwner -d postgres
					if [ $? == 0 ]; then
						echo Datanode slave "("${coordNames[$i]}")": running
					else
						echo Datanode slave "("${coordNames[$i]}")": not running
					fi
				fi
				return
			fi
		fi
	done

}
#==============================================================
# Check each component status (running or not)
# You must install/deploy pgxc_monitor
#==============================================================

function pgxc_ctl_show_component
{
	local i
	# GTM
	if [ "$1" == "$gtmName" ]; then
		echo "$1:" GTM
		echo "     " GTM Master, "port=$gtmMasterPort", "host=$gtmMasterServer", "dir=$gtmMasterDir"
		if [ "$gtmSlaveServer" == "none" ] || [ "$gtmSlaveServer" == "N/A" ]; then
			echo "     " GTM Slave, not configured
		else
			echo "     " GTM Slave, "port=$gtmSlavePort", "host=$gtmSlaveServer", "dir=$gtmSlaveDir"
		fi
		return
	fi
	# GTM Proxy
	for ((i=0; i<${#gtmProxyNames[@]}; i++)); do
		if [ "${gtmProxyNames[$i]}" == "$1" ]; then
			echo "$1:"  GTM Proxy, "port=${gtmProxyPorts[$i]}", "host=${gtmProxyServers[$i]}", "dir=${gtmProxyDirs[$i]}"
			return
		fi
	done
	# Coordinator
	for ((i=0; i<${#coordNames[@]}; i++)); do
		if [ "${coordNames[$i]}" == "$1" ]; then
			echo "$1:" Coordinator
			echo "     " Coordinator Master, port=${coordPorts[$i]}, host=${coordMasterServers[$i]}, dir=${coordMasterDirs[$i]}
			if [ "${coordSlaveServers[$i]}" == "none" ] || [ "${coordSlaveServers[$i]}" == "N/A" ]; then
				echo "     " Coordinator Slave, not configured
			else
				echo "     " Coordinator Slave, port=${coordPorts[$i]}, host=${coordSlaveServers[$i]}, dir=${coordSlaveDirs[$i]}
			fi
			return
		fi
	done
	# Datanode
	for ((i=0; i<${#datanodeNames[@]}; i++)); do
		if [ "${datanodeNames[$i]}" == "$1" ]; then
			echo "$1:" Datanode
			echo "     " Datanode Master, port=${datanodePorts[$i]}, host=${datanodeMasterServers[$i]}, dir=${datanodeMasterDirs[$i]}
			if [ "${datanodeSlaveServers[$i]}" == "none" ] || [ "${datanodeSlaveServers[$i]}" == "N/A" ]; then
				echo "     " Datanode Slave, not configured
			else
				echo "     " Datanode Slave, port=${datanodePorts[$i]}, host=${datanodeSlaveServers[$i]}, dir=${datanodeSlaveDirs[$i]}
			fi
			return
		fi
	done
	echo Component $1 not found.
	return 1
}

function pgxc_ctl_show_gtm_proxy_all
{
	local i
	echo ----------
	if [ "$gtmProxy" != "y" ]; then
		eecho GTM Proxy is not configured
		return 2
	fi
	echo GTM Proxies:
	for ((i=0; i<${#gtmProxyNames[@]}; i++)); do
		pgxc_ctl_show_component ${gtmProxyNames[$i]}
	done
}

function pgxc_ctl_show_coordinator_all
{
	local i
	echo ----------
	echo Coordinators:
	for ((i=0; i<${#coordNames[@]}; i++)); do
		pgxc_ctl_show_component ${coordNames[$i]}
	done
}

function pgxc_ctl_show_datanode_all
{
	local i
	echo ----------
	echo Datanodes:
	for ((i=0; i<${#datanodeNames[@]}; i++)); do
		pgxc_ctl_show_component ${datanodeNames[$i]}
	done
}

function monitor_components
{
	local i
	local j

	# GTM master
	pgxc_monitor -Z gtm -p $gtmMasterPort -h $gtmMasterServer
	if [ $? -eq 0 ]; then
		echo -n GTM master "("$gtmName")": running. "   "
	else
		echo -n GTM master "("$gtmName")": not running. "   "
	fi
	echo host: $gtmMasterServer, port: $gtmMasterPort, dir: $gtmMasterDir

	# GTM slave
	if [ $gtmSlave == y ]; then
		if [ $gtmSlaveServer == none ] || [ $gtmSlaveServer == N/A ]; then
			echo GTM slave "("$gtmName")": not configured.
		else
			pgxc_monitor -Z gtm -p $gtmSlavePort -h $gtmSlaveServer
			if [ $? -eq 0 ]; then
				echo -n GTM slave "("$gtmName")": running. "   "
			else
				echo -n GTM slave "("$gtmName")": not running. "   "
			fi
			echo host: $gtmSlaveServer, port: $gtmSlavePort, dir: $gtmSlaveDir
		fi
	fi

	# GTM proxies
	if [ $gtmProxy == y ]; then
		for ((i=0; i<${#gtmProxyNames[@]}; i++)); do
			if [ ${gtmProxyServers[$i]} != none ] && [ ${gtmProxyServers[$i]} != N/A ]; then
				pgxc_monitor -Z gtm -p ${gtmProxyPorts[$i]} -h ${gtmProxyServers[$i]}
				if [ $? -eq 0 ]; then
					echo -n GTM proxy "("${gtmProxyNames[$i]}")": running. "   "
				else
					echo -n GTM proxy "("${gtmProxyNames[$i]}")": not running. "   "
				fi
				echo host: ${gtmProxyServers[$i]}, port: ${gtmProxyPorts[$i]}, dir: ${gtmProxyDirs[$i]}
			fi
		done
	fi

	# Coordinator masters
	local postmaster_pid
	for ((i=0; i<${#coordNames[@]};i++));do
		if [ ${coordMasterServers[$i]} != none ] && [ ${coordMasterServers[$i]} != N/A ]; then
			pgxc_monitor -Z node -p ${coordPorts[$i]} -h ${coordMasterServers[$i]} -U $pgxcOwner -d postgres
			if [ $? == 0 ]; then
				echo -n Coordinator master "("${coordNames[$i]}")": running. "   "
			else
				echo -n Coordinator master "("${coordNames[$i]}")": not running. "   "
			fi
			echo host: ${coordMasterServers[$i]}, port: ${coordPorts[$i]}, dir: ${coordMasterDirs[$i]}
		fi
	done

	# Coordinator slaves
	if [ $coordSlave == y ]; then
		for ((i=0; i<${#coordNames[@]};i++)); do
			if [ ${coordSlaveServers[$i]} == none ] || [ ${coordSlaveServers[$i]} == N/A ]; then
				if [ ${coordNames[$i]} != none ] && [ $coordNames[$i] != N/A ]; then
					echo Coordinator slave "("${coordNames[$i]}")": not configured
				fi
			else
				pgxc_monitor -Z node -p ${coordPorts[$i]} -h ${coordSlaveServers[$i]} -U $pgxcOwner -d postgres
				if [ $? == 0 ]; then
					echo -n Coordinator slave "("${coordNames[$i]}")": running. "   "
				else
					echo -n Coordinator slave "("${coordNames[$i]}")": not running. "   "
				fi
				echo host: ${coordSlaveServers[$i]}, port: ${coordPorts[$i]}, dir: ${coordSlaveDirs[$i]}
			fi
		done
	fi

	# Datanode masters
	for ((i=0; i<${#datanodeNames[@]}; i++));do
		if [ ${datanodeMasterServers[$i]} != none ] && [ ${datanodeMasterServers[$i]} != N/A ]; then
			pgxc_monitor -Z node -p ${datanodePorts[$i]} -h ${datanodeMasterServers[$i]} -U $pgxcOwner -d postgres
			if [ $? == 0 ]; then
				echo -n Datanode master "("${datanodeNames[$i]}")": running. "   "
			else
				echo -n Datanode master "("${datanodeNames[$i]}")": not running. "   "
			fi
			echo host: ${datanodeMasterServers[$i]}, port: ${datanodePorts[$i]}, dir: ${datanodeMasterDirs[$i]}
		fi
	done

	# Datanode slaves
	if [ $datanodeSlave == y ]; then
		for ((i=0; i<${#datanodeNames[@]}; i++)); do
			if [ ${datanodeSlaveServers[$i]} == none ] || [ ${datanodeSlaveServers[$i]} == N/A ]; then
				if [ ${datanodeNames[$i]} != none ] && [ ${datanodeNames[$i]} != N/A ]; then
					echo Datanode slave "("${datanodeNames[$i]}")": not configured
				fi
			else
				pgxc_monitor -Z node -p ${datanodePorts[$i]} -h ${datanodeSlaveServers[$i]} -U $pgxcOwner -d postgres
				if [ $? == 0 ]; then
					echo -n Datanode slave "("${datanodeNames[$i]}")": running. "   "
				else
					echo -n Datanode slave "("${datanodeNames[$i]}")": not running. "   "
				fi
				echo host: ${datanodeSlaveServers[$i]}, port: ${datanodePorts[$i]}, dir: ${datanodeSlaveDirs[$i]}
			fi
		done
	fi
}


#===============================================================                                             
# Tool function to check -m option to stop coordinator and                                                   
# datanode                                                                                                   
#===============================================================                                             
function check_immediate
{
    case $1 in
		immediate )
            immediate="-m immediate" ;;
        fast )
            immediate="-m fast" ;;
        normal )
            immediate="" ;;
        * )
            echo "ERROR: Please specify immediate, fast or normal"
            exit 1;;
    esac;
}

#==================================================                                                          
#                                                                                                            
# Setup .bashrc file for PATH and LD_LIBRARY_PATH                                                            
#                                                                                                            
#==================================================                                                          

function setup_bashrc
{
    vecho ================================================================
    vecho Setting .bashrc files
    for ((i=0; i< ${#allServers[@]}; i++)); do
		vecho ---- ${allServers[$i]} -------------------------
		doit ssh $pgxcUser@${allServers[$i]} cp .bashrc .bashrc.org
		ssh $pgxcUser@${allServers[$i]} "cat >> .bashrc" <<EOF
# .bachrc addition for Postgres-XC PATH and LD_LIBRARY_PATH
# $datetime
export PATH_ORG=\$PATH
export PATH=$pgxcInstallDir/bin:\$PATH
export LD_LIBRARY_PATH_ORG=\$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=$pgxcInstallDir/lib:\$LD_LIBRARY_PATH
export MANPATH_ORG=\$MANPATH
export MANPATH=$pgxcInstallDir/share/man:\$MANPATH
# End of addition
EOF
	  done
}

function setup_bashrc_individual
{
    vecho ================================================================
    vecho Setting .bashrc files for $pgxcOwner at $1
	doit ssh $pgxcUser@$1 cp .bashrc .bashrc.org
    ssh $pgxcUser@$1 "cat >> .bashrc" <<EOF
# .bachrc addition for Postgres-XC PATH and LD_LIBRARY_PATH
# $datetime
export PATH_ORG=\$PATH
export PATH=$pgxcInstallDir/bin:\$PATH
export LD_LIBRARY_PATH_ORG=\$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=$pgxcInstallDir/lib:\$LD_LIBRARY_PATH
export MANPATH_ORG=\$MANPATH
export MANPATH=$pgxcInstallDir/share/man:\$MANPATH
# End of addition
EOF
}

#==================================================                                                          
#                                                                                                            
# Deploy binaries and other common things to each server                                                     
#                                                                                                            
# All the build materials will be deployed to each                                                           
# servers.                                                                                                   
#                                                                                                            
#=================================================                                                           

function pgxc_deploy_all
{
    vecho ================================================================
    vecho pgxc_deploy_all: copy built materials to all the target servers.

    doall rm -rf $pgxcInstallDir/bin $pgxcInstallDir/include $pgxcInstallDir/lib $pgxcInstallDir/share
    doall mkdir -p $pgxcInstallDir
    vecho tar czCf $pgxcInstallDir $tmpDir/wk.tgz bin include lib share
    tar czCf $pgxcInstallDir $tmpDir/wk.tgz bin include lib share
    cpall $tmpDir/wk.tgz $pgxcInstallDir/wk.tgz
    doall tar xzCf  $pgxcInstallDir $pgxcInstallDir/wk.tgz
    doall rm $pgxcInstallDir/wk.tgz
    doit rm $tmpDir/wk.tgz
}

# First argument is the target node.                                                                         

function pgxc_deploy_individual
{
    vecho ================================================================
    vecho pgxc_deploy_individual: copy built materials to the server $1

    doit ssh $pgxcUser@$1 rm -rf $pgxcInstallDir/bin $pgxcInstallDir/include $pgxcInstallDir/lib $pgxcInstallDir/share
    doit ssh $pgxcUser@$1 mkdir -p $pgxcInstallDir
    doit tar czCf $pgxcInstallDir $tmpDir/wk.tgz bin include lib share
    doit scp $tmpDir/wk.tgz $pgxcUser@$1:$pgxcInstallDir/wk.tgz
    doit ssh $pgxcUser@$1 tar xzCf $pgxcInstallDir $pgxcInstallDir/wk.tgz
    doit ssh $pgxcUser@$1 rm $pgxcInstallDir/wk.tgz
    doit rm $tmpDir/wk.tgz
}

#==================================================
#
# Cleanup work directories
#
#==================================================

# First argument is the server name. second argument is the directory name
# Server name could be none, where the target does not exist.
function pgxc_clean_dir
{
	if [ $# -ne 2 ]; then
		return 2
	fi
	if [ $1 == none ]; then
		return 0
	fi
	doit ssh $pgxcUser@$1 rm -rf $2
	doit ssh $pgxcUser@$1 mkdir -p $2
	doit ssh $pgxcUser@$1 chmod 0700 $2
}

function pgxc_clean_socket
{
	doit ssh $pgxcUser@$1 rm -f /tmp/.s.'*'$2'*'
}

# First argument is the nodename.  The second is "master", "slave" or "all".
function pgxc_clean_node
{
	local i

	log_echo pgxc_clean_node'('$*')'
	if [ $1 == $gtmName ]; then
		shift;
		case $1 in
			master )
				if [ "$gtmMasterServer" != "none" ] && [ "$gtmMasterServer" != "N/A" ]; then
					doit pgxc_clean_dir $gtmMasterServer $gtmMasterDir
					doit pgxc_clean_socket $gtmMasterServer $gtmMasterPort
				else
					eecho GTM $gtmName is not configured.
				fi
				return;;
			slave )
				if [ "$gtmSlaveServer" != "none" ] && [ "$gtmSlaveServer" != "N/A" ]; then
					doit pgxc_clean_dir $gtmSlaveServer $gtmSlaveDir
					doit pgxc_clean_socket $gtmSlaveServer $gtmSlavePort
				else
					eecho GTM slave $gtmName is not configured.
				fi
				return;;
			all )
				if [ "$gtmMasterServer" != "none" ] && [ "$gtmMasterServer" != "N/A" ]; then
					doit pgxc_clean_dir $gtmMasterServer $gtmMasterDir
					doit pgxc_clean_socket $gtmMasterServer $gtmMasterPort
				else
					eecho GTM $gtmName is not configured.
				fi
				if [ "$gtmSlaveServer" != "none" ] && [ "$gtmSlaveServer" != "N/A" ]; then
					doit pgxc_clean_dir $gtmSlaveServer $gtmSlaveDir
					doit pgxc_clean_socket $gtmSlaveServer $gtmSlavePort
				else
					eecho GTM slave $gtmName is not configured.
				fi
				return;;
			* )
				echo ERROR: invalid argument for pgxc_clean_node, $1
				return 1;;
		esac
	fi
	for ((i= 0; i< ${#gtmProxyNames[@]}; i++)); do
		if [ $1 == ${gtmProxyNames[$i]} ]; then
			if [ "${gtmProxyServers[$i]}" != "none" ] && [ "${gtmProxyServers[$i]}" != "N/A" ]; then
				doit pgxc_clean_dir ${gtmProxyServers[$i]} ${gtmProxyDirs[$i]}
				doit pgxc_clean_socket ${gtmProxyServers[$i]} ${gtmProxyPorts[$i]}
			else
				eecho GTM Proxy $1 is not configured.
			fi
			return;
		fi
	done
	for ((i= 0; i< ${#coordNames[@]}; i++)); do
		if [ $1 == ${coordNames[$i]} ]; then
			case $2 in
				master )
					if [ "${coordMasterServers[$i]}" != "none" ] && [ "${coordMasterServers[$i]}" != "N/A" ]; then
						doit pgxc_clean_dir ${coordMasterServers[$i]} ${coordMasterDirs[$i]}
						doit pgxc_clean_socket ${coordMasterServers[$i]} ${coordPorts[$i]}
					else
						eecho Coordinator master $1 is not configured.
					fi
					return;;
				slave )
					if [ "${coordSlaveServers[$i]}" != "none" ] && [ "${coordSlaveServers[$i]}" != "N/A" ]; then
						doit pgxc_clean_dir ${coordSlaveServers[$i]} ${coordSlaveDirs[$i]}
						doit pgxc_clean_socket ${coordSlaveServers[$i]} ${coordPorts[$i]}
					else
						eecho Coordinator slave $1 is not configured.
					fi
					return;;
				all )
					if [ "${coordMasterServers[$i]}" != "none" ] && [ "${coordMasterServers[$i]}" != "N/A" ]; then
						doit pgxc_clean_dir ${coordMasterServers[$i]} ${coordMasterDirs[$i]}
						doit pgxc_clean_socket ${coordMasterServers[$i]} ${coordPorts[$i]}
					else
						eecho Coordinator master $1 is not configured.
					fi
					if [ "${coordSlaveServers[$i]}" != "none" ] && [ "${coordSlaveServers[$i]}" != "N/A" ]; then
						doit pgxc_clean_dir ${coordSlaveServers[$i]} ${coordSlaveDirs[$i]}
						doit pgxc_clean_socket ${coordSlaveServers[$i]} ${coordPorts[$i]}
					else
						eecho Coordinator slave $1 is not configured.
					fi
					return;;
				* )
					echo ERROR: invalid argument for pgxc_clean_node, $1
					return 1;;
			esac
		fi
	done
	for ((i= 0; i< ${#datanodeNames[@]}; i++)); do
		if [ $1 == ${datanodeNames[$i]} ]; then
			case $2 in
				master )
					if [ "${datanodeMasterServers[$i]}" != "none" ] && [ "${datanodeMasterServers[$i]}" != "N/A" ]; then
						doit pgxc_clean_dir ${datanodeMasterServers[$i]} ${datanodeMasterDirs[$i]}
						doit pgxc_clean_socket ${datanodeMasterServers[$i]} ${datanodePorts[$i]}
					else
						eecho Datanode master $1 is not configured.
					fi
					return;;
				slave )
					if [ "${datanodeSlaveServers[$i]}" != "none" ] && [ "${datanodeSlaveServers[$i]}" != "N/A" ]; then
						doit pgxc_clean_dir ${datanodeSlaveServers[$i]} ${datanodeSlaveDirs[$i]}
						doit pgxc_clean_socket ${datanodeSlaveServers[$i]} ${datanodePorts[$i]}
					else
						eecho Datanode slave $1 is not configured.
					fi
					return;;
				all )
					if [ "${datanodeMasterServers[$i]}" != "none" ] && [ "${datanodeMasterServers[$i]}" != "N/A" ]; then
						doit pgxc_clean_dir ${datanodeMasterServers[$i]} ${datanodeMasterDirs[$i]}
						doit pgxc_clean_socket ${datanodeMasterServers[$i]} ${datanodePorts[$i]}
					else
						eecho Datanode master $1 is not configured.
					fi
					if [ "${datanodeSlaveServers[$i]}" != "none" ] && [ "${datanodeSlaveServers[$i]}" != "N/A" ]; then
						doit pgxc_clean_dir ${datanodeSlaveServers[$i]} ${datanodeSlaveDirs[$i]}
						doit pgxc_clean_socket ${datanodeSlaveServers[$i]} ${datanodePorts[$i]}
					else
						eecho Datanode slave $1 is not configured.
					fi
					return;;
				* )
					echo ERROR: invalid argument for pgxc_clean_node, $1
					return 1;;
			esac
		fi
	done
	echo ERROR: no target nodename found, $1
}

# First argument is "master", "slave" or "all"
function pgxc_clean_gtm
{
	log_ehco pgxc_clen_gtm'('$*')'
	if [ $# -ne 1 ];then
		echo Specify master, slave or all
		return 1
	fi
	case $1 in
		master );;
		slave );;
		all );;
		* )
			vecho Specify master, slave or all
			return 1;;
	esac
	if [ $1 == master ] || [ $1 == all ]; then
		pxc_clean_dir $gtmMasterServer $gtmMasterDir
	fi
	if [ $gtm_slave != y ]; then
		if [ $1 == slave ] || [ $1 == all ]; then
			pgxc_clean_dir $gtmSlaveServer $gtmSlaveDir
		fi
	fi
}

# First argument is gtm_proxy name
function pgxc_clean_gtm_proxy
{
	log_echo pgxc_clean_gtm_proxy'('$*')'
	if [ $gtmProxy != y ]; then
		echo gtm_proxy is not configured
		return 1
	fi
	if [ $# -ne 1 ]; then
		echo Specify gtm_proxy name
		return 2
	fi
	local i
	for ((i=0; i<${#gtmProxyNames[@]}; i++));do
		if [ $1 == ${gtmProxyNames[$i]} ]; then
			pgxc_clean_dir ${gtmProxyServers[$i]} ${gtmProxyDirs[$i]}
			return
		fi
	done
	echo specified gtm_proxy is not configured, $1
	return 2
}

# No argument
function pgxc_clean_gtm_proxy_all
{
	log_echo pgxc_clean_gtm_proxy_all'('$*')'
	if [ $gtmProxy != y ]; then
		echo gtm_proxy is not configured
		return 1
	fi
	local i
	for ((i=0; i<${#gtmProxyNames[@]}; i++));do
		pgxc_clean_gtm_proxy ${gtmProxyNames[$i]}
	done
}

# First argument is coordinator name
function pgxc_clean_coordinator_master
{
	log_echo pgxc_clean_coordinator_master'('$*')'
	if [ $# -ne 1 ]; then
		echo specify coordinator name
		return 2
	fi
	local i
	for ((i=0; i<${#coordNames[@]}; i++));do
		if [ $1 == ${coordNames[$i]} ]; then
			pgxc_clean_dir ${coordMasterServers[$i]} ${coordMasterDirs[$i]}
			return
		fi
	done
	echo specified coordinator is not configured, $1
	return 2
}

function pgxc_clean_coordinator_master_all
{
	local i
	log_echo pgxc_clean_coordinator_master_all'('$*')'
	for ((i=0;i<${#coordNames[@]};i++));do
		pgxc_clean_coordinator_master ${coordNames[$i]}
	done
}

# First argument is a coordinator name
function pgxc_clean_coordinator_slave
{
	log_echo pgxc_clean_coordinator_slave'('$*')'
	if [ $coordSlave != y ]; then
		echo Coordinator slave is not configured.
		return 1
	fi
	if [ $# -ne 1 ]; then
		echo Specify coordinator name.
		return 2
	fi
	local i
	for ((i=0;i<${#coordNames[@]};i++));do
		if [ $1 == ${coordNames[$i]} ]; then
			if [ ${coordSlaveServers[$i]} == none ] || [ ${coordSlaveServers[$i]} == N/A ]; then
				echo Specified coordinator slave is not configured, $1
				return 2
			fi
			pgxc_clean_dir ${coordSlaveServers[$i]} ${coordSlaveDirs[$i]}
			return
		fi
	done
	echo Specified coordinator is not configured, $1
	return 2
}

function pgxc_clean_coordinator_slave_all
{
	log_echo pgxc_clean_coordinator_slave_all'('$*')'
	if [ $coordSlave != y ]; then
		echo Coordinator slave is not configured.
		return 1
	fi
	local i
	for ((i=0; i<${#coordNames[@]}; i++));do
		pgxc_clean_coordinator_slave ${coordNames[$i]}
	done
}

function pgxc_clean_coordinator_all
{
	log_echo pgxc_clean_coordinator_all'('$*')'
	pgxc_clean_coordinator_master_all
	pgxc_clean_coordinator_slave_all
}

function pgxc_clean_datanode_master
{
	log_echo pgxc_clean_datanode_master'('$*')'
	if [ $# -ne 1 ]; then
		echo Specify datanode name
		return 2
	fi
	local i
	for ((i=0; i<${datanodeNames[@]}; i++));do
		if [ $1 == ${datanodeNames[$i]} ]; then
			pgxc_clean_dir ${datanodeMasterServers[$i]} ${datanodeMasterDirs[$i]}
			return
		fi
	done
	echo Specified datanode is not configured, $1
	return 2
}

function pgxc_clena_datanode_master_all
{
	local i
	log_echo pgxc_clean_datanode_master_all'('$*')'
	for ((i=0; i<${#datanodeNames[@]}; i++));do
		pgxc_clean_datanode_master ${datanodeNames[$i]}
	done
}

function pgxc_clean_datanode_slave
{
	log_echo pgxc_clean_datanode_slave'('$*')'
	if [ $datanodeSlave != y ]; then
		echo Datanode slave is not configured.
		return 1
	fi
	if [ $# -ne 1 ]; then
		echo Specify datanode name.
		return 2
	fi
	local i
	for ((i=0;i<${#datanodeNames[@]}; i++));do
		if [ $1 == ${datanodeNames[$i]} ]; then
			if [ ${datanodeSlaveServers[$i]} == none ] || [ ${datanodeSlaveServers[$i]} == N/A ]; then
				echo Specified datanode slave is not configured, $1
				return 2
			fi
			pgxc_clean_dir ${datanodeSlaveServers[$i]} ${datanodeSlaveDirs[$i]}
			return
		fi
	done
	echo Specified datanode is not configured, $1
	return 2
}

function pgxc_clean_datanode_slave_all
{
	log_echo pgxc_clean_datanode_slave_all'('$*')'
	if [ $datanodeSlave != y ]; then
		echo Datanode slave is not configured.
		return 1
	fi
	local i
	for ((i=0; i<${#datanodeNames[@]}; i++)); do
		pgxc_clean_datanode_slave ${datanodeNames[$i]}
	done
}

function pgxc_clean_datanode_all
{
	log_echo pgxc_clean_datanode_all'('$*')'
	pgxc_clean_datanode_master_all
	pgxc_clean_datanode_slave_all
}

function pgxc_clean_node_all
{
	local i

	log_echo pgxc_clean_node_all'('$*')'
	pgxc_clean_node $gtmName all
	if [ "$gtmProxy" == "y" ]; then
		for ((i=0; i< ${#gtmProxyNames[@]}; i++)); do
			pgxc_clean_node ${gtmProxyNames[$i]}
		done
	else
		eecho GTM Proxies are not configured.
	fi
	for ((i=0; i< ${#coordNames[@]}; i++)); do
		pgxc_clean_node ${coordNames[$i]} all
	done
	for ((i=0; i< ${#datanodeNames[@]}; i++)); do
		pgxc_clean_node ${datanodeNames[$i]} all
	done
}

# print_stdout host file prompt
function print_stdout
{
	local host
	local file
	if [ $# -ne 3 ]; then
		return 1
	fi
	host=$1
	shift
	file=$1
	shift
	if [ $verbose == y ]; then
		if [ $interactive != y ]; then
			if [ $host != localhost ]; then
				ssh $pgxcUser@$host cat $file
			else
				cat $file >> $logfile
			fi
		else
			echo -n $*
			if [ `readyesno n` == y ]; then
				if [ $host != localhost ]; then
					ssh $pgxcUser@$host cat $file
				else
					cat $file
				fi
			fi
		fi
	fi
	if [ $logOpt == y ]; then
		if [ $host != localhost ]; then 
			(ssh $pgxcUser@$host cat $file) >> $logfile
		else
			cat $file >> $logfile
		fi
	fi
	if [ $host != localhost ]; then
		ssh $pgxcUser@$host rm -f $file
	else
		rm -f $file
	fi
}

# Print without asking, if $verbose == y
function do_stdout
{
	local host
	local file
	if [ $# -ne 2 ]; then
		return 1
	fi
	host=$1
	shift
	file=$1
	shift
	if [ $verbose == y ]; then
		if [ $host != localhost ]; then
			ssh $pgxcUser@$host cat $file
		else
			cat $file
		fi
		if [ $logOpt == y ]; then
			if [ $host != localhost ]; then
				(ssh $pgxcUser@$host cat $file) >> $logfile
			else
				cat $file >> $logfile
			fi
		fi
	fi
	if [ $host != localhost ]; then
		ssh $pgxcUser@$host rm -f $file
	else
		rm -f $file
	fi
}

# Write the file to the log if log is enabled.  $1: file to write
function fileWriteToLog
{
	if [ $logOpt == y ]; then
		cat $1 >> $logfile
	fi
}

function print_initgtm_out
{
	print_stdout $1 $2 "Print initgtm output\?"
}

function print_initdb_out
{
	print_stdout $1 $2 "Print initgtm output\?"
}

function print_pg_ctl_out
{
	print_stdout $1 $2 "Print pg_ctl output\?"
}

function print_gtm_ctl_out
{
	print_stdout $1 $2 "Print gtm_ctl output\?"
}

#===========================================================                                                 
#                                                                                                            
# GTM and GTM slave staff
#                                                                                                            
#===========================================================                                                 

# Reconfigure GTM Master.   Result will be reflected to the configuration file too.
# Parameters are server, port and directory.   Please note that the node name
# is fixed.  You should edit configuration file to change GTM node name.
# You may specify two more arguments, gtm additional configuration file common to
# master/slave and gtm additional configuration file specific to gtm master.
# If you don't specify "no" additional ones, you can specify "none".
function pgxc_configure_gtm_master
{
	log_echo pgxc_configure_gtm_master"("$*")"
    vecho ================================================================
    vecho Configure GTM Master

	if [ $# -lt 3 -o $# -gt 5 ]; then
		echo ERROR: invalid arguments, $*
		return 1
	fi
	gtmMasterServer=$1
	gtmMasterPort=$2
	gtmMasterDir=$3
	vecho New GTM Master config: Server: "'"$gtmMasterServer"'", Port: $gtmMasterPort, Dir: "'"$gtmMasterDir"'"
	# Update configuration file
	cat >> $configFile <<EOF
#====================================
# Updated due to GTM Master reconfiguration
#           $datetime
gtmMasterServer=$gtmMasterServer
gtmMasterPort=$gtmMasterPort
gtmMasterDir=$gtmMasterDir
EOF
	if [ $# -ge 4 ]; then
		gtmExtraConfig=$4
		vecho -n "                       "ExtraConfig: "'"$gtmExtraConfig"'"
		cat >> $configFile <<EOF
gtmExtraConfig=$gtmExtraConfig
EOF
		if [ $# -eq 5 ]; then
			gtmMasterSpecificExtraConfig=$5
			vecho "," ExtraSpecificConfig: "'"$gtmMasterSpecificExtraConfig"'"
			cat >> $configFile <<EOF
gtmMasterSpecificExtraConfig=$gtmMasterSpecificExtraConfig
EOF
		fi
	fi
	vecho ""
	cat >> $configFile <<EOF
# --- End of reconfiguration --------
EOF
	# Backup configuration file
	pgxc_backup_config_file
}

function pgxc_init_gtm_master
{
	local yesno

	log_echo pgxc_init_gtm_master"("$*")"
    vecho ================================================================
    vecho GTM Master initialize

    doit ssh $pgxcUser@$gtmMasterServer "killall -u $pgxcOwner -9 gtm > /dev/null 2>&1"
    doit ssh $pgxcUser@$gtmMasterServer rm -rf $gtmMasterDir
    doit ssh $pgxcUser@$gtmMasterServer mkdir -p $gtmMasterDir
    doit ssh $pgxcUser@$gtmMasterServer "initgtm -Z gtm -D $gtmMasterDir > $tmpDir/initgtm.out 2>&1"
	print_initgtm_out $gtmMasterServer $tmpDir/initgtm.out
    vecho Configuring $gtmMasterServer:$gtmMasterDir/gtm.conf
	vecho     ssh $pgxcUser@$gtmMasterServer '"'cat '>>'  $gtmMasterDir/gtm.conf '<<EOF'
    ssh $pgxcUser@$gtmMasterServer "cat >>  $gtmMasterDir/gtm.conf" <<EOF
#===========================================
# Added at initialization. $datetime
listen_addresses = '*'
EOF
	if [ $gtmExtraConfig != none ]; then
		vecho ssh $pgxcUser@$gtmMasterServer '"'cat '>>' $gtmMasterDir/gtm.conf'"' '<' $gtmExtraConfig
		ssh $pgxcUser@$gtmMasterServer "cat >> $gtmMasterDir/gtm.conf" < $gtmExtraConfig
	fi
	if [ $gtmMasterSpecificExtraConfig != none ]; then
		vecho ssh $pgxcUser@$gtmMasterServer '"'cat '>>' $gtmMasterDir/gtm.conf'"' '<' $gtmSpecificExtraConfig
		ssh $pgxcUser@$gtmMasterServer "cat >> $gtmMasterDir/gtm.conf" < $gtmSpecificExtraConfig
	fi
    vecho ssh $pgxcUser@$gtmMasterServer '"'cat '>>'  $gtmMasterDir/gtm.conf'"'
    ssh $pgxcUser@$gtmMasterServer "cat >>  $gtmMasterDir/gtm.conf" <<EOF
port = $gtmMasterPort
nodename = '$gtmName'
startup = ACT
# End of addition
EOF
	# Next two lines a needed to start GTM with minimum GXID as possible. (default is 10000).
	# Current default GXID initial value will cause datanode slave error because there's too
	# many XID's involved.  This is a dirty hack and should be corrected by running initdb with
	# gtm, or gtm can correct what GXID to start with from all the nodes.
	vecho Initializing starting GXID value...
	vecho 	'('ssh $pgxcUser@$gtmMasterServer gtm -x 2000 -D $gtmMasterDir '&)'
	(ssh $pgxcUser@$gtmMasterServer gtm -x 2000 -D $gtmMasterDir &)
	sleep 1
	vecho ssh $pgxcUser@$gtmMasterServer '"'gtm_ctl stop -Z gtm -D $gtmMasterDir '>' /dev/null '2>&1"'
	ssh $pgxcUser@$gtmMasterServer "gtm_ctl stop -Z gtm -D $gtmMasterDir > /dev/null 2>&1"
}

# Configure gtm_slave.   The arguments are host name, port and directory.
# If you remove (or don't configure) the slave, you specify host name as
# none.   You don't have to worry about the rest of the parameters.
# You can specify additional parameters, extra file to go to gtm.conf 
# file, only to the slave.   The common file should be configured
# using pgxc_configure_gtm_master function.
#function pgxc_configure_gtm_slave
#{
#}


function pgxc_init_gtm_slave
{
	log_echo pgxc_init_gtm_slave'('$*')'
    vecho ================================================================
    vecho $progname:$0 GTM Slave initialize

	if [ "$gtmSlave" != "y" ] || [ "$gtmSlaveServer" == "N/A" ] || [ "$gtmSlaverServer" == "none" ] ; then
		echo $progname:$0 ERROR: GTM Slave is not configured.
		return 1
	fi
	vecho ssh $pgxcUser@$gtmSlaveServer '"'killall -u $pgxcOwner -9 gtm '>' /dev/null '2>&1"'
    ssh $pgxcUser@$gtmSlaveServer "killall -u $pgxcOwner -9 gtm > /dev/null 2>&1"
    doit ssh $pgxcUser@$gtmSlaveServer rm -rf $gtmSlaveDir
    doit ssh $pgxcUser@$gtmSlaveServer mkdir -p $gtmSlaveDir
	vecho ssh $pgxcUser@$gtmSlaveServer '"'initgtm -Z gtm -D $gtmSlaveDir '>' $tmpDir/initgtm.out '2>&1"'
    ssh $pgxcUser@$gtmSlaveServer "initgtm -Z gtm -D $gtmSlaveDir > $tmpDir/initgtm.out 2>&1"
	print_initgtm_out $gtmSlaveServer $tmpDir/initgtm.out
    vecho $pgxcUser@$gtmSlaveServer '"'cat '>>'  $gtmSlaveDir/gtm.conf'"'
    ssh $pgxcUser@$gtmSlaveServer "cat >>  $gtmSlaveDir/gtm.conf"  <<EOF
listen_addresses = '*'
EOF
	if [ $gtmExtraConfig != none ]; then
		vecho ssh $pgxcUser@$gtmSlaveServer '"'cat '>>' $gtmSlaveDir/gtm.conf'"' '<' $gtmExtraConfig
		ssh $pgxcUser@$gtmSlaveServer "cat >> $gtmSlaveDir/gtm.conf" < $gtmExtraConfig
	fi
	if [ $gtmSlaveSpecificExtraConfig != none ]; then
		vecho ssh $pgxcUser@$gtmSlaveServer '"'cat '>>' $gtmSlaveDir/gtm.conf'"' '<' $gtmSlaveSpecificExtraConfig
		ssh $pgxcUser@$gtmSlaveServer "cat >> $gtmSlaveDir/gtm.conf" < $gtmSlaveSpecificExtraConfig
	fi
	vecho ssh $pgxcUser@$gtmSlaveServer '"'cat '>>'  $gtmSlaveDir/gtm.conf'"'
    ssh $pgxcUser@$gtmSlaveServer "cat >>  $gtmSlaveDir/gtm.conf" <<EOF
port = $gtmSlavePort
nodename = '$gtmName'
startup = STANDBY
active_host = '$gtmMasterServer'
active_port = $gtmMasterPort
EOF
}

function pgxc_start_gtm_master
{
	log_echo pgxc_start_gtm_master'('$*')'
	vecho ================================================================
	vecho Starting GTM Master

	vecho ssh $pgxcUser@$gtmMasterServer '"'killall -u $pgxcOwner -9 gtm '>' /dev/null '2>&1"'
	ssh $pgxcUser@$gtmMasterServer "killall -u $pgxcOwner -9 gtm > /dev/null 2>&1"
	doit ssh $pgxcUser@$gtmMasterServer "rm -f $gtmMasterDir/register.node"
	vecho ssh $pgxcUser@$gtmMasterServer '"'gtm_ctl start -Z gtm -D $gtmMasterDir '>' $tmpDir/gtm.out'"'
	ssh $pgxcUser@$gtmMasterServer "gtm_ctl start -Z gtm -D $gtmMasterDir > $tmpDir/gtm.out"
	do_stdout $gtmMasterServer $tmpDir/gtm.out
}

function pgxc_start_gtm_slave
{
	log_echo pgxc_start_gtm_slave'('$*')'
	vecho ================================================================
	vecho $progname:$0 Starting GTM Slave

	if [ $gtmSlaveServer == none ]; then
		eecho ERROR: GTM slave is not configured.
		return 1
	fi
	vecho ssh $pgxcUser@$gtmMasterServer '"'gtm_ctl status -Z gtm -D $gtmMasterDir '>' /dev/null '2>&1"'
	ssh $pgxcUser@$gtmMasterServer "gtm_ctl status -Z gtm -D $gtmMasterDir > /dev/null 2>&1"
	if [ $? -ne 0 ]; then
		echo ERROR: GTM Master is not running. Cannot start the slave.
		return 1
	fi
	vecho ssh $pgxcUser@$gtmSlaveServer '"'killall -u $pgxcOwner -9 gtm '>' /dev/null '2>&1"'
	ssh $pgxcUser@$gtmSlaveServer "killall -u $pgxcOwner -9 gtm >/dev/null 2>&1"
	doit ssh $pgxcUser@$gtmSlaveServer "rm -f $gtmSlaveDir/register.node"
	vecho ssh $pgxcUser@$gtmSlaveServer '"'gtm_ctl start -Z gtm -D $gtmSlaveDir '>' $tmpDir/gtm.out'"'
	ssh $pgxcUser@$gtmSlaveServer "gtm_ctl start -Z gtm -D $gtmSlaveDir > $tmpDir/gtm.out"
	do_stdout $gtmSlaveServer $tmpDir/gtm.out
}


function pgxc_stop_gtm_master
{
	log_echo pgxc_stop_gtm_master'('$*')'
	vecho ================================================================
	vecho Stopping GTM Master
	doit ssh $pgxcUser@$gtmMasterServer gtm_ctl stop -Z gtm -D $gtmMasterDir
}

function pgxc_stop_gtm_slave
{
	log_echo pgxc_stop_gtm_slave'('$*')'
	vecho ================================================================
	vecho Stopping GTM Slave
	if [ $gtmSlaveServer == none ] || [ $gtmSlaveServer == N/A ]; then
		eecho ERROR: GTM slave is not configured.
		return 1
	fi
	doit ssh $pgxcUser@$gtmSlaveServer gtm_ctl stop -Z gtm -D $gtmSlaveDir
}

function pgxc_kill_gtm_master
{
	log_echo pgxc_kill_gtm_master'('$*')'
	vecho ================================================================
	vecho Stopping GTM Master
	vecho ssh $pgxcUser@$gtmMasterServer '"'killall -u $pgxcUser -9 gtm '>'/dev/null '2>&1"'
	ssh $pgxcUser@$gtmMasterServer "killall -u $pgxcUser -9 gtm >/dev/null 2>&1"
	pgxc_clean_socket $gtmMasterServer $gtmMasterPort
}

function pgxc_kill_gtm_slave
{
	log_echo pgxc_kill_gtm_slave'('$*')'
	vecho ================================================================
	vecho Stopping GTM Slave
	if [ $gtmSlaveServer == none ] || [ $gtmSlaveServer == N/A ]; then
		eecho ERROR: GTM slave is not configured.
		return 1
	fi
	vecho ssh $pgxcUser@$gtmSlaveServer '"'killall -u $pgxcUser -9 gtm '>'/dev/null '2>&1"'
	ssh $pgxcUser@$gtmSlaveServer "killall -u $pgxcUser -9 gtm >/dev/null 2>&1"
	pgxc_clean_socket $gtmSlaveServer $gtmSlavePort
}

function pgxc_failover_gtm
{
	log_echo pgxc_failover_gtm
	# Reconnect should be done in separate action.
	vecho ================================================================
	vecho GTM Failover

	if [ $gtmSlaveServer == none ]; then
		eecho ERROR: pgxc_failover_gtm: GTM slave is not available.
		return 1
	fi
	doit pgxc_monitor -Z gtm -p $gtmSlavePort -h $gtmSlaveServer
#	ssh "$pgxcUser@$gtmSlaveServer gtm_ctl status -Z gtm -D $gtmSlaveDir > /dev/null 2>&1"
	if [ $? -ne 0 ]; then
		eecho ERROR: GTM slave is not running.
		return 1
	fi
	# STONITH GTM Master
	# Please note that master and slave should run on different server.
#	vecho $pgxcUser@$gtmMasterServer '"'killall -u $pgxcOwner -9 gtm '>'/dev/null '2>&1"'
#	ssh $pgxcUser@$gtmMasterServer "killall -u $pgxcOwner -9 gtm >/dev/null 2>&1"
	doit ssh $pgxcUser@$gtmSlaveServer gtm_ctl promote -Z gtm -D $gtmSlaveDir
	# Update GTM configuration file as the master
	vecho Reconfigure GTM as Master
	vecho ssh $pgxcUser@$gtmSlaveServer '"'cat '>>' $gtmSlaveDir/gtm.conf'"'
	ssh $pgxcUser@$gtmSlaveServer "cat >> $gtmSlaveDir/gtm.conf" <<EOF
#===================================================
# Updated due to GTM failover
#        $datetime
startup = ACT
#----End of reconfiguration -------------------------
EOF
	# Update configuration
	vecho Reconfiguring whole Postgres-XC cluster
	vecho cat '>>' $configFile
	cat >> $configFile <<EOF
#===================================================
# pgxc configuration file updated due to GTM failover
#        $datetime
gtmMasterServer=$gtmSlaveServer
gtmMasterPort=$gtmSlavePort
gtmMasterDir=$gtmSlaveDir
gtmSlaveServer=none
gtmSlavePort=0
gtmSlaveDir=none
#----End of reconfiguration -------------------------
EOF
	# Bacup config file
	pgxc_backup_config_file
	# Reconfigure myself
	gtmMasterServer=$gtmSlaveServer
	gtmMasterPort=$gtmSlavePort
	gtmMasterDir=$gtmSlaveDir
	gtmSlaveServer=none
	gtmSlavePort=0
	gtmSlaveDir=none
}

#===============================================================================
#
# GTM Proxy staff
#
#===============================================================================

function pgxc_init_gtm_proxy
{
	# First argument is the nodename
	log_echo pgxc_init_gtm_proxy'('$*')'
    vecho ================================================================
    vecho Initialize GTM Proxy $1

	local i

	if [ $# -ne 1 ]; then
		eecho ERROR: Specify gtm_proxy name
		return 1
	fi
	for ((i=0; i< ${#gtmProxyNames[@]}; i++)); do
		if [ $1 == ${gtmProxyNames[$i]} ] && [ ${gtmProxyServers[$i]} != none ] && [ ${gtmProxyServers[$i]} != N/A ]; then
			vecho ssh $pgxcUser@${gtmProxyServers[$i]} '"'killall -u $pgxcOwner -9 gtm_proxy '>'/dev/null '2>&1"'
			ssh $pgxcUser@${gtmProxyServers[$i]} "killall -u $pgxcOwner -9 gtm_proxy >/dev/null 2>&1"
			doit ssh $pgxcUser@${gtmProxyServers[$i]} rm -rf ${gtmProxyDirs[$i]}
			doit ssh $pgxcUser@${gtmProxyServers[$i]} mkdir -p ${gtmProxyDirs[$i]}
			vecho ssh $pgxcUser@${gtmProxyServers[$i]} '"'initgtm -Z gtm_proxy -D ${gtmProxyDirs[$i]} '>' $tmpDir/initgtm.out '2>&1"'
			ssh $pgxcUser@${gtmProxyServers[$i]} "initgtm -Z gtm_proxy -D ${gtmProxyDirs[$i]} > $tmpDir/initgtm.out 2>&1"
			print_initgtm_out ${gtmProxyServers[$i]} $localTmpDir/initgtm.out
			vecho Configuring ${gtmProxyServers[$i]}:${gtmProxyDirs[$i]}/gtm_proxy.conf
			if [ $gtmPxyExtraConfig != none ] && [ $gtmPxyExtrConfig != N/A ]; then
				vecho ssh $pgxcUser@${gtmProxyServers[$i]} '"'cat '>>' ${gtmProxyDirs[$i]}/gtm_proxy.conf'"' '<' $gtmPxyExtraConfig
				ssh $pgxcUser@${gtmProxyServers[$i]} "cat >> ${gtmProxyDirs[$i]}/gtm_proxy.conf" < $gtmPxyExtraConfig
			fi
			vecho ssh $pgxcUser@${gtmProxyServers[$i]} '"'cat '>>' ${gtmProxyDirs[$i]}/gtm_proxy.conf'"'
			ssh $pgxcUser@${gtmProxyServers[$i]} "cat >> ${gtmProxyDirs[$i]}/gtm_proxy.conf" <<EOF
nodename = '${gtmProxyNames[$i]}'
listen_addresses = '*'
port = ${gtmProxyPorts[$i]}
gtm_host = $gtmMasterServer
gtm_port = $gtmMasterPort
worker_threads = 1
gtm_connect_retry_interval = 1
EOF
			return
		fi
	done
	eecho ERROR: specified GTM proxy is not configured, $1
	return 1
}

function pgxc_init_gtm_proxy_all
{
	local i

	log_echo pgxc_init_gtm_proxy_all'('$*')'
	if [ $gtmProxy != y ]; then
		eecho ERROR: gtm_proxy is not configured
		return 1
	fi
	for((i=0;i<${#gtmProxyNames[@]};i++)); do
		if [ ${gtmProxyServers[$i]} != none ] && [ ${gtmProxyServers[$i]} != N/A ]; then
			pgxc_init_gtm_proxy ${gtmProxyNames[$i]}
		fi
	done
}

function pgxc_start_gtm_proxy
{
	# First argument is the nodename
	log_echo pgxc_start_gtm_proxy'('$*')'
    vecho ================================================================
    vecho Start GTM Proxy $1

	if [ $# -ne 1 ]; then
		eecho Error: specify GTM proxy name
		return 1
	fi
	local i
	for ((i=0; i< ${#gtmProxyNames[@]}; i++)); do
		if [ $1 == ${gtmProxyNames[$i]} ]; then
			vecho ssh $pgxcUser@${gtmProxyServers[$i]} '"'killall -u $pgxcOwner -9 gtm_proxy '>'/dev/null '2>&1"'
			ssh $pgxcUser@${gtmProxyServers[$i]} "killall -u $pgxcOwner -9 gtm_proxy >/dev/null 2>&1"
			vecho ssh $pgxcUser@${gtmProxyServers[$i]} '"'gtm_ctl start -Z gtm_proxy -D ${gtmProxyDirs[$i]} '>' $tmpDir/gtm_proxy.out '2>&1"'
			ssh $pgxcUser@${gtmProxyServers[$i]} "gtm_ctl start -Z gtm_proxy -D ${gtmProxyDirs[$i]} > $tmpDir/gtm_proxy.out 2>&1"
			do_stdout ${gtmProxyServers[$i]} $tmpDir/gtm_proxy.out
			return
		fi
	done
	eecho ERROR: specified GTM proxy does not exist, $1
	return 1
}

function pgxc_start_gtm_proxy_all
{
	log_echo pgxc_startgtm_proxy_all'('$*')'
	local i
    vecho ================================================================
    vecho Starting all the GTM proxies
	if [ $gtmProxy != y ]; then
		eecho ERROR: GTM proxy is not configured.
		return 1
	fi
	for((i=0;i<${#gtmProxyNames[@]};i++)); do
		if [ ${gtmProxyServers[$i]} != none ] && [ ${gtmProxyServers[$i]} != N/A ]; then
			pgxc_monitor -Z gtm -p ${gtmProxyPorts[$i]} -h ${gtmProxyServers[$i]}
			if [ $? -eq 0 ]; then
				eecho gtm_proxy"("${gtpmProxyNames[$i]}")" is already running.
			else
				doit pgxc_start_gtm_proxy ${gtmProxyNames[$i]}
			fi
		fi
	done
}

function pgxc_stop_gtm_proxy
{
	# First argument is the nodename
	log_echo pgxc_stop_gtm_proxy'('$*')'
    vecho ================================================================
    vecho Stop GTM Proxy $1

	if [ $# -ne 1 ]; then
		iecho Specify GTM Proxy name
		return 1
	fi
	local i
	for ((i=0; i< ${#gtmProxyNames[@]}; i++)); do
		if [ $1 == ${gtmProxyNames[$i]} ]; then
			doit ssh $pgxcUser@${gtmProxyServers[$i]} gtm_ctl stop -Z gtm_proxy -D ${gtmProxyDirs[$i]}
			return
		fi
	done
	eecho ERROR: specified GTM proxy does not exist, $1
	return 1
}

function pgxc_stop_gtm_proxy_all
{
	log_echo pgxc_stop_gtm_proxy_all'('$*')'
    vecho ================================================================
    vecho Stop all the GTM Proxies

	local i
	if [ $gtmProxy != y ]; then
		eecho Error: GTM Proxy is not configured
		return 1
	fi
	for((i=0;i<${#gtmProxyNames[@]};i++)); do
		if [ ${gtmProxyServers[$i]} == none ] || [ ${gtmProxyServers[$i]} == N/A ]; then
			continue
		fi
		doit pgxc_stop_gtm_proxy ${gtmProxyNames[$i]}
	done
}



function pgxc_kill_gtm_proxy
{
	# First argument is the nodename
	log_echo pgxc_kill_gtm_proxy'('$*')'
    vecho ================================================================
    vecho Kill GTM Proxy $1

	if [ $# -ne 1 ]; then
		eecho ERROR: specify GTM proxy name
		return 1
	fi
	local i
	for ((i=0; i< ${#gtmProxyNames[@]}; i++)); do
		if [ $1 == ${gtmProxyNames[$i]} ]; then
			vecho ssh $pgxcUser@${gtmProxyServers[$i]} '"'killall -u $pgxcOwner -9 gtm_proxy '>'/dev/null '2>&1"'
			ssh $pgxcUser@${gtmProxyServers[$i]} "killall -u $pgxcOwner -9 gtm_proxy >/dev/null 2>&1"
			doit pgxc_clean_socket ${gtmProxyServers[$i]} ${gtmProxyPorts[$i]}
			return
		fi
	done
	eecho ERROR: specified GTM proxy does not exist, $1
	return 1
}

#----------------ここまで: ログ出力拡張: Oct 19, 2012 --------------------------
function pgxc_kill_gtm_proxy_all
{
	log_echo pgxc_lill_gtm_proxy_all "("$*")"
    vecho ================================================================
    vecho Killing all the  GTM Proxies

	local i
	if [ $gtmProxy != y ]; then
		eecho GTM Proxy is not configured
		return 1
	fi
	for((i=0;i<${#gtmProxyNames[@]};i++)); do
		if [ ${gtmProxyServers[$i]} == none ] || [ ${gtmProxySevrers[$i]} == N/A ]; then
			continue
		fi
		pgxc_kill_gtm_proxy ${gtmProxyNames[$i]}
	done
}

function pgxc_reconnect_gtm_proxy
{
	# Reconnect to the current GTM master.   When failed over, the current Master must have been updated.
	# Remember to update gtm_proxy configuration file so that it connects to the new master at the next
	# start.
	# Please note that we assume GTM has already been failed over.
	# First argument is gtm_proxy nodename
	log_echo pgxc_reconnect_gtm_proxy "("$*")"
    vecho ================================================================
    vecho Reconnect GTM Proxy $1

	if [ $# -ne 1 ]; then
		eecho Specify GTM proxy name
		return 1
	fi
	local i
	for ((i=0; i< ${#gtmProxyNames[@]}; i++)); do
		if [ $1 == ${gtmProxyNames[$i]} ]; then
			vecho doit ssh $pgxcUser@${gtmProxyServers[$i]} gtm_ctl reconnect -Z gtm_proxy -D ${gtmProxyDirs[$i]} -o \
				\"-s $gtmMasterServer -t $gtmMasterPort\"

			doit ssh $pgxcUser@${gtmProxyServers[$i]} gtm_ctl reconnect -Z gtm_proxy -D ${gtmProxyDirs[$i]} -o \
				\"-s $gtmMasterServer -t $gtmMasterPort\"
			vecho Reconfiguring GTM Proxy reflect reconnect.
			log_echo ssh $pgxcUser@${gtmProxyServers[$i]} '"'cat '>>' ${gtmProxyDirs[$i]}/gtm_proxy.conf'"'
			ssh $pgxcUser@${gtmProxyServers[$i]} "cat >> ${gtmProxyDirs[$i]}/gtm_proxy.conf" <<EOF
#===================================================
# Updated due to GTM Proxy reconnect
#        $datetime
gtm_host = $gtmMasterServer
gtm_port = $gtmMasterPort
#----End of reconfiguration -------------------------
EOF
			return
		fi
	done
	eecho ERROR: specified GTM proxy does not exist, $1
	return 1

}

function pgxc_reconnect_gtm_proxy_all
{
	log_echo pgxc_reconnect_gtm_proxy_all "("$*")"
    vecho ================================================================
    vecho Reconnect all the GTM proxies

	local i
	if [ $gtmProxy != y ]; then
		eecho GTM Poxy is not configured
		return 1
	fi
	for((i=0;i<${#gtmProxyNames[@]};i++)); do
		if [ ${gtmProxyServers[$i]} == none ] || [ ${gtmProxyServers[$i]} == N/A ]; then
			continue
		fi
		pgxc_reconnect_gtm_proxy ${gtmProxyNames[$i]}
	done
}
#===============================================================================
#
# Coordinator Staff
#
#===============================================================================

function pgxc_init_coordinator_master
{
	# First argument is the nodename
	log_echo pgxc_init_coordinator_master "("$*")"
    vecho ================================================================
    vecho Initialize coordinator master $1

	if [ $# -ne 1 ]; then
		eecho Specify coordinator name
		return 1
	fi
	local i
	for ((i=0; i< ${#coordNames[@]}; i++)); do
		if [ $1 == ${coordNames[$i]} ] && [ ${coordMasterServers[$i]} != none ] && [ ${coordMasterServers[$i]} != N/A ]; then
			psql -p ${coordPorts[$i]} -h ${coordMasterServers[$i]}  -c 'select 1' postgres $pgxcOwner > /dev/null 2> /dev/null
			if [ $? -eq 0 ]; then
				eecho ERROR: target coordinator master is running now.  Stop to configure $1 slave.
				return 1
			fi
			doit ssh $pgxcUser@${coordMasterServers[$i]} rm -rf ${coordMasterDirs[$i]}
			doit ssh $pgxcUser@${coordMasterServers[$i]} mkdir -p ${coordMasterDirs[$i]}
			doit ssh $pgxcUser@${coordMasterServers[$i]} "initdb --nodename ${coordNames[$i]} -D ${coordMasterDirs[$i]} > $tmpDir/initdb.out 2>&1"
			print_initdb_out ${coordMasterServers[$i]} $tmpDir/initdb.out
			vecho Configuring ${coordMasterServers[$i]}:${coordMasterDirs[$i]}/postgresql.conf
		    # Get effective GTM port and host.   If gtm_proxy is not found, then connect to GTM
			local j
			local targetGTMhost
			local targetGTMport
			targetGTMhost=$gtmMasterServer
			targetGTMport=$gtmMasterPort
			for ((j=0; j< ${#gtmProxyServers[@]}; j++)); do
				if [ ${coordMasterServers[$i]} == ${gtmProxyServers[$j]} ]; then
					targetGTMhost=${gtmProxyServers[$j]}
					targetGTMport=${gtmProxyPorts[$j]}
					break
				fi
			done
			if [ $coordExtraConfig != none ] && [ $coordExtraConfig != N/A ]; then
				vecho Configuring $pgxcUser@${coordMasterServer[$i]}:${coaordMasterDirs[$i]}/postgresql.conf using $coordExtraConfig
				ssh $pgxcUser@${coordMasterServers[$i]} "cat >> ${coordMasterDirs[$i]}/postgresql.conf" < $coordExtraConfig
			fi
			if [ ${coordSpecificExraConfig[$i]} != none ] && [ ${coordSpecificExraConfig[$i]} != none ]; then
				vecho Configuring $pgxcUser@${coordMasterServers[$i]}:${coordMasterDirs[$i]}/postgresql.conf using ${coordSpecificExtraConfig[$i]}
				ssh $pgxcUser@${coordMasterServers[$i]} "cat >> ${coordMasterDirs[$i]}/postgresql.conf" < ${coordSpecificExtraConfig[$i]}
			fi
			vecho Configuring  $pgxcUser@${coordMasterServers[$i]}:${coordMasterDirs[$i]}/postgresq
			log_echo ssh $pgxcUser@${coordMasterServers[$i]} '"'cat '>>' ${coordMasterDirs[$i]}/postgresql.conf'"'
			ssh $pgxcUser@${coordMasterServers[$i]} "cat >> ${coordMasterDirs[$i]}/postgresql.conf" <<EOF
#===========================================
# Added at initialization. $datetime
log_destination = 'stderr'
logging_collector = on
log_directory = 'pg_log'
listen_addresses = '*'
port = ${coordPorts[$i]}
max_connections = 100
pooler_port = ${poolerPorts[$i]}
gtm_host = '$targetGTMhost'
gtm_port = $targetGTMport
EOF
			# Additional initialization for log_shipping.
			if [ $coordSlave == y ] && [ ${coordSlaveServers[$i]} != none ] && [ ${coordSlaveServers[$i]} != N/A ]; then
				# At least cleanup remote archive directory.
				pgxc_clean_dir ${coordSlaveServers[$i]} ${coordArchLogDirs[$i]}
				# Then setup postgresql.conf
				log_echo ssh $pgxcUser@${coordMasterServers[$i]} '"'cat '>>' ${coordMasterDirs[$i]}/postgresql.conf'"'
				ssh $pgxcUser@${coordMasterServers[$i]} "cat >> ${coordMasterDirs[$i]}/postgresql.conf" <<EOF
wal_level = hot_standby
archive_mode = on
archive_command = 'rsync %p $pgxcUser@${coordSlaveServers[$i]}:${coordArchLogDirs[$i]}/%f'
max_wal_senders = ${coordMaxWALSenders[$i]}
# End of Addition
EOF
			else
				log_echo ssh $pgxcUser@${coordMasterServers[$i]} '"'cat '>>' ${coordMasterDirs[$i]}/postgresql.conf'"'
				ssh $pgxcUser@${coordMasterServers[$i]} "cat >> ${coordMasterDirs[$i]}/postgresql.conf" <<EOF
# End of Addition
EOF
			fi
			vecho Configuring ${coordMasterServers[$i]}:${coordMasterDirs[$i]}/pg_hba.conf
			log_echo ssh $pgxcUser@${coordMasterServers[$i]} '"'cat '>>' ${coordMasterDirs[$i]}/pg_hba.conf'"'
			ssh $pgxcUser@${coordMasterServers[$i]} "cat >> ${coordMasterDirs[$i]}/pg_hba.conf"  <<EOF
#=================================================
# Addition at initialization, $datetime 
EOF
			if [ $coordExtraPgHba != none ] && [ $coordExtraPgHba != N/A ]; then
				vecho Configuring ${coordMasterServers[$i]}:${coordMasterDirs[$i]}/pg_hba.conf using $coordExtraPgHba
				log_echo ssh $pgxcUser@${coordMasterServers[$i]} '"'cat '>>' ${coordMasterDirs[$i]}/pg_hab.conf'"' '<' $coordExtraPgHba
				ssh $pgxcUser@${coordMasterServers[$i]} "cat >> ${coordMasterDirs[$i]}/pg_hab.conf" < $coordExtraPgHba
			fi
			if [ ${coordSpecificExtraPgHba[$i]} != none ] && [ ${coordSpecificExtraPgHba[$i]} != N/A ]; then
				log_echo ssh $pgxcUser@${coordMasterServers[$i]} '"'cat '>>' ${coordMasterDirs[$i]}/pg_hab.conf'"' '<' ${coordSpecificExtraPgHba[$i]}
				ssh $pgxcUser@${coordMasterServers[$i]} "cat >> ${coordMasterDirs[$i]}/pg_hab.conf" < ${coordSpecificExtraPgHba[$i]}
			fi
			local j
			for ((j=0; j< ${#coordPgHbaEntries[@]}; j++)); do
				log_echo ssh $pgxcUser@${coordMasterServers[$i]} '"'cat '>>' ${coordMasterDirs[$i]}/pg_hba.conf'"'
				ssh $pgxcUser@${coordMasterServers[$i]} "cat >> ${coordMasterDirs[$i]}/pg_hba.conf" <<EOF
host all $pgxcOwner ${coordPgHbaEntries[$j]} trust
EOF
				if [ ${coordSlaveServers[$i]} != none ] && [ ${coordSlaveServers[$i]} != N/A ]; then
					log_echo ssh $pgxcUser@${coordMasterServers[$i]} '"'cat '>>' ${coordMasterDirs[$i]}/pg_hba.conf'"'
					ssh $pgxcUser@${coordMasterServers[$i]} "cat >> ${coordMasterDirs[$i]}/pg_hba.conf"  <<EOF
host replication $pgxcOwner ${coordPgHbaEntries[$j]} trust
EOF
				fi
			done
			log_echo ssh $pgxcUser@${coordMasterServers[$i]} '"'cat '>>' ${coordMasterDirs[$i]}/pg_hba.conf'"'
			ssh $pgxcUser@${coordMasterServers[$i]} "cat >> ${coordMasterDirs[$i]}/pg_hba.conf"  <<EOF
# End of addition
EOF
			return
		fi
	done
	eecho ERROR: specified coordinator is not configured, $1
	return 1
}

function pgxc_init_coordinator_master_all
{
	log_echo pgxc_init_coordinator_master_all'('$*')'
    vecho ================================================================
    vecho Initialize all the coordinator masters

	local i
	for ((i=0; i< ${#coordNames[@]}; i++)); do
		if [ ${coordMasterServers[$i]} != none ] && [ ${coordMasterServers[$i]} != N/A ]; then
			pgxc_init_coordinator_master ${coordNames[$i]}
		fi
	done
}

function pgxc_start_coordinator_master
{
	log_echo pgxc_start_coordinator_master'('$*')'
	# First argument is the coordinator name
    vecho ================================================================
    vecho Start coordinator master $1

	if [ $# -ne 1 ]; then
		eecho Specify coordinator name
		return 1
	fi
	local i
	for ((i=0; i< ${#coordNames[@]}; i++));	do
		if [ $1 == ${coordNames[$i]} ]; then
			log_echo psql -p ${coordPorts[$i]} -h ${coordMasterServers[$i]} postgres $pgxcOwner -c \'select 1\' '>' /dev/null '2>&1'
			psql -p ${coordPorts[$i]} -h ${coordMasterServers[$i]} postgres $pgxcOwner -c 'select 1' > /dev/null 2>&1
			if [ $? -eq 0 ]; then
				eecho ERROR: target coordinator master is running now.
				return 1
			fi
			log_echo ssh $pgxcUser@${coordMasterServers[$i]} '"'pg_ctl start -Z coordinator -D ${coordMasterDirs[$i]} -o -i '>' $tmpDir/coord.out '2>&1"'
			ssh $pgxcUser@${coordMasterServers[$i]} "pg_ctl start -Z coordinator -D ${coordMasterDirs[$i]} -o -i > $tmpDir/coord.out 2>&1"
			do_stdout ${coordMasterServers[$i]} $tmpDir/coord.out
			return
		fi
	done
	echo ERROR: specified coordinator is not configured, $1
	return 1
}

function pgxc_start_coordinator_master_all
{
	log_echo pgxc_start_coordinator_master_all'('$*')'
    vecho ================================================================
    vecho Start all the coordinator masters

	local i
	for ((i=0; i< ${#coordNames[@]}; i++)); do
		if [ ${coordMasterServers[$i]} != none ] && [ ${coordMasterServers[$i]} != N/A ]; then
			doit pgxc_monitor -Z node -p ${coordPorts[$i]} -h ${coordMasterServers[$i]} -U $pgxcOwner -d postgres
			if [ $? -eq 0 ]; then
				eecho coordinator master "("${coordNames[$i]}")" is already running
			else
				pgxc_start_coordinator_master ${coordNames[$i]}
			fi
		fi
	done
}

function pgxc_stop_coordinator_master
{
	log_echo pgxc_stop_coordinator_master'('$*')'
	# First arugument is the coordinator name
    vecho ================================================================
    vecho Stop coordinator master $1

	if [ $# -ne 1 ]; then
		eecho Specify coordinator name
		return 1
	fi
	local i
	for ((i=0; i< ${#coordNames[@]}; i++));	do
		if [ $1 == ${coordNames[$i]} ]; then
			doit ssh $pgxcUser@${coordMasterServers[$i]} pg_ctl stop -Z coordinator -D ${coordMasterDirs[$i]} $immediate
			return
		fi
	done
	eecho ERROR: specified coordinator does not exist, $1
	return 1
}

function pgxc_stop_coordinator_master_all
{
	log_echo pgxc_stop_coordinator_master_all'('$*')'
    vecho ================================================================
    vecho Stop all the coordinator masters

	local i
	for ((i=0; i< ${#coordNames[@]}; i++)); do
		if [ ${coordMasterServers[$i]} != none ] && [ ${coordMasterServers[$i]} != N/A ]; then
			pgxc_stop_coordinator_master ${coordNames[$i]}
		fi
	done
}


function pgxc_kill_coordinator_master
{
	log_echo pgxc_kill_coordinator_master'('$*')'
	# First arugument is the coordinator name

	# It's safer to kill the target coordinator with killall command.  In this case, we need to
	# capture postmaster's pid for the target
    vecho ================================================================
    vecho Kill coordinator master $1

	if [ $# -ne 1 ]; then
		eecho Specify nodename
		return 1
	fi
	local i
	local postmaster_pid
	for ((i=0; i< ${#coordNames[@]}; i++));	do
		if [ $1 == ${coordNames[$i]} ]; then
			if [ ${coordMasterServers[$i]} != none ] && [ ${coordMasterServers[$i]} != N/A ]; then
				postmaster_pid=`get_postmaster_pid ${coordMasterServers[$i]} ${coordMasterDirs[$i]}`
				if [ $postmaster_pid != none ]; then
					doit kill_all_child_parent ${coordMasterServers[$i]} $postmaster_pid
				fi
				doit pgxc_clean_socket ${coordMasterServers[$i]} ${coordPorts[$i]}
			else
				eecho specified coordinator master does not exist, $1
			fi
			return
		fi
	done
	eecho ERROR: specified coordinator does not exist, $1
	return 1
}

function pgxc_kill_coordinator_master_all
{
	log_echo pgxc_kill_coordinator_master_all'('$*')'
    vecho ================================================================
    vecho Start all the coordinator masters

	local i
	for ((i=0; i< ${#coordNames[@]}; i++)); do
		if [ ${coordMasterServers[$i]} != none ] && [ ${coordMasterServers[$i]} != N/A ]; then
			pgxc_kill_coordinator_master ${coordNames[$i]}
		fi
	done
}


# Caution: This function has not been tested yet!  Should test when datanode is ready.
# If a coordinator is not configured with the slave, we should remove it from the cluster
# when it fails.
function pgxc_remove_coordinator_master		# NOT TESTED YET
{
	log_echo pgxc_remove_coordinator_master'('$*')'
	local i
	for ((i=0; i<${#coordNames[@]}; i++)); do
		if [[ ${coordNames[$i]} == $1 ]]; then
			local j
			for ((j=0; j< ${#coordNames[@]}; j++)); do
				if [ $i -ne -$j ]; then
					if [ ${coordMasterServers[$j]} != none ] && [ ${coordMasterServers[$i]} != N/A ]; then
						log_echo psql -p ${coordPorts[$j]} -h ${coordMasterServers[$j]} postgres $pgxcOwner -c '"'DROP NODE ${coordNames[$j]}'"'
						psql -p ${coordPorts[$j]} -h ${coordMasterServers[$j]} postgres $pgxcOwner -c "DROP NODE ${coordNames[$j]}"
					fi
				else
					doit ssh $pgxcUser@${coordMasterServers[$j]} pg_ctl stop -Z coordinator -D ${coordMaseterDirs[$j]} -m immediate
				fi
			done
			${coordMasterServers[$i]}=none
			${coordMasterDirs[$i]}=none
			log_echo Update configuration file $configFile with new cordinaor Master
			cat >> $configFile <<EOF
#=========================================================
# Update due to coordinator master removal, $1, $datetime
coordMasterServers=(${coordMasterServers[@]})
coordMasterDirs=(${coordMasterDirs[@])
# End of update
EOF
			# Backup configiraiton file
			pgxc_backup_config_file
		fi
	done
}

# To construct coordinator slave, pg_basebackup utility is used, which needs master coordinator running.
# If the master coordinator is not running, then we temporary run it.   After copying the base backup,
# the the master will be stopped.  Please be sure that coordinator master is initialized properly.
# If it is running, then it will be restarted to reflect the change to postgresql.conf.
function pgxc_init_coordinator_slave
{
	log_echo pgxc_init_coordinator_slave'('$*')'
	# First argument is the coordinator name
    vecho ================================================================
    vecho Initialize coordinator slave $1

	if [ $# -ne 1 ]; then
		eecho Specify coordinator node name
		return 1
	fi
	if [ "$coordSlave" != "y" ]; then
		eecho No coordinator Slave is configured.
		return 1
	fi
	local i
	local start_master=n
	restart=n
	for ((i=0; i< ${#coordNames[@]}; i++));	do
		if [ $1 == ${coordNames[$i]} ]; then
			if [ ${coordSlaveServers[$i]} == N/A ] || [ ${coordSlaveServers[$i]} == none ]; then
				eecho ERROR: slave for the coordinator $1 is not configured.
				return 1
			fi
		    # Coordinator master should be running
			log_echo psql -p ${coordPorts[$i]} -h ${coordMasterServers[$i]} postgres $pgxcOwner -c \'select 1\' '>' /dev/null '2>&1'
			psql -p ${coordPorts[$i]} -h ${coordMasterServers[$i]} postgres $pgxcOwner -c 'select 1' > /dev/null 2>&1
			if [ $? -ne 0 ]; then
				start_master=y
			fi
  		    # Clean slave's directory
			doit ssh $pgxcUser@${coordSlaveServers[$i]} rm -rf ${coordSlaveDirs[$i]}
			doit ssh $pgxcUser@${coordSlaveServers[$i]} mkdir -p ${coordSlaveDirs[$i]}
			doit ssh $pgxcUser@${coordSlaveServers[$i]} chmod 0700 ${coordSlaveDirs[$i]}
		    # if the master is not running, we just start it and then stop it.
			if [ $start_master == y ]; then
				log_echo Starting the coordinator master to obtain base backup
				log_echo ssh $pgxcUser@${coordMasterServers[$i]} '"'pg_ctl start -Z coordinator -D ${coordMasterDirs[$i]} -o -i '>' $tmpDir/cmd.out'"'
				ssh $pgxcUser@${coordMasterServers[$i]} "pg_ctl start -Z coordinator -D ${coordMasterDirs[$i]} -o -i > $tmpDir/cmd.out"
				do_stdout ${coordMasterServers[$i]} $tmpDir/cmd.out
				sleep 2
			fi
		    # Obtain base backup of the master
			doit ssh $pgxcUser@${coordSlaveServers[$i]} pg_basebackup -p ${coordPorts[$i]} -h ${coordMasterServers[$i]} -D ${coordSlaveDirs[$i]} -x

		    # Configure recovery.conf of the slave
			vecho -- Configure slave\'s recovery.conf, ${coordSlaveServers[$i]}:${coordSlaveDirs[$i]}/recovery.conf
			log_echo ssh $pgxcUser@$coordSlaveServers[$i]} '"'cat '>>' ${coordSlaveDirs[$i]}/recovery.conf'"'
			ssh $pgxcUser@${coordSlaveServers[$i]} "cat >> ${coordSlaveDirs[$i]}/recovery.conf" <<EOF
#==========================================
# Added to initialize the slave, $datetime
standby_mode = on
primary_conninfo = 'host = ${coordMasterServers[$i]} port = ${coordPorts[$i]} user = $pgxcOwner application_name = ${coordNames[$i]}'
restore_command = 'cp ${coordArchLogDirs[$i]}/%f %p'
archive_cleanup_command = 'pg_archivecleanup ${coordArchLogDirs[$i]} %r'
EOF
		    # Configure slave's postgresql.conf
			vecho -- Configure slave\'s postgresql.conf, ${coordSlaveServers[$i]}:${coordSlaveDirs[$i]}/postgresql.conf
			log_echo ssh $pgxcUser@${coordSlaveServers[$i]} '"'cat '>>' ${coordSlaveDirs[$i]}/postgresql.conf'"'
			ssh $pgxcUser@${coordSlaveServers[$i]} "cat >> ${coordSlaveDirs[$i]}/postgresql.conf" <<EOF
#==========================================
# Added to initialize the slave, $datetime
hot_standby = on
port = ${coordPorts[$i]}
EOF
		    # Stop the Master if it was not runnig
			if [ $start_master = y ]; then
				doit ssh $pgxcUser@${coordMasterServers[$i]} pg_ctl stop -Z coordinator -D ${coordMasterDirs[$i]} -m fast
			fi
			return
		fi
	done
	eecho ERROR: specified coordinator is not configured, $1
	return 1
}

function pgxc_init_coordinator_slave_all
{
	log_echo pgxc_init_coordinator_slave_all'('$*')'
	# First argument is the coordinator name
    vecho ================================================================
    vecho Initialize all the coordinator slaves

	local i
	if [ $coordSlave != y ]; then
		eecho Coordinator slaves are not configured.
		return 1
	fi
	for ((i=0;i<${#coordNames[@]};i++)); do
		if [ ${coordNames[$i]} != none ] && [ ${coordNames[$i]} != N/A ]; then
			log_echo psql -p ${coordPorts[$i]} -h ${coordSlaveServers[$i]} postgres $pgxcOwner -c \'select 1\' '>' /dev/null '2>&1'
			psql -p ${coordPorts[$i]} -h ${coordSlaveServers[$i]} postgres $pgxcOwner -c 'select 1' > /dev/null 2>&1
			if [ $? -eq 0 ]; then
				eecho Coordinator slave "("${coordNames[$i]}")" is already running.
				return 1
			fi
			pgxc_init_coordinator_slave ${coordNames[$i]}
		fi
	done
	return
}

function pgxc_start_coordinator_slave
{
	log_echo pgxc_start_coordinator_slave'('$*')'
	# First argument is the coordinator name
    vecho ================================================================
    vecho Start coordinator slave $1

	if [ $coordSlave != y ]; then
		eecho Coordinator slaves are not configured.
		return 1
	fi
	local i
	for ((i=0; i< ${#coordNames[@]}; i++));	do
		if [ $1 == ${coordNames[$i]} ]; then
			if [ ${coordSlaveServers[$i]} == none ] || [ ${coordSlaveServers[$i]} == N/A ]; then
				eecho ERROR: slave for coordinator $1 is not configured.
				return 1
			fi
		  	# Coordinator master should be running
			log_echo psql -p ${coordPorts[$i]} -h ${coordMasterServers[$i]} postgres $pgxcOwner -c \'select 1\' '>' /dev/null '2>&1'
			psql -p ${coordPorts[$i]} -h ${coordMasterServers[$i]} postgres $pgxcOwner -c 'select 1' > /dev/null 2>&1
			if [ $? -ne 0 ]; then
				eecho ERROR: corresponding coordinator master is not running now, $1
				return 1
			fi
		  	# Start the slave
			log_echo ssh $pgxcUser@${coordSlaveServers[$i]} '"'pg_ctl start -Z coordinator -D ${coordSlaveDirs[$i]} -o -i '>' $tmpDir/coord.out'"'
			ssh $pgxcUser@${coordSlaveServers[$i]} "pg_ctl start -Z coordinator -D ${coordSlaveDirs[$i]} -o -i > $tmpDir/coord.out"
			do_stdout ${coordSlaveServers[$i]} $tmpDir/coord.out
		  	# Change the master to synchronous mode
			vecho Change the master to synchrnous mode, $1
			log_echo ssh $pgxcUser@${coordMasterServers[$i]} '"'cat '>>' ${coordMasterDirs[$i]}/postgresql.conf'"'
			ssh $pgxcUser@${coordMasterServers[$i]} "cat >> ${coordMasterDirs[$i]}/postgresql.conf" <<EOF
#==========================================================
# Added to start the slave in sync. mode, $datetime
synchronous_commit = on
synchronous_standby_names = '${coordNames[$i]}'
# End of the addition
EOF
			doit ssh $pgxcUser@${coordMasterServers[$i]} pg_ctl reload -Z coordinator -D ${coordMasterDirs[$i]}
			return
		fi
	done
	eecho ERROR: specified coordinator does not exist, $1
	return 1
}

function pgxc_start_coordinator_slave_all
{
	# First argument is the coordinator name
    vecho ================================================================
    vecho Start all the coordinator slaves

	if [ $coordSlave != y ]; then
		eecho Coordinator slaves are not configured.
		return 1
	fi
	local i
	for ((i=0; i<${#coordNames[@]}; i++)); do
		if [ ${coordNames[$i]} != none ] && [ ${coordNames[$i]} != N/A ]; then
			pgxc_start_coordinator_slave ${coordNames[$i]}
		fi
	done
}

function pgxc_stop_coordinator_slave
{
	log_echo pgxc_stop_coordinator_slave'('$*')'
	# First argument is the coordinator name
    vecho ================================================================
    vecho Stop coordinator slave $1

	if [ $coordSlave != y ]; then
		eecho Coordinator slaves are not configured.
		return 1
	fi
	if [ $# -ne 1 ]; then
		eecho Specify coordinator node name
		return 1
	fi

	local i
	for ((i=0; i< ${#coordNames[@]}; i++));	do
		if [ $1 == ${coordNames[$i]} ]; then
			if [ ${coordSlaveServers[$i]} == none ] || [ ${coordSlaveServers[$i]} == N/A ]; then
				eecho ERROR: slave for the coordinator $1 is not configured.
				return 1
			fi
			# If the master is running, master's switch replication to asynchronous mode.
			log_echo psql -p ${coordPorts[$i]} -h ${coordMasterServers[$i]} postgres $pgxcOwner -c \'select 1\' '>' /dev/null '2>&1'
			psql -p ${coordPorts[$i]} -h ${coordMasterServers[$i]} postgres $pgxcOwner -c 'select 1' > /dev/null 2>&1
			if [ $? -eq 0 ]; then
			    # Switch Master to asynchronous mode.
				vecho Switching master of $1 at ${coordMasterServer[$i]} to asynchronous replication mode.
				log_echo ssh $pgxcUser@${coordMasterServers[$i]} '"'cat '>>' ${coordMasterDirs[$i]}/postgresql.conf'"'
				ssh $pgxcUser@${coordMasterServers[$i]} "cat >> ${coordMasterDirs[$i]}/postgresql.conf" <<EOF
#=======================================
# Updated to trun off the slave $datetime
synchronous_standby_names = ''
# End of the update
EOF
				doit ssh $pgxcUser@${coordMasterServers[$i]} pg_ctl reload -Z coordinator -D ${coordMasterDirs[$i]}
			fi
			doit ssh $pgxcUser@${coordSlaveServers[$i]} pg_ctl stop -Z coordinator -D ${coordSlaveDirs[$i]} $immediate
			return;
		fi
	done
	eecho ERROR: Specified coordinator was not configured, $1
	return 1
}

function pgxc_stop_coordinator_slave_all
{
	log_echo pgxc_stop_coordinator_slave_all'('$*')'
	# First argument is the coordinator name
    vecho ================================================================
    vecho Stop all the coordinator slaves

	if [ $coordSlave != y ]; then
		eecho Coordinator slaves are not configured.
		return 1
	fi
	local i
	for ((i=0; i<${#coordNames[@]}; i++)); do
		if [ ${coordNames[$i]} != none ] && [ ${coordNames[$i]} != N/A ]; then
			pgxc_stop_coordinator_slave ${coordNames[$i]}
		fi
	done
}

function pgxc_kill_coordinator_slave
{
	log_echo pgxc_kill_coordinator_slave'('$*')'
	# First arugument is the coordinator name

	# It's safer to kill the target coordinator with killall command.  In this case, we need to
	# capture postmaster's pid for the target
    vecho ================================================================
    vecho Kill coordinator master $1

	if [ $coordSlave != y ]; then
		eecho Coordinator slaves are not configured.
		return 1
	fi
	if [ $# -ne 1 ]; then
		eecho Specify nodename
		return 1
	fi
	local i
	local postmaster_pid
	for ((i=0; i< ${#coordNames[@]}; i++));	do
		if [ $1 == ${coordNames[$i]} ]; then
			if [ ${coordSlaveServers[$i]} != none ] && [ ${coordSlaveServers[$i]} != N/A ]; then
				postmaster_pid=`get_postmaster_pid ${coordSlaveServers[$i]} ${coordSlaveDirs[$i]}`
				if [ $postmaster_pid != none ]; then
					doit kill_all_child_parent ${coordSlaveServers[$i]} $postmaster_pid
				fi
				doit pgxc_clean_socket ${coordSlaveServers[$i]} ${coordPorts[$i]}
			else
				eecho specified coordinator slave does not exist, $1
			fi
			return
		fi
	done
	eecho ERROR: specified coordinator does not exist, $1
	return 1
}	

# This failover function assumes that no IP address is carried over from corresponding master server.
# If IP address can be carried over, then you don't need a section which issues ALTER NODE statement.
# Instead, you should disable the slave.
function pgxc_failover_coordinator
{
	log_echo pgxc_failover_coordinator'('$*')'
	local fn=pgxc_failover_coordinator

	# First argument is the coordinator name
	decho called: $fn $*
    vecho ================================================================
    vecho Failover coordinator $1

	if [ $# -ne 1 ]; then
		eecho $progname:$fn Error: Specify coordinator name to failover
		return 1
	fi
	if [ $coordSlave != y ]; then
		eecho $progname:$fn No coordinator slaves are configured. Cannot failover
		return 1
	fi

	local i
	for ((i=0; i< ${#coordNames[@]}; i++));	do
		if [ $1 == ${coordNames[$i]} ]; then
			if [ ${coordSlaveServers[$i]} == none ] || [ ${coordSlaveServers[$i]} == N/A ]; then
				eecho $progname:$fn: ERROR, Slave for the coordinator $1 is not configured.  Cannot failover.
				return 1
			fi
			decho "Target coordinator slave to failover:" ${coordNames[$i]} "at" ${coordSlaveServers[$i]}
		    # Find the new local gtm_proxy
			local j
			local targetGTMhost
			local targetGTMport
			targetGTMhost=none
			targetGTMport=0
			for ((j=0; j<${#gtmProxyServers[@]}; j++)); do
				if [ ${coordSlaveServers[$i]} == ${gtmProxyServers[$j]} ]; then
					targetGTMhost=${gtmProxyServers[$j]}
					targetGTMport=${gtmProxyPorts[$j]}
					break
				fi
			done
		    # gtm_proxy has to be configured properly
		    # This can be a bit more flexible so that each component can connect to GTM directly if
		    # gtm_proxy is not configured locally.
			decho "New GTM Proxy:" "$targetGTMHost":"$targetGTMPort"
			if [ "$targetGTMhost" == none ]; then
				eecho $progname:$fn: ERROR, gtm_proxy is not configured at the server ${coordSlaveServers[$i]}.  Cannot failover.
				return 1
			fi
		    # Now promote the slave
			vecho $0: Promoting coordinator slave at ${coordSlaveServers[$i]}:${coordSlaveDirs[$i]}
			doit gtm_util unregister -Z coordinator -p $gtmMasterPort -h gtmMasterServer ${coordNames[$i]}
			# doit ssh $pgxcUser@$gtmMasterServer rm -f $gtmMasterDir/register.node
			doit ssh $pgxcUser@${coordSlaveServers[$i]} pg_ctl promote -Z coordinator -D ${coordSlaveDirs[$i]}
			ssh $pgxcUser@${coordSlaveServers[$i]} rm -rf $tmpDir/cmd.out
			vecho done
		    # Restart the new master with new gtm_proxy
			# The following command is a dirty hack to unregister the old master.   This is only one way available now, but
			# the next version of the core should include an utility to clean it up partially.
		    # Reconfigure new master's gtm_proxy
			vecho Reconfiguring new gtm_proxy for ${coordSlaveServers[$i]}:${coordSlaveDirs[$i]}/postgresql.conf
			log_echo ssh $pgxcUser@${coordSlaveServers[$i]} '"'cat '>>' ${coordSlaveDirs[$i]}/postgresql.conf'"'
			ssh $pgxcUser@${coordSlaveServers[$i]} "cat >> ${coordSlaveDirs[$i]}/postgresql.conf" <<EOF
#=================================================
# Added to promote, $datetime
gtm_host = '$targetGTMhost'
gtm_port = $targetGTMport
# End of addition
EOF
			vecho done
			# doit ssh $pgxcUser@${coordSlaveServers[$i]} pg_ctl stop -w -Z coordinator -D ${coordSlaveDirs[$i]} -o -i -m immediate
			vecho Restarting ${coordNames[$i]} at ${coordSlaveServers[$i]}
			log_echo ssh $pgxcUser@${coordSlaveServers[$i]} pg_ctl restart -Z coordinator -D ${coordSlaveDirs[$i]} -w -o -i '>' $localTmpDir/cmd.out '2>&1'
			ssh $pgxcUser@${coordSlaveServers[$i]} pg_ctl restart -Z coordinator -D ${coordSlaveDirs[$i]} -w -o -i > $localTmpDir/cmd.out 2>&1 &
			sleep 1
			doit cat $localTmpDir/cmd.out
			doit rm -f $localTmpDir/cmd.out

            # Update the configuration variable
			coordMasterServers[$i]="${coordSlaveServers[$i]}"
			coordMasterDirs[$i]="${coordSlaveDirs[$i]}"
			coordSlaveServers[$i]="N/A"
			coordSlaveDirs[$i]="N/A"
		    # Then update the configuration file with this new configuration
			log_echo cat '>>' $configFile
			cat >> $configFile <<EOF
#=====================================================
# Updated due to the coordinator failover, $1, $datetime
coordMasterServers=( ${coordMasterServers[@]} )
coordMasterDirs=( ${coordMasterDirs[@]} )
coordSlaveServers=( ${coordSlaveServers[@]} )
coordSlaveDirs=( ${coordSlaveDirs[@]} )
# End of the update
EOF
			# Backup configration file
			pgxc_backup_config_file
		    # Update other coordinators with this new one ---> first, clean connection for all the users for all the databases
		    # Get all the available users --> Use this coordinator to get usernam
			# It may be better to clean connections.  However, we found that clean connection is not stable enough when some node
			# is gone.   We will wait until it is more stable.
			# It is not clean but I'd like to leave these code for future improvement.
			vecho Update other coordinators with new coordinator configuration.
			vecho Clean all the pooler connections and update the node configuration
			for ((j=0; j< ${#coordMasterServers[@]}; j++)); do
				if [ "${coordMasterServers[$j]}" != none ] && [ "${coordMasterServers[$j]}" != N/A ]; then
					doit pgxc_monitor -Z node -p ${coordPorts[$j]} -h ${coordMasterServers[$j]} -U $pgxcOwner -d postgres
					if [ $? -ne 0 ]; then
						eecho Coordinator ${coordNames[$j]} is not running.   Skip reconfiguration for this.
						continue;
					fi
					# The following code section seemed to be necessary.   However, when some node fails, the current Postgres-XC does not work well and
					# it seems to be okay practically to skip it.
					#
					#for user in $users; do
						#if [ $j != $i ]; then
							#vecho cleaning connection on ${coordMasterServers[$j]} for user $user
							#psql -p ${coordPorts[$j]} -h ${coordMasterServers[$j]} postgres $pgxcOwner -c "CLEAN CONNECTION TO ALL TO USER $user" > /dev/null 2>&1
						#fi
					#done
					#
					# Issue ALTER NODE.  If target coordinator is not running, we have no way to do this now.   May need to run this afterwords.
					# Store the script to elsewhere?  --> Now we don't do this.   May be for further work because we expect everything except
					# for the current coordinator is running healthy.
					cat > $localTmpDir/cmd.sql <<EOF
ALTER NODE ${coordNames[$i]} WITH (HOST='${coordMasterServers[$i]}', PORT=${coordPorts[$i]});
select pgxc_pool_reload();
\q
EOF
					cat $localTmpDir/cmd.sql
					doit psql -p ${coordPorts[$j]} -h ${coordMasterServers[$j]} postgres $pgxcOwner -f $localTmpDir/cmd.sql
					rm -f $localTmpDir/cmd.sql
				fi
			done
			return;
		fi
	done
	eecho ERROR: specified coordinator $1 not configured.
	return 2
}

# $1: database name
function pgxc_clean_connection_all
{
	log_echo pgxc_clean_connection_all'('$*')'
	local fn=pgxc_clean_connection
	local i
	if [ $# -le 0 ]; then
		eecho $progname:$fn no database name specified
		return 2
	fi
	for ((i=0; i<${#coordNames[@]}; i++)); do
		if [ "${coordMasterServers[$i]}" == "none" ] || [ "${coordMasterServers[$i]}" == "N/A" ]; then
			continue;
		fi
		log_echo psql -p ${coordPorts[$i]} -h ${coordMasterServers[$i]} postgres $pgxcOwner -c '"'CLEAN CONNECTION TO ALL FOR DATABASE $1'"'
		psql -p ${coordPorts[$i]} -h ${coordMasterServers[$i]} postgres $pgxcOwner -c "CLEAN CONNECTION TO ALL FOR DATABASE $1"
	done
}

#===============================================================================
#
# Datanode staff
#
#===============================================================================

function pgxc_init_datanode_master
{
	log_echo pgxc_init_datanode_master'('$*')'
	# First argument is the nodename
    vecho ================================================================
    vecho Initialize datanode master $1

	if [ $# -ne 1 ]; then
		eecho Specify datanode name
		return 1
	fi
	local i
	for ((i=0; i< ${#datanodeNames[@]}; i++)); do
		if [ $1 == ${datanodeNames[$i]} ] && [ ${datanodeMasterServers[$i]} != none ] && [ ${datanodeMasterServers[$i]} != N/A ]; then
			psql -p ${datanodePorts[$i]} -h ${datanodeMasterServers[$i]}  -c 'select 1' postgres $pgxcOwner > /dev/null 2> /dev/null
			if [ $? -eq 0 ]; then
				eecho ERROR: target coordinator master is running now.  Stop it to configure.
				return 1
			fi
			doit ssh $pgxcUser@${datanodeMasterServers[$i]} rm -rf ${datanodeMasterDirs[$i]}
			doit ssh $pgxcUser@${datanodeMasterServers[$i]} mkdir -p ${datanodeMasterDirs[$i]}
			doit ssh $pgxcUser@${datanodeMasterServers[$i]} "initdb --nodename ${datanodeNames[$i]} -D ${datanodeMasterDirs[$i]} > $tmpDir/initdb.out 2>&1"
			print_initdb_out ${datanodeMasterServers[$i]} $tmpDir/initdb.out
			vecho Configuring ${datanodeMasterServers[$i]}:${datanodeMasterDirs[$i]}/postgresql.conf
		    # Get effective GTM port and host.   If gtm_proxy is not found, then connect to GTM
			local j
			local targetGTMhost
			local targetGTMport
			targetGTMhost=$gtmMasterServer
			targetGTMport=$gtmMasterPort
			for ((j=0; j< ${#gtmProxyServers[@]}; j++)); do
				if [ ${datanodeMasterServers[$i]} == ${gtmProxyServers[$j]} ]; then
					targetGTMhost=${gtmProxyServers[$j]}
					targetGTMport=${gtmProxyPorts[$j]}
					break
				fi
			done
			if [ $datanodeExtraConfig != none ] && [ $datanodeExtraConfig != N/A ]; then
				vecho Configuring $pgxcUser@${datanodeMasterServer[$i]}:${datanodeMasterDirs[$i]}/postgresql.conf using $datanodeExtraConfig
				log_echo ssh $pgxcUser@${datanodeMasterServers[$i]} '"'cat '>>' ${datanodeMasterDirs[$i]}/postgresql.conf'"' '<' $datanodeExtraConfig
				ssh $pgxcUser@${datanodeMasterServers[$i]} "cat >> ${datanodeMasterDirs[$i]}/postgresql.conf" < $datanodeExtraConfig
			fi
			if [ ${datanodeSpecificExtraConfig[$i]} != none ] && [ ${datanodeSpecificExtraConfig[$i]} != none ]; then
				vecho Configuring $pgxcUser@${datanodeMasterServers[$i]}:${datanodeMasterDirs[$i]}/postgresql.conf using ${datanodeSpecificExtraConfig[$i]}
				log_echo ssh $pgxcUser@${datanodeMasterServers[$i]} '"'cat '>>' ${datanodeMasterDirs[$i]}/postgresql.conf'"' '<' ${datanodeSpecificExtraConfig[$i]}
				ssh $pgxcUser@${datanodeMasterServers[$i]} "cat >> ${datanodeMasterDirs[$i]}/postgresql.conf" < ${datanodeSpecificExtraConfig[$i]}
			fi
			vecho Configuring  $pgxcUser@${datanodeMasterServers[$i]}:${datanodeMasterDirs[$i]}/postgresql.conf
			log_echo ssh $pgxcUser@${datanodeMasterServers[$i]} '"'cat '>>' ${datanodeMasterDirs[$i]}/postgresql.conf'"'
			ssh $pgxcUser@${datanodeMasterServers[$i]} "cat >> ${datanodeMasterDirs[$i]}/postgresql.conf" <<EOF
#===========================================
# Added at initialization. $datetime
log_destination = 'stderr'
logging_collector = on
log_directory = 'pg_log'
listen_addresses = '*'
port = ${datanodePorts[$i]}
max_connections = 100
gtm_host = '$targetGTMhost'
gtm_port = $targetGTMport
EOF
			# Additional initialization for log_shipping.
			if [ $datanodeSlave == y ] && [ ${datanodeSlaveServers[$i]} != none ] && [ ${datanodeSlaveServers[$i]} != N/A ]; then
				# At least cleanup remote archive directory.
				doit pgxc_clean_dir ${datanodeSlaveServers[$i]} ${datanodeArchLogDirs[$i]}
				# Then setup postgresql.conf
				log_echo ssh $pgxcUser@${datanodeMasterServers[$i]} '"'cat '>>' ${datanodeMasterDirs[$i]}/postgresql.conf'"'
				ssh $pgxcUser@${datanodeMasterServers[$i]} "cat >> ${datanodeMasterDirs[$i]}/postgresql.conf" <<EOF
wal_level = hot_standby
archive_mode = on
archive_command = 'rsync %p $pgxcUser@${datanodeSlaveServers[$i]}:${datanodeArchLogDirs[$i]}/%f'
max_wal_senders = ${datanodeMaxWalSenders[$i]}
# End of Addition
EOF
			else
				log_echo ssh $pgxcUser@${datanodeMasterServers[$i]} '"'cat '>>' ${datanodeMasterDirs[$i]}/postgresql.conf'"'
				ssh $pgxcUser@${datanodeMasterServers[$i]} "cat >> ${datanodeMasterDirs[$i]}/postgresql.conf" <<EOF
# End of Addition
EOF
			fi
			vecho Configuring ${cdatanodeMasterServers[$i]}:${datanodeMasterDirs[$i]}/pg_hba.conf
			log_echo ssh $pgxcUser@${datanodeMasterServers[$i]} '"'cat '>>' ${coordMasterDirs[$i]}/pg_hba.conf'"'
			ssh $pgxcUser@${datanodeMasterServers[$i]} "cat >> ${coordMasterDirs[$i]}/pg_hba.conf"  <<EOF
#=================================================
# Addition at initialization, $datetime 
EOF
			if [ $datanodeExtraPgHba != none ] && [ $datanodeExtraPgHba != N/A ]; then
				vecho Configuring ${datanodeMasterServers[$i]}:${datanodeMasterDirs[$i]}/pg_hba.conf using $datanodeExtraPgHba
				log_echo ssh $pgxcUser@${datanodeMasterServers[$i]} '"'cat '>>' ${datanodeMasterDirs[$i]}/pg_hab.conf'"' '<' $datanodeExtraPgHba
				ssh $pgxcUser@${datanodeMasterServers[$i]} "cat >> ${datanodeMasterDirs[$i]}/pg_hab.conf" < $datanodeExtraPgHba
			fi
			if [ ${datanodeSpecificExtraPgHba[$i]} != none ] && [ ${datanodeSpecificExtraPgHba[$i]} != N/A ]; then
				log_echo ssh $pgxcUser@${datanodeMasterServers[$i]} '"'cat '>>' ${datanodeMasterDirs[$i]}/pg_hab.conf'"' '<' ${datanodeSpecificExtraPgHba[$i]}
				ssh $pgxcUser@${datanodeMasterServers[$i]} "cat >> ${datanodeMasterDirs[$i]}/pg_hab.conf" < ${datanodeSpecificExtraPgHba[$i]}
			fi
			local j
			for ((j=0; j< ${#datanodePgHbaEntries[@]}; j++)); do
				log_echo ssh $pgxcUser@${datanodeMasterServers[$i]} '"'cat '>>' ${datanodeMasterDirs[$i]}/pg_hba.conf'"'
				ssh $pgxcUser@${datanodeMasterServers[$i]} "cat >> ${datanodeMasterDirs[$i]}/pg_hba.conf" <<EOF
host all $pgxcOwner ${datanodePgHbaEntries[$j]} trust
EOF
				if [ ${datanodeSlaveServers[$i]} != none ] && [ ${datanodeSlaveServers[$i]} != N/A ]; then
					vecho ssh $pgxcUser@${datanodeMasterServers[$i]} "cat >> ${datanodeMasterDirs[$i]}/pg_hba.conf"
					ssh $pgxcUser@${datanodeMasterServers[$i]} "cat >> ${datanodeMasterDirs[$i]}/pg_hba.conf"  <<EOF
host replication $pgxcOwner ${datanodePgHbaEntries[$j]} trust
EOF
				fi
			done
			log_echo ssh $pgxcUser@${datanodeMasterServers[$i]} '"'cat '>>' ${datanodeMasterDirs[$i]}/pg_hba.conf'"'
			ssh $pgxcUser@${datanodeMasterServers[$i]} "cat >> ${datanodeMasterDirs[$i]}/pg_hba.conf"  <<EOF
# End of addition
EOF
			return
		fi
	done
	eecho ERROR: specified coordinator is not configured, $1
	return 1
}

function pgxc_init_datanode_master_all
{
	log_echo pgxc_init_datanode_master_all'('$*')'
    vecho ================================================================
    vecho Initialize all the datanode masters

	local i
	for ((i=0; i< ${#datanodeNames[@]}; i++)); do
		if [ ${datanodeMasterServers[$i]} != none ] && [ ${datanodeMasterServers[$i]} != N/A ]; then
			pgxc_init_datanode_master ${datanodeNames[$i]}
		fi
	done
}

function pgxc_start_datanode_master
{
	log_echo pgxc_start_datanode_master'('$*')'
	# First argument is the nodename
    vecho ================================================================
    vecho Start datanode master $1

	if [ $# -ne 1 ];then
		eecho ERROR: specify datanode name
		return 1
	fi
	local i
	for ((i=0; i< ${#datanodeNames[@]}; i++)); do
		if [ $1 == ${datanodeNames[$i]} ]; then
			log_echo psql -p ${datanodePorts[$i]} -h ${datanodeMasterServers[$i]} postgres $pgxcOwner -c \'select 1\' '>' /dev/null '2>&1'
			psql -p ${datanodePorts[$i]} -h ${datanodeMasterServers[$i]} postgres $pgxcOwner -c 'select 1' > /dev/null 2>&1
			if [ $? -eq 0 ]; then
				eecho ERROR: target datanode master is running now.
				return 1
			fi
			doit ssh $pgxcUser@${datanodeMasterServers[$i]} "pg_ctl start -Z datanode -D ${datanodeMasterDirs[$i]} -o -i > $tmpDir/datanode.out"
			do_stdout ${datanodeMasterServers[$i]} $tmpDir/datanode.out
			return
		fi
	done
	eecho ERROR: specified datanode is not configured, $1
	return 1
}

function pgxc_start_datanode_master_all
{
	log_echo pgxc_start_datanode_master_all'('$*')'
    vecho ================================================================
    vecho Start all the datanode masters
	
	local i
	for ((i=0;i<${#datanodeNames[@]};i++));do
		pgxc_start_datanode_master ${datanodeNames[$i]}
	done
}

function pgxc_stop_datanode_master
{
	log_echo pgxc_stop_datanode_master'('$*')'
	# First argument is the nodename
    vecho ================================================================
    vecho Stop datanode master $1

	if [ $# -ne 1 ]; then
		eecho ERROR: specify datanode name
		return 1
	fi
	local i
	for ((i=0; i< ${#datanodeNames[@]}; i++)); do
		if [ $1 == ${datanodeNames[$i]} ];  then
			doit ssh $pgxcUser@${datanodeMasterServers[$i]} pg_ctl stop -Z datanode -D ${datanodeMasterDirs[$i]} $immediate
			return
		fi
	done
	eecho ERROR: specified coordinator does not exist, $1
	return 1
}

function pgxc_stop_datanode_master_all
{
	log_echo pgxc_stop_datanode_master_all'('$*')'
    vecho ================================================================
    vecho Stop all the datanode master

	local i
	for ((i=0;i<${#datanodeNames[@]};i++));do
		pgxc_stop_datanode_master ${datanodeNames[$i]}
	done
}

function pgxc_kill_datanode_master
{
	log_echo pgxc_kill_datanode_master'('$*')'
	# First arugument is the nodename
    vecho ================================================================
    vecho Kill coordinator master $1

	if [ $# -ne 1 ]; then
		eecho ERROR: specify datanode name
		return 1
	fi
	local i
	for ((i=0; i< ${#datanodeNames[@]}; i++)); do
		if [ $1 == ${datanodedNames[$i]} ]; then
			if [ ${datanodeMasterServers[$i]} != none ] && [ ${datanodeMasterServers[$i]} != N/A ]; then
				postmaster_pid=`get_postmaster_pid ${datanodeMasterServers[$i]} ${datandoeMasterDirs[$i]}`
				if [ $postmaster_pid != none ]; then
					doit kill_all_child_parent ${datanodeMasterServers[$i]} $postmaster_pid
				fi
				doit pgxc_clean_socket ${datanodeMasterServers[$i]} ${datanodePorts[$i]}
			else
				eecho ERROR: could not find specified coordinator master, $1
			fi
			return
		fi
	done
	eecho ERROR: specified coordinator does not exist, $1
	return 1
}

function pgxc_kill_datanode_master_all
{
	log_echo pgxc_kill_datanode_master_all'('$*')'
    vecho ================================================================
    vecho Kill all the datanode master

	local i
	for ((i=0;i<${#datanodeNames[@]};i++));do
		pgxc_kill_datanode_master ${datanodeNames[$i]}
	done
}

# To construct datanode slave, pg_basebackup utility is used, which needs master coordinator running.
# If the master is not running, then we temporary run it.   After copying the base backup,
# the the master will be stopped.  Please be sure that coordinator master is initialized properly.
# If it is running, then it will be restarted to reflect the change to postgresql.conf.
function pgxc_init_datanode_slave
{
	log_echo pgxc_init_datanode_slave'('$*')'
	# First argument is the datanode name
    vecho ================================================================
    vecho Initialize datanode slave $1

	if [ $# -ne 1 ]; then
		eecho ERROR: specify coordinator node name
		return 1;
	fi
	if [ "$datanodeSlave" != "y" ]; then
		eecho No datanode slave is configured.
		return 1
	fi

	local i
	local start_master=n
	for ((i=0;i<${#datanodeNames[@]};i++)); do
		if [ $1 == ${datanodeNames[$i]} ]; then
			if [ ${datanodeSlaveServers[$i]} == N/A ]; then
				eecho ERROR: slave for the datanode $1 is not configured.
				return 1
			fi
		    # Datanode master should be running
			log_echo psql -p ${datanodePorts[$i]} -h ${datanodeMasterServers[$i]} postgres $pgxcOwner -c \'select 1\' '>' /dev/null 2>&1
			psql -p ${datanodePorts[$i]} -h ${datanodeMasterServers[$i]} postgres $pgxcOwner -c 'select 1' > /dev/null 2>&1
			if [ $? -ne 0 ]; then
				start_master=y
			fi
		    # Clean slave's directory
			doit ssh $pgxcUser@${datanodeSlaveServers[$i]} rm -rf ${datanodeSlaveDirs[$i]}
			doit ssh $pgxcUser@${datanodeSlaveServers[$i]} mkdir -p ${datanodeSlaveDirs[$i]}

		    # if the master is not running, we just start it and then stop it.
			if [ $start_master == y ]; then
				vecho Starting the datanode master to obtain base backup
				doit ssh $pgxcUser@${datanodeMasterServers[$i]} "pg_ctl start -Z datanode -D ${datanodeMasterDirs[$i]} -o -i > $tmpDir/cmd.out"
				do_stdout ${datanodeMasterServers[$i]} $tmpDir/cmd.out
				sleep 2
			fi
		    # Obtain base backup of the master
			doit pgxc_clean_dir ${datanodeSlaveServers[$i]} ${datanodeSlaveDirs[$i]}
			doit ssh $pgxcUser@${datanodeSlaveServers[$i]} pg_basebackup -p ${datanodePorts[$i]} -h ${datanodeMasterServers[$i]} -D ${datanodeSlaveDirs[$i]} -x

		    # Configure recovery.conf of the slave
			vecho -- Configure slave\'s recovery.conf, ${datanodeSlaveServers[$i]}:${datanodeSlaveDirs[$i]}/recovery.conf
			log_echo ssh $pgxcUser@${datanodeSlaveServers[$i]} '"'cat '>>' ${datanodeSlaveDirs[$i]}/recovery.conf'"'
			ssh $pgxcUser@${datanodeSlaveServers[$i]} "cat >> ${datanodeSlaveDirs[$i]}/recovery.conf" <<EOF
#==========================================
# Added to initialize the slave, $datetime
standby_mode = on
primary_conninfo = 'host = ${datanodeMasterServers[$i]} port = ${datanodePorts[$i]} user = $pgxcOwner application_name = ${datanodeNames[$i]}'
restore_command = 'cp ${datanodeArchLogDirs[$i]}/%f %p'
archive_cleanup_command = 'pg_archivecleanup ${datanodeArchLogDirs[$i]} %r'
EOF

		    # Configure slave's postgresql.conf
			vecho -- Configure slave\'s postgresql.conf, ${doatanodeSlaveServers[$i]}:${datanodeSlaveDirs[$i]}/postgresql.conf
			log_echo ssh $pgxcUser@${datanodeSlaveServers[$i]} '"'cat '>>' ${datanodeSlaveDirs[$i]}/postgresql.conf'"'
			ssh $pgxcUser@${datanodeSlaveServers[$i]} "cat >> ${datanodeSlaveDirs[$i]}/postgresql.conf" <<EOF
#==========================================
# Added to startup the slave, $dtetime
hot_standby = on
port = ${datanodePorts[$i]}
EOF
			if [ $start_master == y ]; then
				vecho Stopping the datanode master.
				doit ssh $pgxcUser@${datanodeMasterServers[$i]} "pg_ctl stop -Z datanode -D ${datanodeMasterDirs[$i]} > /dev/null 2>&1"
			fi
			return
		fi
	done
	eecho ERROR: specified coordinator is not configured, $1
	return 1
}

function pgxc_init_datanode_slave_all
{
	log_echo pgxc_init_datanode_slave_all'('$*')'
    vecho ================================================================
    vecho Initialize all the datanode slaves

	local i
	for ((i=0; i< ${#datanodeNames[@]}; i++)); do
		if [ ${datanodeSlaveServers[$i]} != none ] && [ ${datanodeSlaveServers[$i]} != N/A ]; then
			pgxc_init_datanode_slave ${datanodeNames[$i]}
		fi
	done
}

function pgxc_start_datanode_slave
{
	log_echo pgxc_start_datanode_slave'('$*')'
	# First argument is the datanode name
    vecho ================================================================
    vecho Start datanode slave $1

	if [ $datanodeSlave != y ]; then
		eecho ERROR: no datanode slaves are configured
		return 1
	fi
	local i
	for ((i=0; i< ${#datanodeNames[@]}; i++)); do
		if [ $1 == ${datanodeNames[$i]} ]; then
			if [ ${datanodeSlaveServers[$i]} == none ] || [ ${datanodeSlaveServers[$i]} == N/A ]; then
				eecho ERROR: slave for datanode $1 is not configured.
				return 1
			fi
		    # Datanode master should be running
			log_echo psql -p ${datanodePorts[$i]} -h ${datanodeMasterServers[$i]} postgres $pgxcOwner -c \'select 1\' '>' /dev/null '2>&1'
			psql -p ${datanodePorts[$i]} -h ${datanodeMasterServers[$i]} postgres $pgxcOwner -c 'select 1' > /dev/null 2>&1
			if [ $? -ne 0 ] ; then
				eecho ERROR: corresponding datanode master is not running now, $1
				return 1
			fi
		    # Start the slave
			psql -p ${datanodePorts[$i]} -h ${datanodeSlaveServers[$i]} postgres $pgxcOwner -c 'select 1' > /dev/null 2>&1
			if [ $? -eq 0 ] ; then
				eecho ERROR: datanode slave "("${datanodeNames[$i]}")" is already running
				return 1
			fi
			doit ssh $pgxcUser@${datanodeSlaveServers[$i]} "pg_ctl start -Z datanode -D ${datanodeSlaveDirs[$i]} -o -i > $tmpDir/coord.out"
			do_stdout ${datanodeSlaveServers[$i]} $tmpDir/coord.out
		    # Change the master to synchronous mode
			vecho Change the master to synchrnous mode, $1
			log_echo ssh $pgxcUser@${datanodeMasterServers[$i]} '"'cat '>>' ${datanodeMasterDirs[$i]}/postgresql.conf'"'
			ssh $pgxcUser@${datanodeMasterServers[$i]} "cat >> ${datanodeMasterDirs[$i]}/postgresql.conf" <<EOF
#==========================================================
# Added to start the slave in sync. mode, $datetime
synchronous_commit = on
synchronous_standby_names = '${datanodeNames[$i]}'
# End of the addition
EOF
			doit ssh $pgxcUser@${datanodeMasterServers[$i]} pg_ctl reload -Z datanode -D ${datanodeMasterDirs[$i]}
			return
		fi
	done
	eecho ERROR: specified datanode does not exist, $1
	return 1
}

function pgxc_start_datanode_slave_all
{
	log_eco pgxc_start_datanode_slave_all'('$*')'
    vecho ================================================================
    vecho Start all the datanode slaves

	local i
	for ((i=0; i< ${#datanodeNames[@]}; i++)); do
		if [ ${datanodeSlaveServers[$i]} != none ] && [ ${datanodeSlaveServers[$i]} != N/A ]; then
			pgxc_start_datanode_slave ${datanodeNames[$i]}
		fi
	done
}

function pgxc_stop_datanode_slave
{
	log_echo pgxc_stop_datanode_slave'('$*')'
	# First argument is the datanode name
    vecho ================================================================
    vecho Stop datanode slave $1


	if [ $datanodeSlave != y ]; then
		eecho ERROR: no datanode slaves are configured
		return 1
	fi
	local i
	for ((i=0; i< ${#datanodeNames[@]}; i++)); do
		if [ $1 == ${datanodeNames[$i]} ];  then
			if [ ${datanodeSlaveServers[$i]} == none ] || [ ${datanodeSlaveServers[$i]} == N/A ]; then
				eecho ERROR: slave for the datanode $1 is not configured.
			fi
		    # If the master is running, master's switch replication to asynchronous mode.
			psql -p ${datanodePorts[$i]} -h ${datanodeMasterServers[$i]} postgres $pgxcOwner -c 'select 1' > /dev/null 2> /dev/null
			if [ $? -eq 0 ]; then
			    # Switch Master to asynchronous mode.
				vecho Switching master of $1 at ${datanodeMasterServer[$i]} to asynchronous replication mode.
				log_echo ssh $pgxcUser@${datanodeMasterServers[$i]} '"'cat '>>' ${datanodeMasterDirs[$i]}/postgresql.conf'"'
				ssh $pgxcUser@${datanodeMasterServers[$i]} "cat >> ${datanodeMasterDirs[$i]}/postgresql.conf" <<EOF
#=======================================
# Updated to trun off the slave $datetime
synchronous_standby_names = ''
# End of the update
EOF
				doit ssh $pgxcUser@${datanodeMasterServers[$i]} pg_ctl reload -Z datanode -D ${datanodeMasterDirs[$i]}
			fi
			doit ssh $pgxcUser@${datanodeSlaveServers[$i]} pg_ctl stop -Z datanode -D ${datanodeSlaveDirs[$i]} $immediate
			return;
		fi
	done
	eecho ERROR: Specified datanode is not configureed, $1
	return 1
}
function pgxc_stop_datanode_slave_all
{
	log_echo pgxc_stop_datanode_slave_all'('$*')'
    vecho ================================================================
    vecho Stop all the datanode slaves

	local i
	for ((i=0; i< ${#datanodeNames[@]}; i++)); do
		if [ ${datanodeSlaveServers[$i]} != none ] && [ ${datanodeSlaveServers[$i]} != N/A ]; then
			pgxc_stop_datanode_slave ${datanodeNames[$i]}
		fi
	done
}

function pgxc_kill_datanode_master
{
	log_echo pgxc_kill_datanode_master'('$*')'
	# First argument is the datanodeinator name
    vecho ================================================================
    vecho Kill datanode slave $1

	local i
	local postmaster_pid
	for ((i=0; i< ${#datanodeNames[@]}; i++)); do
		if [ $1 == ${datanodeNames[$i]} ]; then
			if [ ${datanodeMasterServers[$i]} != none ] && [ ${datanodeMasterServers[$i]} != N/A ]; then
				postmaster_pid=`get_postmaster_pid ${datanodeMasterServers[$i]} ${datanodeMasterDirs[$i]}`
				if [ $postmaster_pid != none ]; then
					doit kill_all_child_parent ${datanodeMasterServers[$i]} $postmaster_pid
				fi
			else
				eecho ERROR: specified coordinator master does not exist, $1
			fi
			return
		fi
	done
	eecho ERROR: specified datanode master is not configured, $1
	return 1
}

function pgxc_kill_datanode_master_all
{
	log_echo pgxc_kill_datanode_master_all'('$*')'
    vecho ================================================================
    vecho Kill all the datanode masters

	local i
	for ((i=0; i< ${#datanodeNames[@]}; i++)); do
		if [ ${datanodeMasterServers[$i]} != none ] && [ ${datanodeMasterServers[$i]} != N/A ]; then
			pgxc_kill_datanode_master ${datanodeNames[$i]}
		fi
	done
}

# Please note that this function does not take care of anything but just kill the processes.
function pgxc_kill_datanode_slave
{
	log_echo pgxc_kill_datanode_slave'('$*')'
	# First argument is the datanodeinator name
    vecho ================================================================
    vecho Kill datanode slave $1

	local i
	local postmaster_pid
	for ((i=0; i< ${#datanodeNames[@]}; i++)); do
		if [ $1 == ${datanodeNames[$i]} ]; then
			if [ ${datanodeSlaveServers[$i]} != none ] && [ ${datanodeSlaveServers[$i]} != N/A ]; then
				postmaster_pid=`get_postmaster_pid ${datanodeSlaveServers[$i]} ${datanodeSlaveDirs[$i]}`
				if [ $postmaster_pid != none ]; then
					doit kill_all_child_parent ${datanodeSlaveServers[$i]} $postmaster_pid
				fi
				doit pgxc_clean_socket ${datanodeSlaveServers[$i]} ${datanodePorts[$i]}
			else
				eecho ERROR: specified coordinator master does not exist, $1
			fi
			return
		fi
	done
	eecho ERROR: specified datanode master is not configured, $1
	return 1
}

# Select coordinator
function pgxc_kill_datanode_slave_all
{
	log_echo pgxc_kill_datanode_slave_all'('$*')'
    vecho ================================================================
    vecho Kill all the datanode masters

	local i
	for ((i=0; i< ${#datanodeNames[@]}; i++)); do
		if [ ${datanodeSlaveServers[$i]} != none ] && [ ${datanodeSlaveServers[$i]} != N/A ]; then
			pgxc_kill_datanode_slave ${datanodeNames[$i]}
		fi
	done
}


selected_coord="none"
selected_coord_port=0
selected_coord_host="none"

function pgxc_find_coordinator
{
	local fn=pgxc_find_coordinator
	selected_coord="none"
	selected_coord_port=0
	selected_coord_host="none"
	local i
	log_echo $progname-$fn $*
	if [ $# -le 0 ]; then
		eecho $progname-$fn missing coordinator name
		return 2
	fi
	if [ "$1" == "none" ] || [ "$1" == "N/A" ]; then
		eecho "$progname:$fn" invalid coordinator name $1
		return 2
	fi
	for ((i=0;i<${#coordNames[@]};i++));do
		if [ "${coordNames[$i]}" == "$1" ]; then
			if [ "${coordMasterServers[$i]}" == "none" ] || [ "${coordMasterServers[$i]}" == "N/A" ]; then
				eecho "$progname:$fn" specified coordinator $1 does not have master
				return 2
			fi
			selected_coord=${coordNames[$i]}
			selected_coord_port=${coordPorts[$i]}
			selected_coord_host=${coordMasterServers[$i]}
			return;
		fi
	done
	eecho $progname-$fn specified coordinator $1 not found
	return 1
}

function pgxc_select_coordinator
{
	selected_coord="none"
	selected_coord_port=0
	selected_coord_host="none"
	local i
	local s
	s=$(($RANDOM%${#coordMasterServers[@]}))
	for ((i=$s;i<${#coordMasterServers[@]};i++)); do
		if [ ${coordMasterServers[$i]} != "none" ] && [ ${coordMasterServers[$i]} != "N/A" ]; then
			pgxc_monitor -Z node -p ${coordPorts[$i]} -h ${coordMasterServers[$i]} -U $pgxcOwner -d postgres
			if [ $? == 0 ]; then
				# Need this block not to select failed coordinator.  This may happen if a server fails
				# and datanode fails over first.
				selected_coord=${coordNames[$i]}
				selected_coord_port=${coordPorts[$i]}
				selected_coord_host=${coordMasterServers[$i]}
				return 0
			fi
		fi
	done
	for ((i=0;i<$s;i++)); do
		if [ ${coordMasterServers[$i]} != "none" ] && [ ${coordMasterServers[$i]} != "N/A" ]; then
			pgxc_monitor -Z node -p ${coordPorts[$i]} -h ${coordMasterServers[$i]} -U $pgxcOwner -d postgres
			if [ $? == 0 ]; then
				# Need this block not to select failed coordinator.  This may happen if a server fails
				# and datanode fails over first.
				selected_coord=${coordNames[$i]}
				selected_coord_port=${coordPorts[$i]}
				selected_coord_host=${coordMasterServers[$i]}
				return 0
			fi
		fi
	done
	return 1
}

# Connects to the target ($1) from the coordinator ($2) using specified user ($3).
# Issue "select 1" using EXECUTE DIRECT.
# Need to make CLEAN CONNECTION work.
function pgxc_dummy_connect
{
	local i
	if [ $# -ne 3 ]; then
		echo pgxc_dummy_connect target coordinator user
		return 1
	fi
	for ((i=0;i<${#coordNames[@]}; i++)); do
		if [ $2 == ${coordNames[$i]} ]; then
			cat > $localTmpDir/cmd_dummy.sql <<EOF
EXECUTE DIRECT ON ($1) 'SELECT 1';
\q
EOF
			psql -p ${coordPorts[$i]} -h ${coordMasterServers[$i]} -f $localTmpDir/cmd_dummy.sql postgres $3
			rm -f $localTmpDir/cmd_dummy.sql
			return
		fi
	done
	echo ERROR: coordinator $2 not found.
	return 1
}

# This failover function assumes that no IP address is carried over from corresponding master server.
# If IP address can be carried over, then you don't need a section which issues ALTER NODE statement.
# Instead, you should disable the slave.
function pgxc_failover_datanode
{
	log_echo pgxc_failover_datanode'('$*')'
	local fn=pgxc_failover_datanode

	# First argument is the datanode name
	decho called: $fn $*
    vecho ================================================================
    vecho Failover datanode $1

	if [ $# -ne 1 ]; then
		eecho $progname:$fn Error: Specify datanode name to failover
		return 1
	fi
	if [ $datanodeSlave != y ]; then
		echo $progname:$fn No datanode slaves are configured. Cannot failover
		return 1
	fi

	local i
	for ((i=0; i< ${#datanodeNames[@]}; i++));	do
		if [ $1 == ${datanodeNames[$i]} ]; then
			if [ ${datanodeSlaveServers[$i]} == none ] || [ ${datanodeSlaveServers[$i]} == N/A ]; then
				eecho $progname:$0: ERROR, Slave for the datanode $1 is not configured.  Cannot failover.
				return 1
			fi
			decho "Target coordinator slave to failover:" ${datanodeNames[$i]} "at" ${datanodeSlaveServers[$i]}
		    # Find the new local gtm_proxy
			local j
			local targetGTMhost
			local targetGTMport
			targetGTMhost=none
			targetGTMport=0
			for ((j=0; j<${#gtmProxyServers[@]}; j++)); do
				if [ ${datanodeSlaveServers[$i]} == ${gtmProxyServers[$j]} ]; then
					targetGTMhost=${gtmProxyServers[$j]}
					targetGTMport=${gtmProxyPorts[$j]}
					break
				fi
			done
		    # gtm_proxy has to be configured properly
		    # This can be a bit more flexible so that each component can connect to GTM directly if
		    # gtm_proxy is not configured locally.
			decho "New GTM Proxy:" "$targetGTMHost":"$targetGTMPort"
			if [ "$targetGTMhost" == none ]; then
				eecho $progname:$fn: ERROR, gtm_proxy is not configured at the server ${datanodeSlaveServers[$i]}.  Cannot failover.
				return 1
			fi
		    # Now promote the slave
			vecho Promoting datnode slave at ${datanodeSlaveServers[$i]}:${datanodeSlaveDirs[$i]}
			doit gtm_util unregister -Z datanode -p $gtmMasterPort -h $gtmMasterServer ${datanodeNames[$i]}
			# doit ssh $pgxcUser@$gtmMasterServer rm -f $gtmMasterDir/register.node
			doit ssh $pgxcUser@${datanodeSlaveServers[$i]} pg_ctl promote -Z datanode -D ${datanodeSlaveDirs[$i]}
			ssh $pgxcUser@${datanodeSlaveServers[$i]} rm -rf $tmpDir/cmd.out
			vecho done
		    # Restart the new master with new gtm_proxy
			# The following command is a dirty hack to unregister the old master.   This is only one way available now, but
			# the next version of the core should include an utility to clean it up partially.
		    # Reconfigure new master's gtm_proxy
			vecho Reconfiguring new gtm_proxy for ${datanodeSlaveServers[$i]}:${datanodeSlaveDirs[$i]}/postgresql.conf
			log_echo ssh $pgxcUser@${datanodeSlaveServers[$i]} '"'cat '>>' ${datanodeSlaveDirs[$i]}/postgresql.conf'"'
			ssh $pgxcUser@${datanodeSlaveServers[$i]} "cat >> ${datanodeSlaveDirs[$i]}/postgresql.conf" <<EOF
#=================================================
# Added to promote, $datetime
gtm_host = '$targetGTMhost'
gtm_port = $targetGTMport
# End of addition
EOF
			vecho done
			# doit ssh $pgxcUser@${datanodeSlaveServers[$i]} pg_ctl stop -w -Z datanode -D ${datanodeSlaveDirs[$i]} -o -i -m immediate
			vecho Restarting ${datanodeNames[$i]} at ${datanodeSlaveServers[$i]}
			log_echo ssh $pgxcUser@${datanodeSlaveServers[$i]} pg_ctl restart -w -Z datanode -D ${datanodeSlaveDirs[$i]} -w -o -i '>' $localTmpDir/cmd.out '2>&1 &'
			ssh $pgxcUser@${datanodeSlaveServers[$i]} pg_ctl restart -w -Z datanode -D ${datanodeSlaveDirs[$i]} -w -o -i > $localTmpDir/cmd.out 2>&1 &
			sleep 2		# do we need it?
			doit cat $localTmpDir/cmd.out
			doit rm -f $localTmpDir/cmd.out

		    # Update other coordinators with this new one ---> first, clean connection for all the users for all the databases
		    # Get all the available users --> Use this coordinator to get usernam
			# It may be better to clean connections.  However, we found that clean connection is not stable enough when some node
			# is gone.   We will wait until it is more stable.
			# It is not clean but I'd like to leave these code for future improvement.
			vecho Update coordinators with new datanode configuration.
		  	# Clean all the pooler connections and update the node configuration
			vecho Clean all the pooler connections and update the node configuration
			for ((j=0; j< ${#coordMasterServers[@]}; j++)); do
				if [ "${coordMasterServers[$j]}" != none ] && [ "${coordMasterServers[$j]}" != N/A ]; then
					doit pgxc_monitor -Z node -p ${coordPorts[$j]} -h ${coordMasterServers[$j]} -U $pgxcOwner -d postgres
					# The following block is needed because a coordinator may leave failed when a server fails and
					# the datanode may fail-over first.  --> In this case, the coordinator should be given a chance
					# to reconfigure itself with this datanode.   Coordinator should failover first.
					if [ $? -ne 0 ]; then
						eecho ERROR: coordinator ${coordNames[$j]} is not running.   Skip reconfiguration for this.
						continue;
					fi
					# Issue ALTER NODE.  If target coordinator is not running, we have no way to do this now.   May need to run this afterwords.
					# Store the script to elsewhere?  --> Now we don't do this.   May be for further work because we expect everything except
					# for the current coordinator is running healthy.
					log_echo cat '>>' $localTmpDir/cmd.sql
					cat >> $localTmpDir/cmd.sql <<EOF
ALTER NODE ${datanodeNames[$i]} WITH (HOST='${datanodeSlaveServers[$i]}', PORT=${datanodePorts[$i]});
select pgxc_pool_reload();
\q
EOF
					cat $localTmpDir/cmd.sql
					psql -p ${coordPorts[$j]} -h ${coordMasterServers[$j]} postgres $pgxcOwner -f $localTmpDir/cmd.sql
					rm -f $localTmpDir/cmd.sql
				fi
			done
            # Update the configuration variable
			datanodeMasterServers[$i]="${datanodeSlaveServers[$i]}"
			datanodeMasterDirs[$i]="${datanodeSlaveDirs[$i]}"
			datanodeSlaveServers[$i]="none"
			datanodeSlaveDirs[$i]="none"
		    # Then update the configuration file with this new configuration
			log_echo cat '>>' $configFile
			cat >> $configFile <<EOF
#=====================================================
# Updated due to the coordinator failover, $1, $datetime
datanodeMasterServers=( ${datanodeMasterServers[@]} )
datanodeMasterDirs=( ${datanodeMasterDirs[@]} )
datanodeSlaveServers=( ${datanodeSlaveServers[@]} )
datanodeSlaveDirs=( ${datanodeSlaveDirs[@]} )
# End of the update
EOF
			# Backup configuraiton file
			pgxc_backup_config_file
			return;
		fi
	done
	eecho ERROR: specified coordinator $1 not configured.
	return 2
}


function pgxc_configure_nodes
{
	log_echo pgxc_configure_nodes'('$*')'
	# First argument is a coordinator name
    vecho ================================================================
    vecho Configure nodes for coordinator $1

	if [ $# -ne 1 ]; then
		eecho ERROR: specify coorinator name.
		return 2
	fi
	local i
	local j
	for ((i=0; i<${#coordNames[@]}; i++));do
		if [ $1 == ${coordNames[$i]} ] && [ ${coordNames[$i]} != none ] && [ ${coordNames[$i]} != N/A ]; then
			rm -rf $localTmpDir/cmd.sql
			touch $localTmpDir/cmd.sql
			for ((j=0; j<${#coordNames[@]}; j++));do
				vecho Setup pgxc_node for ${coordNames[$j]} at ${coordNames[$i]}.
				# Setup coordinators
				decho i=$i, j=$j
				if [ $i != $j ]; then
					#echo CREATE NODE
					cat >> $localTmpDir/cmd.sql <<EOF
CREATE NODE ${coordNames[$j]} WITH (TYPE='coordinator', HOST='${coordMasterServers[$j]}', PORT=${coordPorts[$j]});
EOF
				else
					#echo ALTER NODE
					cat >> $localTmpDir/cmd.sql <<EOF
ALTER NODE ${coordNames[$j]} WITH (HOST='${coordMasterServers[$j]}', PORT=${coordPorts[$j]});
EOF
				fi
			done
			for ((j=0; j<${#datanodeNames[@]}; j++)); do
				vecho Setup pgxc_node for ${datanodeNames[$j]} at ${coordNames[$i]}.
				# Setup datanodes
				cat >> $localTmpDir/cmd.sql <<EOF
CREATE NODE ${datanodeNames[$j]} WITH (TYPE='datanode', HOST='${datanodeMasterServers[$j]}', PORT=${datanodePorts[$j]});
EOF
				if [ ${datanodeNames[$j]} == $primaryDatanode ]; then
					# Primary node
					cat >> $localTmpDir/cmd.sql <<EOF
ALTER NODE  ${datanodeNames[$j]} WITH (PRIMARY);
EOF
				fi
				if [ ${datanodeMasterServers[$j]} == ${coordMasterServers[$i]} ]; then
					# Preferred node
					cat >> $localTmpDir/cmd.sql <<EOF
ALTER NODE  ${datanodeNames[$j]} WITH (PREFERRED);
EOF
				fi
			done
			cat >> $localTmpDir/cmd.sql <<EOF
\q
EOF
			ddo cat $localTmpDir/cmd.sql
			if [ "$verbose" == "y" ] || [ $logOpt == "y" ]; then
				doit psql -h ${coordMasterServers[$i]} -p ${coordPorts[$i]} -a -f $localTmpDir/cmd.sql postgres $pgxcOwner
			else
				psql -h ${coordMasterServers[$i]} -p ${coordPorts[$i]} -f $localTmpDir/cmd.sql postgres $pgxcOwner
			fi
			rm -rf $localTmpDir/cmd.sql
			return
		fi
	done
	eecho Coordinator $1 is not configured.
	return 1
}

function pgxc_configure_nodes_all
{
	log_echo pgxc_configure_node'('$*')'
    vecho ================================================================
    vecho Configure nodes for all the coordinators

	local i
	for ((i=0;i<${#coordNames[@]};i++)); do
		if [ "${coordMasterServers[$i]}" != "none" ] && [ "${coordMasterServers[$i]}" != "N/A" ]; then
			pgxc_configure_nodes ${coordNames[$i]}
		fi
	done
}


function start_all
{
	log_echo start_all'('$*')'
	pgxc_start_gtm_master
	if [ $gtmSlave == y ]; then
		pgxc_start_gtm_slave
	fi
	if [ $gtmProxy == y ]; then
		pgxc_start_gtm_proxy_all
	fi
	pgxc_start_datanode_master_all
	if [ $datanodeSlave == y ]; then
		pgxc_start_datanode_slave_all
	fi
	pgxc_start_coordinator_master_all
	if [ $coordSlave == y ]; then
		pgxc_start_coordinator_slave_all
	fi
}

function init_all
{
	log_echo init_all'('$*')'
	pgxc_init_gtm_master
	if [ $gtmSlave == y ]; then
		pgxc_init_gtm_slave
	fi
	if [ $gtmProxy == y ]; then
		pgxc_init_gtm_proxy_all
	fi
	pgxc_init_datanode_master_all
	pgxc_start_gtm_master
	if [ $gtmProxy == y ]; then
		pgxc_start_gtm_proxy_all
	fi
	if [ $datanodeSlave == y ]; then
		pgxc_init_datanode_slave_all
	fi
	pgxc_init_coordinator_master_all
	if [ $coordSlave == y ]; then
		pgxc_init_coordinator_slave_all
	fi
	if [ $gtmProxy == y ]; then
		pgxc_stop_gtm_proxy_all
	fi
	if [ $gtmSlave == y ]; then
		pgxc_stop_gtm_slave
	fi
	pgxc_stop_gtm_master
	start_all
	pgxc_configure_nodes_all
}

function stop_all
{
	log_echo stop_all'('$*')'
	if [ $coordSlave == y ]; then
		pgxc_stop_coordinator_slave_all
	fi
	pgxc_stop_coordinator_master_all
	if [ $datanodeSlave == y ]; then
		pgxc_stop_datanode_slave_all
	fi
	pgxc_stop_datanode_master_all
	if [ $gtmProxy == y ]; then
		pgxc_stop_gtm_proxy_all
	fi
	if [ $gtmSlave == y ]; then
		pgxc_stop_gtm_slave
	fi
	pgxc_stop_gtm_master
}

function clean_all
{
	log_echo clean_all'('$*')'
	local immediate_bk
	immediate_bk="$immediate"
	immediate="-m immediate"
	stop_all
	pgxc_clean_node_all
	immediate="$immediate_bk"
}

# Start --- all | gtm [master|slave|all] | gtm_proxy | coordinator [master|slave|all] | datanode [master|slave|all] | nodename [master|slave|all]
function pgxc_start_something
{
	log_echo pgxc_start_something'('*')'
	local i

	if [ "$1" == "all" ] || [ "$1" == "" ]; then
		start_all
		return
	fi
	if [ "$1" == "gtm" ] || [ "$1" == "$gtmName"]; then
		if [ "$2" == "master" ]; then
			pgxc_monitor -Z gtm $gtmMasterPort -h $gtmMasterServer
			if [ $? -eq 0 ]; then
				eecho GTM master is already running.
				return 1
			fi
			pgxc_start_gtm_master
			return;
		fi
		if [ "$2" == "slave" ]; then
			if [ $gtmSlave != y ]; then
				eecho GTM Slave is not configured.  Cannot start.
				return;
			fi
			pgxc_monitor -Z gtm -p $gtmMasterPort -h $gtmMasterServer
			if [ $? -eq 0 ]; then
				pgxc_start_gtm_slave
			else
				eecho GTM master is not running. Cannot start the slave.
			fi
			return;
		fi
		if [ "$2" == "all" ] || [ "$2" == "" ]; then
			pgxc_monitor -Z gtm $gtmMasterPort -h $gtmMasterServer
			if [ $? -eq 0 ]; then
				eecho GTM master is already running.
				return 1
			fi
			pgxc_start_gtm_master
			if [ $gtmSlave == y ]; then
				pgxc_start_gtm_slave
			fi
		fi
		return
	fi
	if [ "$1" == "gtm_proxy" ]; then
		pgxc_start_gtm_proxy_all
		return
	fi
	if [ "$1" == "coordinator" ]; then
		if [ "$2" == "master" ]; then
			pgxc_start_coordinator_master_all
		elif [ "$2" == "slave" ]; then
			pgxc_start_coordinator_slave_all
		elif [ "$2" == "all" ] || [ "$2" == "" ]; then
			pgxc_start_coordinator_master_all
			pgxc_start_coordinator_slave_all
		else
			eecho Invalid argument for start coordinator command, $2
		fi
		return
	fi
	if [ "$1" == "datanode" ]; then
		if [ "$2" == "master" ]; then
			pgxc_start_datanode_master_all
		elif [ "$2" == "slave" ]; then
			pgxc_start_datanode_slave_all
		elif [ "$2" == "all" ] || [ "$2" == "" ]; then
			pgxc_start_datanode_master_all
			pgxc_start_datanode_slave_all
		else
			eecho Invalid argument for start coordinator command, $2
		fi
		return
	fi
	# General nodename specification
	# Have done GTM
	# GTM proxy?
	for ((i=0; i<${#gtmProxyNames[@]}; i++)); do
		if [ "$1" == "${gtmProxyNames[$i]}" ]; then
			pgxc_start_gtm_proxy $1
			return
		fi
	done
	# Coordinator?
	for ((i=0; i<${#coordNames[@]}; i++)); do
		if [ "$1" == "${coordNames[$i]}" ]; then
			if [ "$2" == "master" ]; then
				pgxc_start_coordinator_master $1
			elif [ "$2" == "slave" ]; then
				pgxc_start_coordinator_slave $1
			elif [ "$2" == "all" ] || [ "$2" == "" ]; then
				pgxc_start_coordinator_master $1
				pgxc_start_coordinator_slave $1
			else
				eecho Invalid start coordinator command option, $2
			fi
			return
		fi
	done
	# Datanode?
	for ((i=0; i<${#datanodeNames[@]}; i++)); do
		if [ "$1" == "${datanodeNames[$i]}" ]; then
			if [ "$2" == "master" ]; then
				pgxc_start_datanode_master $1
			elif [ "$2" == "slave" ]; then
				pgxc_start_datanode_slave $1
			elif [ "$2" == "all" ] || [ "$2" == "" ]; then
				pgxc_start_datanode_master $1
				pgxc_start_datanode_slave $1
			else
				eecho Invalid start datanode command option, $2
			fi
			return
		fi
	done
	eecho No component named $1 found
	return 1
}

# Stop --- all | gtm [master|slave|all] | gtm_proxy | coordinator [master|slave|all] | datanode [master|slave|all] | nodename [master|slave|all]
function pgxc_stop_something
{
	local i

	log_echo pgxc_stop_something'('$*')'
	if [ "$1" == "all" ] || [ "$1" == "" ]; then
		stop_all
		return
	fi
	if [ "$1" == "gtm" ] || [ "$1" == "$gtmName" ]; then
		if [ "$2" == "master" ]; then
			pgxc_monitor -Z gtm -p $gtmMasterPort -h $gtmMasterServer
			if [ $? -ne 0 ]; then
				eecho GTM master is not running. Cannot stop it.
				return 1
			fi
			pgxc_stop_gtm_master
			return;
		fi
		if [ "$2" == "slave" ]; then
			if [ $gtmSlave != y ]; then
				eecho GTM Slave is not configured.  Cannot stop.
				return;
			fi
			pgxc_monitor -Z gtm -p $gtmSlavePort -h $gtmSlaveServer
			if [ $? -eq 0 ]; then
				pgxc_stop_gtm_slave
			else
				eecho GTM slave is not running. Cannot stop it.
			fi
			return;
		fi
		if [ "$2" == "all" ] || [ "$2" == "" ]; then
			pgxc_monitor -Z gtm $gtmMasterPort -h $gtmMasterServer
			if [ $? -ne 0 ]; then
				eecho GTM master is not running.
			else
				pgxc_start_gtm_master
			fi
			if [ $gtmSlave == y ]; then
				pgxc_monitor -Z gtm -p $gtmSlavePort -h gtmSlaveServer
				if [ $? -eq 0 ]; then
					pgxc_start_gtm_slave
				else
					eecho GTM slave is not running
				fi
			fi
			return
		else
			eecho Invalid stop gtm option, $2
			return 2
		fi
		return
	fi
	if [ "$1" == "gtm_proxy" ]; then
		pgxc_stop_gtm_proxy_all
		return
	fi
	if [ "$1" == "coordinator" ]; then
		if [ "$2" == "master" ]; then
			pgxc_stop_coordinator_master_all
		elif [ "$2" == "slave" ]; then
			pgxc_stop_coordinator_slave_all
		elif [ "$2" == "all" ] || [ "$2" == "" ]; then
			pgxc_stop_coordinator_master_all
			pgxc_stop_coordinator_slave_all
		else
			eecho Invalid argument for start coordinator command, $2
		fi
		return
	fi
	if [ "$1" == "datanode" ]; then
		if [ "$2" == "master" ]; then
			pgxc_stop_datanode_master_all
		elif [ "$2" == "slave" ]; then
			pgxc_stop_datanode_slave_all
		elif [ "$2" == "all" ] || [ "$2" == "" ]; then
			pgxc_stop_datanode_master_all
			pgxc_stop_datanode_slave_all
		else
			eecho Invalid argument for stop coordinator command, $2
		fi
		return
	fi
	# General nodename specification
	# GTM case: has been handled
	# GTM proxy?
	for ((i=0; i<${#gtmProxyNames[@]}; i++)); do
		if [ "$1" == "${gtmProxyNames[$i]}" ]; then
			pgxc_stop_gtm_proxy $1
			return
		fi
	done
	# Coordinator?
	for ((i=0; i<${#coordNames[@]}; i++)); do
		if [ "$1" == "${coordNames[$i]}" ]; then
			if [ "$2" == "master" ]; then
				pgxc_stop_coordinator_master $1
			elif [ "$2" == "slave" ]; then
				pgxc_stop_coordinator_slave $1
			elif [ "$2" == "all" ] || [ "$2" == "" ]; then
				pgxc_stop_coordinator_master $1
				pgxc_stop_coordinator_slave $1
			else
				eecho Invalid stop coordinator command option, $2
			fi
			return
		fi
	done
	# Datanode?
	for ((i=0; i<${#datanodeNames[@]}; i++)); do
		if [ "$1" == "${datanodeNames[$i]}" ]; then
			if [ "$2" == "master" ]; then
				pgxc_stop_datanode_master $1
			elif [ "$2" == "slave" ]; then
				pgxc_stop_datanode_slave $1
			elif [ "$2" == "all" ] || [ "$2" == "" ]; then
				pgxc_stop_datanode_master $1
				pgxc_stop_datanode_slave $1
			else
				eecho Invalid stop datanode command option, $2
			fi
			return
		fi
	done
	eecho No component named $1 found
	return 1
}

# failover --- gtm | component name
function pgxc_failover_something
{
	local i

	log_echo pgxc_failover_something'('$*')'
	if [ "$1" == "gtm" ] || [ "$1" == "$gtmName" ]; then
		pgxc_failover_gtm
		pgxc_reconnect_gtm_proxy_all
		return
	fi
	for ((i=0; i<${#coordNames[@]};i++)); do
		if [ "$1" == ${coordNames[$i]} ]; then
			pgxc_failover_coordinator $1
			return
		fi
	done
	for ((i=0; i<${#datanodeNames[@]}; i++)); do
		if [ "$1" == ${datanodeNames[$i]} ]; then
			pgxc_failover_datanode $1
			return
		fi
	done
	eecho No such componen found to failover $1
	return 1
}

function pgxc_kill_something
{
	local n

	log_echo pgxc_kill_something'('$*')'
	if [ "$1" == "" ]; then
		return 2
	fi
	for ((n=0; n<${#gtmProxyNames[@]}; n++)); do
		if [ "$1" == "${gtmProxyNames[$n]}" ]; then
			pgxc_kill_gtm_proxy $1
			return
		fi
	done
	if [ "$2" != "master" ] && [ "$2" != "slave" ]; then
		eecho Specify master or slave.
		return 2
	fi
	if [ $1 == $gtmName ]; then
		if [ $2 == master ]; then
			pgxc_kill_gtm_master
		else
			pgxc_kill_gtm_slave
		fi
		return;
	fi
	for ((n=0; n<${#coordNames[@]}; n++)); do
		if [ $1 == ${coordNames[$n]} ]; then
			if [ $2 == master ]; then
				pgxc_kill_coordinator_master $1
			else
				pgxc_kill_coordinator_slave $1
			fi
			return;
		fi
	done
	for ((n=0; n<${#datanodeNames[@]}; n++)); do
		if [ $1 == ${datanodeNames[$n]} ]; then
			if [ $2 == master ]; then
				pgxc_kill_datanode_master $1
			else
				pgxc_kill_datanode_slave $1
			fi
			return;
		fi
	done
}

function kill_all
{
	log_echo kill_all'('$*')'
	doall killall -u $pgxcUser -9 postgres gtm_proxy gtm
}

function test_failover
{
	kill_all
	init_all
	start_all
	pgxc_kill_coordinator_master coord1
	pgxc_failover_coordinator coord1
}

##############################################################################
#
# Help commands
#
##############################################################################

function help_command
{
	echo Command you can type:
	echo "  "  - clean Createdb Createuser deploy Dropdb Dropuser end failover 
	echo "  "  - init kill monitor prepare Psql q start stop Vacuumdb xcset xcshow
	echo For details, type cmdname ?
	
	echo
	echo If you are familiar with internal functions, you can type "function" name and its argument directory.
	echo pgxc_ctl will also accept any command for bash.
}

###############################################################################
#
#   EXECUTING SECTION
#
###############################################################################

#=======================================================
# Things to be done at first
#=======================================================

# Handle options
progname=$0
moretodo=y
cmd_with_log=null
while [ $moretodo == y ]; do
	if [ $# -gt 0 ]; then
		case $1 in
			-v )
				shift;
				verbose=y;
				continue;;
			--verbose )
				shift;
				verbose=y;
				continue;;
			--silent )
				verbose=n;
				continue;;
			-d ) # debug option
				shift;
				DEBUG=y;
				continue;;
			--debug )
				shift;
				DEBUG=y;
				continue;;
			-c ) # Configuraton file
				shift;
				if [ $# -le 0 ]; then
					echo ERROR: no -c option value found
					exit 1
				else
					$configFile=$1
					shift
				fi;
				continue;;
			--configuration ) # Configuraion file
				shift;
				if [ $# -le 0 ]; then
					echo ERROR: no --configuration option value found
					exit 1
				else
					$configFile=$1
					shift
				fi;
				continue;;
			-f ) # input_file
				shift;
				if [ "$1" == "" ]; then
					echo Epecify input file
				else
					$progname < $1
				fi
				continue;;
			--interactive ) # interactive
				shift;
				interactive=y
				continue;;
			--batch )
				shift
				interactive=n
				continue;;
			--with-log )
				shift
				cmd_with_log=y
				continue;;
			--without-log )
				shift
				cmd_with_log=n
				continue;;
			* )
				moretodo=n
				continue;;
		esac
	else
		moretodo=n
	fi
done

# Read configuration file --> Should be activated only when debug option is off
if [ -f $configFile ]; then
	source $configFile
fi
# Log option can be overriden by command-line option
if [ "$cmd_with_log" != "null" ]; then
	cmdOpt=$cmd_with_log
fi
if [ "$logOpt" == "y" ]; then
	set_log_file
fi

log_echo '===== Start PGXC_CTL ==========' `date`

# Check configuration -- Mayh need more detailed check.

# Check if slaves are configured and makeup each server to N/A if needed
handle_no_slaves
# Construct the server list
makeServerList

# For interactive operation ---> Finally this should be a part of pgxc_ctl interactive command
# interactive=y --> should be set by options
firstline=y
lastline=n
while [ 1 ]; do
	if [ $lastline = "y" ]; then
		break
	fi
	if [ $firstline == "y" ] && [ "$1" != "" ]; then
		cmdname=$1;	shift
		p1=$1; shift
		p2=$1; shift
		p3=$1; shift
		p4=$1; shift
		cmdparam=$*
		firstline=n
		lastline=y
		set_interactive off
	else
		echo -n "$xc_prompt"
		read cmdname p1 p2 p3 p4 cmdparam
	fi
	log_echo_with_date '======' USER COMMAND '=====' $cmdname $p1 $p2 $p3 $p4 $cmdparam

	if [ "$cmdname" == "" ];  then
		continue
	fi
	if [ "$cmdname" == "exit" ] || [ "$cmdname" == "q" ]; then
		if [ "$p1" == '?' ]; then
			echo Exit command finishes pgxc_ctl.
			continue
		fi
		break
	fi
	if [ "$cmdname" == "?" ] || [ "$cmdname" == "help" ]; then
		help_command
		continue
	fi

	# Init --- Now accepts only all or default to initialize everything here.
	# If you'd like to initialize individual component, use internal function
	# directly.
	if [ "$cmdname" == "init" ]; then
		if [ "$p1" != "" ] && [ "$p1" != "all" ]; then
			eecho Invalid init command argument, $p1
			continue
		fi
		init_all
		continue
	fi

	# Start --- all | gtm [master|slave|all] | gtm_proxy | coordinator [master|slave|all] | datanode [master|slave|all] | nodename [master|slave|all]
	if [ "$cmdname" == "start" ]; then
		pgxc_start_something $p1 $p2 $p3 $p4 $cmdparam
		continue
	fi

	# Stop --- all | gtm [master|slave|all] | gtm_proxy | coordiantor [master|slave|all] | datanode [master|slave|all] | nodename [master|slave|all]
	if [ "$cmdname" == "stop" ]; then
		pgxc_stop_something $p1 $p2 $p3 $p4 $cmdparam
		continue
	fi
	# Kill -- kills processes will kill command
	if [ "$cmdname" == "kill" ]; then
		if [ "$p1" == "" ]; then
			eecho Specify what to kill "("all/component name")"
		elif [ "$p1" == "all" ]; then
			kill_all
		else
			pgxc_kill_something $p1 $p2 $p3 $p4 $cmdparam
			continue
		fi
	fi
	if [ "$cmdname" == "Psql" ]; then
		if [ "$p1" != "-" ]; then
			pgxc_select_coordinator
			if [ "$selected_coord" == "none" ]; then
				eecho No available coordinator found
				continue;
			fi
			psql -p $selected_coord_port -h $selected_coord_host $p1 $p2 $p3 $p4 $cmdparam
			continue
		else
			pgxc_find_coordinator $p2
			if [ "$selected_coord" == "none" ]; then
				eecho No available coordinator found
				continue;
			fi
			psql -p $selected_coord_port -h $selected_coord_host $p3 $p4 $cmdparam
			continue
		fi
		continue
	fi
	if [ "$cmdname" == "clean" ]; then
		if [ "$p1" == "all" ] || [ "$p1" == "" ]; then
			clean_all
		else
			pgxc_clean_node $p1 $p2 $p3 $p4 $cmdparam
		fi
		continue
	fi

	# failover --- gtm | component name
	if [ "$cmdname" == "failover" ]; then
		pgxc_failover_something $p1 $p2 $p3 $p4 $cmdparam
		continue
	fi
	if [ "$cmdname" == "Createdb" ]; then
		if [ "$p1" == "" ]; then
			eecho Specify database name to create
			continue
		fi
		if [ "$p1" == "-" ]; then
			pgxc_find_coordinator $p2
			if [ "$selected_coord" == "none" ]; then
				eecho No available coordinator found
				continue;
			fi
			createdb -p $selected_coord_port -h $selected_coord_host $p3 $p4 $cmdparam
			continue
		else
			pgxc_select_coordinator
			if [ "$selected_coord" == "none" ]; then
				eecho No available coordinator found
				continue;
			fi
			createdb -p $selected_coord_port -h $selected_coord_host $p1 $p2 $p3 $p4 $cmdparam
			continue
		fi
	fi
	if [ "$cmdname" == "Dropdb" ]; then
		if [ "$p1" == "" ]; then
			eecho Specify database name to drop
			continue
		fi
		if [ "$p1" == "-" ]; then
			pgxc_find_coordinator $p2
			if [ "$selected_coord" == "none" ]; then
				eecho No available coordinator found
				continue;
			fi
			if [ "$p3" == "" ]; then
				echo Specify database name to drop
				continue
			fi
			pgxc_clean_connection_all $p3
			dropdb -p $selected_coord_port -h $selected_coord_host $p3 $p4 $cmdparam
			continue
		else
			pgxc_select_coordinator
			if [ "$selected_coord" == "none" ]; then
				eecho No available coordinator found
				continue;
			fi
			pgxc_clean_connection_all $p1
			dropdb -p $selected_coord_port -h $selected_coord_host $p1 $p2 $p3 $p4 $cmdparam
			continue
		fi
	fi
	if [ "$cmdname" == "Createuser" ]; then
		if [ "$p1" == "" ]; then
			eecho Specify coordinator user name to create
			continue
		fi
		if [ "$p1" == "-" ]; then
			pgxc_find_coordinator $p2
			if [ "$selected_coord" == "none" ]; then
				eecho No available coordinator found
				continue;
			fi
			createuser -p $selected_coord_port -h $selected_coord_host $p3 $p4 $cmdparam
			continue
		else
			pgxc_select_coordinator
			if [ "$selected_coord" == "none" ]; then
				eecho No available coordinator found
				continue;
			fi
			createuser -p $selected_coord_port -h $selected_coord_host $p1 $p2 $p3 $p4 $cmdparam
			continue
		fi
		continue
	fi
	if [ "$cmdname" == "Dropuser" ]; then
		if [ "$p1" == "" ]; then
			eecho Specify user name to drop
			continue
		fi
		if [ "$p1" == "-" ]; then
			pgxc_find_coordinator $p2
			if [ "$selected_coord" == "none" ]; then
				eecho No available coordinator found
				continue;
			fi
			# we may need to clean connection for this user
			dropuser -p $selected_coord_port -h $selected_coord_host $p3 $p4 $cmdparam
			continue
		else
			pgxc_select_coordinator
			if [ "$selected_coord" == "none" ]; then
				eecho No available coordinator found
				continue;
			fi
			# we may need to clean connection for this user
			dropuser -p $selected_coord_port -h $selected_coord_host $p1 $p2 $p3 $p4 $cmdparam
			continue
		fi
		continue
	fi
	if [ "$cmdname" == "Vacuumdb" ]; then
		if [ "$p1" != "-" ]; then
			pgxc_select_coordinator
			continue
			if [ "$selected_coord" == "none" ]; then
				eecho No available coordinator found
				continue;
			fi
			vacuumdb -p $selected_coord_port -h $selected_coord_host $p1 $p2 $p3 $p4 $cmdparam
			continue
		else
			pgxc_find_coordinator $p2
			if [ "$selected_coord" == "none" ]; then
				eecho No available coordinator found
				continue;
			fi
			vacuumdb -p $selected_coord_port -h $selected_coord_host $p3 $p4 $cmdparam
			continue
		fi
	fi
	if [ "$cmdname" == "prepare" ]; then
		if [ "$p1" == "config" ]; then
			create_config_file_template
		else
			eecho Type prepare config
		fi
		continue
	fi
	if [ "$cmdname" == "monitor" ]; then
		if [ "$p1" == "all" ]; then
			monitor_components
		else
			pgxc_ctl_monitor $p1 $p2 $p3 $p4 $cmdparam
		fi
		continue
	fi
	if [ "$cmdname" == "xcshow" ]; then
		if [ "$p1" == "" ]; then
			echo Specify item to show
		elif [ "$p1" == "config" ]; then
			print_config
		elif [ "$p1" == "verbose" ]; then
			echo verbose=$verbose
		elif [ "$p1" == "interactive" ]; then
			echo interactive=$interactive
		elif [ "$p1" == "log" ]; then
			echo -n log option = $logOpt
			if [ "$logOpt" == "y" ]; then
				echo , logDir: $logDir
			else
				echo ""
			fi
		elif [ "$p1" == "backup" ]; then
			echo -n config backup = $configBackup
			if [ "$configBackup" == "y" ]; then
				echo "backup host:" $configBackupHost, "backup file:" $configBackupFile
			else
				echo ""
			fi
		elif [ "$p1" == "gtm" ]; then
			echo GTM list
			pgxc_ctl_show_component $gtmName
		elif [ "$p1" == "gtm_proxy" ]; then
			pgxc_ctl_show_gtm_proxy_all
		elif [ "$p1" == "coordinator" ]; then
			pgxc_ctl_show_coordinator_all
		elif [ "$p1" == "datanode" ]; then
			pgxc_ctl_show_datanode_all
		else
			pgxc_ctl_show_component $p1 $p2 $p3 $p4 $cmdparam
		fi
		continue
	fi
	if [ "$cmdname" == "xcset" ]; then
		if [ "$p1" == "verbose" ]; then
			set_verbose $p2
		elif [ "$p1" == "log" ]; then
			if [ "$p2" == "" ]; then
				oeecho Specify log file name.
			else
				change_log_file $p2
			fi
		elif [ "$p1" == "interactive" ]; then
			set_interactive $p2
		else
			echo "Invalid xcset option $p1"
		fi
		continue
	fi
	if [ "$cmdname" == "deploy" ]; then
		if [ "$p1" == "all" ]; then
			pgxc_deploy_all
		elif [ "$p1" == "" ]; then
			eecho Specify server name to deploy.
		else
			pgxc_deploy_individual $p1
		fi
		continue
	fi
	# Need the following trick to handle variable value assignment and echo.
	#
	# Note: we need to write the command output to the log too.
	cat > $localTmpDir/wk.cmd <<EOF
$cmdname $p1 $p2 $p3 $p4 $cmdparam
EOF
	source $localTmpDir/wk.cmd
	rm $localTmpDir/wk.cmd
	continue
done
