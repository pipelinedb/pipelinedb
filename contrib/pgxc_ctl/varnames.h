/*-------------------------------------------------------------------------
 *
 * varnames.h
 *
*    Variable name definition of Postgres-XC configuration and operation tool.
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef VARNAMES_H
#define VAR_VARNAMES_H

/* Install Directory */
#define VAR_pgxcInstallDir	"pgxcInstallDir" /* Not mandatory */

/* Overall */
#define VAR_pgxcOwner		"pgxcOwner"
#define VAR_pgxcUser		"pgxcUser"
#define VAR_tmpDir			"tmpDir"
#define VAR_localTmpDir		"localTmpDir"
#define VAR_logOpt			"logOpt"
#define VAR_logDir			"logDir"
#define VAR_configBackup	"configBackup"
#define VAR_configBackupHost	"configBackupHost"
#define VAR_configBackupDir	"configBackupDir"
#define VAR_configBackupFile	"configBackupFile"
#define VAR_allServers		"allServers"

/* GTM overall */
#define VAR_gtmName			"gtmName"

/* GTM master */
#define VAR_gtmMasterServer	"gtmMasterServer"
#define VAR_gtmMasterPort	"gtmMasterPort"
#define VAR_gtmMasterDir	"gtmMasterDir"
#define VAR_gtmExtraConfig	"gtmExtraConfig"
#define VAR_gtmMasterSpecificExtraConfig "gtmMasterSpecificExtraConfig"

/* GTM slave */
#define VAR_gtmSlave 		"gtmSlave"
#define VAR_gtmSlaveServer	"gtmSlaveServer"
#define VAR_gtmSlavePort	"gtmSlavePort"
#define VAR_gtmSlaveDir		"gtmSlaveDir"
#define VAR_gtmSlaveSpecificExtraConfig	"gtmSlaveSpecificExtraConfig"

/* GTM Proxy */
#define VAR_gtmProxy		"gtmProxy"
#define VAR_gtmProxyNames	"gtmProxyNames"
#define VAR_gtmProxyServers	"gtmProxyServers"
#define VAR_gtmProxyPorts	"gtmProxyPorts"
#define VAR_gtmProxyDirs	"gtmProxyDirs"
#define VAR_gtmPxyExtraConfig	"gtmPxyExtraConfig"
#define VAR_gtmPxySpecificExtraConfig	"gtmPxySpecificExtraConfig"

/* Coordinators overall */
#define VAR_coordNames		"coordNames"
#define VAR_coordPorts		"coordPorts"
#define VAR_poolerPorts		"poolerPorts"
#define VAR_coordPgHbaEntries	"coordPgHbaEntries"

/* Coordinators master */
#define VAR_coordMasterServers	"coordMasterServers"
#define VAR_coordMasterDirs		"coordMasterDirs"
#define VAR_coordMaxWALSenders	"coordMaxWALSenders"

/* Coordinators slave */
#define VAR_coordSlave		"coordSlave"
#define VAR_coordSlaveServers	"coordSlaveServers"
#define VAR_coordSlaveSync	"coordSlaveSync"
#define VAR_coordSlaveDirs	"coordSlaveDirs"
#define VAR_coordArchLogDirs	"coordArchLogDirs"

/* Coordinator configuration files */
#define VAR_coordExtraConfig	"coordExtraConfig"
#define VAR_coordSpecificExtraConfig	"coordSpecificExtraConfig"
#define VAR_coordExtraPgHba		"coordExtraPgHba"
#define VAR_coordSpecificExtraPgHba	"coordSpecificExtraPgHba"

/* Coordinators additional slaves */
/* Actual additional slave configuration will be obtained from coordAdditionalSlaveSet */
#define VAR_coordAdditionalSlaves	"coordAdditionalSlaves"
#define VAR_coordAdditionalSlaveSet	"coordAdditionalSlaveSet"


/* Datanodes overall */
#define VAR_coordAdditionalSlaveSet	"coordAdditionalSlaveSet"
#define VAR_datanodeNames			"datanodeNames"
#define VAR_datanodePorts			"datanodePorts"
#define VAR_datanodePgHbaEntries	"datanodePgHbaEntries"
#define VAR_primaryDatanode			"primaryDatanode"

/* Datanode masters */
#define VAR_datanodeMasterServers	"datanodeMasterServers"
#define VAR_datanodeMasterDirs		"datanodeMasterDirs"
#define VAR_datanodeMaxWALSenders	"datanodeMaxWALSenders"

/* Datanode slaves */
#define VAR_datanodeSlave			"datanodeSlave"
#define VAR_datanodeSlaveServers	"datanodeSlaveServers"
#define VAR_datanodeSlaveSync		"datanodeSlaveSync"
#define VAR_datanodeSlaveDirs		"datanodeSlaveDirs"
#define VAR_datanodeArchLogDirs		"datanodeArchLogDirs"

/* Datanode configuration files */
#define VAR_datanodeExtraConfig		"datanodeExtraConfig"
#define VAR_datanodeSpecificExtraConfig	"datanodeSpecificExtraConfig"
#define VAR_datanodeExtraPgHba		"datanodeExtraPgHba"
#define VAR_datanodeSpecificExtraPgHba	"datanodeSpecificExtraPgHba"

/* Datanode additional slaves */
/* Actual additional slave configuration will be obtained from datanodeAdditionalSlaveSet */
#define VAR_datanodeAdditionalSlaves	"datanodeAdditionalSlaves"
#define VAR_datanodeAdditionalSlaveSet	"datanodeAdditionalSlaveSet"

/* WAL Archives */
/* Actual wal archive will be obtained from walArchiveSet */
#define VAR_walArchive		"walArchive"
#define VAR_walArchiveSet	"walArchiveSet"

/* Connection to datanode/coordinator */

#define VAR_pgxcCtlName		"pgxcCtlName"
#define VAR_defaultDatabase	"defaultDatabase"

/* Other Options */

#define VAR_pgxc_ctl_home	"pgxc_ctl_home"
#define VAR_xc_prompt		"xc_prompt"
#define VAR_verbose			"verbose"
#define VAR_logDir			"logDir"
#define VAR_logFile			"logFile"
#define VAR_tmpDir			"tmpDir"
#define VAR_localTmpDir		"localTmpDir"
#define VAR_configFile		"configFile"
#define VAR_echoAll			"echoAll"
#define VAR_debug			"debug"
#define VAR_logMessage		"logMessage"
#define VAR_printMessage	"printMessage"	
#define VAR_logLocation		"logLocation"
#define VAR_printLocation	"printLocation"

#endif /* VARNAMES_H */
