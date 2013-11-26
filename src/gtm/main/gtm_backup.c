#include "gtm/gtm_c.h"
#include "gtm/gtm_lock.h"
#include "gtm/gtm_txn.h"
#include "gtm/gtm_seq.h"
#include "gtm/gtm_backup.h"
#include "gtm/elog.h"

GTM_RWLock gtm_bkup_lock;
bool gtm_need_bkup;

extern char GTMControlFile[];



void GTM_WriteRestorePoint(void)
{
	FILE *f;

	GTM_RWLockAcquire(&gtm_bkup_lock, GTM_LOCKMODE_WRITE);
	if (!gtm_need_bkup)
	{
		GTM_RWLockRelease(&gtm_bkup_lock);
		return;
	}

	f = fopen(GTMControlFile, "w");
	if (f == NULL)
	{
		ereport(LOG, (errno,
					  errmsg("Cannot open control file"),
					  errhint("%s", strerror(errno))));
		return;
	}
	gtm_need_bkup = FALSE;
	GTM_RWLockRelease(&gtm_bkup_lock);
	GTM_WriteRestorePointXid(f);
	GTM_WriteRestorePointSeq(f);
	fclose(f);
}

void GTM_WriteBarrierBackup(char *barrier_id)
{
#define MyMAXPATH 1023

	FILE  *f;
	char BarrierFilePath[MyMAXPATH+1];
	extern char *GTMDataDir;

	snprintf(BarrierFilePath, MyMAXPATH, "%s/GTM_%s.control", GTMDataDir, barrier_id);
	if ((f = fopen(BarrierFilePath, "w")) == NULL)
	{
		ereport(LOG, (errno,
					  errmsg("Cannot open control file"),
					  errhint("%s", strerror(errno))));
		return;
	}
	GTM_RWLockAcquire(&gtm_bkup_lock, GTM_LOCKMODE_WRITE);
	gtm_need_bkup = FALSE;
	GTM_RWLockRelease(&gtm_bkup_lock);
	GTM_WriteRestorePointXid(f);
	GTM_WriteRestorePointSeq(f);
	fclose(f);
}
	

void GTM_MakeBackup(char *path)
{
	FILE *f = fopen(path, "w");

	if (f == NULL)
	{
		ereport(LOG, (errno,
					  errmsg("Cannot open backup file %s", path),
					  errhint("%s", strerror(errno))));
		return;
	}
	GTM_SaveTxnInfo(f);
	GTM_SaveSeqInfo(f);
	fclose(f);
}

void GTM_SetNeedBackup(void)
{
	GTM_RWLockAcquire(&gtm_bkup_lock, GTM_LOCKMODE_READ);
	gtm_need_bkup = TRUE;
	GTM_RWLockRelease(&gtm_bkup_lock);
}

bool GTM_NeedBackup(void)
{
	return gtm_need_bkup;
}
