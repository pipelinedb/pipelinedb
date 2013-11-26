/*-------------------------------------------------------------------------
 *
 * gtm_lock.h
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */

#ifndef GTM_LOCK_H
#define GTM_LOCK_H

#include <pthread.h>

typedef struct GTM_RWLock
{
	pthread_rwlock_t lk_lock;
} GTM_RWLock;

typedef struct GTM_MutexLock
{
	pthread_mutex_t lk_lock;
} GTM_MutexLock;

typedef enum GTM_LockMode
{
	GTM_LOCKMODE_WRITE,
	GTM_LOCKMODE_READ
} GTM_LockMode;

typedef struct GTM_CV
{
	pthread_cond_t	cv_condvar;
} GTM_CV;

extern bool GTM_RWLockAcquire(GTM_RWLock *lock, GTM_LockMode mode);
extern bool GTM_RWLockRelease(GTM_RWLock *lock);
extern int GTM_RWLockInit(GTM_RWLock *lock);
extern int GTM_RWLockDestroy(GTM_RWLock *lock);
extern bool GTM_RWLockConditionalAcquire(GTM_RWLock *lock, GTM_LockMode mode);

extern bool GTM_MutexLockAcquire(GTM_MutexLock *lock);
extern bool GTM_MutexLockRelease(GTM_MutexLock *lock);
extern int GTM_MutexLockInit(GTM_MutexLock *lock);
extern int GTM_MutexLockDestroy(GTM_MutexLock *lock);
extern bool GTM_MutexLockConditionalAcquire(GTM_MutexLock *lock);

extern int GTM_CVInit(GTM_CV *cv);
extern int GTM_CVDestroy(GTM_CV *cv);
extern int GTM_CVSignal(GTM_CV *cv);
extern int GTM_CVBcast(GTM_CV *cv);
extern int GTM_CVWait(GTM_CV *cv, GTM_MutexLock *lock);

#endif
