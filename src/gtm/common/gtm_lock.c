/*-------------------------------------------------------------------------
 *
 * gtm_lock.c
 *	Handling for locks in GTM
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#include "gtm/gtm_c.h"
#include "gtm/gtm_lock.h"
#include "gtm/elog.h"

/*
 * Acquire the request lock. Block if the lock is not available
 *
 * TODO We should track the locks acquired in the thread specific context. If an
 * error is thrown and cought, we don't want to keep holding to those locks
 * since that would lead to a deadlock. Right now, we assume that the caller
 * will appropriately catch errors and release the locks sanely.
 */
bool
GTM_RWLockAcquire(GTM_RWLock *lock, GTM_LockMode mode)
{
	int status = EINVAL;

	switch (mode)
	{
		case GTM_LOCKMODE_WRITE:
			status = pthread_rwlock_wrlock(&lock->lk_lock);
			break;

		case GTM_LOCKMODE_READ:
			status = pthread_rwlock_rdlock(&lock->lk_lock);
			break;

		default:
			elog(ERROR, "Invalid lockmode");
			break;
	}

	return status ? false : true;
}

/*
 * Release previously acquired lock
 */
bool
GTM_RWLockRelease(GTM_RWLock *lock)
{
	int status;
	status = pthread_rwlock_unlock(&lock->lk_lock);
	return status ? false : true;
}

/*
 * Initialize a lock
 */
int
GTM_RWLockInit(GTM_RWLock *lock)
{
	return pthread_rwlock_init(&lock->lk_lock, NULL);
}

/*
 * Destroy a lock
 */
int
GTM_RWLockDestroy(GTM_RWLock *lock)
{
	return pthread_rwlock_destroy(&lock->lk_lock);
}

/*
 * Conditionally acquire a lock. If the lock is not available, the function
 * immediately returns without blocking.
 *
 * Returns true if lock is successfully acquired. Otherwise returns false
 */
bool
GTM_RWLockConditionalAcquire(GTM_RWLock *lock, GTM_LockMode mode)
{
	int status = EINVAL;

	switch (mode)
	{
		case GTM_LOCKMODE_WRITE:
			status = pthread_rwlock_trywrlock(&lock->lk_lock);
			break;

		case GTM_LOCKMODE_READ:
			status = pthread_rwlock_tryrdlock(&lock->lk_lock);
			break;

		default:
			elog(ERROR, "Invalid lockmode");
			break;
	}

	return status ? false : true;
}

/*
 * Initialize a mutex lock
 */
int
GTM_MutexLockInit(GTM_MutexLock *lock)
{
	return pthread_mutex_init(&lock->lk_lock, NULL);
}

/*
 * Destroy a mutex lock
 */
int
GTM_MutexLockDestroy(GTM_MutexLock *lock)
{
	return pthread_mutex_destroy(&lock->lk_lock);
}

/*
 * Acquire a mutex lock
 *
 * Return true if the lock is successfully acquired, else return false.
 */
bool
GTM_MutexLockAcquire(GTM_MutexLock *lock)
{
	int status = pthread_mutex_lock(&lock->lk_lock);
	return status ? false : true;
}

/*
 * Release previously acquired lock
 */
bool
GTM_MutexLockRelease(GTM_MutexLock *lock)
{
	return pthread_mutex_unlock(&lock->lk_lock);
}

/*
 * Conditionally acquire a lock. If the lock is not available, the function
 * immediately returns without blocking.
 *
 * Returns true if lock is successfully acquired. Otherwise returns false
 */
bool
GTM_MutexLockConditionalAcquire(GTM_MutexLock *lock)
{
	int status = pthread_mutex_trylock(&lock->lk_lock);
	return status ? false : true;
}

/*
 * Initialize a condition variable
 */
int
GTM_CVInit(GTM_CV *cv)
{
	return pthread_cond_init(&cv->cv_condvar, NULL);
}

/*
 * Destroy the conditional variable
 */
int
GTM_CVDestroy(GTM_CV *cv)
{
	return pthread_cond_destroy(&cv->cv_condvar);
}

/*
 * Wake up all the threads waiting on this conditional variable
 */
int
GTM_CVBcast(GTM_CV *cv)
{
	return pthread_cond_broadcast(&cv->cv_condvar);
}

/*
 * Wake up only one thread waiting on this conditional variable
 */
int
GTM_CVSignal(GTM_CV *cv)
{
	return pthread_cond_signal(&cv->cv_condvar);
}

/*
 * Wait on a conditional variable. The caller must have acquired the mutex lock
 * already.
 */
int
GTM_CVWait(GTM_CV *cv, GTM_MutexLock *lock)
{
	return pthread_cond_wait(&cv->cv_condvar, &lock->lk_lock);
}
