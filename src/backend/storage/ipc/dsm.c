/*-------------------------------------------------------------------------
 *
 * dsm.c
 *	  manage dynamic shared memory segments
 *
 * This file provides a set of services to make programming with dynamic
 * shared memory segments more convenient.  Unlike the low-level
 * facilities provided by dsm_impl.h and dsm_impl.c, mappings and segments
 * created using this module will be cleaned up automatically.  Mappings
 * will be removed when the resource owner under which they were created
 * is cleaned up, unless dsm_keep_mapping() is used, in which case they
 * have session lifespan.  Segments will be removed when there are no
 * remaining mappings, or at postmaster shutdown in any case.  After a
 * hard postmaster crash, remaining segments will be removed, if they
 * still exist, at the next postmaster startup.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/dsm.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#ifndef WIN32
#include <sys/mman.h>
#endif
#include <sys/stat.h>

#include "lib/ilist.h"
#include "miscadmin.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/pg_shmem.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/resowner_private.h"

#define PG_DYNSHMEM_CONTROL_MAGIC		0x9a503d32

/*
 * There's no point in getting too cheap here, because the minimum allocation
 * is one OS page, which is probably at least 4KB and could easily be as high
 * as 64KB.  Each currently sizeof(dsm_control_item), currently 8 bytes.
 */
#define PG_DYNSHMEM_FIXED_SLOTS			64
#define PG_DYNSHMEM_SLOTS_PER_BACKEND	2

#define INVALID_CONTROL_SLOT		((uint32) -1)

/* Backend-local tracking for on-detach callbacks. */
typedef struct dsm_segment_detach_callback
{
	on_dsm_detach_callback function;
	Datum		arg;
	slist_node	node;
} dsm_segment_detach_callback;

/* Backend-local state for a dynamic shared memory segment. */
struct dsm_segment
{
	dlist_node	node;			/* List link in dsm_segment_list. */
	ResourceOwner resowner;		/* Resource owner. */
	dsm_handle	handle;			/* Segment name. */
	uint32		control_slot;	/* Slot in control segment. */
	void	   *impl_private;	/* Implementation-specific private data. */
	void	   *mapped_address; /* Mapping address, or NULL if unmapped. */
	Size		mapped_size;	/* Size of our mapping. */
	slist_head	on_detach;		/* On-detach callbacks. */
};

/* Shared-memory state for a dynamic shared memory segment. */
typedef struct dsm_control_item
{
	dsm_handle	handle;
	uint32		refcnt;			/* 2+ = active, 1 = moribund, 0 = gone */
} dsm_control_item;

/* Layout of the dynamic shared memory control segment. */
typedef struct dsm_control_header
{
	uint32		magic;
	uint32		nitems;
	uint32		maxitems;
	dsm_control_item item[FLEXIBLE_ARRAY_MEMBER];
} dsm_control_header;

static void dsm_cleanup_for_mmap(void);
static void dsm_postmaster_shutdown(int code, Datum arg);
static dsm_segment *dsm_create_descriptor(void);
static bool dsm_control_segment_sane(dsm_control_header *control,
						 Size mapped_size);
static uint64 dsm_control_bytes_needed(uint32 nitems);

/* Has this backend initialized the dynamic shared memory system yet? */
static bool dsm_init_done = false;

/*
 * List of dynamic shared memory segments used by this backend.
 *
 * At process exit time, we must decrement the reference count of each
 * segment we have attached; this list makes it possible to find all such
 * segments.
 *
 * This list should always be empty in the postmaster.  We could probably
 * allow the postmaster to map dynamic shared memory segments before it
 * begins to start child processes, provided that each process adjusted
 * the reference counts for those segments in the control segment at
 * startup time, but there's no obvious need for such a facility, which
 * would also be complex to handle in the EXEC_BACKEND case.  Once the
 * postmaster has begun spawning children, there's an additional problem:
 * each new mapping would require an update to the control segment,
 * which requires locking, in which the postmaster must not be involved.
 */
static dlist_head dsm_segment_list = DLIST_STATIC_INIT(dsm_segment_list);

/*
 * Control segment information.
 *
 * Unlike ordinary shared memory segments, the control segment is not
 * reference counted; instead, it lasts for the postmaster's entire
 * life cycle.  For simplicity, it doesn't have a dsm_segment object either.
 */
static dsm_handle dsm_control_handle;
static dsm_control_header *dsm_control;
static Size dsm_control_mapped_size = 0;
static void *dsm_control_impl_private = NULL;

/*
 * Start up the dynamic shared memory system.
 *
 * This is called just once during each cluster lifetime, at postmaster
 * startup time.
 */
void
dsm_postmaster_startup(PGShmemHeader *shim)
{
	void	   *dsm_control_address = NULL;
	uint32		maxitems;
	Size		segsize;

	Assert(!IsUnderPostmaster);

	/* If dynamic shared memory is disabled, there's nothing to do. */
	if (dynamic_shared_memory_type == DSM_IMPL_NONE)
		return;

	/*
	 * If we're using the mmap implementations, clean up any leftovers.
	 * Cleanup isn't needed on Windows, and happens earlier in startup for
	 * POSIX and System V shared memory, via a direct call to
	 * dsm_cleanup_using_control_segment.
	 */
	if (dynamic_shared_memory_type == DSM_IMPL_MMAP)
		dsm_cleanup_for_mmap();

	/* Determine size for new control segment. */
	maxitems = PG_DYNSHMEM_FIXED_SLOTS
		+ PG_DYNSHMEM_SLOTS_PER_BACKEND * MaxBackends;
	elog(DEBUG2, "dynamic shared memory system will support %u segments",
		 maxitems);
	segsize = dsm_control_bytes_needed(maxitems);

	/*
	 * Loop until we find an unused identifier for the new control segment. We
	 * sometimes use 0 as a sentinel value indicating that no control segment
	 * is known to exist, so avoid using that value for a real control
	 * segment.
	 */
	for (;;)
	{
		Assert(dsm_control_address == NULL);
		Assert(dsm_control_mapped_size == 0);
		dsm_control_handle = random();
		if (dsm_control_handle == 0)
			continue;
		if (dsm_impl_op(DSM_OP_CREATE, dsm_control_handle, segsize,
						&dsm_control_impl_private, &dsm_control_address,
						&dsm_control_mapped_size, ERROR))
			break;
	}
	dsm_control = dsm_control_address;
	on_shmem_exit(dsm_postmaster_shutdown, PointerGetDatum(shim));
	elog(DEBUG2,
		 "created dynamic shared memory control segment %u (%zu bytes)",
		 dsm_control_handle, segsize);
	shim->dsm_control = dsm_control_handle;

	/* Initialize control segment. */
	dsm_control->magic = PG_DYNSHMEM_CONTROL_MAGIC;
	dsm_control->nitems = 0;
	dsm_control->maxitems = maxitems;
}

/*
 * Determine whether the control segment from the previous postmaster
 * invocation still exists.  If so, remove the dynamic shared memory
 * segments to which it refers, and then the control segment itself.
 */
void
dsm_cleanup_using_control_segment(dsm_handle old_control_handle)
{
	void	   *mapped_address = NULL;
	void	   *junk_mapped_address = NULL;
	void	   *impl_private = NULL;
	void	   *junk_impl_private = NULL;
	Size		mapped_size = 0;
	Size		junk_mapped_size = 0;
	uint32		nitems;
	uint32		i;
	dsm_control_header *old_control;

	/* If dynamic shared memory is disabled, there's nothing to do. */
	if (dynamic_shared_memory_type == DSM_IMPL_NONE)
		return;

	/*
	 * Try to attach the segment.  If this fails, it probably just means that
	 * the operating system has been rebooted and the segment no longer
	 * exists, or an unrelated proces has used the same shm ID.  So just fall
	 * out quietly.
	 */
	if (!dsm_impl_op(DSM_OP_ATTACH, old_control_handle, 0, &impl_private,
					 &mapped_address, &mapped_size, DEBUG1))
		return;

	/*
	 * We've managed to reattach it, but the contents might not be sane. If
	 * they aren't, we disregard the segment after all.
	 */
	old_control = (dsm_control_header *) mapped_address;
	if (!dsm_control_segment_sane(old_control, mapped_size))
	{
		dsm_impl_op(DSM_OP_DETACH, old_control_handle, 0, &impl_private,
					&mapped_address, &mapped_size, LOG);
		return;
	}

	/*
	 * OK, the control segment looks basically valid, so we can get use it to
	 * get a list of segments that need to be removed.
	 */
	nitems = old_control->nitems;
	for (i = 0; i < nitems; ++i)
	{
		dsm_handle	handle;
		uint32		refcnt;

		/* If the reference count is 0, the slot is actually unused. */
		refcnt = old_control->item[i].refcnt;
		if (refcnt == 0)
			continue;

		/* Log debugging information. */
		handle = old_control->item[i].handle;
		elog(DEBUG2, "cleaning up orphaned dynamic shared memory with ID %u (reference count %u)",
			 handle, refcnt);

		/* Destroy the referenced segment. */
		dsm_impl_op(DSM_OP_DESTROY, handle, 0, &junk_impl_private,
					&junk_mapped_address, &junk_mapped_size, LOG);
	}

	/* Destroy the old control segment, too. */
	elog(DEBUG2,
		 "cleaning up dynamic shared memory control segment with ID %u",
		 old_control_handle);
	dsm_impl_op(DSM_OP_DESTROY, old_control_handle, 0, &impl_private,
				&mapped_address, &mapped_size, LOG);
}

/*
 * When we're using the mmap shared memory implementation, "shared memory"
 * segments might even manage to survive an operating system reboot.
 * But there's no guarantee as to exactly what will survive: some segments
 * may survive, and others may not, and the contents of some may be out
 * of date.  In particular, the control segment may be out of date, so we
 * can't rely on it to figure out what to remove.  However, since we know
 * what directory contains the files we used as shared memory, we can simply
 * scan the directory and blow everything away that shouldn't be there.
 */
static void
dsm_cleanup_for_mmap(void)
{
	DIR		   *dir;
	struct dirent *dent;

	/* Open the directory; can't use AllocateDir in postmaster. */
	if ((dir = AllocateDir(PG_DYNSHMEM_DIR)) == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open directory \"%s\": %m",
						PG_DYNSHMEM_DIR)));

	/* Scan for something with a name of the correct format. */
	while ((dent = ReadDir(dir, PG_DYNSHMEM_DIR)) != NULL)
	{
		if (strncmp(dent->d_name, PG_DYNSHMEM_MMAP_FILE_PREFIX,
					strlen(PG_DYNSHMEM_MMAP_FILE_PREFIX)) == 0)
		{
			char		buf[MAXPGPATH];

			snprintf(buf, MAXPGPATH, PG_DYNSHMEM_DIR "/%s", dent->d_name);

			elog(DEBUG2, "removing file \"%s\"", buf);

			/* We found a matching file; so remove it. */
			if (unlink(buf) != 0)
			{
				int			save_errno;

				save_errno = errno;
				closedir(dir);
				errno = save_errno;

				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not remove file \"%s\": %m", buf)));
			}
		}
	}

	/* Cleanup complete. */
	FreeDir(dir);
}

/*
 * At shutdown time, we iterate over the control segment and remove all
 * remaining dynamic shared memory segments.  We avoid throwing errors here;
 * the postmaster is shutting down either way, and this is just non-critical
 * resource cleanup.
 */
static void
dsm_postmaster_shutdown(int code, Datum arg)
{
	uint32		nitems;
	uint32		i;
	void	   *dsm_control_address;
	void	   *junk_mapped_address = NULL;
	void	   *junk_impl_private = NULL;
	Size		junk_mapped_size = 0;
	PGShmemHeader *shim = (PGShmemHeader *) DatumGetPointer(arg);

	/*
	 * If some other backend exited uncleanly, it might have corrupted the
	 * control segment while it was dying.  In that case, we warn and ignore
	 * the contents of the control segment.  This may end up leaving behind
	 * stray shared memory segments, but there's not much we can do about that
	 * if the metadata is gone.
	 */
	nitems = dsm_control->nitems;
	if (!dsm_control_segment_sane(dsm_control, dsm_control_mapped_size))
	{
		ereport(LOG,
				(errmsg("dynamic shared memory control segment is corrupt")));
		return;
	}

	/* Remove any remaining segments. */
	for (i = 0; i < nitems; ++i)
	{
		dsm_handle	handle;

		/* If the reference count is 0, the slot is actually unused. */
		if (dsm_control->item[i].refcnt == 0)
			continue;

		/* Log debugging information. */
		handle = dsm_control->item[i].handle;
		elog(DEBUG2, "cleaning up orphaned dynamic shared memory with ID %u",
			 handle);

		/* Destroy the segment. */
		dsm_impl_op(DSM_OP_DESTROY, handle, 0, &junk_impl_private,
					&junk_mapped_address, &junk_mapped_size, LOG);
	}

	/* Remove the control segment itself. */
	elog(DEBUG2,
		 "cleaning up dynamic shared memory control segment with ID %u",
		 dsm_control_handle);
	dsm_control_address = dsm_control;
	dsm_impl_op(DSM_OP_DESTROY, dsm_control_handle, 0,
				&dsm_control_impl_private, &dsm_control_address,
				&dsm_control_mapped_size, LOG);
	dsm_control = dsm_control_address;
	shim->dsm_control = 0;
}

/*
 * Prepare this backend for dynamic shared memory usage.  Under EXEC_BACKEND,
 * we must reread the state file and map the control segment; in other cases,
 * we'll have inherited the postmaster's mapping and global variables.
 */
static void
dsm_backend_startup(void)
{
	/* If dynamic shared memory is disabled, reject this. */
	if (dynamic_shared_memory_type == DSM_IMPL_NONE)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("dynamic shared memory is disabled"),
				 errhint("Set dynamic_shared_memory_type to a value other than \"none\".")));

#ifdef EXEC_BACKEND
	{
		void	   *control_address = NULL;

		/* Attach control segment. */
		Assert(dsm_control_handle != 0);
		dsm_impl_op(DSM_OP_ATTACH, dsm_control_handle, 0,
					&dsm_control_impl_private, &control_address,
					&dsm_control_mapped_size, ERROR);
		dsm_control = control_address;
		/* If control segment doesn't look sane, something is badly wrong. */
		if (!dsm_control_segment_sane(dsm_control, dsm_control_mapped_size))
		{
			dsm_impl_op(DSM_OP_DETACH, dsm_control_handle, 0,
						&dsm_control_impl_private, &control_address,
						&dsm_control_mapped_size, WARNING);
			ereport(FATAL,
					(errcode(ERRCODE_INTERNAL_ERROR),
			  errmsg("dynamic shared memory control segment is not valid")));
		}
	}
#endif

	dsm_init_done = true;
}

#ifdef EXEC_BACKEND
/*
 * When running under EXEC_BACKEND, we get a callback here when the main
 * shared memory segment is re-attached, so that we can record the control
 * handle retrieved from it.
 */
void
dsm_set_control_handle(dsm_handle h)
{
	Assert(dsm_control_handle == 0 && h != 0);
	dsm_control_handle = h;
}
#endif

/*
 * Create a new dynamic shared memory segment.
 */
dsm_segment *
dsm_create(Size size)
{
	dsm_segment *seg = dsm_create_descriptor();
	uint32		i;
	uint32		nitems;

	/* Unsafe in postmaster (and pointless in a stand-alone backend). */
	Assert(IsUnderPostmaster);

	if (!dsm_init_done)
		dsm_backend_startup();

	/* Loop until we find an unused segment identifier. */
	for (;;)
	{
		Assert(seg->mapped_address == NULL && seg->mapped_size == 0);
		seg->handle = random();
		if (dsm_impl_op(DSM_OP_CREATE, seg->handle, size, &seg->impl_private,
						&seg->mapped_address, &seg->mapped_size, ERROR))
			break;
	}

	/* Lock the control segment so we can register the new segment. */
	LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);

	/* Search the control segment for an unused slot. */
	nitems = dsm_control->nitems;
	for (i = 0; i < nitems; ++i)
	{
		if (dsm_control->item[i].refcnt == 0)
		{
			dsm_control->item[i].handle = seg->handle;
			/* refcnt of 1 triggers destruction, so start at 2 */
			dsm_control->item[i].refcnt = 2;
			seg->control_slot = i;
			LWLockRelease(DynamicSharedMemoryControlLock);
			return seg;
		}
	}

	/* Verify that we can support an additional mapping. */
	if (nitems >= dsm_control->maxitems)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("too many dynamic shared memory segments")));

	/* Enter the handle into a new array slot. */
	dsm_control->item[nitems].handle = seg->handle;
	/* refcnt of 1 triggers destruction, so start at 2 */
	dsm_control->item[nitems].refcnt = 2;
	seg->control_slot = nitems;
	dsm_control->nitems++;
	LWLockRelease(DynamicSharedMemoryControlLock);

	return seg;
}

/*
 * Attach a dynamic shared memory segment.
 *
 * See comments for dsm_segment_handle() for an explanation of how this
 * is intended to be used.
 *
 * This function will return NULL if the segment isn't known to the system.
 * This can happen if we're asked to attach the segment, but then everyone
 * else detaches it (causing it to be destroyed) before we get around to
 * attaching it.
 */
dsm_segment *
dsm_attach(dsm_handle h)
{
	dsm_segment *seg;
	dlist_iter	iter;
	uint32		i;
	uint32		nitems;

	/* Unsafe in postmaster (and pointless in a stand-alone backend). */
	Assert(IsUnderPostmaster);

	if (!dsm_init_done)
		dsm_backend_startup();

	/*
	 * Since this is just a debugging cross-check, we could leave it out
	 * altogether, or include it only in assert-enabled builds.  But since the
	 * list of attached segments should normally be very short, let's include
	 * it always for right now.
	 *
	 * If you're hitting this error, you probably want to attempt to find an
	 * existing mapping via dsm_find_mapping() before calling dsm_attach() to
	 * create a new one.
	 */
	dlist_foreach(iter, &dsm_segment_list)
	{
		seg = dlist_container(dsm_segment, node, iter.cur);
		if (seg->handle == h)
			elog(ERROR, "can't attach the same segment more than once");
	}

	/* Create a new segment descriptor. */
	seg = dsm_create_descriptor();
	seg->handle = h;

	/* Bump reference count for this segment in shared memory. */
	LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);
	nitems = dsm_control->nitems;
	for (i = 0; i < nitems; ++i)
	{
		/* If the reference count is 0, the slot is actually unused. */
		if (dsm_control->item[i].refcnt == 0)
			continue;

		/* If the handle doesn't match, it's not the slot we want. */
		if (dsm_control->item[i].handle != seg->handle)
			continue;

		/*
		 * If the reference count is 1, the slot is still in use, but the
		 * segment is in the process of going away.  Treat that as if we
		 * didn't find a match.
		 */
		if (dsm_control->item[i].refcnt == 1)
			break;

		/* Otherwise we've found a match. */
		dsm_control->item[i].refcnt++;
		seg->control_slot = i;
		break;
	}
	LWLockRelease(DynamicSharedMemoryControlLock);

	/*
	 * If we didn't find the handle we're looking for in the control segment,
	 * it probably means that everyone else who had it mapped, including the
	 * original creator, died before we got to this point. It's up to the
	 * caller to decide what to do about that.
	 */
	if (seg->control_slot == INVALID_CONTROL_SLOT)
	{
		dsm_detach(seg);
		return NULL;
	}

	/* Here's where we actually try to map the segment. */
	dsm_impl_op(DSM_OP_ATTACH, seg->handle, 0, &seg->impl_private,
				&seg->mapped_address, &seg->mapped_size, ERROR);

	return seg;
}

/*
 * At backend shutdown time, detach any segments that are still attached.
 * (This is similar to dsm_detach_all, except that there's no reason to
 * unmap the control segment before exiting, so we don't bother.)
 */
void
dsm_backend_shutdown(void)
{
	while (!dlist_is_empty(&dsm_segment_list))
	{
		dsm_segment *seg;

		seg = dlist_head_element(dsm_segment, node, &dsm_segment_list);
		dsm_detach(seg);
	}
}

/*
 * Detach all shared memory segments, including the control segments.  This
 * should be called, along with PGSharedMemoryDetach, in processes that
 * might inherit mappings but are not intended to be connected to dynamic
 * shared memory.
 */
void
dsm_detach_all(void)
{
	void	   *control_address = dsm_control;

	while (!dlist_is_empty(&dsm_segment_list))
	{
		dsm_segment *seg;

		seg = dlist_head_element(dsm_segment, node, &dsm_segment_list);
		dsm_detach(seg);
	}

	if (control_address != NULL)
		dsm_impl_op(DSM_OP_DETACH, dsm_control_handle, 0,
					&dsm_control_impl_private, &control_address,
					&dsm_control_mapped_size, ERROR);
}

/*
 * Resize an existing shared memory segment.
 *
 * This may cause the shared memory segment to be remapped at a different
 * address.  For the caller's convenience, we return the mapped address.
 */
void *
dsm_resize(dsm_segment *seg, Size size)
{
	Assert(seg->control_slot != INVALID_CONTROL_SLOT);
	dsm_impl_op(DSM_OP_RESIZE, seg->handle, size, &seg->impl_private,
				&seg->mapped_address, &seg->mapped_size, ERROR);
	return seg->mapped_address;
}

/*
 * Remap an existing shared memory segment.
 *
 * This is intended to be used when some other process has extended the
 * mapping using dsm_resize(), but we've still only got the initial
 * portion mapped.  Since this might change the address at which the
 * segment is mapped, we return the new mapped address.
 */
void *
dsm_remap(dsm_segment *seg)
{
	dsm_impl_op(DSM_OP_ATTACH, seg->handle, 0, &seg->impl_private,
				&seg->mapped_address, &seg->mapped_size, ERROR);

	return seg->mapped_address;
}

/*
 * Detach from a shared memory segment, destroying the segment if we
 * remove the last reference.
 *
 * This function should never fail.  It will often be invoked when aborting
 * a transaction, and a further error won't serve any purpose.  It's not a
 * complete disaster if we fail to unmap or destroy the segment; it means a
 * resource leak, but that doesn't necessarily preclude further operations.
 */
void
dsm_detach(dsm_segment *seg)
{
	/*
	 * Invoke registered callbacks.  Just in case one of those callbacks
	 * throws a further error that brings us back here, pop the callback
	 * before invoking it, to avoid infinite error recursion.
	 */
	while (!slist_is_empty(&seg->on_detach))
	{
		slist_node *node;
		dsm_segment_detach_callback *cb;
		on_dsm_detach_callback function;
		Datum		arg;

		node = slist_pop_head_node(&seg->on_detach);
		cb = slist_container(dsm_segment_detach_callback, node, node);
		function = cb->function;
		arg = cb->arg;
		pfree(cb);

		function(seg, arg);
	}

	/*
	 * Try to remove the mapping, if one exists.  Normally, there will be, but
	 * maybe not, if we failed partway through a create or attach operation.
	 * We remove the mapping before decrementing the reference count so that
	 * the process that sees a zero reference count can be certain that no
	 * remaining mappings exist.  Even if this fails, we pretend that it
	 * works, because retrying is likely to fail in the same way.
	 */
	if (seg->mapped_address != NULL)
	{
		dsm_impl_op(DSM_OP_DETACH, seg->handle, 0, &seg->impl_private,
					&seg->mapped_address, &seg->mapped_size, WARNING);
		seg->impl_private = NULL;
		seg->mapped_address = NULL;
		seg->mapped_size = 0;
	}

	/* Reduce reference count, if we previously increased it. */
	if (seg->control_slot != INVALID_CONTROL_SLOT)
	{
		uint32		refcnt;
		uint32		control_slot = seg->control_slot;

		LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);
		Assert(dsm_control->item[control_slot].handle == seg->handle);
		Assert(dsm_control->item[control_slot].refcnt > 1);
		refcnt = --dsm_control->item[control_slot].refcnt;
		seg->control_slot = INVALID_CONTROL_SLOT;
		LWLockRelease(DynamicSharedMemoryControlLock);

		/* If new reference count is 1, try to destroy the segment. */
		if (refcnt == 1)
		{
			/*
			 * If we fail to destroy the segment here, or are killed before we
			 * finish doing so, the reference count will remain at 1, which
			 * will mean that nobody else can attach to the segment.  At
			 * postmaster shutdown time, or when a new postmaster is started
			 * after a hard kill, another attempt will be made to remove the
			 * segment.
			 *
			 * The main case we're worried about here is being killed by a
			 * signal before we can finish removing the segment.  In that
			 * case, it's important to be sure that the segment still gets
			 * removed. If we actually fail to remove the segment for some
			 * other reason, the postmaster may not have any better luck than
			 * we did.  There's not much we can do about that, though.
			 */
			if (dsm_impl_op(DSM_OP_DESTROY, seg->handle, 0, &seg->impl_private,
							&seg->mapped_address, &seg->mapped_size, WARNING))
			{
				LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);
				Assert(dsm_control->item[control_slot].handle == seg->handle);
				Assert(dsm_control->item[control_slot].refcnt == 1);
				dsm_control->item[control_slot].refcnt = 0;
				LWLockRelease(DynamicSharedMemoryControlLock);
			}
		}
	}

	/* Clean up our remaining backend-private data structures. */
	if (seg->resowner != NULL)
		ResourceOwnerForgetDSM(seg->resowner, seg);
	dlist_delete(&seg->node);
	pfree(seg);
}

/*
 * Keep a dynamic shared memory mapping until end of session.
 *
 * By default, mappings are owned by the current resource owner, which
 * typically means they stick around for the duration of the current query
 * only.
 */
void
dsm_keep_mapping(dsm_segment *seg)
{
	if (seg->resowner != NULL)
	{
		ResourceOwnerForgetDSM(seg->resowner, seg);
		seg->resowner = NULL;
	}
}

/*
 * Keep a dynamic shared memory segment until postmaster shutdown.
 *
 * This function should not be called more than once per segment;
 * on Windows, doing so will create unnecessary handles which will
 * consume system resources to no benefit.
 *
 * Note that this function does not arrange for the current process to
 * keep the segment mapped indefinitely; if that behavior is desired,
 * dsm_keep_mapping() should be used from each process that needs to
 * retain the mapping.
 */
void
dsm_keep_segment(dsm_segment *seg)
{
	/*
	 * Bump reference count for this segment in shared memory. This will
	 * ensure that even if there is no session which is attached to this
	 * segment, it will remain until postmaster shutdown.
	 */
	LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);
	dsm_control->item[seg->control_slot].refcnt++;
	LWLockRelease(DynamicSharedMemoryControlLock);

	dsm_impl_keep_segment(seg->handle, seg->impl_private);
}

/*
 * Find an existing mapping for a shared memory segment, if there is one.
 */
dsm_segment *
dsm_find_mapping(dsm_handle h)
{
	dlist_iter	iter;
	dsm_segment *seg;

	dlist_foreach(iter, &dsm_segment_list)
	{
		seg = dlist_container(dsm_segment, node, iter.cur);
		if (seg->handle == h)
			return seg;
	}

	return NULL;
}

/*
 * Get the address at which a dynamic shared memory segment is mapped.
 */
void *
dsm_segment_address(dsm_segment *seg)
{
	Assert(seg->mapped_address != NULL);
	return seg->mapped_address;
}

/*
 * Get the size of a mapping.
 */
Size
dsm_segment_map_length(dsm_segment *seg)
{
	Assert(seg->mapped_address != NULL);
	return seg->mapped_size;
}

/*
 * Get a handle for a mapping.
 *
 * To establish communication via dynamic shared memory between two backends,
 * one of them should first call dsm_create() to establish a new shared
 * memory mapping.  That process should then call dsm_segment_handle() to
 * obtain a handle for the mapping, and pass that handle to the
 * coordinating backend via some means (e.g. bgw_main_arg, or via the
 * main shared memory segment).  The recipient, once in position of the
 * handle, should call dsm_attach().
 */
dsm_handle
dsm_segment_handle(dsm_segment *seg)
{
	return seg->handle;
}

/*
 * Register an on-detach callback for a dynamic shared memory segment.
 */
void
on_dsm_detach(dsm_segment *seg, on_dsm_detach_callback function, Datum arg)
{
	dsm_segment_detach_callback *cb;

	cb = MemoryContextAlloc(TopMemoryContext,
							sizeof(dsm_segment_detach_callback));
	cb->function = function;
	cb->arg = arg;
	slist_push_head(&seg->on_detach, &cb->node);
}

/*
 * Unregister an on-detach callback for a dynamic shared memory segment.
 */
void
cancel_on_dsm_detach(dsm_segment *seg, on_dsm_detach_callback function,
					 Datum arg)
{
	slist_mutable_iter iter;

	slist_foreach_modify(iter, &seg->on_detach)
	{
		dsm_segment_detach_callback *cb;

		cb = slist_container(dsm_segment_detach_callback, node, iter.cur);
		if (cb->function == function && cb->arg == arg)
		{
			slist_delete_current(&iter);
			pfree(cb);
			break;
		}
	}
}

/*
 * Discard all registered on-detach callbacks without executing them.
 */
void
reset_on_dsm_detach(void)
{
	dlist_iter	iter;

	dlist_foreach(iter, &dsm_segment_list)
	{
		dsm_segment *seg = dlist_container(dsm_segment, node, iter.cur);

		/* Throw away explicit on-detach actions one by one. */
		while (!slist_is_empty(&seg->on_detach))
		{
			slist_node *node;
			dsm_segment_detach_callback *cb;

			node = slist_pop_head_node(&seg->on_detach);
			cb = slist_container(dsm_segment_detach_callback, node, node);
			pfree(cb);
		}

		/*
		 * Decrementing the reference count is a sort of implicit on-detach
		 * action; make sure we don't do that, either.
		 */
		seg->control_slot = INVALID_CONTROL_SLOT;
	}
}

/*
 * Create a segment descriptor.
 */
static dsm_segment *
dsm_create_descriptor(void)
{
	dsm_segment *seg;

	ResourceOwnerEnlargeDSMs(CurrentResourceOwner);

	seg = MemoryContextAlloc(TopMemoryContext, sizeof(dsm_segment));
	dlist_push_head(&dsm_segment_list, &seg->node);

	/* seg->handle must be initialized by the caller */
	seg->control_slot = INVALID_CONTROL_SLOT;
	seg->impl_private = NULL;
	seg->mapped_address = NULL;
	seg->mapped_size = 0;

	seg->resowner = CurrentResourceOwner;
	ResourceOwnerRememberDSM(CurrentResourceOwner, seg);

	slist_init(&seg->on_detach);

	return seg;
}

/*
 * Sanity check a control segment.
 *
 * The goal here isn't to detect everything that could possibly be wrong with
 * the control segment; there's not enough information for that.  Rather, the
 * goal is to make sure that someone can iterate over the items in the segment
 * without overrunning the end of the mapping and crashing.  We also check
 * the magic number since, if that's messed up, this may not even be one of
 * our segments at all.
 */
static bool
dsm_control_segment_sane(dsm_control_header *control, Size mapped_size)
{
	if (mapped_size < offsetof(dsm_control_header, item))
		return false;			/* Mapped size too short to read header. */
	if (control->magic != PG_DYNSHMEM_CONTROL_MAGIC)
		return false;			/* Magic number doesn't match. */
	if (dsm_control_bytes_needed(control->maxitems) > mapped_size)
		return false;			/* Max item count won't fit in map. */
	if (control->nitems > control->maxitems)
		return false;			/* Overfull. */
	return true;
}

/*
 * Compute the number of control-segment bytes needed to store a given
 * number of items.
 */
static uint64
dsm_control_bytes_needed(uint32 nitems)
{
	return offsetof(dsm_control_header, item)
		+sizeof(dsm_control_item) * (uint64) nitems;
}
