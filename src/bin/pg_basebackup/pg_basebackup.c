/*-------------------------------------------------------------------------
 *
 * pg_basebackup.c - receive a base backup using streaming replication protocol
 *
 * Author: Magnus Hagander <magnus@hagander.net>
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_basebackup/pg_basebackup.c
 *-------------------------------------------------------------------------
 */

/*
 * We have to use postgres.h not postgres_fe.h here, because there's so much
 * backend-only stuff in the XLOG include files we need.  But we need a
 * frontend-ish environment otherwise.	Hence this ugly hack.
 */
#define FRONTEND 1
#include "postgres.h"
#include "libpq-fe.h"

#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>

#ifdef HAVE_LIBZ
#include <zlib.h>
#endif

#include "getopt_long.h"

#include "receivelog.h"
#include "streamutil.h"


/* Global options */
char	   *basedir = NULL;
char		format = 'p';		/* p(lain)/t(ar) */
char	   *label = "pg_basebackup base backup";
bool		showprogress = false;
int			verbose = 0;
int			compresslevel = 0;
bool		includewal = false;
bool		streamwal = false;
bool		fastcheckpoint = false;
int			standby_message_timeout = 10 * 1000;		/* 10 sec = default */

/* Progress counters */
static uint64 totalsize;
static uint64 totaldone;
static int	tablespacecount;

/* Pipe to communicate with background wal receiver process */
#ifndef WIN32
static int	bgpipe[2] = {-1, -1};
#endif

/* Handle to child process */
static pid_t bgchild = -1;

/* End position for xlog streaming, empty string if unknown yet */
static XLogRecPtr xlogendptr;

#ifndef WIN32
static int	has_xlogendptr = 0;
#else
static volatile LONG has_xlogendptr = 0;
#endif

/* Function headers */
static void usage(void);
static void verify_dir_is_empty_or_create(char *dirname);
static void progress_report(int tablespacenum, const char *filename);

static void ReceiveTarFile(PGconn *conn, PGresult *res, int rownum);
static void ReceiveAndUnpackTarFile(PGconn *conn, PGresult *res, int rownum);
static void BaseBackup(void);

static bool reached_end_position(XLogRecPtr segendpos, uint32 timeline, bool segment_finished);

#ifdef HAVE_LIBZ
static const char *
get_gz_error(gzFile gzf)
{
	int			errnum;
	const char *errmsg;

	errmsg = gzerror(gzf, &errnum);
	if (errnum == Z_ERRNO)
		return strerror(errno);
	else
		return errmsg;
}
#endif


static void
usage(void)
{
	printf(_("%s takes a base backup of a running PostgreSQL server.\n\n"),
		   progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]...\n"), progname);
	printf(_("\nOptions controlling the output:\n"));
	printf(_("  -D, --pgdata=DIRECTORY   receive base backup into directory\n"));
	printf(_("  -F, --format=p|t         output format (plain (default), tar)\n"));
	printf(_("  -x, --xlog               include required WAL files in backup (fetch mode)\n"));
	printf(_("  -X, --xlog-method=fetch|stream\n"
		   "                           include required WAL files with specified method\n"));
	printf(_("  -z, --gzip               compress tar output\n"));
	printf(_("  -Z, --compress=0-9       compress tar output with given compression level\n"));
	printf(_("\nGeneral options:\n"));
	printf(_("  -c, --checkpoint=fast|spread\n"
		   "                           set fast or spread checkpointing\n"));
	printf(_("  -l, --label=LABEL        set backup label\n"));
	printf(_("  -P, --progress           show progress information\n"));
	printf(_("  -v, --verbose            output verbose messages\n"));
	printf(_("  --help                   show this help, then exit\n"));
	printf(_("  --version                output version information, then exit\n"));
	printf(_("\nConnection options:\n"));
	printf(_("  -s, --statusint=INTERVAL time between status packets sent to server (in seconds)\n"));
	printf(_("  -h, --host=HOSTNAME      database server host or socket directory\n"));
	printf(_("  -p, --port=PORT          database server port number\n"));
	printf(_("  -U, --username=NAME      connect as specified database user\n"));
	printf(_("  -w, --no-password        never prompt for password\n"));
	printf(_("  -W, --password           force password prompt (should happen automatically)\n"));
	printf(_("\nReport bugs to <pgsql-bugs@postgresql.org>.\n"));
}


/*
 * Called in the background process every time data is received.
 * On Unix, we check to see if there is any data on our pipe
 * (which would mean we have a stop position), and if it is, check if
 * it is time to stop.
 * On Windows, we are in a single process, so we can just check if it's
 * time to stop.
 */
static bool
reached_end_position(XLogRecPtr segendpos, uint32 timeline, bool segment_finished)
{
	if (!has_xlogendptr)
	{
#ifndef WIN32
		fd_set		fds;
		struct timeval tv;
		int			r;

		/*
		 * Don't have the end pointer yet - check our pipe to see if it has
		 * been sent yet.
		 */
		FD_ZERO(&fds);
		FD_SET(bgpipe[0], &fds);

		MemSet(&tv, 0, sizeof(tv));

		r = select(bgpipe[0] + 1, &fds, NULL, NULL, &tv);
		if (r == 1)
		{
			char		xlogend[64];

			MemSet(xlogend, 0, sizeof(xlogend));
			r = read(bgpipe[0], xlogend, sizeof(xlogend));
			if (r < 0)
			{
				fprintf(stderr, _("%s: could not read from ready pipe: %s\n"),
						progname, strerror(errno));
				exit(1);
			}

			if (sscanf(xlogend, "%X/%X", &xlogendptr.xlogid, &xlogendptr.xrecoff) != 2)
			{
				fprintf(stderr, _("%s: could not parse xlog end position \"%s\"\n"),
						progname, xlogend);
				exit(1);
			}
			has_xlogendptr = 1;

			/*
			 * Fall through to check if we've reached the point further
			 * already.
			 */
		}
		else
		{
			/*
			 * No data received on the pipe means we don't know the end
			 * position yet - so just say it's not time to stop yet.
			 */
			return false;
		}
#else

		/*
		 * On win32, has_xlogendptr is set by the main thread, so if it's not
		 * set here, we just go back and wait until it shows up.
		 */
		return false;
#endif
	}

	/*
	 * At this point we have an end pointer, so compare it to the current
	 * position to figure out if it's time to stop.
	 */
	if (segendpos.xlogid > xlogendptr.xlogid ||
		(segendpos.xlogid == xlogendptr.xlogid &&
		 segendpos.xrecoff >= xlogendptr.xrecoff))
		return true;

	/*
	 * Have end pointer, but haven't reached it yet - so tell the caller to
	 * keep streaming.
	 */
	return false;
}

typedef struct
{
	PGconn	   *bgconn;
	XLogRecPtr	startptr;
	char		xlogdir[MAXPGPATH];
	char	   *sysidentifier;
	int			timeline;
} logstreamer_param;

static int
LogStreamerMain(logstreamer_param *param)
{
	if (!ReceiveXlogStream(param->bgconn, param->startptr, param->timeline,
						   param->sysidentifier, param->xlogdir,
						reached_end_position, standby_message_timeout, true))

		/*
		 * Any errors will already have been reported in the function process,
		 * but we need to tell the parent that we didn't shutdown in a nice
		 * way.
		 */
		return 1;

	PQfinish(param->bgconn);
	return 0;
}

/*
 * Initiate background process for receiving xlog during the backup.
 * The background stream will use its own database connection so we can
 * stream the logfile in parallel with the backups.
 */
static void
StartLogStreamer(char *startpos, uint32 timeline, char *sysidentifier)
{
	logstreamer_param *param;

	param = xmalloc0(sizeof(logstreamer_param));
	param->timeline = timeline;
	param->sysidentifier = sysidentifier;

	/* Convert the starting position */
	if (sscanf(startpos, "%X/%X", &param->startptr.xlogid, &param->startptr.xrecoff) != 2)
	{
		fprintf(stderr, _("%s: invalid format of xlog location: %s\n"),
				progname, startpos);
		disconnect_and_exit(1);
	}
	/* Round off to even segment position */
	param->startptr.xrecoff -= param->startptr.xrecoff % XLOG_SEG_SIZE;

#ifndef WIN32
	/* Create our background pipe */
	if (pipe(bgpipe) < 0)
	{
		fprintf(stderr, _("%s: could not create pipe for background process: %s\n"),
				progname, strerror(errno));
		disconnect_and_exit(1);
	}
#endif

	/* Get a second connection */
	param->bgconn = GetConnection();
	if (!param->bgconn)
		/* Error message already written in GetConnection() */
		exit(1);

	/*
	 * Always in plain format, so we can write to basedir/pg_xlog. But the
	 * directory entry in the tar file may arrive later, so make sure it's
	 * created before we start.
	 */
	snprintf(param->xlogdir, sizeof(param->xlogdir), "%s/pg_xlog", basedir);
	verify_dir_is_empty_or_create(param->xlogdir);

	/*
	 * Start a child process and tell it to start streaming. On Unix, this is
	 * a fork(). On Windows, we create a thread.
	 */
#ifndef WIN32
	bgchild = fork();
	if (bgchild == 0)
	{
		/* in child process */
		exit(LogStreamerMain(param));
	}
	else if (bgchild < 0)
	{
		fprintf(stderr, _("%s: could not create background process: %s\n"),
				progname, strerror(errno));
		disconnect_and_exit(1);
	}

	/*
	 * Else we are in the parent process and all is well.
	 */
#else							/* WIN32 */
	bgchild = _beginthreadex(NULL, 0, (void *) LogStreamerMain, param, 0, NULL);
	if (bgchild == 0)
	{
		fprintf(stderr, _("%s: could not create background thread: %s\n"),
				progname, strerror(errno));
		disconnect_and_exit(1);
	}
#endif
}

/*
 * Verify that the given directory exists and is empty. If it does not
 * exist, it is created. If it exists but is not empty, an error will
 * be give and the process ended.
 */
static void
verify_dir_is_empty_or_create(char *dirname)
{
	switch (pg_check_dir(dirname))
	{
		case 0:

			/*
			 * Does not exist, so create
			 */
			if (pg_mkdir_p(dirname, S_IRWXU) == -1)
			{
				fprintf(stderr,
						_("%s: could not create directory \"%s\": %s\n"),
						progname, dirname, strerror(errno));
				disconnect_and_exit(1);
			}
			return;
		case 1:

			/*
			 * Exists, empty
			 */
			return;
		case 2:

			/*
			 * Exists, not empty
			 */
			fprintf(stderr,
					_("%s: directory \"%s\" exists but is not empty\n"),
					progname, dirname);
			disconnect_and_exit(1);
		case -1:

			/*
			 * Access problem
			 */
			fprintf(stderr, _("%s: could not access directory \"%s\": %s\n"),
					progname, dirname, strerror(errno));
			disconnect_and_exit(1);
	}
}


/*
 * Print a progress report based on the global variables. If verbose output
 * is enabled, also print the current file name.
 */
static void
progress_report(int tablespacenum, const char *filename)
{
	int			percent = (int) ((totaldone / 1024) * 100 / totalsize);
	char		totaldone_str[32];
	char		totalsize_str[32];

	/*
	 * Avoid overflowing past 100% or the full size. This may make the total
	 * size number change as we approach the end of the backup (the estimate
	 * will always be wrong if WAL is included), but that's better than having
	 * the done column be bigger than the total.
	 */
	if (percent > 100)
		percent = 100;
	if (totaldone / 1024 > totalsize)
		totalsize = totaldone / 1024;

	/*
	 * Separate step to keep platform-dependent format code out of
	 * translatable strings.  And we only test for INT64_FORMAT availability
	 * in snprintf, not fprintf.
	 */
	snprintf(totaldone_str, sizeof(totaldone_str), INT64_FORMAT, totaldone / 1024);
	snprintf(totalsize_str, sizeof(totalsize_str), INT64_FORMAT, totalsize);

	if (verbose)
	{
		if (!filename)

			/*
			 * No filename given, so clear the status line (used for last
			 * call)
			 */
			fprintf(stderr,
					ngettext("%s/%s kB (100%%), %d/%d tablespace %35s",
							 "%s/%s kB (100%%), %d/%d tablespaces %35s",
							 tablespacecount),
			totaldone_str, totalsize_str, tablespacenum, tablespacecount, "");
		else
			fprintf(stderr,
					ngettext("%s/%s kB (%d%%), %d/%d tablespace (%-30.30s)",
							 "%s/%s kB (%d%%), %d/%d tablespaces (%-30.30s)",
							 tablespacecount),
					totaldone_str, totalsize_str, percent, tablespacenum, tablespacecount, filename);
	}
	else
		fprintf(stderr,
				ngettext("%s/%s kB (%d%%), %d/%d tablespace",
						 "%s/%s kB (%d%%), %d/%d tablespaces",
						 tablespacecount),
				totaldone_str, totalsize_str, percent, tablespacenum, tablespacecount);

	fprintf(stderr, "\r");
}


/*
 * Receive a tar format file from the connection to the server, and write
 * the data from this file directly into a tar file. If compression is
 * enabled, the data will be compressed while written to the file.
 *
 * The file will be named base.tar[.gz] if it's for the main data directory
 * or <tablespaceoid>.tar[.gz] if it's for another tablespace.
 *
 * No attempt to inspect or validate the contents of the file is done.
 */
static void
ReceiveTarFile(PGconn *conn, PGresult *res, int rownum)
{
	char		filename[MAXPGPATH];
	char	   *copybuf = NULL;
	FILE	   *tarfile = NULL;

#ifdef HAVE_LIBZ
	gzFile		ztarfile = NULL;
#endif

	if (PQgetisnull(res, rownum, 0))

		/*
		 * Base tablespaces
		 */
		if (strcmp(basedir, "-") == 0)
		{
#ifdef HAVE_LIBZ
			if (compresslevel != 0)
			{
				ztarfile = gzdopen(dup(fileno(stdout)), "wb");
				if (gzsetparams(ztarfile, compresslevel, Z_DEFAULT_STRATEGY) != Z_OK)
				{
					fprintf(stderr, _("%s: could not set compression level %d: %s\n"),
							progname, compresslevel, get_gz_error(ztarfile));
					disconnect_and_exit(1);
				}
			}
			else
#endif
				tarfile = stdout;
		}
		else
		{
#ifdef HAVE_LIBZ
			if (compresslevel != 0)
			{
				snprintf(filename, sizeof(filename), "%s/base.tar.gz", basedir);
				ztarfile = gzopen(filename, "wb");
				if (gzsetparams(ztarfile, compresslevel, Z_DEFAULT_STRATEGY) != Z_OK)
				{
					fprintf(stderr, _("%s: could not set compression level %d: %s\n"),
							progname, compresslevel, get_gz_error(ztarfile));
					disconnect_and_exit(1);
				}
			}
			else
#endif
			{
				snprintf(filename, sizeof(filename), "%s/base.tar", basedir);
				tarfile = fopen(filename, "wb");
			}
		}
	else
	{
		/*
		 * Specific tablespace
		 */
#ifdef HAVE_LIBZ
		if (compresslevel != 0)
		{
			snprintf(filename, sizeof(filename), "%s/%s.tar.gz", basedir, PQgetvalue(res, rownum, 0));
			ztarfile = gzopen(filename, "wb");
			if (gzsetparams(ztarfile, compresslevel, Z_DEFAULT_STRATEGY) != Z_OK)
			{
				fprintf(stderr, _("%s: could not set compression level %d: %s\n"),
						progname, compresslevel, get_gz_error(ztarfile));
				disconnect_and_exit(1);
			}
		}
		else
#endif
		{
			snprintf(filename, sizeof(filename), "%s/%s.tar", basedir, PQgetvalue(res, rownum, 0));
			tarfile = fopen(filename, "wb");
		}
	}

#ifdef HAVE_LIBZ
	if (compresslevel != 0)
	{
		if (!ztarfile)
		{
			/* Compression is in use */
			fprintf(stderr, _("%s: could not create compressed file \"%s\": %s\n"),
					progname, filename, get_gz_error(ztarfile));
			disconnect_and_exit(1);
		}
	}
	else
#endif
	{
		/* Either no zlib support, or zlib support but compresslevel = 0 */
		if (!tarfile)
		{
			fprintf(stderr, _("%s: could not create file \"%s\": %s\n"),
					progname, filename, strerror(errno));
			disconnect_and_exit(1);
		}
	}

	/*
	 * Get the COPY data stream
	 */
	res = PQgetResult(conn);
	if (PQresultStatus(res) != PGRES_COPY_OUT)
	{
		fprintf(stderr, _("%s: could not get COPY data stream: %s"),
				progname, PQerrorMessage(conn));
		disconnect_and_exit(1);
	}

	while (1)
	{
		int			r;

		if (copybuf != NULL)
		{
			PQfreemem(copybuf);
			copybuf = NULL;
		}

		r = PQgetCopyData(conn, &copybuf, 0);
		if (r == -1)
		{
			/*
			 * End of chunk. Close file (but not stdout).
			 *
			 * Also, write two completely empty blocks at the end of the tar
			 * file, as required by some tar programs.
			 */
			char		zerobuf[1024];

			MemSet(zerobuf, 0, sizeof(zerobuf));
#ifdef HAVE_LIBZ
			if (ztarfile != NULL)
			{
				if (gzwrite(ztarfile, zerobuf, sizeof(zerobuf)) != sizeof(zerobuf))
				{
					fprintf(stderr, _("%s: could not write to compressed file \"%s\": %s\n"),
							progname, filename, get_gz_error(ztarfile));
					disconnect_and_exit(1);
				}
			}
			else
#endif
			{
				if (fwrite(zerobuf, sizeof(zerobuf), 1, tarfile) != 1)
				{
					fprintf(stderr, _("%s: could not write to file \"%s\": %s\n"),
							progname, filename, strerror(errno));
					disconnect_and_exit(1);
				}
			}

#ifdef HAVE_LIBZ
			if (ztarfile != NULL)
			{
				if (gzclose(ztarfile) != 0)
				{
					fprintf(stderr, _("%s: could not close compressed file \"%s\": %s\n"),
							progname, filename, get_gz_error(ztarfile));
					disconnect_and_exit(1);
				}
			}
			else
#endif
			{
				if (strcmp(basedir, "-") != 0)
				{
					if (fclose(tarfile) != 0)
					{
						fprintf(stderr, _("%s: could not close file \"%s\": %s\n"),
								progname, filename, strerror(errno));
						disconnect_and_exit(1);
					}
				}
			}

			break;
		}
		else if (r == -2)
		{
			fprintf(stderr, _("%s: could not read COPY data: %s"),
					progname, PQerrorMessage(conn));
			disconnect_and_exit(1);
		}

#ifdef HAVE_LIBZ
		if (ztarfile != NULL)
		{
			if (gzwrite(ztarfile, copybuf, r) != r)
			{
				fprintf(stderr, _("%s: could not write to compressed file \"%s\": %s\n"),
						progname, filename, get_gz_error(ztarfile));
				disconnect_and_exit(1);
			}
		}
		else
#endif
		{
			if (fwrite(copybuf, r, 1, tarfile) != 1)
			{
				fprintf(stderr, _("%s: could not write to file \"%s\": %s\n"),
						progname, filename, strerror(errno));
				disconnect_and_exit(1);
			}
		}
		totaldone += r;
		if (showprogress)
			progress_report(rownum, filename);
	}							/* while (1) */

	if (copybuf != NULL)
		PQfreemem(copybuf);
}

/*
 * Receive a tar format stream from the connection to the server, and unpack
 * the contents of it into a directory. Only files, directories and
 * symlinks are supported, no other kinds of special files.
 *
 * If the data is for the main data directory, it will be restored in the
 * specified directory. If it's for another tablespace, it will be restored
 * in the original directory, since relocation of tablespaces is not
 * supported.
 */
static void
ReceiveAndUnpackTarFile(PGconn *conn, PGresult *res, int rownum)
{
	char		current_path[MAXPGPATH];
	char		filename[MAXPGPATH];
	int			current_len_left;
	int			current_padding = 0;
	char	   *copybuf = NULL;
	FILE	   *file = NULL;

	if (PQgetisnull(res, rownum, 0))
		strcpy(current_path, basedir);
	else
		strcpy(current_path, PQgetvalue(res, rownum, 1));

	/*
	 * Get the COPY data
	 */
	res = PQgetResult(conn);
	if (PQresultStatus(res) != PGRES_COPY_OUT)
	{
		fprintf(stderr, _("%s: could not get COPY data stream: %s"),
				progname, PQerrorMessage(conn));
		disconnect_and_exit(1);
	}

	while (1)
	{
		int			r;

		if (copybuf != NULL)
		{
			PQfreemem(copybuf);
			copybuf = NULL;
		}

		r = PQgetCopyData(conn, &copybuf, 0);

		if (r == -1)
		{
			/*
			 * End of chunk
			 */
			if (file)
				fclose(file);

			break;
		}
		else if (r == -2)
		{
			fprintf(stderr, _("%s: could not read COPY data: %s"),
					progname, PQerrorMessage(conn));
			disconnect_and_exit(1);
		}

		if (file == NULL)
		{
			int			filemode;

			/*
			 * No current file, so this must be the header for a new file
			 */
			if (r != 512)
			{
				fprintf(stderr, _("%s: invalid tar block header size: %d\n"),
						progname, r);
				disconnect_and_exit(1);
			}
			totaldone += 512;

			if (sscanf(copybuf + 124, "%11o", &current_len_left) != 1)
			{
				fprintf(stderr, _("%s: could not parse file size\n"),
						progname);
				disconnect_and_exit(1);
			}

			/* Set permissions on the file */
			if (sscanf(&copybuf[100], "%07o ", &filemode) != 1)
			{
				fprintf(stderr, _("%s: could not parse file mode\n"),
						progname);
				disconnect_and_exit(1);
			}

			/*
			 * All files are padded up to 512 bytes
			 */
			current_padding =
				((current_len_left + 511) & ~511) - current_len_left;

			/*
			 * First part of header is zero terminated filename
			 */
			snprintf(filename, sizeof(filename), "%s/%s", current_path, copybuf);
			if (filename[strlen(filename) - 1] == '/')
			{
				/*
				 * Ends in a slash means directory or symlink to directory
				 */
				if (copybuf[156] == '5')
				{
					/*
					 * Directory
					 */
					filename[strlen(filename) - 1] = '\0';		/* Remove trailing slash */
					if (mkdir(filename, S_IRWXU) != 0)
					{
						/*
						 * When streaming WAL, pg_xlog will have been created
						 * by the wal receiver process, so just ignore failure
						 * on that.
						 */
						if (!streamwal || strcmp(filename + strlen(filename) - 8, "/pg_xlog") != 0)
						{
							fprintf(stderr,
							_("%s: could not create directory \"%s\": %s\n"),
									progname, filename, strerror(errno));
							disconnect_and_exit(1);
						}
					}
#ifndef WIN32
					if (chmod(filename, (mode_t) filemode))
						fprintf(stderr, _("%s: could not set permissions on directory \"%s\": %s\n"),
								progname, filename, strerror(errno));
#endif
				}
				else if (copybuf[156] == '2')
				{
					/*
					 * Symbolic link
					 */
					filename[strlen(filename) - 1] = '\0';		/* Remove trailing slash */
					if (symlink(&copybuf[157], filename) != 0)
					{
						fprintf(stderr,
								_("%s: could not create symbolic link from \"%s\" to \"%s\": %s\n"),
						 progname, filename, &copybuf[157], strerror(errno));
						disconnect_and_exit(1);
					}
				}
				else
				{
					fprintf(stderr, _("%s: unrecognized link indicator \"%c\"\n"),
							progname, copybuf[156]);
					disconnect_and_exit(1);
				}
				continue;		/* directory or link handled */
			}

			/*
			 * regular file
			 */
			file = fopen(filename, "wb");
			if (!file)
			{
				fprintf(stderr, _("%s: could not create file \"%s\": %s\n"),
						progname, filename, strerror(errno));
				disconnect_and_exit(1);
			}

#ifndef WIN32
			if (chmod(filename, (mode_t) filemode))
				fprintf(stderr, _("%s: could not set permissions on file \"%s\": %s\n"),
						progname, filename, strerror(errno));
#endif

			if (current_len_left == 0)
			{
				/*
				 * Done with this file, next one will be a new tar header
				 */
				fclose(file);
				file = NULL;
				continue;
			}
		}						/* new file */
		else
		{
			/*
			 * Continuing blocks in existing file
			 */
			if (current_len_left == 0 && r == current_padding)
			{
				/*
				 * Received the padding block for this file, ignore it and
				 * close the file, then move on to the next tar header.
				 */
				fclose(file);
				file = NULL;
				totaldone += r;
				continue;
			}

			if (fwrite(copybuf, r, 1, file) != 1)
			{
				fprintf(stderr, _("%s: could not write to file \"%s\": %s\n"),
						progname, filename, strerror(errno));
				disconnect_and_exit(1);
			}
			totaldone += r;
			if (showprogress)
				progress_report(rownum, filename);

			current_len_left -= r;
			if (current_len_left == 0 && current_padding == 0)
			{
				/*
				 * Received the last block, and there is no padding to be
				 * expected. Close the file and move on to the next tar
				 * header.
				 */
				fclose(file);
				file = NULL;
				continue;
			}
		}						/* continuing data in existing file */
	}							/* loop over all data blocks */

	if (file != NULL)
	{
		fprintf(stderr, _("%s: COPY stream ended before last file was finished\n"), progname);
		disconnect_and_exit(1);
	}

	if (copybuf != NULL)
		PQfreemem(copybuf);
}


static void
BaseBackup(void)
{
	PGresult   *res;
	char	   *sysidentifier;
	uint32		timeline;
	char		current_path[MAXPGPATH];
	char		escaped_label[MAXPGPATH];
	int			i;
	char		xlogstart[64];
	char		xlogend[64];

	/*
	 * Connect in replication mode to the server
	 */
	conn = GetConnection();
	if (!conn)
		/* Error message already written in GetConnection() */
		exit(1);

	/*
	 * Run IDENTIFY_SYSTEM so we can get the timeline
	 */
	res = PQexec(conn, "IDENTIFY_SYSTEM");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, _("%s: could not identify system: %s"),
				progname, PQerrorMessage(conn));
		disconnect_and_exit(1);
	}
	if (PQntuples(res) != 1 || PQnfields(res) != 3)
	{
		fprintf(stderr, _("%s: could not identify system, got %d rows and %d fields\n"),
				progname, PQntuples(res), PQnfields(res));
		disconnect_and_exit(1);
	}
	sysidentifier = strdup(PQgetvalue(res, 0, 0));
	timeline = atoi(PQgetvalue(res, 0, 1));
	PQclear(res);

	/*
	 * Start the actual backup
	 */
	PQescapeStringConn(conn, escaped_label, label, sizeof(escaped_label), &i);
	snprintf(current_path, sizeof(current_path), "BASE_BACKUP LABEL '%s' %s %s %s %s",
			 escaped_label,
			 showprogress ? "PROGRESS" : "",
			 includewal && !streamwal ? "WAL" : "",
			 fastcheckpoint ? "FAST" : "",
			 includewal ? "NOWAIT" : "");

	if (PQsendQuery(conn, current_path) == 0)
	{
		fprintf(stderr, _("%s: could not send base backup command: %s"),
				progname, PQerrorMessage(conn));
		disconnect_and_exit(1);
	}

	/*
	 * Get the starting xlog position
	 */
	res = PQgetResult(conn);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, _("%s: could not initiate base backup: %s"),
				progname, PQerrorMessage(conn));
		disconnect_and_exit(1);
	}
	if (PQntuples(res) != 1)
	{
		fprintf(stderr, _("%s: no start point returned from server\n"),
				progname);
		disconnect_and_exit(1);
	}
	strcpy(xlogstart, PQgetvalue(res, 0, 0));
	if (verbose && includewal)
		fprintf(stderr, "xlog start point: %s\n", xlogstart);
	PQclear(res);
	MemSet(xlogend, 0, sizeof(xlogend));

	/*
	 * Get the header
	 */
	res = PQgetResult(conn);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, _("%s: could not get backup header: %s"),
				progname, PQerrorMessage(conn));
		disconnect_and_exit(1);
	}
	if (PQntuples(res) < 1)
	{
		fprintf(stderr, _("%s: no data returned from server\n"), progname);
		disconnect_and_exit(1);
	}

	/*
	 * Sum up the total size, for progress reporting
	 */
	totalsize = totaldone = 0;
	tablespacecount = PQntuples(res);
	for (i = 0; i < PQntuples(res); i++)
	{
		if (showprogress)
			totalsize += atol(PQgetvalue(res, i, 2));

		/*
		 * Verify tablespace directories are empty. Don't bother with the
		 * first once since it can be relocated, and it will be checked before
		 * we do anything anyway.
		 */
		if (format == 'p' && !PQgetisnull(res, i, 1))
			verify_dir_is_empty_or_create(PQgetvalue(res, i, 1));
	}

	/*
	 * When writing to stdout, require a single tablespace
	 */
	if (format == 't' && strcmp(basedir, "-") == 0 && PQntuples(res) > 1)
	{
		fprintf(stderr, _("%s: can only write single tablespace to stdout, database has %d\n"),
				progname, PQntuples(res));
		disconnect_and_exit(1);
	}

	/*
	 * If we're streaming WAL, start the streaming session before we start
	 * receiving the actual data chunks.
	 */
	if (streamwal)
	{
		if (verbose)
			fprintf(stderr, _("%s: starting background WAL receiver\n"),
					progname);
		StartLogStreamer(xlogstart, timeline, sysidentifier);
	}

	/*
	 * Start receiving chunks
	 */
	for (i = 0; i < PQntuples(res); i++)
	{
		if (format == 't')
			ReceiveTarFile(conn, res, i);
		else
			ReceiveAndUnpackTarFile(conn, res, i);
	}							/* Loop over all tablespaces */

	if (showprogress)
	{
		progress_report(PQntuples(res), NULL);
		fprintf(stderr, "\n");	/* Need to move to next line */
	}
	PQclear(res);

	/*
	 * Get the stop position
	 */
	res = PQgetResult(conn);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, _("%s: could not get WAL end position from server: %s"),
				progname, PQerrorMessage(conn));
		disconnect_and_exit(1);
	}
	if (PQntuples(res) != 1)
	{
		fprintf(stderr, _("%s: no WAL end position returned from server\n"),
				progname);
		disconnect_and_exit(1);
	}
	strcpy(xlogend, PQgetvalue(res, 0, 0));
	if (verbose && includewal)
		fprintf(stderr, "xlog end point: %s\n", xlogend);
	PQclear(res);

	res = PQgetResult(conn);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, _("%s: final receive failed: %s"),
				progname, PQerrorMessage(conn));
		disconnect_and_exit(1);
	}

	if (bgchild > 0)
	{
#ifndef WIN32
		int			status;
		int			r;
#else
		DWORD		status;
#endif

		if (verbose)
			fprintf(stderr, _("%s: waiting for background process to finish streaming...\n"), progname);

#ifndef WIN32
		if (write(bgpipe[1], xlogend, strlen(xlogend)) != strlen(xlogend))
		{
			fprintf(stderr, _("%s: could not send command to background pipe: %s\n"),
					progname, strerror(errno));
			disconnect_and_exit(1);
		}

		/* Just wait for the background process to exit */
		r = waitpid(bgchild, &status, 0);
		if (r == -1)
		{
			fprintf(stderr, _("%s: could not wait for child process: %s\n"),
					progname, strerror(errno));
			disconnect_and_exit(1);
		}
		if (r != bgchild)
		{
			fprintf(stderr, _("%s: child %d died, expected %d\n"),
					progname, r, (int) bgchild);
			disconnect_and_exit(1);
		}
		if (!WIFEXITED(status))
		{
			fprintf(stderr, _("%s: child process did not exit normally\n"),
					progname);
			disconnect_and_exit(1);
		}
		if (WEXITSTATUS(status) != 0)
		{
			fprintf(stderr, _("%s: child process exited with error %d\n"),
					progname, WEXITSTATUS(status));
			disconnect_and_exit(1);
		}
		/* Exited normally, we're happy! */
#else							/* WIN32 */

		/*
		 * On Windows, since we are in the same process, we can just store the
		 * value directly in the variable, and then set the flag that says
		 * it's there.
		 */
		if (sscanf(xlogend, "%X/%X", &xlogendptr.xlogid, &xlogendptr.xrecoff) != 2)
		{
			fprintf(stderr, _("%s: could not parse xlog end position \"%s\"\n"),
					progname, xlogend);
			disconnect_and_exit(1);
		}
		InterlockedIncrement(&has_xlogendptr);

		/* First wait for the thread to exit */
		if (WaitForSingleObjectEx((HANDLE) bgchild, INFINITE, FALSE) != WAIT_OBJECT_0)
		{
			_dosmaperr(GetLastError());
			fprintf(stderr, _("%s: could not wait for child thread: %s\n"),
					progname, strerror(errno));
			disconnect_and_exit(1);
		}
		if (GetExitCodeThread((HANDLE) bgchild, &status) == 0)
		{
			_dosmaperr(GetLastError());
			fprintf(stderr, _("%s: could not get child thread exit status: %s\n"),
					progname, strerror(errno));
			disconnect_and_exit(1);
		}
		if (status != 0)
		{
			fprintf(stderr, _("%s: child thread exited with error %u\n"),
					progname, (unsigned int) status);
			disconnect_and_exit(1);
		}
		/* Exited normally, we're happy */
#endif
	}

	/*
	 * End of copy data. Final result is already checked inside the loop.
	 */
	PQclear(res);
	PQfinish(conn);

	if (verbose)
		fprintf(stderr, "%s: base backup completed\n", progname);
}


int
main(int argc, char **argv)
{
	static struct option long_options[] = {
		{"help", no_argument, NULL, '?'},
		{"version", no_argument, NULL, 'V'},
		{"pgdata", required_argument, NULL, 'D'},
		{"format", required_argument, NULL, 'F'},
		{"checkpoint", required_argument, NULL, 'c'},
		{"xlog", no_argument, NULL, 'x'},
		{"xlog-method", required_argument, NULL, 'X'},
		{"gzip", no_argument, NULL, 'z'},
		{"compress", required_argument, NULL, 'Z'},
		{"label", required_argument, NULL, 'l'},
		{"host", required_argument, NULL, 'h'},
		{"port", required_argument, NULL, 'p'},
		{"username", required_argument, NULL, 'U'},
		{"no-password", no_argument, NULL, 'w'},
		{"password", no_argument, NULL, 'W'},
		{"statusint", required_argument, NULL, 's'},
		{"verbose", no_argument, NULL, 'v'},
		{"progress", no_argument, NULL, 'P'},
		{NULL, 0, NULL, 0}
	};
	int			c;

	int			option_index;

	progname = get_progname(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_basebackup"));

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage();
			exit(0);
		}
		else if (strcmp(argv[1], "-V") == 0
				 || strcmp(argv[1], "--version") == 0)
		{
			puts("pg_basebackup (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	while ((c = getopt_long(argc, argv, "D:F:xX:l:zZ:c:h:p:U:s:wWvP",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'D':
				basedir = xstrdup(optarg);
				break;
			case 'F':
				if (strcmp(optarg, "p") == 0 || strcmp(optarg, "plain") == 0)
					format = 'p';
				else if (strcmp(optarg, "t") == 0 || strcmp(optarg, "tar") == 0)
					format = 't';
				else
				{
					fprintf(stderr, _("%s: invalid output format \"%s\", must be \"plain\" or \"tar\"\n"),
							progname, optarg);
					exit(1);
				}
				break;
			case 'x':
				if (includewal)
				{
					fprintf(stderr, _("%s: cannot specify both --xlog and --xlog-method\n"),
							progname);
					exit(1);
				}

				includewal = true;
				streamwal = false;
				break;
			case 'X':
				if (includewal)
				{
					fprintf(stderr, _("%s: cannot specify both --xlog and --xlog-method\n"),
							progname);
					exit(1);
				}

				includewal = true;
				if (strcmp(optarg, "f") == 0 ||
					strcmp(optarg, "fetch") == 0)
					streamwal = false;
				else if (strcmp(optarg, "s") == 0 ||
						 strcmp(optarg, "stream") == 0)
					streamwal = true;
				else
				{
					fprintf(stderr, _("%s: invalid xlog-method option \"%s\", must be empty, \"fetch\", or \"stream\"\n"),
							progname, optarg);
					exit(1);
				}
				break;
			case 'l':
				label = xstrdup(optarg);
				break;
			case 'z':
#ifdef HAVE_LIBZ
				compresslevel = Z_DEFAULT_COMPRESSION;
#else
				compresslevel = 1;		/* will be rejected below */
#endif
				break;
			case 'Z':
				compresslevel = atoi(optarg);
				if (compresslevel <= 0 || compresslevel > 9)
				{
					fprintf(stderr, _("%s: invalid compression level \"%s\"\n"),
							progname, optarg);
					exit(1);
				}
				break;
			case 'c':
				if (pg_strcasecmp(optarg, "fast") == 0)
					fastcheckpoint = true;
				else if (pg_strcasecmp(optarg, "spread") == 0)
					fastcheckpoint = false;
				else
				{
					fprintf(stderr, _("%s: invalid checkpoint argument \"%s\", must be \"fast\" or \"spread\"\n"),
							progname, optarg);
					exit(1);
				}
				break;
			case 'h':
				dbhost = xstrdup(optarg);
				break;
			case 'p':
				dbport = xstrdup(optarg);
				break;
			case 'U':
				dbuser = xstrdup(optarg);
				break;
			case 'w':
				dbgetpassword = -1;
				break;
			case 'W':
				dbgetpassword = 1;
				break;
			case 's':
				standby_message_timeout = atoi(optarg) * 1000;
				if (standby_message_timeout < 0)
				{
					fprintf(stderr, _("%s: invalid status interval \"%s\"\n"),
							progname, optarg);
					exit(1);
				}
				break;
			case 'v':
				verbose++;
				break;
			case 'P':
				showprogress = true;
				break;
			default:

				/*
				 * getopt_long already emitted a complaint
				 */
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
						progname);
				exit(1);
		}
	}

	/*
	 * Any non-option arguments?
	 */
	if (optind < argc)
	{
		fprintf(stderr,
				_("%s: too many command-line arguments (first is \"%s\")\n"),
				progname, argv[optind]);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	/*
	 * Required arguments
	 */
	if (basedir == NULL)
	{
		fprintf(stderr, _("%s: no target directory specified\n"), progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	/*
	 * Mutually exclusive arguments
	 */
	if (format == 'p' && compresslevel != 0)
	{
		fprintf(stderr,
				_("%s: only tar mode backups can be compressed\n"),
				progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	if (format != 'p' && streamwal)
	{
		fprintf(stderr,
				_("%s: wal streaming can only be used in plain mode\n"),
				progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

#ifndef HAVE_LIBZ
	if (compresslevel != 0)
	{
		fprintf(stderr,
				_("%s: this build does not support compression\n"),
				progname);
		exit(1);
	}
#endif

	/*
	 * Verify that the target directory exists, or create it. For plaintext
	 * backups, always require the directory. For tar backups, require it
	 * unless we are writing to stdout.
	 */
	if (format == 'p' || strcmp(basedir, "-") != 0)
		verify_dir_is_empty_or_create(basedir);


	BaseBackup();

	return 0;
}
