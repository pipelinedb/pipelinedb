/*-------------------------------------------------------------------------
 *
 * receivelog.c - receive transaction log files using the streaming
 *				  replication protocol.
 *
 * Author: Magnus Hagander <magnus@hagander.net>
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_basebackup/receivelog.c
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <sys/stat.h>
#include <unistd.h>

/* local includes */
#include "receivelog.h"
#include "streamutil.h"

#include "libpq-fe.h"
#include "access/xlog_internal.h"


/* fd and filename for currently open WAL file */
static int	walfile = -1;
static char current_walfile_name[MAXPGPATH] = "";
static bool reportFlushPosition = false;
static XLogRecPtr lastFlushPosition = InvalidXLogRecPtr;

static PGresult *HandleCopyStream(PGconn *conn, XLogRecPtr startpos,
				 uint32 timeline, char *basedir,
			   stream_stop_callback stream_stop, int standby_message_timeout,
				 char *partial_suffix, XLogRecPtr *stoppos);

static bool ReadEndOfStreamingResult(PGresult *res, XLogRecPtr *startpos,
						 uint32 *timeline);

/*
 * Open a new WAL file in the specified directory.
 *
 * The file will be padded to 16Mb with zeroes. The base filename (without
 * partial_suffix) is stored in current_walfile_name.
 */
static bool
open_walfile(XLogRecPtr startpoint, uint32 timeline, char *basedir,
			 char *partial_suffix)
{
	int			f;
	char		fn[MAXPGPATH];
	struct stat statbuf;
	char	   *zerobuf;
	int			bytes;
	XLogSegNo	segno;

	XLByteToSeg(startpoint, segno);
	XLogFileName(current_walfile_name, timeline, segno);

	snprintf(fn, sizeof(fn), "%s/%s%s", basedir, current_walfile_name,
			 partial_suffix ? partial_suffix : "");
	f = open(fn, O_WRONLY | O_CREAT | PG_BINARY, S_IRUSR | S_IWUSR);
	if (f == -1)
	{
		fprintf(stderr,
				_("%s: could not open transaction log file \"%s\": %s\n"),
				progname, fn, strerror(errno));
		return false;
	}

	/*
	 * Verify that the file is either empty (just created), or a complete
	 * XLogSegSize segment. Anything in between indicates a corrupt file.
	 */
	if (fstat(f, &statbuf) != 0)
	{
		fprintf(stderr,
				_("%s: could not stat transaction log file \"%s\": %s\n"),
				progname, fn, strerror(errno));
		close(f);
		return false;
	}
	if (statbuf.st_size == XLogSegSize)
	{
		/* File is open and ready to use */
		walfile = f;
		return true;
	}
	if (statbuf.st_size != 0)
	{
		fprintf(stderr,
				_("%s: transaction log file \"%s\" has %d bytes, should be 0 or %d\n"),
				progname, fn, (int) statbuf.st_size, XLogSegSize);
		close(f);
		return false;
	}

	/* New, empty, file. So pad it to 16Mb with zeroes */
	zerobuf = pg_malloc0(XLOG_BLCKSZ);
	for (bytes = 0; bytes < XLogSegSize; bytes += XLOG_BLCKSZ)
	{
		if (write(f, zerobuf, XLOG_BLCKSZ) != XLOG_BLCKSZ)
		{
			fprintf(stderr,
					_("%s: could not pad transaction log file \"%s\": %s\n"),
					progname, fn, strerror(errno));
			free(zerobuf);
			close(f);
			unlink(fn);
			return false;
		}
	}
	free(zerobuf);

	if (lseek(f, SEEK_SET, 0) != 0)
	{
		fprintf(stderr,
				_("%s: could not seek to beginning of transaction log file \"%s\": %s\n"),
				progname, fn, strerror(errno));
		close(f);
		return false;
	}
	walfile = f;
	return true;
}

/*
 * Close the current WAL file (if open), and rename it to the correct
 * filename if it's complete. On failure, prints an error message to stderr
 * and returns false, otherwise returns true.
 */
static bool
close_walfile(char *basedir, char *partial_suffix, XLogRecPtr pos)
{
	off_t		currpos;

	if (walfile == -1)
		return true;

	currpos = lseek(walfile, 0, SEEK_CUR);
	if (currpos == -1)
	{
		fprintf(stderr,
			 _("%s: could not determine seek position in file \"%s\": %s\n"),
				progname, current_walfile_name, strerror(errno));
		return false;
	}

	if (fsync(walfile) != 0)
	{
		fprintf(stderr, _("%s: could not fsync file \"%s\": %s\n"),
				progname, current_walfile_name, strerror(errno));
		return false;
	}

	if (close(walfile) != 0)
	{
		fprintf(stderr, _("%s: could not close file \"%s\": %s\n"),
				progname, current_walfile_name, strerror(errno));
		walfile = -1;
		return false;
	}
	walfile = -1;

	/*
	 * If we finished writing a .partial file, rename it into place.
	 */
	if (currpos == XLOG_SEG_SIZE && partial_suffix)
	{
		char		oldfn[MAXPGPATH];
		char		newfn[MAXPGPATH];

		snprintf(oldfn, sizeof(oldfn), "%s/%s%s", basedir, current_walfile_name, partial_suffix);
		snprintf(newfn, sizeof(newfn), "%s/%s", basedir, current_walfile_name);
		if (rename(oldfn, newfn) != 0)
		{
			fprintf(stderr, _("%s: could not rename file \"%s\": %s\n"),
					progname, current_walfile_name, strerror(errno));
			return false;
		}
	}
	else if (partial_suffix)
		fprintf(stderr,
				_("%s: not renaming \"%s%s\", segment is not complete\n"),
				progname, current_walfile_name, partial_suffix);

	lastFlushPosition = pos;
	return true;
}


/*
 * Check if a timeline history file exists.
 */
static bool
existsTimeLineHistoryFile(char *basedir, TimeLineID tli)
{
	char		path[MAXPGPATH];
	char		histfname[MAXFNAMELEN];
	int			fd;

	/*
	 * Timeline 1 never has a history file. We treat that as if it existed,
	 * since we never need to stream it.
	 */
	if (tli == 1)
		return true;

	TLHistoryFileName(histfname, tli);

	snprintf(path, sizeof(path), "%s/%s", basedir, histfname);

	fd = open(path, O_RDONLY | PG_BINARY, 0);
	if (fd < 0)
	{
		if (errno != ENOENT)
			fprintf(stderr, _("%s: could not open timeline history file \"%s\": %s\n"),
					progname, path, strerror(errno));
		return false;
	}
	else
	{
		close(fd);
		return true;
	}
}

static bool
writeTimeLineHistoryFile(char *basedir, TimeLineID tli, char *filename, char *content)
{
	int			size = strlen(content);
	char		path[MAXPGPATH];
	char		tmppath[MAXPGPATH];
	char		histfname[MAXFNAMELEN];
	int			fd;

	/*
	 * Check that the server's idea of how timeline history files should be
	 * named matches ours.
	 */
	TLHistoryFileName(histfname, tli);
	if (strcmp(histfname, filename) != 0)
	{
		fprintf(stderr, _("%s: server reported unexpected history file name for timeline %u: %s\n"),
				progname, tli, filename);
		return false;
	}

	snprintf(path, sizeof(path), "%s/%s", basedir, histfname);

	/*
	 * Write into a temp file name.
	 */
	snprintf(tmppath, MAXPGPATH, "%s.tmp", path);

	unlink(tmppath);

	fd = open(tmppath, O_WRONLY | O_CREAT | PG_BINARY, S_IRUSR | S_IWUSR);
	if (fd < 0)
	{
		fprintf(stderr, _("%s: could not create timeline history file \"%s\": %s\n"),
				progname, tmppath, strerror(errno));
		return false;
	}

	errno = 0;
	if ((int) write(fd, content, size) != size)
	{
		int			save_errno = errno;

		/*
		 * If we fail to make the file, delete it to release disk space
		 */
		close(fd);
		unlink(tmppath);
		errno = save_errno;

		fprintf(stderr, _("%s: could not write timeline history file \"%s\": %s\n"),
				progname, tmppath, strerror(errno));
		return false;
	}

	if (fsync(fd) != 0)
	{
		close(fd);
		fprintf(stderr, _("%s: could not fsync file \"%s\": %s\n"),
				progname, tmppath, strerror(errno));
		return false;
	}

	if (close(fd) != 0)
	{
		fprintf(stderr, _("%s: could not close file \"%s\": %s\n"),
				progname, tmppath, strerror(errno));
		return false;
	}

	/*
	 * Now move the completed history file into place with its final name.
	 */
	if (rename(tmppath, path) < 0)
	{
		fprintf(stderr, _("%s: could not rename file \"%s\" to \"%s\": %s\n"),
				progname, tmppath, path, strerror(errno));
		return false;
	}

	return true;
}

/*
 * Send a Standby Status Update message to server.
 */
static bool
sendFeedback(PGconn *conn, XLogRecPtr blockpos, int64 now, bool replyRequested)
{
	char		replybuf[1 + 8 + 8 + 8 + 8 + 1];
	int			len = 0;

	replybuf[len] = 'r';
	len += 1;
	fe_sendint64(blockpos, &replybuf[len]);		/* write */
	len += 8;
	if (reportFlushPosition)
		fe_sendint64(lastFlushPosition, &replybuf[len]);		/* flush */
	else
		fe_sendint64(InvalidXLogRecPtr, &replybuf[len]);		/* flush */
	len += 8;
	fe_sendint64(InvalidXLogRecPtr, &replybuf[len]);	/* apply */
	len += 8;
	fe_sendint64(now, &replybuf[len]);	/* sendTime */
	len += 8;
	replybuf[len] = replyRequested ? 1 : 0;		/* replyRequested */
	len += 1;

	if (PQputCopyData(conn, replybuf, len) <= 0 || PQflush(conn))
	{
		fprintf(stderr, _("%s: could not send feedback packet: %s"),
				progname, PQerrorMessage(conn));
		return false;
	}

	return true;
}

/*
 * Check that the server version we're connected to is supported by
 * ReceiveXlogStream().
 *
 * If it's not, an error message is printed to stderr, and false is returned.
 */
bool
CheckServerVersionForStreaming(PGconn *conn)
{
	int			minServerMajor,
				maxServerMajor;
	int			serverMajor;

	/*
	 * The message format used in streaming replication changed in 9.3, so we
	 * cannot stream from older servers. And we don't support servers newer
	 * than the client; it might work, but we don't know, so err on the safe
	 * side.
	 */
	minServerMajor = 903;
	maxServerMajor = PG_VERSION_NUM / 100;
	serverMajor = PQserverVersion(conn) / 100;
	if (serverMajor < minServerMajor)
	{
		const char *serverver = PQparameterStatus(conn, "server_version");

		fprintf(stderr, _("%s: incompatible server version %s; client does not support streaming from server versions older than %s\n"),
				progname,
				serverver ? serverver : "'unknown'",
				"9.3");
		return false;
	}
	else if (serverMajor > maxServerMajor)
	{
		const char *serverver = PQparameterStatus(conn, "server_version");

		fprintf(stderr, _("%s: incompatible server version %s; client does not support streaming from server versions newer than %s\n"),
				progname,
				serverver ? serverver : "'unknown'",
				PG_VERSION);
		return false;
	}
	return true;
}

/*
 * Receive a log stream starting at the specified position.
 *
 * If sysidentifier is specified, validate that both the system
 * identifier and the timeline matches the specified ones
 * (by sending an extra IDENTIFY_SYSTEM command)
 *
 * All received segments will be written to the directory
 * specified by basedir. This will also fetch any missing timeline history
 * files.
 *
 * The stream_stop callback will be called every time data
 * is received, and whenever a segment is completed. If it returns
 * true, the streaming will stop and the function
 * return. As long as it returns false, streaming will continue
 * indefinitely.
 *
 * standby_message_timeout controls how often we send a message
 * back to the master letting it know our progress, in seconds.
 * This message will only contain the write location, and never
 * flush or replay.
 *
 * If 'partial_suffix' is not NULL, files are initially created with the
 * given suffix, and the suffix is removed once the file is finished. That
 * allows you to tell the difference between partial and completed files,
 * so that you can continue later where you left.
 *
 * Note: The log position *must* be at a log segment start!
 */
bool
ReceiveXlogStream(PGconn *conn, XLogRecPtr startpos, uint32 timeline,
				  char *sysidentifier, char *basedir,
				  stream_stop_callback stream_stop,
				  int standby_message_timeout, char *partial_suffix)
{
	char		query[128];
	char		slotcmd[128];
	PGresult   *res;
	XLogRecPtr	stoppos;

	/*
	 * The caller should've checked the server version already, but doesn't do
	 * any harm to check it here too.
	 */
	if (!CheckServerVersionForStreaming(conn))
		return false;

	if (replication_slot != NULL)
	{
		/*
		 * Report the flush position, so the primary can know what WAL we'll
		 * possibly re-request, and remove older WAL safely.
		 *
		 * We only report it when a slot has explicitly been used, because
		 * reporting the flush position makes one eligible as a synchronous
		 * replica. People shouldn't include generic names in
		 * synchronous_standby_names, but we've protected them against it so
		 * far, so let's continue to do so in the situations when possible. If
		 * they've got a slot, though, we need to report the flush position,
		 * so that the master can remove WAL.
		 */
		reportFlushPosition = true;
		sprintf(slotcmd, "SLOT \"%s\" ", replication_slot);
	}
	else
	{
		reportFlushPosition = false;
		slotcmd[0] = 0;
	}

	if (sysidentifier != NULL)
	{
		/* Validate system identifier hasn't changed */
		res = PQexec(conn, "IDENTIFY_SYSTEM");
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(stderr,
					_("%s: could not send replication command \"%s\": %s"),
					progname, "IDENTIFY_SYSTEM", PQerrorMessage(conn));
			PQclear(res);
			return false;
		}
		if (PQntuples(res) != 1 || PQnfields(res) < 3)
		{
			fprintf(stderr,
					_("%s: could not identify system: got %d rows and %d fields, expected %d rows and %d or more fields\n"),
					progname, PQntuples(res), PQnfields(res), 1, 3);
			PQclear(res);
			return false;
		}
		if (strcmp(sysidentifier, PQgetvalue(res, 0, 0)) != 0)
		{
			fprintf(stderr,
					_("%s: system identifier does not match between base backup and streaming connection\n"),
					progname);
			PQclear(res);
			return false;
		}
		if (timeline > atoi(PQgetvalue(res, 0, 1)))
		{
			fprintf(stderr,
				_("%s: starting timeline %u is not present in the server\n"),
					progname, timeline);
			PQclear(res);
			return false;
		}
		PQclear(res);
	}

	/*
	 * initialize flush position to starting point, it's the caller's
	 * responsibility that that's sane.
	 */
	lastFlushPosition = startpos;

	while (1)
	{
		/*
		 * Fetch the timeline history file for this timeline, if we don't have
		 * it already.
		 */
		if (!existsTimeLineHistoryFile(basedir, timeline))
		{
			snprintf(query, sizeof(query), "TIMELINE_HISTORY %u", timeline);
			res = PQexec(conn, query);
			if (PQresultStatus(res) != PGRES_TUPLES_OK)
			{
				/* FIXME: we might send it ok, but get an error */
				fprintf(stderr, _("%s: could not send replication command \"%s\": %s"),
					progname, "TIMELINE_HISTORY", PQresultErrorMessage(res));
				PQclear(res);
				return false;
			}

			/*
			 * The response to TIMELINE_HISTORY is a single row result set
			 * with two fields: filename and content
			 */
			if (PQnfields(res) != 2 || PQntuples(res) != 1)
			{
				fprintf(stderr,
						_("%s: unexpected response to TIMELINE_HISTORY command: got %d rows and %d fields, expected %d rows and %d fields\n"),
						progname, PQntuples(res), PQnfields(res), 1, 2);
			}

			/* Write the history file to disk */
			writeTimeLineHistoryFile(basedir, timeline,
									 PQgetvalue(res, 0, 0),
									 PQgetvalue(res, 0, 1));

			PQclear(res);
		}

		/*
		 * Before we start streaming from the requested location, check if the
		 * callback tells us to stop here.
		 */
		if (stream_stop(startpos, timeline, false))
			return true;

		/* Initiate the replication stream at specified location */
		snprintf(query, sizeof(query), "START_REPLICATION %s%X/%X TIMELINE %u",
				 slotcmd,
				 (uint32) (startpos >> 32), (uint32) startpos,
				 timeline);
		res = PQexec(conn, query);
		if (PQresultStatus(res) != PGRES_COPY_BOTH)
		{
			fprintf(stderr, _("%s: could not send replication command \"%s\": %s"),
					progname, "START_REPLICATION", PQresultErrorMessage(res));
			PQclear(res);
			return false;
		}
		PQclear(res);

		/* Stream the WAL */
		res = HandleCopyStream(conn, startpos, timeline, basedir, stream_stop,
							   standby_message_timeout, partial_suffix,
							   &stoppos);
		if (res == NULL)
			goto error;

		/*
		 * Streaming finished.
		 *
		 * There are two possible reasons for that: a controlled shutdown, or
		 * we reached the end of the current timeline. In case of
		 * end-of-timeline, the server sends a result set after Copy has
		 * finished, containing information about the next timeline. Read
		 * that, and restart streaming from the next timeline. In case of
		 * controlled shutdown, stop here.
		 */
		if (PQresultStatus(res) == PGRES_TUPLES_OK)
		{
			/*
			 * End-of-timeline. Read the next timeline's ID and starting
			 * position. Usually, the starting position will match the end of
			 * the previous timeline, but there are corner cases like if the
			 * server had sent us half of a WAL record, when it was promoted.
			 * The new timeline will begin at the end of the last complete
			 * record in that case, overlapping the partial WAL record on the
			 * the old timeline.
			 */
			uint32		newtimeline;
			bool		parsed;

			parsed = ReadEndOfStreamingResult(res, &startpos, &newtimeline);
			PQclear(res);
			if (!parsed)
				goto error;

			/* Sanity check the values the server gave us */
			if (newtimeline <= timeline)
			{
				fprintf(stderr,
						_("%s: server reported unexpected next timeline %u, following timeline %u\n"),
						progname, newtimeline, timeline);
				goto error;
			}
			if (startpos > stoppos)
			{
				fprintf(stderr,
						_("%s: server stopped streaming timeline %u at %X/%X, but reported next timeline %u to begin at %X/%X\n"),
						progname,
						timeline, (uint32) (stoppos >> 32), (uint32) stoppos,
				  newtimeline, (uint32) (startpos >> 32), (uint32) startpos);
				goto error;
			}

			/* Read the final result, which should be CommandComplete. */
			res = PQgetResult(conn);
			if (PQresultStatus(res) != PGRES_COMMAND_OK)
			{
				fprintf(stderr,
				   _("%s: unexpected termination of replication stream: %s"),
						progname, PQresultErrorMessage(res));
				PQclear(res);
				goto error;
			}
			PQclear(res);

			/*
			 * Loop back to start streaming from the new timeline. Always
			 * start streaming at the beginning of a segment.
			 */
			timeline = newtimeline;
			startpos = startpos - (startpos % XLOG_SEG_SIZE);
			continue;
		}
		else if (PQresultStatus(res) == PGRES_COMMAND_OK)
		{
			PQclear(res);

			/*
			 * End of replication (ie. controlled shut down of the server).
			 *
			 * Check if the callback thinks it's OK to stop here. If not,
			 * complain.
			 */
			if (stream_stop(stoppos, timeline, false))
				return true;
			else
			{
				fprintf(stderr, _("%s: replication stream was terminated before stop point\n"),
						progname);
				goto error;
			}
		}
		else
		{
			/* Server returned an error. */
			fprintf(stderr,
					_("%s: unexpected termination of replication stream: %s"),
					progname, PQresultErrorMessage(res));
			PQclear(res);
			goto error;
		}
	}

error:
	if (walfile != -1 && close(walfile) != 0)
		fprintf(stderr, _("%s: could not close file \"%s\": %s\n"),
				progname, current_walfile_name, strerror(errno));
	walfile = -1;
	return false;
}

/*
 * Helper function to parse the result set returned by server after streaming
 * has finished. On failure, prints an error to stderr and returns false.
 */
static bool
ReadEndOfStreamingResult(PGresult *res, XLogRecPtr *startpos, uint32 *timeline)
{
	uint32		startpos_xlogid,
				startpos_xrecoff;

	/*----------
	 * The result set consists of one row and two columns, e.g:
	 *
	 *	next_tli | next_tli_startpos
	 * ----------+-------------------
	 *		   4 | 0/9949AE0
	 *
	 * next_tli is the timeline ID of the next timeline after the one that
	 * just finished streaming. next_tli_startpos is the XLOG position where
	 * the server switched to it.
	 *----------
	 */
	if (PQnfields(res) < 2 || PQntuples(res) != 1)
	{
		fprintf(stderr,
				_("%s: unexpected result set after end-of-timeline: got %d rows and %d fields, expected %d rows and %d fields\n"),
				progname, PQntuples(res), PQnfields(res), 1, 2);
		return false;
	}

	*timeline = atoi(PQgetvalue(res, 0, 0));
	if (sscanf(PQgetvalue(res, 0, 1), "%X/%X", &startpos_xlogid,
			   &startpos_xrecoff) != 2)
	{
		fprintf(stderr,
			_("%s: could not parse next timeline's starting point \"%s\"\n"),
				progname, PQgetvalue(res, 0, 1));
		return false;
	}
	*startpos = ((uint64) startpos_xlogid << 32) | startpos_xrecoff;

	return true;
}

/*
 * The main loop of ReceiveXLogStream. Handles the COPY stream after
 * initiating streaming with the START_STREAMING command.
 *
 * If the COPY ends (not necessarily successfully) due a message from the
 * server, returns a PGresult and sets sets *stoppos to the last byte written.
 * On any other sort of error, returns NULL.
 */
static PGresult *
HandleCopyStream(PGconn *conn, XLogRecPtr startpos, uint32 timeline,
				 char *basedir, stream_stop_callback stream_stop,
				 int standby_message_timeout, char *partial_suffix,
				 XLogRecPtr *stoppos)
{
	char	   *copybuf = NULL;
	int64		last_status = -1;
	XLogRecPtr	blockpos = startpos;
	bool		still_sending = true;

	while (1)
	{
		int			r;
		int			xlogoff;
		int			bytes_left;
		int			bytes_written;
		int64		now;
		int			hdr_len;

		if (copybuf != NULL)
		{
			PQfreemem(copybuf);
			copybuf = NULL;
		}

		/*
		 * Check if we should continue streaming, or abort at this point.
		 */
		if (still_sending && stream_stop(blockpos, timeline, false))
		{
			if (!close_walfile(basedir, partial_suffix, blockpos))
			{
				/* Potential error message is written by close_walfile */
				goto error;
			}
			if (PQputCopyEnd(conn, NULL) <= 0 || PQflush(conn))
			{
				fprintf(stderr, _("%s: could not send copy-end packet: %s"),
						progname, PQerrorMessage(conn));
				goto error;
			}
			still_sending = false;
		}

		/*
		 * Potentially send a status message to the master
		 */
		now = feGetCurrentTimestamp();
		if (still_sending && standby_message_timeout > 0 &&
			feTimestampDifferenceExceeds(last_status, now,
										 standby_message_timeout))
		{
			/* Time to send feedback! */
			if (!sendFeedback(conn, blockpos, now, false))
				goto error;
			last_status = now;
		}

		r = PQgetCopyData(conn, &copybuf, 1);
		if (r == 0)
		{
			/*
			 * No data available. Wait for some to appear, but not longer than
			 * the specified timeout, so that we can ping the server.
			 */
			fd_set		input_mask;
			struct timeval timeout;
			struct timeval *timeoutptr;

			FD_ZERO(&input_mask);
			FD_SET(PQsocket(conn), &input_mask);
			if (standby_message_timeout && still_sending)
			{
				int64		targettime;
				long		secs;
				int			usecs;

				targettime = last_status + (standby_message_timeout - 1) * ((int64) 1000);
				feTimestampDifference(now,
									  targettime,
									  &secs,
									  &usecs);
				if (secs <= 0)
					timeout.tv_sec = 1; /* Always sleep at least 1 sec */
				else
					timeout.tv_sec = secs;
				timeout.tv_usec = usecs;
				timeoutptr = &timeout;
			}
			else
				timeoutptr = NULL;

			r = select(PQsocket(conn) + 1, &input_mask, NULL, NULL, timeoutptr);
			if (r == 0 || (r < 0 && errno == EINTR))
			{
				/*
				 * Got a timeout or signal. Continue the loop and either
				 * deliver a status packet to the server or just go back into
				 * blocking.
				 */
				continue;
			}
			else if (r < 0)
			{
				fprintf(stderr, _("%s: select() failed: %s\n"),
						progname, strerror(errno));
				goto error;
			}
			/* Else there is actually data on the socket */
			if (PQconsumeInput(conn) == 0)
			{
				fprintf(stderr,
						_("%s: could not receive data from WAL stream: %s"),
						progname, PQerrorMessage(conn));
				goto error;
			}
			continue;
		}
		if (r == -1)
		{
			PGresult   *res = PQgetResult(conn);

			/*
			 * The server closed its end of the copy stream.  If we haven't
			 * closed ours already, we need to do so now, unless the server
			 * threw an error, in which case we don't.
			 */
			if (still_sending)
			{
				if (!close_walfile(basedir, partial_suffix, blockpos))
				{
					/* Error message written in close_walfile() */
					PQclear(res);
					goto error;
				}
				if (PQresultStatus(res) == PGRES_COPY_IN)
				{
					if (PQputCopyEnd(conn, NULL) <= 0 || PQflush(conn))
					{
						fprintf(stderr,
								_("%s: could not send copy-end packet: %s"),
								progname, PQerrorMessage(conn));
						PQclear(res);
						goto error;
					}
					PQclear(res);
					res = PQgetResult(conn);
				}
				still_sending = false;
			}
			if (copybuf != NULL)
				PQfreemem(copybuf);
			*stoppos = blockpos;
			return res;
		}
		if (r == -2)
		{
			fprintf(stderr, _("%s: could not read COPY data: %s"),
					progname, PQerrorMessage(conn));
			goto error;
		}

		/* Check the message type. */
		if (copybuf[0] == 'k')
		{
			int			pos;
			bool		replyRequested;

			/*
			 * Parse the keepalive message, enclosed in the CopyData message.
			 * We just check if the server requested a reply, and ignore the
			 * rest.
			 */
			pos = 1;			/* skip msgtype 'k' */
			pos += 8;			/* skip walEnd */
			pos += 8;			/* skip sendTime */

			if (r < pos + 1)
			{
				fprintf(stderr, _("%s: streaming header too small: %d\n"),
						progname, r);
				goto error;
			}
			replyRequested = copybuf[pos];

			/* If the server requested an immediate reply, send one. */
			if (replyRequested && still_sending)
			{
				if (reportFlushPosition && lastFlushPosition < blockpos &&
					walfile != -1)
				{
					/*
					 * If a valid flush location needs to be reported,
					 * flush the current WAL file so that the latest flush
					 * location is sent back to the server. This is necessary to
					 * see whether the last WAL data has been successfully
					 * replicated or not, at the normal shutdown of the server.
					 */
					if (fsync(walfile) != 0)
					{
						fprintf(stderr, _("%s: could not fsync file \"%s\": %s\n"),
								progname, current_walfile_name, strerror(errno));
						goto error;
					}
					lastFlushPosition = blockpos;
				}

				now = feGetCurrentTimestamp();
				if (!sendFeedback(conn, blockpos, now, false))
					goto error;
				last_status = now;
			}
		}
		else if (copybuf[0] == 'w')
		{
			/*
			 * Once we've decided we don't want to receive any more, just
			 * ignore any subsequent XLogData messages.
			 */
			if (!still_sending)
				continue;

			/*
			 * Read the header of the XLogData message, enclosed in the
			 * CopyData message. We only need the WAL location field
			 * (dataStart), the rest of the header is ignored.
			 */
			hdr_len = 1;		/* msgtype 'w' */
			hdr_len += 8;		/* dataStart */
			hdr_len += 8;		/* walEnd */
			hdr_len += 8;		/* sendTime */
			if (r < hdr_len)
			{
				fprintf(stderr, _("%s: streaming header too small: %d\n"),
						progname, r);
				goto error;
			}
			blockpos = fe_recvint64(&copybuf[1]);

			/* Extract WAL location for this block */
			xlogoff = blockpos % XLOG_SEG_SIZE;

			/*
			 * Verify that the initial location in the stream matches where we
			 * think we are.
			 */
			if (walfile == -1)
			{
				/* No file open yet */
				if (xlogoff != 0)
				{
					fprintf(stderr,
							_("%s: received transaction log record for offset %u with no file open\n"),
							progname, xlogoff);
					goto error;
				}
			}
			else
			{
				/* More data in existing segment */
				/* XXX: store seek value don't reseek all the time */
				if (lseek(walfile, 0, SEEK_CUR) != xlogoff)
				{
					fprintf(stderr,
						  _("%s: got WAL data offset %08x, expected %08x\n"),
					   progname, xlogoff, (int) lseek(walfile, 0, SEEK_CUR));
					goto error;
				}
			}

			bytes_left = r - hdr_len;
			bytes_written = 0;

			while (bytes_left)
			{
				int			bytes_to_write;

				/*
				 * If crossing a WAL boundary, only write up until we reach
				 * XLOG_SEG_SIZE.
				 */
				if (xlogoff + bytes_left > XLOG_SEG_SIZE)
					bytes_to_write = XLOG_SEG_SIZE - xlogoff;
				else
					bytes_to_write = bytes_left;

				if (walfile == -1)
				{
					if (!open_walfile(blockpos, timeline,
									  basedir, partial_suffix))
					{
						/* Error logged by open_walfile */
						goto error;
					}
				}

				if (write(walfile,
						  copybuf + hdr_len + bytes_written,
						  bytes_to_write) != bytes_to_write)
				{
					fprintf(stderr,
							_("%s: could not write %u bytes to WAL file \"%s\": %s\n"),
							progname, bytes_to_write, current_walfile_name,
							strerror(errno));
					goto error;
				}

				/* Write was successful, advance our position */
				bytes_written += bytes_to_write;
				bytes_left -= bytes_to_write;
				blockpos += bytes_to_write;
				xlogoff += bytes_to_write;

				/* Did we reach the end of a WAL segment? */
				if (blockpos % XLOG_SEG_SIZE == 0)
				{
					if (!close_walfile(basedir, partial_suffix, blockpos))
						/* Error message written in close_walfile() */
						goto error;

					xlogoff = 0;

					if (still_sending && stream_stop(blockpos, timeline, true))
					{
						if (PQputCopyEnd(conn, NULL) <= 0 || PQflush(conn))
						{
							fprintf(stderr, _("%s: could not send copy-end packet: %s"),
									progname, PQerrorMessage(conn));
							goto error;
						}
						still_sending = false;
						break;	/* ignore the rest of this XLogData packet */
					}
				}
			}
			/* No more data left to write, receive next copy packet */
		}
		else
		{
			fprintf(stderr, _("%s: unrecognized streaming header: \"%c\"\n"),
					progname, copybuf[0]);
			goto error;
		}
	}

error:
	if (copybuf != NULL)
		PQfreemem(copybuf);
	return NULL;
}
