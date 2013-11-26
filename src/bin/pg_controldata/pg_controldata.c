/*
 * pg_controldata
 *
 * reads the data from $PGDATA/global/pg_control
 *
 * copyright (c) Oliver Elphick <olly@lfix.co.uk>, 2001;
 * licence: BSD
 *
 * src/bin/pg_controldata/pg_controldata.c
 */

/*
 * We have to use postgres.h not postgres_fe.h here, because there's so much
 * backend-only stuff in the XLOG include files we need.  But we need a
 * frontend-ish environment otherwise.	Hence this ugly hack.
 */
#define FRONTEND 1

#include "postgres.h"

#include <unistd.h>
#include <time.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "access/xlog.h"
#include "catalog/pg_control.h"


static void
usage(const char *progname)
{
	printf(_("%s displays control information of a PostgreSQL database cluster.\n\n"), progname);
	printf
		(
		 _(
		   "Usage:\n"
		   "  %s [OPTION] [DATADIR]\n\n"
		   "Options:\n"
		   "  --help         show this help, then exit\n"
		   "  --version      output version information, then exit\n"
		   ),
		 progname
		);
	printf(_("\nIf no data directory (DATADIR) is specified, "
			 "the environment variable PGDATA\nis used.\n\n"));
	printf(_("Report bugs to <pgsql-bugs@postgresql.org>.\n"));
}


static const char *
dbState(DBState state)
{
	switch (state)
	{
		case DB_STARTUP:
			return _("starting up");
		case DB_SHUTDOWNED:
			return _("shut down");
		case DB_SHUTDOWNED_IN_RECOVERY:
			return _("shut down in recovery");
		case DB_SHUTDOWNING:
			return _("shutting down");
		case DB_IN_CRASH_RECOVERY:
			return _("in crash recovery");
		case DB_IN_ARCHIVE_RECOVERY:
			return _("in archive recovery");
		case DB_IN_PRODUCTION:
			return _("in production");
	}
	return _("unrecognized status code");
}

static const char *
wal_level_str(WalLevel wal_level)
{
	switch (wal_level)
	{
		case WAL_LEVEL_MINIMAL:
			return "minimal";
		case WAL_LEVEL_ARCHIVE:
			return "archive";
		case WAL_LEVEL_HOT_STANDBY:
			return "hot_standby";
	}
	return _("unrecognized wal_level");
}


int
main(int argc, char *argv[])
{
	ControlFileData ControlFile;
	int			fd;
	char		ControlFilePath[MAXPGPATH];
	char	   *DataDir;
	pg_crc32	crc;
	time_t		time_tmp;
	char		pgctime_str[128];
	char		ckpttime_str[128];
	char		sysident_str[32];
	const char *strftime_fmt = "%c";
	const char *progname;

	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_controldata"));

	progname = get_progname(argv[0]);

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage(progname);
			exit(0);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			puts("pg_controldata (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	if (argc > 1)
		DataDir = argv[1];
	else
		DataDir = getenv("PGDATA");
	if (DataDir == NULL)
	{
		fprintf(stderr, _("%s: no data directory specified\n"), progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
		exit(1);
	}

	snprintf(ControlFilePath, MAXPGPATH, "%s/global/pg_control", DataDir);

	if ((fd = open(ControlFilePath, O_RDONLY | PG_BINARY, 0)) == -1)
	{
		fprintf(stderr, _("%s: could not open file \"%s\" for reading: %s\n"),
				progname, ControlFilePath, strerror(errno));
		exit(2);
	}

	if (read(fd, &ControlFile, sizeof(ControlFileData)) != sizeof(ControlFileData))
	{
		fprintf(stderr, _("%s: could not read file \"%s\": %s\n"),
				progname, ControlFilePath, strerror(errno));
		exit(2);
	}
	close(fd);

	/* Check the CRC. */
	INIT_CRC32(crc);
	COMP_CRC32(crc,
			   (char *) &ControlFile,
			   offsetof(ControlFileData, crc));
	FIN_CRC32(crc);

	if (!EQ_CRC32(crc, ControlFile.crc))
		printf(_("WARNING: Calculated CRC checksum does not match value stored in file.\n"
				 "Either the file is corrupt, or it has a different layout than this program\n"
				 "is expecting.  The results below are untrustworthy.\n\n"));

	/*
	 * This slightly-chintzy coding will work as long as the control file
	 * timestamps are within the range of time_t; that should be the case in
	 * all foreseeable circumstances, so we don't bother importing the
	 * backend's timezone library into pg_controldata.
	 *
	 * Use variable for format to suppress overly-anal-retentive gcc warning
	 * about %c
	 */
	time_tmp = (time_t) ControlFile.time;
	strftime(pgctime_str, sizeof(pgctime_str), strftime_fmt,
			 localtime(&time_tmp));
	time_tmp = (time_t) ControlFile.checkPointCopy.time;
	strftime(ckpttime_str, sizeof(ckpttime_str), strftime_fmt,
			 localtime(&time_tmp));

	/*
	 * Format system_identifier separately to keep platform-dependent format
	 * code out of the translatable message string.
	 */
	snprintf(sysident_str, sizeof(sysident_str), UINT64_FORMAT,
			 ControlFile.system_identifier);

	printf(_("pg_control version number:            %u\n"),
		   ControlFile.pg_control_version);
	if (ControlFile.pg_control_version % 65536 == 0 && ControlFile.pg_control_version / 65536 != 0)
		printf(_("WARNING: possible byte ordering mismatch\n"
				 "The byte ordering used to store the pg_control file might not match the one\n"
				 "used by this program.  In that case the results below would be incorrect, and\n"
				 "the PostgreSQL installation would be incompatible with this data directory.\n"));
	printf(_("Catalog version number:               %u\n"),
		   ControlFile.catalog_version_no);
	printf(_("Database system identifier:           %s\n"),
		   sysident_str);
	printf(_("Database cluster state:               %s\n"),
		   dbState(ControlFile.state));
	printf(_("pg_control last modified:             %s\n"),
		   pgctime_str);
	printf(_("Latest checkpoint location:           %X/%X\n"),
		   ControlFile.checkPoint.xlogid,
		   ControlFile.checkPoint.xrecoff);
	printf(_("Prior checkpoint location:            %X/%X\n"),
		   ControlFile.prevCheckPoint.xlogid,
		   ControlFile.prevCheckPoint.xrecoff);
	printf(_("Latest checkpoint's REDO location:    %X/%X\n"),
		   ControlFile.checkPointCopy.redo.xlogid,
		   ControlFile.checkPointCopy.redo.xrecoff);
	printf(_("Latest checkpoint's TimeLineID:       %u\n"),
		   ControlFile.checkPointCopy.ThisTimeLineID);
	printf(_("Latest checkpoint's full_page_writes: %s\n"),
		   ControlFile.checkPointCopy.fullPageWrites ? _("on") : _("off"));
	printf(_("Latest checkpoint's NextXID:          %u/%u\n"),
		   ControlFile.checkPointCopy.nextXidEpoch,
		   ControlFile.checkPointCopy.nextXid);
	printf(_("Latest checkpoint's NextOID:          %u\n"),
		   ControlFile.checkPointCopy.nextOid);
	printf(_("Latest checkpoint's NextMultiXactId:  %u\n"),
		   ControlFile.checkPointCopy.nextMulti);
	printf(_("Latest checkpoint's NextMultiOffset:  %u\n"),
		   ControlFile.checkPointCopy.nextMultiOffset);
	printf(_("Latest checkpoint's oldestXID:        %u\n"),
		   ControlFile.checkPointCopy.oldestXid);
	printf(_("Latest checkpoint's oldestXID's DB:   %u\n"),
		   ControlFile.checkPointCopy.oldestXidDB);
	printf(_("Latest checkpoint's oldestActiveXID:  %u\n"),
		   ControlFile.checkPointCopy.oldestActiveXid);
	printf(_("Time of latest checkpoint:            %s\n"),
		   ckpttime_str);
	printf(_("Minimum recovery ending location:     %X/%X\n"),
		   ControlFile.minRecoveryPoint.xlogid,
		   ControlFile.minRecoveryPoint.xrecoff);
	printf(_("Backup start location:                %X/%X\n"),
		   ControlFile.backupStartPoint.xlogid,
		   ControlFile.backupStartPoint.xrecoff);
	printf(_("Backup end location:                  %X/%X\n"),
		   ControlFile.backupEndPoint.xlogid,
		   ControlFile.backupEndPoint.xrecoff);
	printf(_("End-of-backup record required:        %s\n"),
		   ControlFile.backupEndRequired ? _("yes") : _("no"));
	printf(_("Current wal_level setting:            %s\n"),
		   wal_level_str(ControlFile.wal_level));
	printf(_("Current max_connections setting:      %d\n"),
		   ControlFile.MaxConnections);
	printf(_("Current max_prepared_xacts setting:   %d\n"),
		   ControlFile.max_prepared_xacts);
	printf(_("Current max_locks_per_xact setting:   %d\n"),
		   ControlFile.max_locks_per_xact);
	printf(_("Maximum data alignment:               %u\n"),
		   ControlFile.maxAlign);
	/* we don't print floatFormat since can't say much useful about it */
	printf(_("Database block size:                  %u\n"),
		   ControlFile.blcksz);
	printf(_("Blocks per segment of large relation: %u\n"),
		   ControlFile.relseg_size);
	printf(_("WAL block size:                       %u\n"),
		   ControlFile.xlog_blcksz);
	printf(_("Bytes per WAL segment:                %u\n"),
		   ControlFile.xlog_seg_size);
	printf(_("Maximum length of identifiers:        %u\n"),
		   ControlFile.nameDataLen);
	printf(_("Maximum columns in an index:          %u\n"),
		   ControlFile.indexMaxKeys);
	printf(_("Maximum size of a TOAST chunk:        %u\n"),
		   ControlFile.toast_max_chunk_size);
	printf(_("Date/time type storage:               %s\n"),
		   (ControlFile.enableIntTimes ? _("64-bit integers") : _("floating-point numbers")));
	printf(_("Float4 argument passing:              %s\n"),
		   (ControlFile.float4ByVal ? _("by value") : _("by reference")));
	printf(_("Float8 argument passing:              %s\n"),
		   (ControlFile.float8ByVal ? _("by value") : _("by reference")));
	return 0;
}
