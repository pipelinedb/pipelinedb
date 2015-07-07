#!/usr/bin/perl
#$| = 1;
use File::Basename;
use File::Temp 'tempdir';
#use File::Which;
use File::Copy;
use File::Path;
use Cwd 'abs_path';
use Getopt::Long;
use strict;


##################################################################
# Usage ./run_test.pl <testname> [<testname>]
#
#  Create the spatial database 'postgis_reg' (or whatever $DB 
#  is set to) if it doesn't already exist.
#
#  Run the <testname>.sql script
#  Check output against <testname>_expected
##################################################################

##################################################################
# Global configuration items
##################################################################

our $DB = $ENV{"POSTGIS_REGRESS_DB"} || "postgis_reg";
our $REGDIR = abs_path(dirname($0));
our $SHP2PGSQL = $REGDIR . "/../loader/shp2pgsql";
our $PGSQL2SHP = $REGDIR . "/../loader/pgsql2shp";
our $RASTER2PGSQL = $REGDIR . "/../raster/loader/raster2pgsql";
our $sysdiff = !system("diff --strip-trailing-cr $0 $0 2> /dev/null");

##################################################################
# Parse command line opts
##################################################################

my $OPT_CLEAN = 0;
my $OPT_NODROP = 0;
my $OPT_NOCREATE = 0;
my $OPT_UPGRADE = 0;
my $OPT_WITH_TOPO = 0;
my $OPT_WITH_RASTER = 0;
my $OPT_WITH_SFCGAL = 0;
my $OPT_EXPECT = 0;
my $OPT_EXTENSIONS = 0;
my $OPT_EXTVERSION = '';
my $OPT_UPGRADE_PATH = '';
my $OPT_UPGRADE_FROM = '';
my $OPT_UPGRADE_TO = '';
my $VERBOSE = 0;

GetOptions (
	'verbose' => \$VERBOSE,
	'clean' => \$OPT_CLEAN,
	'nodrop' => \$OPT_NODROP, 
	'upgrade' => \$OPT_UPGRADE,
	'upgrade-path=s' => \$OPT_UPGRADE_PATH,
	'nocreate' => \$OPT_NOCREATE,
	'topology' => \$OPT_WITH_TOPO,
	'raster' => \$OPT_WITH_RASTER,
	'sfcgal' => \$OPT_WITH_SFCGAL,
	'expect' => \$OPT_EXPECT,
	'extensions' => \$OPT_EXTENSIONS
	);

if ( @ARGV < 1 )
{
	usage();
}

if ( $OPT_UPGRADE_PATH )
{
  if ( ! $OPT_EXTENSIONS )
  {
    die "--upgrade-path is only supported with --extensions"
  }
  $OPT_UPGRADE = 1; # implied 
  my @path = split ('--', $OPT_UPGRADE_PATH);
  $OPT_UPGRADE_FROM = $path[0]
    || die "Malformed upgrade path, <from>--<to> expected, $OPT_UPGRADE_PATH given";
  $OPT_UPGRADE_TO = $path[1]
    || die "Malformed upgrade path, <from>--<to> expected, $OPT_UPGRADE_PATH given";
  print "Upgrade path: ${OPT_UPGRADE_FROM} --> ${OPT_UPGRADE_TO}\n";
}



##################################################################
# Set the locale to "C" so error messages match
# Save original locale to set back
##################################################################

my $ORIG_LC_ALL = $ENV{"LC_ALL"};
my $ORIG_LANG = $ENV{"LANG"};
$ENV{"LC_ALL"} = "C";
$ENV{"LANG"} = "C";

# Add locale info to the psql options
my $PGOPTIONS = $ENV{"PGOPTIONS"} . " -c lc_messages=C -c client_min_messages=NOTICE";
$ENV{"PGOPTIONS"} = $PGOPTIONS;

# Bring the path info in
my $PATH = $ENV{"PATH"}; # this is useless

# Calculate the regression directory locations
my $STAGED_INSTALL_DIR = $REGDIR . "/00-regress-install";
my $STAGED_SCRIPTS_DIR = $STAGED_INSTALL_DIR . "/share/contrib/postgis";

my $OBJ_COUNT_PRE = 0;
my $OBJ_COUNT_POST = 0;

##################################################################
# Check that we have the executables we need
##################################################################

print "PATH is $PATH\n";

#foreach my $exec ( ("psql", "createdb", "createlang", "dropdb") )
#{
#	my $execdir = which( $exec );
#	print "Checking for $exec ... ";
#	if ( $execdir )
#	{
#		print "found $execdir\n";
#	}
#	else
#	{
#		print "failed\n";
#		die "Unable to find $exec executable. Please ensure it is on your PATH.\n";
#	}
#}

foreach my $exec ( ($SHP2PGSQL, $PGSQL2SHP) )
{
	printf "Checking for %s ... ", basename($exec);
	if ( -x $exec )
	{
		print "found\n";
	}
	else
	{
		print "failed\n";
		die "Unable to find $exec executable.\n";
	}
	
}

if ( $OPT_WITH_RASTER )
{
	print "Checking for raster2pgsql ... ";
	if ( -x $RASTER2PGSQL )
	{
		print "found\n";
	}
	else
	{
		print "failed\n";
		die "Unable to find raster2pgsql executable.\n";
	}
}

##################################################################
# Set up the temporary directory
##################################################################

my $TMPDIR;
if ( $ENV{'PGIS_REG_TMPDIR'} )
{
	$TMPDIR = $ENV{'PGIS_REG_TMPDIR'};
}
elsif ( -d "/tmp/" && -w "/tmp/" )
{
	$TMPDIR = "/tmp/pgis_reg";
}
else
{
	$TMPDIR = tempdir( CLEANUP => 0 );
}

mkdir $TMPDIR if ( ! -d $TMPDIR );

# Set log name
my $REGRESS_LOG = "${TMPDIR}/regress_log";

# Report
print "TMPDIR is $TMPDIR\n";


##################################################################
# Prepare the database
##################################################################

my @dblist = grep(/$DB/, split(/\n/, `psql -Xl`));
my $dbcount = @dblist;

if ( $dbcount == 0 )
{
	if ( $OPT_NOCREATE )
	{
		print "Database $DB does not exist.\n";
		print "Run without the --nocreate flag to create it.\n";
		exit(1);
	}
	else
	{
		create_spatial();
	}
}
else
{
	if ( $OPT_NOCREATE )
	{
		print "Using existing database $DB\n";
	}
	else
	{
		print "Database $DB already exists.\n";
		print "Drop it, or run with the --nocreate flag to use it.\n";
		exit(1);
	}
}

my $libver = sql("select postgis_lib_version()");

if ( ! $libver )
{
	`dropdb $DB`;
	print "\nSomething went wrong (no PostGIS installed in $DB).\n";
	print "For details, check $REGRESS_LOG\n\n";
	exit(1);
}


sub create_upgrade_test_objects
{
  # TODO: allow passing the "upgrade-init" script via commandline

  my $query = "create table upgrade_test(g1 geometry, g2 geography";
  $query .= ", r raster" if ( $OPT_WITH_RASTER );
  $query .= ")";
  my $ret = sql($query);
  unless ( $ret =~ /^CREATE/ ) {
    `dropdb $DB`;
    print "\nSomething went wrong creating upgrade_test table: $ret.\n";
    exit(1);
  }

  if ( $OPT_WITH_RASTER )
  {
    $query = "insert into upgrade_test(r) ";
    $query .= "select ST_AddBand(ST_MakeEmptyRaster(10, 10, 1, 1, 2, 2, 0, 0,4326), 1, '8BSI'::text, -129, NULL);";
    $query .= "set client_min_messages to error; select AddRasterConstraints('upgrade_test', 'r')";
    $ret = sql($query);
    unless ( $ret =~ /^t$/ ) {
      `dropdb $DB`;
      print "\nSomething went wrong adding raster constraints to upgrade_test: " . $ret . "\n";
      exit(1);
    }
  }

  if ( $OPT_WITH_TOPO )
  {
    $query = "select topology.createTopology('upgrade_test');";
    $ret = sql($query);
    unless ( $ret =~ /^[1-9][0-9]*$/ ) {
      `dropdb $DB`;
      print "\nSomething went wrong adding upgrade_test topology: " . $ret . "\n";
      exit(1);
    }
  }
}

sub drop_upgrade_test_objects
{
  # TODO: allow passing the "upgrade-cleanup" script via commandline

  my $ret = sql("drop table upgrade_test;");
  unless ( $ret =~ /^DROP/ ) {
    `dropdb $DB`;
    print "\nSomething went wrong dropping spatial tables: $ret.\n";
    exit(1);
  }

  if ( $OPT_WITH_TOPO )
  {
    my $query = "SELECT topology.DropTopology('upgrade_test');";
    $ret = sql($query);
    unless ( $ret =~ /^Topology 'upgrade_test' dropped$/ ) {
      `dropdb $DB`;
      print "\nSomething went wrong dropping upgrade_test topology: " . $ret . "\n";
      exit(1);
    }
  }
}


if ( $OPT_UPGRADE )
{
  create_upgrade_test_objects();

  if ( $OPT_EXTENSIONS )
  {
    upgrade_spatial_extensions();
  }
  else
  {
	  upgrade_spatial();
  }

  drop_upgrade_test_objects();

  # Update libver
  $libver = sql("select postgis_lib_version()");
}


##################################################################
# Report PostGIS environment
##################################################################

my $geosver =  sql("select postgis_geos_version()");
my $projver = sql("select postgis_proj_version()");
my $svnrev = sql("select postgis_svn_version()");
my $libbuilddate = sql("select postgis_lib_build_date()");
my $pgsqlver = sql("select version()");
my $gdalver = sql("select postgis_gdal_version()") if $OPT_WITH_RASTER;
my $sfcgalver = sql("select postgis_sfcgal_version()") if $OPT_WITH_SFCGAL;
my $scriptver = sql("select postgis_scripts_installed()");
my $raster_scriptver = sql("select postgis_raster_scripts_installed()")
  if ( $OPT_WITH_RASTER );

print "$pgsqlver\n";
print "  Postgis $libver - r${svnrev} - $libbuilddate\n";
print "  scripts ${scriptver}\n";
print "  raster scripts ${raster_scriptver}\n" if ( $OPT_WITH_RASTER );
print "  GEOS: $geosver\n" if $geosver;
print "  PROJ: $projver\n" if $projver;
print "  SFCGAL: $sfcgalver\n" if $sfcgalver;
print "  GDAL: $gdalver\n" if $gdalver;


##################################################################
# Set up some global variables
##################################################################
my $RUN = 0;
my $FAIL = 0;
my $SKIP = 0;
our $TEST = "";

##################################################################
# Run the tests
##################################################################

print "\nRunning tests\n\n";

foreach $TEST (@ARGV)
{
	# catch a common mistake (strip trailing .sql)
	$TEST =~ s/.sql$//;

	start_test($TEST);

	# Check for a "-pre.pl" file in case there are setup commands 
    eval_file("${TEST}-pre.pl");

	# Check for a "-pre.sql" file in case there is setup SQL needed before
	# the test can be run.
	if ( -r "${TEST}-pre.sql" )
	{	
		run_simple_sql("${TEST}-pre.sql");
		show_progress();
	}

	# Check .dbf *before* .sql as loader test could
	# create the .sql
	# Check for .dbf not just .shp since the loader can load
	# .dbf files without a .shp.
	if ( -r "${TEST}.dbf" )
	{
		pass() if ( run_loader_test() );
	}
	elsif ( -r "${TEST}.tif" )
	{
		my $rv = run_raster_loader_test();
		pass() if $rv;
	}
	elsif ( -r "${TEST}.sql" )
	{
		my $rv = run_simple_test("${TEST}.sql", "${TEST}_expected");
		pass() if $rv;
	}
	elsif ( -r "${TEST}.dmp" )
	{
		pass() if run_dumper_test();
	}
	else
	{
		print " skipped (can't read any ${TEST}.{sql,dbf,tif,dmp})\n";
		$SKIP++;
		next;
	}

	if ( -r "${TEST}-post.sql" )
	{
		my $rv = run_simple_sql("${TEST}-post.sql");
		if ( ! $rv )
		{
			print " ... but cleanup sql failed!";
		}
	}
	
	# Check for a "-post.pl" file in case there are teardown commands 
    eval_file("${TEST}-post.pl");
	
}


################################################################### 
# Uninstall postgis (serves as an uninstall test)
##################################################################

# We only test uninstall if we've been asked to drop 
# and we did create
# and nobody requested raster or topology
# (until they have an uninstall script themself)

if ( (! $OPT_NODROP) && $OBJ_COUNT_PRE > 0 )
{
	uninstall_spatial();
}

##################################################################
# Summary report
##################################################################

print "\nRun tests: $RUN\n";
print "Failed: $FAIL\n";

if ( $OPT_CLEAN )
{
	rmtree($TMPDIR);
}

if ( ! ($OPT_NODROP || $OPT_NOCREATE) )
{
	sleep(1);
	system("dropdb $DB");
}
else
{
	print "Drop database ${DB} manually\n";
}

# Set the locale back to the original
$ENV{"LC_ALL"} = $ORIG_LC_ALL;
$ENV{"LANG"} = $ORIG_LANG;

exit($FAIL);



##################################################################
# Utility functions
#

sub usage 
{
	die qq{
Usage: $0 [<options>] <testname> [<testname>]
Options:
  -v, --verbose   be verbose about failures
  --nocreate      do not create the regression database on start
  --upgrade       source the upgrade scripts on start
  --nodrop        do not drop the regression database on exit
  --raster        load also raster extension
  --topology      load also topology extension
  --sfcgal        use also sfcgal backend
  --clean         cleanup test logs on exit
  --expect        save obtained output as expected
  --extension     load using extensions
  --upgrade-path  upgrade path, format <from>--<to>
};

}

# start_test <name>
sub start_test
{
    my $test = shift;
    print " $test ";
	$RUN++;
    show_progress();
}

# Print a entry
sub echo_inline
{
	my $msg = shift;
	print $msg;
}

# Print a single dot
sub show_progress
{
	print ".";
}

# pass <msg> 
sub pass
{
  my $msg = shift;
  printf(" ok %s\n", $msg);
}

# fail <msg> <log>
sub fail
{
	my $msg = shift;
	my $log = shift;

	if ( ! $log )
	{
		printf(" failed (%s)\n", $msg);
	}
	elsif ( $VERBOSE == 1 )
	{
		printf(" failed (%s: %s)\n", $msg, $log);
		print "-----------------------------------------------------------------------------\n";
		open(LOG, "$log") or die "Cannot open log file $log\n";
		print while(<LOG>);
		close(LOG);
		print "-----------------------------------------------------------------------------\n";
	}
	else
	{
		printf(" failed (%s: %s)\n", $msg, $log);
	}

	$FAIL++;
}

  

##################################################################
# run_simple_sql 
#   Run an sql script and hide results unless it fails.
#   SQL input file name is $1
##################################################################
sub run_simple_sql
{
	my $sql = shift;

	if ( ! -r $sql ) 
	{
		fail("can't read $sql");
		return 0;
	}

	# Dump output to a temp file.
	my $tmpfile = sprintf("%s/test_%s_tmp", $TMPDIR, $RUN);
	my $cmd = "psql -v \"VERBOSITY=terse\" -tXA $DB < $sql > $tmpfile 2>&1";
	#print($cmd);
	my $rv = system($cmd);
	# Check if psql errored out.
	if ( $rv != 0 ) 
	{
		fail("Unable to run sql script $sql", $tmpfile);
		return 0;
	}
	
	# Check for ERROR lines
	open FILE, "$tmpfile";
	my @lines = <FILE>;
	close FILE;
	my @errors = grep(/^ERROR/, @lines);
	
	if ( @errors > 0 )
	{
		fail("Errors while running sql script $sql", $tmpfile);
		return 0;
	}

	unlink $tmpfile;
	return 1;
}

sub drop_table
{
	my $tblname = shift;
	my $cmd = "psql -tXA -d $DB -c \"DROP TABLE IF EXISTS $tblname\" >> $REGRESS_LOG 2>&1";
	my $rv = system($cmd);
	die "Could not run: $cmd\n" if $rv;	
}

sub sql
{
	my $sql = shift;
	my $result = `psql -tXA -d $DB -c "$sql"`;
	$result =~ s/\n$//;
	$result;
}

sub eval_file
{
    my $file = shift;
    my $pl;
    if ( -r $file )
    {
        #open(PL, $file);
        #$pl = <PL>;
        #close(PL);
        #eval($pl);
				do $file;
    }
}

##################################################################
# run_simple_test 
#   Run an sql script and compare results with the given expected output
#   SQL input is ${TEST}.sql, expected output is {$TEST}_expected
##################################################################
sub run_simple_test
{
	my $sql = shift;
	my $expected = shift;
	my $msg = shift;

	if ( ! -r "$sql" )
	{
		fail("can't read $sql");
		return 0;
	}
	
	if ( ! $OPT_EXPECT )
	{
		if ( ! -r "$expected" )
		{
			fail("can't read $expected");
			return 0;
		}
	}

	show_progress();

	my $outfile = sprintf("%s/test_%s_out", $TMPDIR, $RUN);
	my $betmpdir = sprintf("%s/pgis_reg_tmp/", $TMPDIR);
	my $tmpfile = sprintf("%s/test_%s_tmp", $betmpdir, $RUN);
	my $diffile = sprintf("%s/test_%s_diff", $TMPDIR, $RUN);

	mkpath($betmpdir);
	chmod 0777, $betmpdir;

	my $scriptdir;
	if ( $OPT_EXTENSIONS ) {
		# TODO: allow override this default with env variable ?
		my $pgis_majmin = $libver;
		$pgis_majmin =~ s/^([1-9]*\.[1-9]*).*/\1/;
		$scriptdir = `pg_config --sharedir`;
		chop $scriptdir;
		$scriptdir .= "/contrib/postgis-" . $pgis_majmin;
	} else {
		$scriptdir = $STAGED_SCRIPTS_DIR;
	}
	my $cmd = "psql -v \"VERBOSITY=terse\""
          . " -v \"tmpfile='$tmpfile'\""
          . " -v \"scriptdir=$scriptdir\""
          . " -v \"regdir=$REGDIR\""
          . " -tXA $DB < $sql > $outfile 2>&1";
	my $rv = system($cmd);

	# Check for ERROR lines
	open(FILE, "$outfile");
	my @lines = <FILE>;
	close(FILE);

	# Strip the lines we don't care about
	@lines = grep(!/^\$/, @lines);
	@lines = grep(!/^(INSERT|DELETE|UPDATE|SELECT|COPY|DO)/, @lines);
	@lines = grep(!/^(CONTEXT|RESET|ANALYZE)/, @lines);
	@lines = grep(!/^(DROP|CREATE|ALTER|VACUUM)/, @lines);
	@lines = grep(!/^(LOG|SET|TRUNCATE)/, @lines);
	@lines = grep(!/^LINE \d/, @lines);
	@lines = grep(!/^\s+$/, @lines);

	# Morph values into expected forms
	for ( my $i = 0; $i < @lines; $i++ )
	{
		$lines[$i] =~ s/Infinity/inf/g;
		$lines[$i] =~ s/Inf/inf/g;
		$lines[$i] =~ s/1\.#INF/inf/g;
		$lines[$i] =~ s/[eE]([+-])0+(\d+)/e$1$2/g;
		$lines[$i] =~ s/Self-intersection .*/Self-intersection/;
		$lines[$i] =~ s/^ROLLBACK/COMMIT/;
		$lines[$i] =~ s/^psql.*(NOTICE|WARNING|ERROR):/\1:/g;
	}
	
	# Write out output file
	open(FILE, ">$outfile");
	foreach my $l (@lines) 
	{
		print FILE $l;
	}
	close(FILE);

	# Clean up interim stuff
	#remove_tree($betmpdir);
	
	if ( $OPT_EXPECT )
	{
		print " (expected)";
		copy($outfile, $expected);
	}
	else
	{
		my $diff = diff($expected, $outfile);
		if ( $diff )
		{
			open(FILE, ">$diffile");
			print FILE $diff;
			close(FILE);
			fail("${msg}diff expected obtained", $diffile);
			return 0;
		}
		else
		{
			unlink $outfile;
			return 1;
		}
	}
	
	return 1;
}

##################################################################
# This runs the loader once and checks the output of it.
# It will NOT run if neither the expected SQL nor the expected
# select results file exists, unless you pass true for the final
# parameter.
#
# $1 - Description of this run of the loader, used for error messages.
# $2 - Table name to load into.
# $3 - The name of the file containing what the
#      SQL generated by shp2pgsql should look like.
# $4 - The name of the file containing the expected results of
#      SELECT geom FROM _tblname should look like.
# $5 - Command line options for shp2pgsql.
# $6 - If you pass true, this will run the loader even if neither
#      of the expected results files exists (though of course
#      the results won't be compared with anything).
##################################################################
sub run_loader_and_check_output
{
	my $description = shift;
	my $tblname = shift;
	my $expected_sql_file = shift;
	my $expected_select_results_file = shift;
	my $loader_options = shift;
	my $run_always = shift;

	my ( $cmd, $rv );
	my $outfile = "${TMPDIR}/loader.out";
	my $errfile = "${TMPDIR}/loader.err";
	
	# ON_ERROR_STOP is used by psql to return non-0 on an error
	my $psql_opts = " --no-psqlrc --variable ON_ERROR_STOP=true";

	if ( $run_always || -r $expected_sql_file || -r $expected_select_results_file )
	{
		show_progress();
		# Produce the output SQL file.
		$cmd = "$SHP2PGSQL $loader_options -g the_geom ${TEST}.shp $tblname > $outfile 2> $errfile";
		$rv = system($cmd);

		if ( $rv )
		{
			fail(" $description: running shp2pgsql", "$errfile");
			return 0;
		}

		# Compare the output SQL file with the expected if there is one.
		if ( -r $expected_sql_file )
		{
			show_progress();
			my $diff = diff($expected_sql_file, $outfile);
			if ( $diff )
			{
				fail(" $description: actual SQL does not match expected.", "$outfile");
				return 0;
			}
		}
		
		# Run the loader SQL script.
		show_progress();
		$cmd = "psql $psql_opts -f $outfile $DB > $errfile 2>&1";
		$rv = system($cmd);
		if ( $rv )
		{
			fail(" $description: running shp2pgsql output","$errfile");
			return 0;
		}

		# Run the select script (if there is one)
		if ( -r "${TEST}.select.sql" )
		{
			$rv = run_simple_test("${TEST}.select.sql",$expected_select_results_file, $description);
			return 0 if ( ! $rv );
		}
	}
	return 1;
}

##################################################################
# This runs the dumper once and checks the output of it.
# It will NOT run if the expected shp file does not exist, unless
# you pass true for the final parameter.
#
# $1 - Description of this run of the dumper, used for error messages.
# $2 - Table name to dump from.
# $3 - "Expected" .shp file to compare with.
# $4 - If you pass true, this will run the loader even if neither
#      of the expected results files exists (though of course
#      the results won't be compared with anything).
##################################################################
sub run_dumper_and_check_output
{
	my $description = shift;
	my $tblname = shift;
	my $expected_shp_file = shift;
	my $run_always = shift;

	my ($cmd, $rv);
	my $errfile = "${TMPDIR}/dumper.err";
	
	if ( $run_always || -r $expected_shp_file ) 
	{
		show_progress();
		$cmd = "${PGSQL2SHP} -f ${TMPDIR}/dumper $DB $tblname > $errfile 2>&1";
		$rv = system($cmd);
	
		if ( $rv )
		{
			fail("$description: dumping loaded table", $errfile);
			return 0;
		}

		# Compare with expected output if there is any.
		
		if ( -r $expected_shp_file )
		{
			show_progress();
			
			my $diff = diff($expected_shp_file,  "$TMPDIR/dumper.shp");
			if ( $diff )
			{
#				ls -lL "${TMPDIR}"/dumper.shp "$_expected_shp_file" > "${TMPDIR}"/dumper.diff
				fail("$description: dumping loaded table", "${TMPDIR}/dumper.diff");
				return 0;
			}
		}
	}
	return 1;
}


##################################################################
# This runs the loader once and checks the output of it.
# It will NOT run if neither the expected SQL nor the expected
# select results file exists, unless you pass true for the final
# parameter.
#
# $1 - Description of this run of the loader, used for error messages.
# $2 - Table name to load into.
# $3 - The name of the file containing what the
#      SQL generated by shp2pgsql should look like.
# $4 - The name of the file containing the expected results of
#      SELECT rast FROM _tblname should look like.
# $5 - Command line options for raster2pgsql.
# $6 - If you pass true, this will run the loader even if neither
#      of the expected results files exists (though of course
#      the results won't be compared with anything).
##################################################################
sub run_raster_loader_and_check_output
{
	my $description = shift;
	my $tblname = shift;
	my $expected_sql_file = shift;
	my $expected_select_results_file = shift;
	my $loader_options = shift;
	my $run_always = shift;
	
	# ON_ERROR_STOP is used by psql to return non-0 on an error
	my $psql_opts="--no-psqlrc --variable ON_ERROR_STOP=true";

	my ($cmd, $rv);
	my $outfile = "${TMPDIR}/loader.out";
	my $errfile = "${TMPDIR}/loader.err";

	if ( $run_always || -r $expected_sql_file || -r $expected_select_results_file ) 
	{
		show_progress();

		# Produce the output SQL file.
		$cmd = "$RASTER2PGSQL $loader_options ${TEST}.tif $tblname > $outfile 2> $errfile";
		$rv = system($cmd);
		
		if ( $rv )
		{
		    fail("$description: running raster2pgsql", $errfile);
		    return 0;
	    }
	    
	    if ( -r $expected_sql_file )
	    {
	        show_progress();
			my $diff = diff($expected_sql_file, $outfile);
			if ( $diff )
			{
				fail(" $description: actual SQL does not match expected.", "$outfile");
				return 0;
			}
	        
        }

		# Run the loader SQL script.
		show_progress();
		$cmd = "psql $psql_opts -f $outfile $DB > $errfile 2>&1";
    	$rv = system($cmd);
    	if ( $rv )
    	{
    		fail(" $description: running raster2pgsql output","$errfile");
    		return 0;
    	}

    	# Run the select script (if there is one)
    	if ( -r "${TEST}.select.sql" )
    	{
    		$rv = run_simple_test("${TEST}.select.sql",$expected_select_results_file, $description);
    		return 0 if ( ! $rv );
    	}
	}
    	
    return 1;
}



##################################################################
#  run_loader_test 
#
#  Load a shapefile with different methods, create a 'select *' SQL
#  test and run simple test with provided expected output. 
#
#  SHP input is ${TEST}.shp, expected output is {$TEST}.expected
##################################################################
sub run_loader_test 
{
	# See if there is a custom command-line options file
	my $opts_file = "${TEST}.opts";
	my $custom_opts="";
	
	if ( -r $opts_file )
	{
		open(FILE, $opts_file);
		my @opts = <FILE>;
		close(FILE);
		@opts = grep(!/^\s*#/, @opts);
		map(s/\n//, @opts);
		$custom_opts = join(" ", @opts);
	}

	my $tblname="loadedshp";

	# If we have some expected files to compare with, run in wkt mode.
	if ( ! run_loader_and_check_output("wkt test", $tblname, "${TEST}-w.sql.expected", "${TEST}-w.select.expected", "-w $custom_opts") )
	{
		return 0;
	}
	drop_table($tblname);

	# If we have some expected files to compare with, run in geography mode.
	if ( ! run_loader_and_check_output("geog test", $tblname, "${TEST}-G.sql.expected", "${TEST}-G.select.expected", "-G $custom_opts") )
	{
		return 0;
	}
	# If we have some expected files to compare with, run the dumper and compare shape files.
	if ( ! run_dumper_and_check_output("dumper geog test", $tblname, "${TEST}-G.shp.expected") )
	{
		return 0;
	}
	drop_table($tblname);

	# Always run in wkb ("normal") mode, even if there are no expected files to compare with.
	if( ! run_loader_and_check_output("wkb test", $tblname, "${TEST}.sql.expected", "${TEST}.select.expected", "$custom_opts", "true") )
	{
		return 0;
	}
	# If we have some expected files to compare with, run the dumper and compare shape files.
	if( ! run_dumper_and_check_output("dumper wkb test", $tblname, "${TEST}.shp.expected") )
	{
		return 0;
	}
	drop_table($tblname);

	# Some custom parameters can be incompatible with -D.
	if ( $custom_opts )
	{
		# If we have some expected files to compare with, run in wkt dump mode.
		if ( ! run_loader_and_check_output("wkt dump test", $tblname, "${TEST}-wD.sql.expected") )
		{
			return 0;
		}
		drop_table($tblname);

		# If we have some expected files to compare with, run in wkt dump mode.
		if ( ! run_loader_and_check_output("geog dump test", $tblname, "${TEST}-GD.sql.expected") )
		{
			return 0;
		}
		drop_table($tblname);

		# If we have some expected files to compare with, run in wkb dump mode.
		if ( ! run_loader_and_check_output("wkb dump test", $tblname, "${TEST}-D.sql.expected") )
		{
			return 0;
		}
		drop_table($tblname);
	}
	
	return 1;
}

##################################################################
#  run_dumper_test 
#
#  Run dumper and compare output with various expectances
#  test and run simple test with provided expected output. 
#
# input is ${TEST}.dmp, where last line is considered to be the
# [table|query] argument for pgsql2shp and all the previous lines,
# if any are 
#
##################################################################
sub run_dumper_test 
{
  my $dump_file  = "${TEST}.dmp";

  # ON_ERROR_STOP is used by psql to return non-0 on an error
  my $psql_opts="--no-psqlrc --variable ON_ERROR_STOP=true";

  my $shpfile = "${TMPDIR}/dumper-" . basename(${TEST}) . "-shp";
  my $outfile = "${TMPDIR}/dumper-" . basename(${TEST}) . ".out";
  my $errfile = "${TMPDIR}/dumper-" . basename(${TEST}) . ".err";

  # Produce the output SHP file.
  open DUMPFILE, "$dump_file" or die "Cannot open dump file $dump_file\n";
  sleep(1);
  my @dumplines = <DUMPFILE>;
  close DUMPFILE;
  my $dumpstring = join '', @dumplines;
  chop($dumpstring);
  my @cmd = ("${PGSQL2SHP}", "-f", ${shpfile}, ${DB}, ${dumpstring});
  open my $stdout_save, '>&', *STDOUT or die "Cannot dup stdout\n";
  open my $stderr_save, '>&', *STDERR or die "Cannot dup stdout\n";
  open STDOUT, ">${outfile}" or die "Cannot write to ${outfile}\n";
  open STDERR, ">${errfile}" or die "Cannot write to ${errfile}\n";
  my $rv = system(@cmd);
  open STDERR, '>&', $stderr_save;
  open STDOUT, '>&', $stdout_save;
  #sleep(3);
  show_progress();

  if ( $rv )
  {
    fail("dumping", "$errfile");
    return 0;
  }

  my $numtests = 0;
  foreach my $ext ("shp","prj","dbf","shx") {
    my $obtained = ${shpfile}.".".$ext;
    my $expected = ${TEST}."_expected.".$ext;
    if ( $OPT_EXPECT )
    {
      copy($obtained, $expected);
    }
    elsif ( -r ${expected} ) {
      show_progress();
      $numtests++;
      my $diff = diff($expected,  $obtained);
      if ( $diff )
      {
        my $diffile = sprintf("%s/dumper_test_%s_diff", $TMPDIR, $ext);
        open(FILE, ">$diffile");
        print FILE $diff;
        close(FILE);
        fail("diff expected obtained", $diffile);
        return 0;
      }
    }
  }

  #show_progress();

  if ( $OPT_EXPECT ) {
    print " (expected)";
  }
  elsif ( ! $numtests ) {
    fail("no expectances!");
    return 0;
  }

	return 1;
}


##################################################################
#  run_raster_loader_test 
##################################################################
sub run_raster_loader_test
{
	# See if there is a custom command-line options file
	my $opts_file = "${TEST}.opts";
	my $custom_opts="";
	
	if ( -r $opts_file )
	{
		open(FILE, $opts_file);
		my @opts = <FILE>;
		close(FILE);
		@opts = grep(!/^\s*#/, @opts);
		map(s/\n//, @opts);
		$custom_opts = join(" ", @opts);
	}

	my $tblname="loadedrast";

	# If we have some expected files to compare with, run in geography mode.
	if ( ! run_raster_loader_and_check_output("test", $tblname, "${TEST}.sql.expected", "${TEST}.select.expected", $custom_opts, "true") )
	{
		return 0;
	}
	
	drop_table($tblname);
	
	return 1;
}


##################################################################
# Count database objects
##################################################################
sub count_db_objects
{
	my $count = sql("WITH counts as (
		select count(*) from pg_type union all 
		select count(*) from pg_proc union all 
		select count(*) from pg_cast union all
		select count(*) from pg_aggregate union all
		select count(*) from pg_operator union all
		select count(*) from pg_opclass union all
		select count(*) from pg_namespace
			where nspname NOT LIKE 'pg_%' union all
		select count(*) from pg_opfamily ) 
		select sum(count) from counts");

 	return $count;
}


##################################################################
# Create the spatial database
##################################################################
sub create_spatial 
{
	my ($cmd, $rv);
	print "Creating database '$DB' \n";

	$cmd = "createdb --encoding=UTF-8 --template=template0 --lc-collate=C $DB > $REGRESS_LOG";
	$rv = system($cmd);
	$cmd = "createlang plpgsql $DB >> $REGRESS_LOG 2>&1";
	$rv = system($cmd);

	# Count database objects before installing anything
	$OBJ_COUNT_PRE = count_db_objects();

	if ( $OPT_EXTENSIONS ) 
	{
		prepare_spatial_extensions();
	}
	else
	{
		prepare_spatial();
	}
}


sub load_sql_file
{
	my $file = shift;
	my $strict = shift;
	
	if ( $strict && ! -e $file )
	{
		die "Unable to find $file\n"; 
	}
	
	if ( -e $file )
	{
		# ON_ERROR_STOP is used by psql to return non-0 on an error
		my $psql_opts = "--no-psqlrc --variable ON_ERROR_STOP=true";
		my $cmd = "psql $psql_opts -Xf $file $DB >> $REGRESS_LOG 2>&1";
		print "  $file\n" if $VERBOSE;
		my $rv = system($cmd);
		if ( $rv )
		{
		  fail "Error encountered loading $file", $REGRESS_LOG;
		  exit 1
		}
	}
	return 1;
}

# Prepare the database for spatial operations (extension method)
sub prepare_spatial_extensions
{
	# ON_ERROR_STOP is used by psql to return non-0 on an error
	my $psql_opts = "--no-psqlrc --variable ON_ERROR_STOP=true";
	my $sql = "CREATE EXTENSION postgis";
	if ( $OPT_UPGRADE_FROM ) {
		$sql .= " VERSION '" . $OPT_UPGRADE_FROM . "'";
	}

	print "Preparing db '${DB}' using: ${sql}\n"; 

	my $cmd = "psql $psql_opts -c \"". $sql . "\" $DB >> $REGRESS_LOG 2>&1";
	my $rv = system($cmd);

  if ( $rv ) {
  	fail "Error encountered creating EXTENSION POSTGIS", $REGRESS_LOG;
  	die;
	}

	if ( $OPT_WITH_TOPO )
	{
		my $sql = "CREATE EXTENSION postgis_topology";
		if ( $OPT_UPGRADE_FROM ) {
			$sql .= " VERSION '" . $OPT_UPGRADE_FROM . "'";
		}
 		$cmd = "psql $psql_opts -c \"" . $sql . "\" $DB >> $REGRESS_LOG 2>&1";
		$rv = system($cmd);
  	if ( $rv ) {
  		fail "Error encountered creating EXTENSION POSTGIS_TOPOLOGY", $REGRESS_LOG;
  		die;
		}
 	}

	if ( $OPT_WITH_SFCGAL )
	{
		my $sql = "CREATE EXTENSION postgis_sfcgal";
		if ( $OPT_UPGRADE_FROM ) {
			$sql .= " VERSION '" . $OPT_UPGRADE_FROM . "'";
		}
 		$cmd = "psql $psql_opts -c \"" . $sql . "\" $DB >> $REGRESS_LOG 2>&1";
		$rv = system($cmd);
		if ( $rv ) {
		  fail "Error encountered creating EXTENSION POSTGIS_SFCGAL", $REGRESS_LOG;
		  die;
		}
	}

 	return 1;
}

# Prepare the database for spatial operations (old method)
sub prepare_spatial
{
	print "Loading PostGIS into '${DB}' \n";

	# Load postgis.sql into the database
	load_sql_file("${STAGED_SCRIPTS_DIR}/postgis.sql", 1);
	load_sql_file("${STAGED_SCRIPTS_DIR}/postgis_comments.sql", 0);
	
	if ( $OPT_WITH_TOPO )
	{
		print "Loading Topology into '${DB}'\n";
		load_sql_file("${STAGED_SCRIPTS_DIR}/topology.sql", 1);
		load_sql_file("${STAGED_SCRIPTS_DIR}/topology_comments.sql", 0);
	}
	
	if ( $OPT_WITH_RASTER )
	{
		print "Loading Raster into '${DB}'\n";
		load_sql_file("${STAGED_SCRIPTS_DIR}/rtpostgis.sql", 1);
		load_sql_file("${STAGED_SCRIPTS_DIR}/raster_comments.sql", 0);
	}

	if ( $OPT_WITH_SFCGAL )
	{
		print "Loading SFCGAL into '${DB}'\n";
		load_sql_file("${STAGED_SCRIPTS_DIR}/sfcgal.sql", 1);
		load_sql_file("${STAGED_SCRIPTS_DIR}/sfcgal_comments.sql", 0);
	}

	return 1;
}

# Upgrade an existing database (soft upgrade)
sub upgrade_spatial
{
    print "Upgrading PostGIS in '${DB}' \n" ;

    my $script = `ls ${STAGED_SCRIPTS_DIR}/postgis_upgrade.sql`;
    chomp($script);

    if ( -e $script )
    {
        print "Upgrading core\n";
        load_sql_file($script);
    }
    else
    {
        die "$script not found\n";
    }
    
    if ( $OPT_WITH_TOPO ) 
    {
        my $script = `ls ${STAGED_SCRIPTS_DIR}/topology_upgrade.sql`;
        chomp($script);
        if ( -e $script )
        {
            print "Upgrading topology\n";
            load_sql_file($script);
        }
        else
        {
            die "$script not found\n";
        }
    }
    
    if ( $OPT_WITH_RASTER ) 
    {
        my $script = `ls ${STAGED_SCRIPTS_DIR}/rtpostgis_upgrade.sql`;
        chomp($script);
        if ( -e $script )
        {
            print "Upgrading raster\n";
            load_sql_file($script);
        }
        else
        {
            die "$script not found\n";
        }
    }
    return 1;
}

# Upgrade an existing database (soft upgrade, extension method)
sub upgrade_spatial_extensions
{
    # ON_ERROR_STOP is used by psql to return non-0 on an error
    my $psql_opts = "--no-psqlrc --variable ON_ERROR_STOP=true";
    my $nextver = $OPT_UPGRADE_TO ? "${OPT_UPGRADE_TO}" : "${libver}next";
    my $sql = "ALTER EXTENSION postgis UPDATE TO '${nextver}'";

    print "Upgrading PostGIS in '${DB}' using: ${sql}\n" ;

    my $cmd = "psql $psql_opts -c \"" . $sql . "\" $DB >> $REGRESS_LOG 2>&1";
    #print "CMD: " . $cmd . "\n";
    my $rv = system($cmd);
    if ( $rv ) {
      fail "Error encountered altering EXTENSION POSTGIS", $REGRESS_LOG;
      die;
    }

    if ( $OPT_WITH_TOPO ) 
    {
      my $sql = "ALTER EXTENSION postgis_topology UPDATE TO '${nextver}'";
      my $cmd = "psql $psql_opts -c \"" . $sql . "\" $DB >> $REGRESS_LOG 2>&1";
      my $rv = system($cmd);
      if ( $rv ) {
        fail "Error encountered altering EXTENSION POSTGIS_TOPOLOGY", $REGRESS_LOG;
        die;
      }
    }
    
    return 1;
}

sub drop_spatial
{
	my $ok = 1;

  	if ( $OPT_WITH_TOPO )
	{
		load_sql_file("${STAGED_SCRIPTS_DIR}/uninstall_topology.sql");
	}
	if ( $OPT_WITH_RASTER )
	{
		load_sql_file("${STAGED_SCRIPTS_DIR}/uninstall_rtpostgis.sql");
	}
	if ( $OPT_WITH_SFCGAL )
	{
		load_sql_file("${STAGED_SCRIPTS_DIR}/uninstall_sfcgal.sql");
	}
	load_sql_file("${STAGED_SCRIPTS_DIR}/uninstall_postgis.sql");

  	return 1;
}

sub drop_spatial_extensions
{
    # ON_ERROR_STOP is used by psql to return non-0 on an error
    my $psql_opts="--no-psqlrc --variable ON_ERROR_STOP=true";
    my $ok = 1; 
    my ($cmd, $rv);
    
    if ( $OPT_WITH_TOPO )
    {
        # NOTE: "manually" dropping topology schema as EXTENSION does not
        #       take care of that itself, see
        #       http://trac.osgeo.org/postgis/ticket/2138
        $cmd = "psql $psql_opts -c \"DROP EXTENSION postgis_topology; DROP SCHEMA topology;\" $DB >> $REGRESS_LOG 2>&1";
        $rv = system($cmd);
      	$ok = 0 if $rv;
    }

    if ( $OPT_WITH_SFCGAL )
    {
        $cmd = "psql $psql_opts -c \"DROP EXTENSION postgis_sfcgal;\" $DB >> $REGRESS_LOG 2>&1";
        $rv = system($cmd);
        $ok = 0 if $rv;
    }
    
    $cmd = "psql $psql_opts -c \"DROP EXTENSION postgis\" $DB >> $REGRESS_LOG 2>&1";
    $rv = system($cmd);
  	die "\nError encountered dropping EXTENSION POSTGIS, see $REGRESS_LOG for details\n\n"
  	    if $rv;

    return $ok;
}

# Drop spatial from an existing database
sub uninstall_spatial
{
	my $ok;
	start_test("uninstall");
	
	if ( $OPT_EXTENSIONS )
	{	
		$ok = drop_spatial_extensions();
	}
	else
	{
		$ok = drop_spatial();
	}

	if ( $ok ) 
	{
		show_progress(); # on to objects count
		$OBJ_COUNT_POST = count_db_objects();
		
		if ( $OBJ_COUNT_POST != $OBJ_COUNT_PRE )
		{
			fail("Object count pre-install ($OBJ_COUNT_PRE) != post-uninstall ($OBJ_COUNT_POST)");
			return 0;
		}
		else
		{
			pass("($OBJ_COUNT_PRE)");
			return 1;
		}
	}
	
	return 0;
}  

sub diff
{
	my ($expected_file, $obtained_file) = @_;
	my $diffstr = '';

	if ( $sysdiff ) {
		$diffstr = `diff --strip-trailing-cr -u $expected_file $obtained_file 2>&1`;
		return $diffstr;
	}

	open(OBT, $obtained_file) || return "Cannot open $obtained_file\n";
	open(EXP, $expected_file) || return "Cannot open $expected_file\n";
	my $lineno = 0;
	while (!eof(OBT) or !eof(EXP)) {
		# TODO: check for premature end of one or the other ?
		my $obtline=<OBT>;
		my $expline=<EXP>;
		$obtline =~ s/\r?\n$//; # Strip line endings
		$expline =~ s/\r?\n$//; # Strip line endings
		$lineno++;
		if ( $obtline ne $expline ) {
			my $diffln .= "$lineno.OBT: $obtline\n";
			$diffln .= "$lineno.EXP: $expline\n";
			$diffstr .= $diffln;
		}
	}
	close(OBT);
	close(EXP);
	return $diffstr;
}
