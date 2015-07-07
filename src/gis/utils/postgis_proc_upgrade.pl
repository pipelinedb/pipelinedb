#!/usr/bin/perl -w

#
# PostGIS - Spatial Types for PostgreSQL
# http://postgis.net
#
# Copyright (C) 2014 Sandro Santilli <strk@keybit.net>
# Copyright (C) 2009-2010 Paul Ramsey <pramsey@opengeo.org>
# Copyright (C) 2005 Refractions Research Inc.
#
# This is free software; you can redistribute and/or modify it under
# the terms of the GNU General Public Licence. See the COPYING file.
#

#
# This script produces an .sql file containing
# CREATE OR REPLACE calls for each function
# in postgis.sql
#
# In addition, the transaction contains
# a check for Major postgis_lib_version() 
# to match the one contained in lwpostgis.sql
#
# This never happens by just running make install
# as MODULE_FILENAME contains SO_MAJOR under
# all architectures.
#
#

eval "exec perl -w $0 $@"
	if (0);

use strict;
use warnings;

#
# Conditionally upgraded types and operators. Only include these
# if the major numbers in version_from are less than the version_to
# number.
#
# TODO: move configuration outside of code
#
my $objs = {
 	"102" => { 
		"aggregates" => {
			"st_extent(geometry)" => 1,
			"st_memcollect(geometry)" => 1,
			"st_memunion(geometry)" => 1,
			"st_accum(geometry)" => 1,
			"st_union(geometry)" => 1,
			"st_collect(geometry)" => 1,
			"st_polygonize(geometry)" => 1,
			"st_makeline(geometry)" => 1
		}
	},
 	"105" => { 
		"views" => {
			"geography_columns" => 1
		},
	},
 	"200" => { 
		"aggregates" => {
			"st_3dextent(geometry)" => 1,
      "topology.topoelementarray_agg(topology.topoelement)" => 1
		}
	},
 	"201" => { 
		"aggregates" => {
			"st_samealignment(raster)" => 1,
			"st_union(raster,unionarg[])" => 1,
			"st_union(raster,integer,text)" => 1,
			"st_union(raster,integer)" => 1,
			"st_union(raster)" => 1,
			"st_union(raster,text)" => 1
		}
	},
 	"202" => { 
		"aggregates" => {
			"st_summarystatsagg(raster,integer,boolean,double precision)" => 1,
			"st_summarystatsagg(raster,boolean,double precision)" => 1,
			"st_summarystatsagg(raster,integer)" => 1,
			"st_countagg(raster,integer,boolean,double precision)" => 1,
			"st_countagg(raster,integer,boolean)" => 1,
			"st_countagg(raster,boolean)" => 1
		}
	}
};

sub find_last_updated
{
  my $type = shift;
  my $sig = shift;
  for my $ver ( sort { $b cmp $a } keys %$objs ) {
    if ( $objs->{$ver}->{$type}->{$sig} ) {
      return $ver;
    }
  }
  return 0;
}

sub parse_last_updated
{
  my $comment = shift;
  if ( $comment =~ m/.*(?:Availability|Changed|Updated):\s([^\.])\.([^.]*)/s ) {
    return $1*100 + $2;
  }
  return 0;
}

sub parse_missing
{
  my $comment = shift;
  my @missing = ();
  if ( $comment =~ m/.*(?:Missing in):\s([^\.])\.([^.]*)/s ) {
    push(@missing, $1*100 + $2);
  }
  return join(',',@missing);
}


#
# Commandline argument handling
#
($#ARGV == 0) ||
die "Usage: perl postgis_proc_upgrade.pl <postgis.sql> <version_from> [<schema>]\nCreates a new SQL script to upgrade all of the PostGIS functions.\n"
	if ( @ARGV < 1 || @ARGV > 3 );

my $sql_file = $ARGV[0];
my $module = 'postgis';
my $soname = '';
my $version_to = "";
my $version_to_num = 0;
my $version_from = $ARGV[1];
my $version_from_num = 0;
my $schema = "";
$schema = $ARGV[2] if @ARGV > 2;

die "Unable to open input SQL file $sql_file\n"
	if ( ! -f $sql_file );

#
# Search the SQL file for the target version number (the 
# version we are upgrading *to*.
#
open( INPUT, $sql_file ) || die "Couldn't open file: $sql_file\n";
while(<INPUT>)
{
	#
	# Since 1.1.0 scripts/lib/release versions are the same
	#
	if (/INSTALL VERSION: (.*)/)
	{
				$version_to = $1;
				#last;
	}
	elsif (/TYPE raster/)
	{
        $module = 'postgis_raster';
	}
	elsif (m@('\$libdir/[^']*')@)
	{
        $soname = $1;
	}
}
close(INPUT); 

die "Unable to locate target new version number in $sql_file\n"
 	if( ! $version_to );

if ( $version_to =~ /(\d+)\.(\d+)\..*/ )
{
	$version_to = $1 . "." . $2;
	$version_to_num = 100 * $1 + $2; 
}
else
{
	die "Version to number invalid, must be of form X.X.X\n";
}

print qq{
--
-- UPGRADE SCRIPT TO PostGIS $version_to
--

};

print "LOAD $soname;\n" if ($soname);

print "BEGIN;\n";
print "SET search_path TO $schema;\n" if $schema;

#
# Add in the conditional check function to ensure this script is
# not being applied to a major version update.
#
while(<DATA>)
{
	s/NEWVERSION/$version_to/g;
  s/MODULE/$module/g;
	print;
}

#
# Go through the SQL file and strip out objects that cannot be 
# applied to an existing, loaded database: types and operators
# and operator classes that have already been defined.
#
my $comment = '';
open( INPUT, $sql_file ) || die "Couldn't open file: $sql_file\n";
while(<INPUT>)
{

	if ( /^\-\-/ ) {
		$comment .= $_;
		next;
	}

	#
	# Allow through deprecations from postgis_drop.sql
	#
	print if ( /^drop function /i );
	print if ( /^drop aggregate /i );

	if ( /^create or replace function/i )
	{
		print $_;
		my $endfunc = 0;
		while(<INPUT>)
		{
			print $_;
			$endfunc = 1 if /^\s*(\$\$\s*)?LANGUAGE /;
			last if ( $endfunc && /\;/ );
		}
	}

	if ( /^create type (\w+)/i )
	{
		my $newtype = $1;
		my $def .= $_;
		while(<INPUT>)
		{
			$def .= $_;
			last if /\)/;
		}

    my $last_updated = parse_last_updated($comment);
    if ( ! $last_updated ) {
      print STDERR "WARNING: no last updated info for type '${newtype}'\n";
      $last_updated = find_last_updated("types", $newtype);
    }
    my $missing = parse_missing($comment);
    print "-- Type ${newtype} -- LastUpdated: ${last_updated}\n";
      print <<"EOF";
DO LANGUAGE 'plpgsql'
\$postgis_proc_upgrade\$
BEGIN
  IF $last_updated > version_from_num
EOF
      print "OR version_from_num IN ( ${missing} )" if ( $missing );
      print <<"EOF";
     FROM _postgis_upgrade_info
  THEN
      EXECUTE \$postgis_proc_upgrade_parsed_def\$ $def \$postgis_proc_upgrade_parsed_def\$;
  END IF;
END
\$postgis_proc_upgrade\$;
EOF
	}

	if ( /^do *language .*\$\$/i )
	{
		print;
		while(<INPUT>)
		{
			print;
			last if /\$\$/;
		}
	}

	# This code handles casts by dropping and recreating them.
	if ( /^create cast\s+\(\s*(\w+)\s+as\s+(\w+)\)/i )
	{
		my $type1 = $1;
		my $type2 = $2;
		my $def = $_;
    unless (/;$/) { 
      while(<INPUT>) {
        $def .= $_;
        last if /;$/;
      }
    }
		print "DROP CAST IF EXISTS ($type1 AS $type2);\n";
		print $def;
	}

	# This code handles aggregates by dropping and recreating them.
	if ( /^create aggregate\s+([^(]+)\s*\(/i )
	{
		my $aggname = $1;
    #print "-- Aggname ${aggname}\n";
		my $aggtype = 'unknown';
		my $def = $_;
    if ( /^create aggregate\s+\S+\s*\(([^)]*)\)/i ) {
	    $aggtype = $1;
      $aggtype =~ s/\s*,\s*/,/g; # drop spaces around commas
      $aggtype =~ s/\s\s*/ /g; # collapse multiple spaces into one
    }
		while(<INPUT>)
		{
			$def .= $_;
			$aggtype = $1 if ( /basetype\s*=\s*([^,]*)\s*,/i );
			last if /\);/;
		}
		my $aggsig = "$aggname($aggtype)";

    #print "-- Checking comment $comment\n";
    my $last_updated = parse_last_updated($comment);
    if ( ! $last_updated ) {
      print STDERR "WARNING: no last updated info for aggregate '${aggsig}'\n";
      $last_updated = find_last_updated("aggregates", $aggsig);
    }
    print "-- Aggregate ${aggsig} -- LastUpdated: ${last_updated}\n";
      print <<"EOF";
DO LANGUAGE 'plpgsql'
\$postgis_proc_upgrade\$
BEGIN
  IF $last_updated > version_from_num OR (
      $last_updated = version_from_num AND version_from_isdev
    ) FROM _postgis_upgrade_info
  THEN
    EXECUTE 'DROP AGGREGATE IF EXISTS $aggsig';
    EXECUTE \$postgis_proc_upgrade_parsed_def\$ $def \$postgis_proc_upgrade_parsed_def\$;
  END IF;
END
\$postgis_proc_upgrade\$;
EOF
	}
	
	# This code handles operators by creating them if we are doing a major upgrade
	if ( /^create operator\s+(\S+)\s*\(/i )
	{
		my $opname = $1;
		my $optype = 'unknown';
		my $def = $_;
		while(<INPUT>)
		{
			$def .= $_;
			$optype = $1 if ( /leftarg\s*=\s*(\w+)\s*,/i );
			last if /\);/;
		}
		my $opsig = $optype . " " . $opname;

    my $last_updated = parse_last_updated($comment);
    if ( ! $last_updated ) {
      print STDERR "WARNING: no last updated info for operator '${opsig}'\n";
      $last_updated = find_last_updated("operators", $opsig);
    }
    print "-- Operator ${opsig} -- LastUpdated: ${last_updated}\n";
      print <<"EOF";
DO LANGUAGE 'plpgsql'
\$postgis_proc_upgrade\$
BEGIN
  IF $last_updated > version_from_num FROM _postgis_upgrade_info THEN
    EXECUTE \$postgis_proc_upgrade_parsed_def\$ $def \$postgis_proc_upgrade_parsed_def\$;
  END IF;
END
\$postgis_proc_upgrade\$;
EOF
	}

	# Always output create ore replace view (see ticket #1097)
	if ( /^create or replace view\s+(\S+)\s*/i )
	{
		print;
		while(<INPUT>)
		{
			print;
			last if /\;\s*$/;
		}
	}

	# Always output create ore replace rule 
	if ( /^create or replace rule\s+(\S+)\s*/i )
	{
		print;
		while(<INPUT>)
		{
			print;
			last if /\;\s*$/;
		}
	}

	# This code handles operator classes by creating them if we are doing a major upgrade
	if ( /^create operator class\s+(\w+)\s*/i )
	{
		my $opclassname = $1;
		my $opctype = 'unknown';
		my $opcidx = 'unknown';
		my $def = $_;
		my $last_updated;
		my $subcomment = '';
		my @subobjects; # minversion, definition
		while(<INPUT>)
		{
			if ( /^\s*\-\-/ ) {
				$subcomment .= $_;
				next;
			}

			$def .= $_;
			$opctype = $1 if ( /for type (\w+) /i );
			$opcidx = $1 if ( /using (\w+) /i );

			# Support adding members at later versions
			if ( /\s+(OPERATOR|FUNCTION)\s+[0-9]+\s+ / )
			{
				my $last_updated = parse_last_updated($subcomment);
				if ( $last_updated )
				{
					my $subdefn = $_;
					chop $subdefn;
					$subdefn =~ s/[,;]$//; # strip ending comma or semicolon
					# argument types must be specified in ALTER OPERATOR FAMILY
					if ( $subdefn =~ m/\s+(OPERATOR.*)(FOR.*)/ )
					{
						$subdefn = $1.'('.$opctype.','.$opctype.') '.$2;
					}
					elsif ( $subdefn =~ m/\s+(FUNCTION\s+[0-9]+ )(.*)/ )
					{
						$subdefn = $1.'('.$opctype.','.$opctype.') '.$2;
					}
					push @subobjects, [$last_updated, $subdefn];
				}
				$subcomment = '';
			}
			last if /\);/;
		}
		$opctype =~ tr/A-Z/a-z/;
		$opcidx =~ tr/A-Z/a-z/;

    $last_updated = parse_last_updated($comment);
    if ( ! $last_updated ) {
      print STDERR "WARNING: no last updated info for operator class '${opclassname}'\n";
      $last_updated = find_last_updated("opclasses", $opclassname);
    }
    print "-- Operator class ${opclassname} -- LastUpdated: ${last_updated}\n";
    print <<"EOF";
DO LANGUAGE 'plpgsql'
\$postgis_proc_upgrade\$
BEGIN
  IF $last_updated > version_from_num FROM _postgis_upgrade_info THEN
    EXECUTE \$postgis_proc_upgrade_parsed_def\$
    $def    \$postgis_proc_upgrade_parsed_def\$;
EOF
    my $ELSE="ELSE -- version_from >= $last_updated";
    for my $subobj ( @subobjects )
    {
      $last_updated = @{$subobj}[0];
      $def = @{$subobj}[1];
      print <<"EOF";
  $ELSE
    -- Last Updated: ${last_updated}
    IF $last_updated > version_from_num FROM _postgis_upgrade_info THEN
      EXECUTE \$postgis_proc_upgrade_parsed_def\$
        ALTER OPERATOR FAMILY ${opclassname} USING ${opcidx}
          ADD $def;
      \$postgis_proc_upgrade_parsed_def\$;
    END IF;
EOF
      $ELSE="";
    }
    print <<"EOF";
  END IF; -- version_from >= $last_updated
END
\$postgis_proc_upgrade\$;
EOF
	}

	$comment = '';
}

close( INPUT );

print "DROP TABLE _postgis_upgrade_info;\n";

print "COMMIT;\n";

1;

__END__

CREATE OR REPLACE FUNCTION postgis_major_version_check()
RETURNS text
AS '
DECLARE
	old_scripts text;
	new_scripts text;
	old_maj text;
	new_maj text;
BEGIN
	--
	-- This uses postgis_lib_version() rather then
	-- MODULE_scripts_installed() as in 1.0 because
	-- in the 1.0 => 1.1 transition that would result
	-- in an impossible upgrade:
	--
	--   from 0.3.0 to 1.1.0
	--
	-- Next releases will still be ok as
	-- postgis_lib_version() and MODULE_scripts_installed()
	-- would both return actual PostGIS release number.
	-- 
	BEGIN
		SELECT into old_scripts MODULE_lib_version();
	EXCEPTION WHEN OTHERS THEN
		RAISE DEBUG ''Got %'', SQLERRM;
		SELECT into old_scripts MODULE_scripts_installed();
	END;
	SELECT into new_scripts ''NEWVERSION'';
	SELECT into old_maj substring(old_scripts from 1 for 2);
	SELECT into new_maj substring(new_scripts from 1 for 2);

	IF old_maj != new_maj THEN
		RAISE EXCEPTION ''Upgrade of MODULE from version % to version % requires a dump/reload. See PostGIS manual for instructions'', old_scripts, new_scripts;
	ELSE
		RETURN ''Scripts versions checked for upgrade: ok'';
	END IF;
END
'
LANGUAGE 'plpgsql';

SELECT postgis_major_version_check();

DROP FUNCTION postgis_major_version_check();

CREATE TEMPORARY TABLE _postgis_upgrade_info AS WITH versions AS (
  SELECT 'NEWVERSION'::text as upgraded,
  MODULE_scripts_installed() as installed
) SELECT
  upgraded as scripts_upgraded,
  installed as scripts_installed,
  substring(upgraded from '([0-9]*)\.')::int * 100 +
  substring(upgraded from '[0-9]*\.([0-9]*)\.')::int
    as version_to_num,
  substring(installed from '([0-9]*)\.')::int * 100 +
  substring(installed from '[0-9]*\.([0-9]*)\.')::int
    as version_from_num,
  position('dev' in  installed)::bool
    as version_from_isdev
  FROM versions
;
 
