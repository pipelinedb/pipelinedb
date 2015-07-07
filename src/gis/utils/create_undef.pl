#!/usr/bin/perl

#
# PostGIS - Spatial Types for PostgreSQL
# http://postgis.net
#
# Copyright (C) 2011 OpenGeo.org
# Copyright (C) 2009-2010 Paul Ramsey <pramsey@opengeo.org>
# Copyright (C) 2001-2005 Refractions Research Inc.
#
# This is free software; you can redistribute and/or modify it under
# the terms of the GNU General Public Licence. See the COPYING file.
#

use warnings;
use strict;
use POSIX 'strftime';

eval "exec perl -w $0 $@"
	if (0);


($#ARGV == 1) || die "Usage: perl create_undef.pl <postgis.sql> <pgsql_version #>\nCreates a new SQL script to delete all the PostGIS functions.\n";

# drops are in the following order:
#	1. Indexing system stuff
#	2. Meta datatables <not done>
#	3. Aggregates 
#	3. Casts
#	4. Operators 
#	5. Functions
#	6. Types
#	7. Tables

my @aggs = ();
my @casts = ();
my @funcs = ();
my @types = ();
my %type_funcs = ();
my @type_funcs= (); # function to drop _after_ type drop
my @ops = ();
my @opcs = ();
my @views = ();
my @tables = ();
my @schemas = ();

my $version = $ARGV[1];

sub strip_default {
	my $line = shift;
	# strip quotes first
	$line =~ s/'[^']*'//ig;
	# drop default then
	$line =~ s/DEFAULT [^,)]*//ig;
	return $line;
}

my $time = POSIX::strftime("%c", localtime);
print "-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --\n";
print "-- \n";
print "-- PostGIS - Spatial Types for PostgreSQL \n";
print "-- http://postgis.net \n";
print "-- \n";
print "-- This is free software; you can redistribute and/or modify it under \n";
print "-- the terms of the GNU General Public Licence. See the COPYING file. \n";
print "-- \n";
print "-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --\n";
print "-- \n";
print "-- Generated on: " . $time . "\n";
print "--           by: " . $0 . "\n";
print "--         from: " . $ARGV[0] . "\n";
print "-- \n";
print "-- Do not edit manually, your changes will be lost.\n";
print "-- \n";
print "-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --\n";
print "\n";

print "BEGIN;\n\n";

open( INPUT, $ARGV[0] ) || die "Couldn't open file: $ARGV[0]\n";

while( my $line = <INPUT>)
{
	if ($line =~ /^create (or replace )?function/i) {
		my $defn = $line;
		while( not $defn =~ /\)/ ) {
			$defn .= <INPUT>;
		}
		push (@funcs, $defn)
	}
	elsif ($line =~ /^create or replace view\s*(\w+)/i) {
		push (@views, $1);
	}
	elsif ($line =~ /^create table \s*([\w\.]+)/i) {
		push (@tables, $1);
	}
	elsif ($line =~ /^create schema \s*([\w\.]+)/i) {
		push (@schemas, $1);
	}
	elsif ( $line =~ /^create operator class (\w+)/i ) {
		my $opcname = $1;
		my $am = '';
		while( not $line =~ /;\s*$/ ) {
			if ( $line =~ /( USING (\w+))/ ) {
				$am = $1;
				last;
			}
			$line .= <INPUT>;
		}
		if ( $am eq '' ) {
			die "Couldn't parse CREATE OPERATOR CLASS $opcname\n";
		} else {
			$opcname .= $am;
		}
		push (@opcs, $opcname)
	}
	elsif ($line =~ /^create operator.*\(/i) {
		my $defn = $line;
		while( not $defn =~ /;\s*$/ ) {
			$defn .= <INPUT>;
		}
		push (@ops, $defn)
	}
	elsif ($line =~ /^create aggregate/i) {
		my $defn = $line;
		while( not $defn =~ /;\s*$/ ) {
			$defn .= <INPUT>;
		}
		push (@aggs, $defn)
	}
	elsif ($line =~ /^create type ([\w\.]+)/i) {
		push (@types, $1);
		while( not $line =~ /;\s*$/ ) {
			$line = <INPUT>;
			if ( $line =~ /(input|output|send|receive|typmod_in|typmod_out|analyze)\s*=\s*(\w+)/ ) {
        my $role = ${1};
        my $fname = ${2};
				$type_funcs{$fname} = $role;
			}
		}
	}
	elsif ($line =~ /^create domain ([\w\.]+)/i) {
		push (@types, $1);
	}
	elsif ($line =~ /^create cast/i) {
		push (@casts, $line)
	}
}

close( INPUT );

print "-- Drop all views.\n";
foreach my $view (@views)
{
	print "DROP VIEW IF EXISTS $view;\n";
}

print "-- Drop all tables.\n";
# we reverse table definitions so foreign key constraints
# are more likely not to get in our way
@tables = reverse(@tables);
foreach my $table (@tables)
{
	print "DROP TABLE $table;\n";
}


print "-- Drop all aggregates.\n";
foreach my $agg (@aggs)
{
	if ( $agg =~ /create aggregate\s*([\w\.]+)\s*\(\s*.*basetype = ([\w\.]+)/ism )
	{
		print "DROP AGGREGATE IF EXISTS $1 ($2);\n";
	}
	elsif ( $agg =~ /create aggregate\s*([\w\.]+)\s*\(\s*([\w,\.\s\[\]]+)\s*\)/ism )
	{
		print "DROP AGGREGATE IF EXISTS $1 ($2);\n";
	}
	else 
	{
		die "Couldn't parse AGGREGATE line: $agg\n";
	}
}

print "-- Drop all operators classes and families.\n";
foreach my $opc (@opcs)
{
	print "DROP OPERATOR CLASS $opc;\n";
	print "DROP OPERATOR FAMILY $opc;\n";
}

print "-- Drop all operators.\n";
foreach my $op (@ops)
{
	if ($op =~ /create operator ([^(]+)\s*\(.*LEFTARG\s*=\s*(\w+),\s*RIGHTARG\s*=\s*(\w+).*/ism )
	{
		print "DROP OPERATOR $1 ($2,$3) CASCADE;\n";
	}
	else
	{
		die "Couldn't parse OPERATOR line: $op\n";
	}
}

	
print "-- Drop all casts.\n";
foreach my $cast (@casts)
{
	if ($cast =~ /create cast\s*\((.+?)\)/i )
	{
		print "DROP CAST ($1);\n";
	}
	else
	{
		die "Couldn't parse CAST line: $cast\n";
	}
}

print "-- Drop all functions except " . (keys %type_funcs) . " needed for type definition.\n";

foreach my $fn (@funcs)
{
	if ($fn =~ /.* function ([^(]+)\((.*)\)/is ) # can be multiline
	{
		my $fn_nm = $1;
		my $fn_arg = $2;
		$fn_arg =~ s/\-\-.*\n//g;
		$fn_arg =~ s/\n//g;

		$fn_arg = strip_default($fn_arg);
		if ( ! exists($type_funcs{$fn_nm}) )
		{
			print "DROP FUNCTION IF EXISTS $fn_nm ($fn_arg);\n";
		} 
		else
		{
			if ( $type_funcs{$fn_nm} =~ /(typmod|analyze)/ ) {
				push(@type_funcs, $fn);
			}
		}
	}
	else
	{
		die "Couldn't parse FUNCTION line: $fn\n";
	}
}


print "-- Drop all types.\n";
foreach my $type (@types)
{
	print "DROP TYPE $type CASCADE;\n";
}

print "-- Drop all functions needed for types definition.\n";
foreach my $fn (@type_funcs)
{
	if ($fn =~ /.* function ([^(]+)\((.*)\)/i )
	{
		my $fn_nm = $1;
		my $fn_arg = $2;

		$fn_arg =~ s/DEFAULT [\w']+//ig;

		print "DROP FUNCTION IF EXISTS $fn_nm ($fn_arg);\n";
	}
	else
	{
		die "Couldn't parse line: $fn\n";
	}
}

print "-- Drop all schemas.\n";
if (@schemas)
{
  print <DATA>;
  foreach my $schema (@schemas)
  {
    print "SELECT undef_helper.StripFromSearchPath('$schema');\n";
    print "DROP SCHEMA \"$schema\";\n";
  }
  print "DROP SCHEMA undef_helper CASCADE;\n";
}


print "\n";

print "COMMIT;\n";

1;

__END__
create schema undef_helper;
--{
--  StripFromSearchPath(schema_name)
--
-- Strips the specified schema from the database search path
-- 
-- This is a helper function for uninstall
-- We may want to move this function as a generic helper
--
CREATE OR REPLACE FUNCTION undef_helper.StripFromSearchPath(a_schema_name varchar)
RETURNS text
AS
$$
DECLARE
	var_result text;
	var_search_path text;
BEGIN
	SELECT reset_val INTO var_search_path FROM pg_settings WHERE name = 'search_path';
	IF var_search_path NOT LIKE '%' || quote_ident(a_schema_name) || '%' THEN
		var_result := a_schema_name || ' not in database search_path';
	ELSE
    var_search_path := btrim( regexp_replace(
        replace(var_search_path, a_schema_name, ''), ', *,', ','),
        ', ');
    RAISE NOTICE 'New search_path: %', var_search_path;
		EXECUTE 'ALTER DATABASE ' || quote_ident(current_database()) || ' SET search_path = ' || var_search_path;
		var_result := a_schema_name || ' has been stripped off database search_path ';
	END IF;
  
  RETURN var_result;
END
$$
LANGUAGE 'plpgsql' VOLATILE STRICT;

--} StripFromSearchPath
