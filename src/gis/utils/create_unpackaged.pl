#!/usr/bin/perl

#
# PostGIS - Spatial Types for PostgreSQL
# http://postgis.net
#
# Copyright (C) 2013 Sandro Santilli <strk@keybit.net>
#
# This is free software; you can redistribute and/or modify it under
# the terms of the GNU General Public Licence. See the COPYING file.
#

use warnings;
use strict;
use POSIX 'strftime';

eval "exec perl -w $0 $@"
	if (0);


die "Usage: perl $0 <extname> [<sql>]\n"
  . "  Prints SQL to add objects created by given SQL\n"
  . "  to extension with given name.\n"
unless @ARGV;

my $extname = shift(@ARGV);

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
my @ops = ();
my @opcs = ();
my @views = ();
my @tables = ();
my @schemas = ();

sub strip_default {
	my $line = shift;
	# strip quotes first
	$line =~ s/'[^']*'//ig;
	# drop default then
	$line =~ s/DEFAULT [^,)]*//ig;
	return $line;
}

while( my $line = <>)
{
	if ($line =~ /^create (or replace )?function/i) {
		my $defn = $line;
		while( not $defn =~ /\)/ ) {
			$defn .= <>;
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
			$line .= <>;
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
			$defn .= <>;
		}
		push (@ops, $defn)
	}
	elsif ($line =~ /^create aggregate/i) {
		my $defn = $line;
		while( not $defn =~ /;\s*$/ ) {
			$defn .= <>;
		}
		push (@aggs, $defn)
	}
	elsif ($line =~ /^create type ([\w\.]+)/i) {
		push (@types, $1);
		while( not $line =~ /;\s*$/ ) {
			$line = <>;
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

#close( INPUT );

my $addprefix = "ALTER EXTENSION $extname ADD";

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
print "--          for: " . $extname . "\n";
print "--         from: " . ( @ARGV ? $ARGV[0] : '-' ) . "\n";
print "-- \n";
print "-- Do not edit manually, your changes will be lost.\n";
print "-- \n";
print "-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --\n";
print "\n";

print "-- complain if script is sourced in psql\n";
print '\echo Use "CREATE EXTENSION ' . ${extname} .
      '" to load this file. \quit';
print "\n\n";

print "-- Register all views.\n";
foreach my $view (@views)
{
	print "$addprefix VIEW $view;\n";
}

print "-- Register all tables.\n";
# we reverse table definitions so foreign key constraints
# are more likely not to get in our way
@tables = reverse(@tables);
foreach my $table (@tables)
{
	print "$addprefix TABLE $table;\n";
}


print "-- Register all aggregates.\n";
foreach my $agg (@aggs)
{
	if ( $agg =~ /create aggregate\s*([\w\.]+)\s*\(\s*.*basetype = ([\w\.]+)/ism )
	{
		print "$addprefix AGGREGATE $1 ($2);\n";
	}
	elsif ( $agg =~ /create aggregate\s*([\w\.]+)\s*\(\s*([\w,\.\s\[\]]+)\s*\)/ism )
	{
		print "$addprefix AGGREGATE $1 ($2);\n";
	}
	else 
	{
		die "Couldn't parse AGGREGATE line: $agg\n";
	}
}

print "-- Register all operators classes and families.\n";
foreach my $opc (@opcs)
{
	print "$addprefix OPERATOR CLASS $opc;\n";
	print "$addprefix OPERATOR FAMILY $opc;\n";
}

print "-- Register all operators.\n";
foreach my $op (@ops)
{
	if ($op =~ /create operator ([^(]+)\s*\(.*LEFTARG\s*=\s*(\w+),\s*RIGHTARG\s*=\s*(\w+).*/ism )
	{
		print "$addprefix OPERATOR $1 ($2,$3);\n";
	}
	else
	{
		die "Couldn't parse OPERATOR line: $op\n";
	}
}

	
print "-- Register all casts.\n";
foreach my $cast (@casts)
{
	if ($cast =~ /create cast\s*\((.+?)\)/i )
	{
		print "$addprefix CAST ($1);\n";
	}
	else
	{
		die "Couldn't parse CAST line: $cast\n";
	}
}

print "-- Register all functions except " . (keys %type_funcs) . " needed for type definition.\n";
my @type_funcs= (); # function to drop _after_ type drop
foreach my $fn (@funcs)
{
	if ($fn =~ /.* function ([^(]+)\((.*)\)/is ) # can be multiline
	{
		my $fn_nm = $1;
		my $fn_arg = $2;

		$fn_arg = strip_default($fn_arg);
		if ( ! exists($type_funcs{$fn_nm}) )
		{
			print "$addprefix FUNCTION $fn_nm ($fn_arg);\n";
		} 
		else
		{
			push(@type_funcs, $fn);
		}
	}
	else
	{
		die "Couldn't parse FUNCTION line: $fn\n";
	}
}

print "-- Add all functions needed for types definition (needed?).\n";
foreach my $fn (@type_funcs)
{
	if ($fn =~ /.* function ([^(]+)\((.*)\)/i )
	{
		my $fn_nm = $1;
		my $fn_arg = $2;

		$fn_arg =~ s/DEFAULT [\w']+//ig;

		print "$addprefix FUNCTION $fn_nm ($fn_arg);\n";
	}
	else
	{
		die "Couldn't parse line: $fn\n";
	}
}

print "-- Register all types.\n";
foreach my $type (@types)
{
	print "$addprefix TYPE $type;\n";
}


# NOTE:  cannot add schema "topology" to extension "postgis_topology"
#        because the schema contains the extension
#
#print "-- Register all schemas.\n";
#if (@schemas)
#{
#  foreach my $schema (@schemas)
#  {
#    print "$addprefix SCHEMA \"$schema\";\n";
#  }
#}


print "\n";

1;
