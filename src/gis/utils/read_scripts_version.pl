#!/usr/bin/perl

my $debug = 0;

my @files = ( 
	"postgis.sql.in.c",
	"geography.sql.in.c",
	"long_xact.sql.in.c" 
	);

my $rev = 0;

foreach $f (@files)
{
	my $file = "./postgis/$f";
	if( -f $file )
	{
		my $r = 0;
		open(F, $file);
		while(<F>)
		{
            $r = $1 if /\$Id: \S+ (\d+) /;
		}
		print "$f got revision $r\n" if $debug && $r;
  		$rev = $r if $r > $rev; 
	}
	else 
	{
		die "Could not open input file $f\n";
	}
}

print "\nMaximum scripts revision: $rev\n\n" if $debug;

print $rev if ! $debug;


