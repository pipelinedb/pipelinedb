#!/usr/bin/perl -w
use strict;

sub Usage {
    print "Usage: mk-sql.pl pgver file-in.sql\n";
    exit 1;
}

my $ver = shift @ARGV || Usage();
my $fin = shift @ARGV || Usage();

my $nver;

if ($ver =~ /^PostgreSQL (\d+)\.(\d+)/) {
    $nver = $1 * 100 + $2;
}
else {
    die "Failed to parse '$ver' as /^PostgreSQL (\\d+)\\.(\\d+)/\n/\n";
}

open(IN, $fin) || die "Failed to open file '$fin' : $!\n";
while (my $x = <IN>) {
    if ($nver >= 804) {
        $x =~ s/\$libdir\/lib/\$libdir\//;
    }
    if ($nver < 901) {
        $x =~ s/^\\echo/--\\echo/;
    }
    print $x;
}
close(IN);
