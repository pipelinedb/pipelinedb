#!/usr/bin/perl

$ENV{"LC_ALL"} = "C";

use warnings;
use strict;

my $top_srcdir = ".";
my $rev_file = $top_srcdir.'/postgis_svn_revision.h';

my $target = 'local';
$target = $ARGV[0] if $ARGV[0];

# Read the svn revision number
my $svn_rev = &read_rev($target);

# Write it 
&write_defn($svn_rev);


sub read_rev {

  my $target = shift;

  #print STDERR "Target: $target\n";

  my $svn_info;

  if ( $target eq "local" ) {
    if ( -d $top_srcdir."/.svn" ) {
      #print STDERR "There's a ". $top_srcdir."/.svn dir\n";
      $svn_info  = &read_rev_svn($target);
    } elsif ( -d $top_srcdir."/.git" ) {
      #print STDERR "There's a ". $top_srcdir."/.git dir\n";
      $svn_info  = &read_rev_git();
    } else {
      print STDERR "Can't fetch local revision (neither .svn nor .git found)\n";
      $svn_info = 0;
    }
  } else {
    $svn_info  = &read_rev_svn($target);
  }

  return $svn_info;

}

sub read_rev_git {

  # TODO: test on old systems, I think I saw some `which`
  #       implementations returning "nothing found" or something
  #       like that, making the later if ( ! $svn_exe ) always false
  #
  my $git_exe = `which git`;
  if ( ! $git_exe ) {
    print STDERR "Can't fetch SVN revision: no git executable found\n";
    return 0;
  }
  chop($git_exe);

  my $cmd = "\"${git_exe}\" log --grep=git-svn -1 | grep git-svn | cut -d@ -f2 | cut -d' ' -f1";
  #print STDERR "cmd: ${cmd}\n";
  my $rev  = `$cmd`;

  if ( ! $rev ) {
    print STDERR "Can't fetch SVN revision from git log\n";
    $rev = 0;
  } else {
    chop($rev);
  }

  return $rev;
}

sub read_rev_svn {

  my $target = shift;

  # TODO: test on old systems, I think I saw some `which`
  #       implementations returning "nothing found" or something
  #       like that, making the later if ( ! $svn_exe ) always false
  #
  my $svn_exe = `which svn`;
  if ( ! $svn_exe ) {
    print STDERR "Can't fetch SVN revision: no svn executable found\n";
    return 0;
  }
  chop($svn_exe);


  my $svn_info;
  if ( $target eq "local" ) {
    $svn_info  = `"${svn_exe}" info`;
  } else {
    $svn_info  = `"${svn_exe}" info $target`;
  }

  my $rev;
  if ( $svn_info =~ /Last Changed Rev: (\d+)/ ) {
    $rev = $1;
  } else {
    print STDERR "Can't fetch SVN revision: no 'Loast Changed Rev' in `svn info` output\n";
    $rev = 0;
  }

  return $rev;
}

sub write_defn {
  my $rev = shift;
  my $oldrev = 0;

  # Do not override the file if new detected
  # revision isn't zero nor different from the existing one
  if ( -f $rev_file ) {
    open(IN, "<$rev_file");
    my $oldrevline = <IN>;
    if ( $oldrevline =~ /POSTGIS_SVN_REVISION (.*)/ ) {
      $oldrev = $1;
    }
    close(IN);
    if ( $rev == 0 or $rev == $oldrev ) {
      print STDERR "Not updating existing rev file at $oldrev\n";
      return;
    }
  }

  my $string = "#define POSTGIS_SVN_REVISION $rev\n";
  open(OUT,">$rev_file");
  print OUT $string;
  close(OUT);
  print STDERR "Wrote rev file at $rev\n";
}
