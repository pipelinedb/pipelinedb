#!/usr/bin/perl -w

# 
# TODO:
#
# Apply oid filters on table2
#

use Pg;

$VERBOSE = 0;

sub usage
{
	local($me) = `basename $0`;
	chop($me);
	print STDERR "$me [-v] [-vacuum] <table1> <table2> [<col1>] [<col2>]\n";
}

$TABLE1='';
$TABLE2='';
$COLUMN1='';
$COLUMN2='';
for ($i=0; $i<@ARGV; $i++)
{
	if ( $ARGV[$i] =~ m/^-/ )
	{
		if ( $ARGV[$i] eq '-v' )
		{
			$VERBOSE++;
		}
		elsif ( $ARGV[$i] eq '-vacuum' )
		{
			$VACUUM=1;
		}
		else
		{
			print STDERR "Unknown option $ARGV[$i]:\n";
			usage();
			exit(1);
		}
	}
	elsif ( ! $TABLE1 )
	{
		$TABLE1 = $ARGV[$i];
	}
	elsif ( ! $TABLE2 )
	{
		$TABLE2 = $ARGV[$i];
	}
	elsif ( ! $COLUMN1 )
	{
		$COLUMN1 = $ARGV[$i];
	}
	elsif ( ! $COLUMN2 )
	{
		$COLUMN2 = $ARGV[$i];
	}
	else
	{
		print STDERR "Too many options:\n";
		usage();
		exit(1);
	}
}

if ( ! $TABLE1 || ! $TABLE2 )
{
	usage();
	exit 1;
}


$SCHEMA1 = 'public';
$COLUMN1 = 'the_geom' if ( $COLUMN1 eq '' );
if ( $TABLE1 =~ /(.*)\.(.*)/ ) 
{
	$SCHEMA1 = $1;
	$TABLE1 = $2;
}
$SCHEMA2 = 'public';
$COLUMN2 = 'the_geom' if ( $COLUMN2 eq '' );
if ( $TABLE2 =~ /(.*)\.(.*)/ ) 
{
	$SCHEMA2 = $1;
	$TABLE2 = $2;
}

#connect
$conn = Pg::connectdb("");
if ( $conn->status != PGRES_CONNECTION_OK ) {
        print STDERR $conn->errorMessage;
	exit(1);
}

if ( $VERBOSE )
{
	print "Table1: \"$SCHEMA1\".\"$TABLE1\".\"$COLUMN1\"\n";
	print "Table2: \"$SCHEMA2\".\"$TABLE2\".\"$COLUMN2\"\n";
}

# Get geometry types

#$TYPE1 = get_geometry_type($SCHEMA1, $TABLE1, $COLUMN1);
#$TYPE2 = get_geometry_type($SCHEMA2, $TABLE2, $COLUMN2);

# vacuum analyze table
if ( $VACUUM )
{
	print "VACUUM ANALYZE\n";
	vacuum($SCHEMA1, $TABLE1);
	vacuum($SCHEMA2, $TABLE2);
}

# get number of features from pg_class.ntuples
# (correct if vacuum have been run after last insertion/deletions)
$NROWS1 = get_num_rows($SCHEMA1, $TABLE1);
$NROWS2 = get_num_rows($SCHEMA2, $TABLE2);
$TOTROWS = $NROWS1*$NROWS2;


# Test join selectivity

($est,$real) = test_join();
$delta = $est-$real;
$error = $delta/$TOTROWS;
$error = int(($error)*10000)/100;

#print "      Types: $TYPE1 - $TYPE2\n";
print "       Rows: $NROWS1 x $NROWS2 = $TOTROWS\n";
print "  Estimated: ".$est."\n";
print "       Real: ".$real."\n";
print "  Error: ".$error."%\n";

##################################################################

sub print_extent
{
	local($ext) = shift;
	local($s);

	$s = $ext->{'xmin'}." ".$ext->{'ymin'}."  ";
	$s .= $ext->{'xmax'}." ".$ext->{'ymax'};

	return $s;
}

sub split_extent
{
	local($ext) = shift;
	local($bps) = shift;

	local($width, $height, $cell_width, $cell_height);
	local($x,$y);
	local(@stack);

	$width = $ext->{'xmax'} - $ext->{'xmin'};
	$height = $ext->{'ymax'} - $ext->{'ymin'};
	$cell_width = $width / $bps;
	$cell_height = $height / $bps;

	if ($VERBOSE)
	{
		print "cell_w: $cell_width\n";
		print "cell_h: $cell_height\n";
	}

	@stack = ();
	for ($x=0; $x<$bps; $x++)
	{
		for($y=0; $y<$bps; $y++)
		{
			local(%cell);
			$cell{'xmin'} = $ext->{'xmin'}+$x*$cell_width;
			$cell{'ymin'} = $ext->{'ymin'}+$y*$cell_height;
			$cell{'xmax'} = $ext->{'xmin'}+($x+1)*$cell_width;
			$cell{'ymax'} = $ext->{'ymin'}+($y+1)*$cell_height;
			print "cell: ".print_extent(\%cell)."\n" if ($VERBOSE);
			push(@stack, \%cell);
		}
	}
	return @stack;
}

sub test_join
{
	local($ext) = shift;

	# Test whole extent query
	$query = 'explain analyze select count(1) from "'.
		$SCHEMA1.'"."'.$TABLE1.'" t1, "'.$SCHEMA2.'"."'.$TABLE2.
		'" t2 WHERE t1."'.$COLUMN1.'" && '.
		' t2."'.$COLUMN2.'"';
	print $query."\n";
	$res = $conn->exec($query);
	if ( $res->resultStatus != PGRES_TUPLES_OK )  {
		print STDERR $conn->errorMessage;
		exit(1);
	}

	while ( ($row=$res->fetchrow) )
	{
		if ( ($row =~ /.* rows=([0-9]+) .* rows=([0-9]+) /) && ! ($row =~ /.*Aggregate.*/) )
		{
			$est = $1;
			$real = $2;
			last;
		}
	}

	return ($est,$real);
}

sub get_geometry_type
{
	my $schema = shift;
	my $table = shift;
	my $col = shift;
	my $query = 'select distinct geometrytype("'.$col.'") from "'.$schema.'"."'.$table.'"';
	my $res = $conn->exec($query);
	if ( $res->resultStatus != PGRES_TUPLES_OK )  {
		print STDERR $conn->errorMessage;
		exit(1);
	}
	if ( $res->ntuples() > 1 ) {
		print STDERR "Mixed geometry types in \"$SCHEMA1\".\"$TABLE1\".\"$COLUMN1\"\n";
		exit(1);
	}
	return $res->getvalue(0, 0);
}

sub vacuum
{
	my $SCHEMA = shift;
	my $TABLE = shift;
	my $query = 'vacuum analyze "'.$SCHEMA.'"."'.$TABLE.'"';
	my $res = $conn->exec($query);
	if ( $res->resultStatus != PGRES_COMMAND_OK )  {
		print STDERR $conn->errorMessage;
		exit(1);
	}
}

sub get_num_rows
{
	my $SCHEMA = shift;
	my $TABLE = shift;
	my $query = 'SELECT c.reltuples FROM pg_class c, pg_namespace n '.
		"WHERE c.relnamespace = n.oid AND n.nspname = '$SCHEMA' ".
		" AND c.relname = '$TABLE'";
	my $res = $conn->exec($query);
	if ( $res->resultStatus != PGRES_TUPLES_OK )  {
		print STDERR $conn->errorMessage;
		exit(1);
	}
	return $res->getvalue(0, 0);
}

# 
# $Log$
# Revision 1.3  2005/04/18 13:50:14  strk
# Fixed bug in table2 schema parsing.
#
# Revision 1.2  2004/12/23 14:48:25  strk
# Fixed help string, and added a TODO item
#
# Revision 1.1  2004/12/22 17:02:17  strk
# initial revision
#
# Revision 1.8  2004/03/08 17:21:57  strk
# changed error computation code to delta/totrows
#
# Revision 1.7  2004/03/06 18:02:48  strk
# Comma-separated bps values accepted
#
# Revision 1.6  2004/03/05 21:06:04  strk
# Added -vacuum switch
#
# Revision 1.5  2004/03/05 21:03:18  strk
# Made the -bps switch specify the exact level(s) at which to run the test
#
# Revision 1.4  2004/03/05 16:40:30  strk
# rewritten split_extent to be more datatype-conservative
#
# Revision 1.3  2004/03/05 16:01:02  strk
# added -bps switch to set maximun query level. reworked command line parsing
#
# Revision 1.2  2004/03/05 15:29:35  strk
# more verbose output
#
# Revision 1.1  2004/03/05 11:52:24  strk
# initial import
#
#
