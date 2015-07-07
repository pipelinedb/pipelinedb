#!/usr/bin/perl -w

# 
# TODO:
#
#	accept a finer boxesPerSide specification
#	eg. 1-3 or 1-32/5
#

use Pg;

$VERBOSE = 0;

sub usage
{
	local($me) = `basename $0`;
	chop($me);
	print STDERR "$me [-v] [-vacuum] [-bps <bps>[,<bps>]] <table> <col>\n";
}

$TABLE='';
$COLUMN='';
for ($i=0; $i<@ARGV; $i++)
{
	if ( $ARGV[$i] =~ m/^-/ )
	{
		if ( $ARGV[$i] eq '-v' )
		{
			$VERBOSE++;
		}
		elsif ( $ARGV[$i] eq '-bps' )
		{
			$bps_spec = $ARGV[++$i];
			push(@bps_list, split(',', $bps_spec));
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
	elsif ( ! $TABLE )
	{
		$TABLE = $ARGV[$i];
	}
	elsif ( ! $COLUMN )
	{
		$COLUMN = $ARGV[$i];
	}
	else
	{
		print STDERR "Too many options:\n";
		usage();
		exit(1);
	}
}

if ( ! $TABLE || ! $COLUMN )
{
	usage();
	exit 1;
}


$SCHEMA = 'public';
$COLUMN = 'the_geom' if ( $COLUMN eq '' );
if ( $TABLE =~ /(.*)\.(.*)/ ) 
{
	$SCHEMA = $1;
	$TABLE = $2;
}

#connect
$conn = Pg::connectdb("");
if ( $conn->status != PGRES_CONNECTION_OK ) {
        print STDERR $conn->errorMessage;
	exit(1);
}

if ( $VERBOSE )
{
	print "Table: \"$SCHEMA\".\"$TABLE\"\n";
	print "Column: \"$COLUMN\"\n";
}


# parse extent
$ext{'xmin'} = -180;
$ext{'ymin'} = -90;
$ext{'xmax'} = 180;
$ext{'ymax'} = 90;

# vacuum analyze table
if ( $VACUUM )
{
	print "VACUUM ANALYZE\n";
	$query = 'vacuum analyze "'.$SCHEMA.'"."'.$TABLE.'"';
	$res = $conn->exec($query);
	if ( $res->resultStatus != PGRES_COMMAND_OK )  {
		print STDERR $conn->errorMessage;
		exit(1);
	}
}

# get number of features from pg_class.ntuples
# (correct if vacuum have been run after last insertion/deletions)
$query = 'SELECT c.reltuples FROM pg_class c, pg_namespace n '.
	"WHERE c.relnamespace = n.oid AND n.nspname = '$SCHEMA' ".
	" AND c.relname = '$TABLE'";
$res = $conn->exec($query);
if ( $res->resultStatus != PGRES_TUPLES_OK )  {
	print STDERR $conn->errorMessage;
	exit(1);
}
$TOTROWS=$res->getvalue(0, 0);


@extents = ( \%ext );

#print "  Type: $TYPE\n";
print "  Rows: $TOTROWS\n";

print "  bps\test\treal\tdelta\terror%\n";
print "----------------------------------------------------------\n";

for ($i=0; $i<@bps_list; $i++)
{
	$bps=$bps_list[$i];
	@extents = split_extent(\%ext, $bps);

	$best_error=0;
	$worst_error=10000;
	$sum_error=0;
	$count_errors=0;
	$try=0;
	while ( ($cell_ext=pop(@extents)) )
	{
		($est,$real) = test_extent($cell_ext);
		$delta = $est-$real;

		print "    $bps\t".$est."\t".$real."\t$delta";

		$error = $delta/$TOTROWS;
		$count_errors++;

		print "\t".(int(($error)*10000)/100)."\n";

		$abs_error = abs($error);
		$sum_error += $abs_error;
		if ( $try == 0 || $abs_error > abs($worst_error) )
		{
			$worst_error = $error;
		}
		if ( $try == 0 || $abs_error < abs($best_error) )
		{
			$best_error = $error;
		}
		$try++;
	}
	$avg_error = $sum_error/$count_errors;
	print "    $bps\t".
		"(best/worst/avg)   \t".
		(int($best_error*10000)/100)."\t".
		(int($worst_error*10000)/100)."\t".
		"+-".(int($avg_error*10000)/100)."\n";
}


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

sub test_extent
{
	local($ext) = shift;

	# Test whole extent query
	$query = 'explain analyze select 1 from "' . $SCHEMA . '"."' . $TABLE .'" WHERE "' . $COLUMN . '"' .
			' && ST_GeographyFromText(' . "'POLYGON((" .
			$ext->{'xmin'} . " " . $ext->{'ymin'} . ", " .
			$ext->{'xmin'} . " " . $ext->{'ymax'} . ", " .
			$ext->{'xmax'} . " " . $ext->{'ymax'} . ", " .
			$ext->{'xmax'} . " " . $ext->{'ymin'} . ", " .
			$ext->{'xmin'} . " " . $ext->{'ymin'} .
			"))')";

	$res = $conn->exec($query);
	if ( $res->resultStatus != PGRES_TUPLES_OK )  {
		print STDERR $conn->errorMessage;
		exit(1);
	}
	while ( ($row=$res->fetchrow()) )
	{
		next unless $row =~ /.* rows=([0-9]+) .* rows=([0-9]+) /;
		$est = $1;
		$real = $2;
		last;
	}

	return ($est,$real);
}

# 
# $Log$
# Revision 1.9  2005/04/18 13:30:25  strk
# Fixed to work against LWGEOM installations
#
# Revision 1.8.4.1  2005/04/18 13:28:19  strk
# Fixed to work against LWGEOM installations
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
