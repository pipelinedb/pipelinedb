#!/usr/bin/perl -w

# 
# TODO:
#
#	accept a finer boxesPerSide specification
#	eg. 1-3 or 1-32/5
#

use Pg;
use Time::HiRes("gettimeofday");

$VERBOSE = 0;
$USE_GIST = 1;

sub usage
{
	local($me) = `basename $0`;
	chop($me);
	print STDERR "$me [-v] [-vacuum] [-bps <bps>[,<bps>]] <ext_table> <ext_col> <chk_table> <chk_col>\n";
}

$ETABLE='';
$ECOLUMN='';
$CTABLE='';
$CCOLUMN='';
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
	elsif ( ! $ETABLE )
	{
		$ETABLE = $ARGV[$i];
	}
	elsif ( ! $ECOLUMN )
	{
		$ECOLUMN = $ARGV[$i];
	}
	elsif ( ! $CTABLE )
	{
		$CTABLE = $ARGV[$i];
	}
	elsif ( ! $CCOLUMN )
	{
		$CCOLUMN = $ARGV[$i];
	}
	else
	{
		print STDERR "Too many options:\n";
		usage();
		exit(1);
	}
}

if ( ! $ETABLE || ! $ECOLUMN )
{
	usage();
	exit 1;
}


$ESCHEMA = 'public';
$ECOLUMN = 'the_geom' if ( $ECOLUMN eq '' );
if ( $ETABLE =~ /(.*)\.(.*)/ ) 
{
	$ESCHEMA = $1;
	$ETABLE = $2;
}

$CSCHEMA = 'public';
$CCOLUMN = 'the_geom' if ( $CCOLUMN eq '' );
if ( $CTABLE =~ /(.*)\.(.*)/ ) 
{
	$CSCHEMA = $1;
	$CTABLE = $2;
}

#connect
$conn = Pg::connectdb("");
if ( $conn->status != PGRES_CONNECTION_OK ) {
        print STDERR $conn->errorMessage;
	exit(1);
}

if ( $VERBOSE )
{
	print "Table: \"$ESCHEMA\".\"$ETABLE\"\n";
	print "Column: \"$ECOLUMN\"\n";
}

# Get extent
$query = 'select extent("'.$ECOLUMN.'")::box3d from "'.$ESCHEMA.'"."'.$ETABLE.'"';
$res = $conn->exec($query);
if ( $res->resultStatus != PGRES_TUPLES_OK )  {
	print STDERR $conn->errorMessage;
	exit(1);
}
$EXTENT = $res->getvalue(0, 0);

# Get check geom type
$query = 'select distinct geometrytype("'.$CCOLUMN.'"), getSRID("'.$CCOLUMN.'") from "'.$CSCHEMA.'"."'.$CTABLE.'"';
$res = $conn->exec($query);
if ( $res->resultStatus != PGRES_TUPLES_OK )  {
	print STDERR $conn->errorMessage;
	exit(1);
}
$CTYPE = $res->getvalue(0, 0);
$CSRID = $res->getvalue(0, 1);

# find srid of extent table
$query = 'select srid("'.$ECOLUMN.'") from "'.$ESCHEMA.'"."'.$ETABLE.'"';
$res = $conn->exec($query);
if ( $res->resultStatus != PGRES_TUPLES_OK )  {
	print STDERR $conn->errorMessage;
	exit(1);
}
$ESRID = $res->getvalue(0, 0);

if ( $ESRID != $CSRID )
{
	die "SRID of extent ($ESRID) and check ($CSRID) differ\n";
}
$SRID=$ESRID;

# parse extent
$EXTENT =~ /^BOX3D\((.*) (.*) (.*),(.*) (.*) (.*)\)$/;
$ext{xmin} = $1;
$ext{ymin} = $2;
$ext{xmax} = $4;
$ext{ymax} = $5;

# vacuum analyze table
if ( $VACUUM )
{
	print "VACUUM ANALYZE\n";
	$query = 'vacuum analyze "'.$ESCHEMA.'"."'.$ETABLE.'"';
	$res = $conn->exec($query);
	if ( $res->resultStatus != PGRES_COMMAND_OK )  {
		print STDERR $conn->errorMessage;
		exit(1);
	}
}

# get number of features from pg_class.ntuples
# (correct if vacuum have been run after last insertion/deletions)
$query = 'SELECT c.reltuples FROM pg_class c, pg_namespace n '.
	"WHERE c.relnamespace = n.oid AND n.nspname = '$CSCHEMA' ".
	" AND c.relname = '$CTABLE'";
$res = $conn->exec($query);
if ( $res->resultStatus != PGRES_TUPLES_OK )  {
	print STDERR $conn->errorMessage;
	exit(1);
}
$TOTROWS=$res->getvalue(0, 0);

# Disable index scan
if (!$USE_GIST)
{
	$query = 'SET enable_indexscan = off';
	$res = $conn->exec($query);
	if ( $res->resultStatus != PGRES_COMMAND_OK )  {
		print STDERR $conn->errorMessage;
		exit(1);
	}
}

@extents = ( \%ext );

print "Extent: ".print_extent(\%ext)."\n";
print "  Type: $CTYPE\n";
print "  Rows: $TOTROWS\n";

print "  bps\tt\tintr\tdist\tintr/dist\n";
print "----------------------------------------------------------\n";

for ($i=0; $i<@bps_list; $i++)
{
	$bps=$bps_list[$i];
	@extents = split_extent(\%ext, $bps);

	$best_dist=0;
	$best_intr=0;
	$sum_fact=0;
	$try=0;
	while ( ($cell_ext=pop(@extents)) )
	{
		local($icount, $dcount, $itime, $dtime, $fact);

		local($sec,$usec) = gettimeofday();
		$icount = test_intersects($cell_ext);
		local($sec2,$usec2) = gettimeofday();
		$itime = (($sec2*1000000)+$usec2)-(($sec*1000000)+$usec);

		local($sec,$usec) = gettimeofday();
		$dcount = test_distance($cell_ext);
		local($sec2,$usec2) = gettimeofday();
		$dtime = (($sec2*1000000)+$usec2)-(($sec*1000000)+$usec);

		$fact = int(($itime/$dtime)*100)/100;

		if ( $icount ne $dcount ) {
			die "intersects gave $icount true valus, distance $dcount\n";
		}

		print "    $bps\t".(int(($icount/$TOTROWS)*100)/100)."\t".$itime."\t".$dtime."\t".$fact."\n";

		if ( $try == 0 || $fact > $best_dist )
		{
			$best_dist = $fact;
		}
		if ( $try == 0 || $fact < $best_intr ) 
		{
			$best_intr = $fact;
		}
		$sum_fact += abs($fact);
		$try++;
	}
	$avg_fact = int(($sum_fact/$try)*100)/100;

	print "    $bps    ".
		"(min/max/avg)  \t".
		($best_intr)."\t".
		($best_dist)."\t".
		($avg_fact)."\n";

	print "    $bps\tworst\t".
		(($best_dist)*100)."%\t".
		int(((1/$best_intr)*10000)/100)."%\n";

	print "    $bps\tbest\t".
		int(((1/$best_dist)*10000)/100)."%\t".
		(($best_intr)*100)."%\n";
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

sub test_intersects
{
	local($ext) = shift;

	# Test whole extent query
	if ( $USE_GIST )
	{
		$query = 'select count(oid) from "'.
			$CSCHEMA.'"."'.$CTABLE.'"'.
			' WHERE "'.$CCOLUMN.'" && '.
			"setSRID('BOX3D(".$ext->{'xmin'}." ".
			$ext->{'ymin'}.", ".$ext->{'xmax'}." ".
			$ext->{'ymax'}.")'::box3d::geometry, $SRID)".
			' AND intersects("'.$CCOLUMN.'", '.
			"setSRID('BOX3D(".$ext->{'xmin'}." ".
			$ext->{'ymin'}.", ".$ext->{'xmax'}." ".
			$ext->{'ymax'}.")'::box3d::geometry, $SRID));";
	}
	else
	{
		$query = 'select count(oid) from "'.
			$CSCHEMA.'"."'.$CTABLE.
			'" WHERE intersects("'.$CCOLUMN.'", '.
			"setSRID('BOX3D(".$ext->{'xmin'}." ".
			$ext->{'ymin'}.", ".$ext->{'xmax'}." ".
			$ext->{'ymax'}.")'::box3d::geometry, $SRID));";
	}
	$res = $conn->exec($query);
	if ( $res->resultStatus != PGRES_TUPLES_OK )  {
		print STDERR "$query: ".$conn->errorMessage;
		exit(1);
	}
	$row=$res->fetchrow();
	return $row;

}

sub test_distance
{
	local($ext) = shift;

	# Test whole extent query
	if ( $USE_GIST )
	{
		$query = 'select count(oid) from "'.
			$CSCHEMA.'"."'.$CTABLE.'"'.
			' WHERE "'.$CCOLUMN.'" && '.
			"setSRID('BOX3D(".$ext->{'xmin'}." ".
			$ext->{'ymin'}.", ".$ext->{'xmax'}." ".
			$ext->{'ymax'}.")'::box3d::geometry, $SRID)".
			' AND distance("'.$CCOLUMN.'", '.
			"setSRID('BOX3D(".$ext->{'xmin'}." ".
			$ext->{'ymin'}.", ".$ext->{'xmax'}." ".
			$ext->{'ymax'}.")'::box3d::geometry, $SRID))=0;";
	}
	else
	{
		$query = 'select count(oid) from "'.
			$CSCHEMA.'"."'.$CTABLE.
			'" WHERE distance("'.$CCOLUMN.'", '.
			"setSRID('BOX3D(".$ext->{'xmin'}." ".
			$ext->{'ymin'}.", ".$ext->{'xmax'}." ".
			$ext->{'ymax'}."'::box3d::geometry, $SRID))=0;";
	}
	$res = $conn->exec($query);
	if ( $res->resultStatus != PGRES_TUPLES_OK )  {
		print STDERR "$query: ".$conn->errorMessage;
		exit(1);
	}
	$row=$res->fetchrow();
	return $row;

}

# 
# $Log$
# Revision 1.4  2004/09/27 08:23:45  strk
# Added USE_GIST variable on top of file. Changed true values report
# as fraction of total rows.
#
# Revision 1.3  2004/09/24 12:20:56  strk
# Added worst and best percentile for both intersects and distance
#
# Revision 1.2  2004/09/24 11:58:01  strk
# approximated nums to 2 decimal digits
#
# Revision 1.1  2004/09/24 11:35:45  strk
# initial intersects profiler frontend implementation
#
