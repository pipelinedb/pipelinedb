use File::Basename;
use Cwd 'abs_path';

my $REGDIR = abs_path(dirname($0));
my $RASTERDIR = abs_path($REGDIR . "/../raster/test/regress");
my $FILERASTER = $RASTERDIR . "/loader/testraster.tif";

link "loader/testraster.tif", "loader/BasicOutDB.tif";

open(OPTS, '>', 'loader/BasicOutDB.opts');
print OPTS "-F -C -R \"$FILERASTER\"\n";
close(OPTS);
