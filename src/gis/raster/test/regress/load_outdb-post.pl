use File::Basename;
use Cwd 'abs_path';

my $REGDIR = abs_path(dirname($0));
my $RASTERDIR = abs_path($REGDIR . "/../raster/test/regress");

unlink $RASTERDIR . '/' . $TEST . '-pre.sql';
#unlink $RASTERDIR . '/' . $TEST . '-post.sql';
