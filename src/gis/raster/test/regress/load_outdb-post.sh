SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ] ; do SOURCE="$(readlink "$SOURCE")"; done
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

rm -f "$DIR/$TEST-pre.sql"
#rm -f "$DIR/$TEST-post.sql"
