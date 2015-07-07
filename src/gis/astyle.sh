#!/bin/bash

# Run astyle on the code base ready for release

# If astyle doesn't exist, exit the script and do nothing
echo "FOO" | astyle > /dev/null
RET=$?
if [ $RET -ne 0 ]; then
	echo "Could not find astyle - aborting." 
	exit
fi


RET=`astyle --version 2>&1`
if [ "$RET" != "Artistic Style Version 1.23" ]; then
	echo "Only 1.23 astyle version is 'allowed'"
	exit
fi

# Find all "pure" C files in the codebase
#   - not .in.c used for .sql generation
#   - not lex.yy.c or wktparse.tab.c as these are generated files
CFILES=`find . -name '*.c' -not \( -name '*.in.c' -o -name '*_parse.c' -o -name '*_lex.c' \)`

# Run the standard format on the files, and do not 
# leave .orig files around for altered files.
astyle --style=ansi --indent=tab --suffix=none $CFILES
