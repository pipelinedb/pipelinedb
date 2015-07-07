#
# Have pg_config in your PATH
#

#
# Below should be fine
#
CFLAGS=-I`pg_config --includedir` -L`pg_config --libdir` -lpq

all: readwkb

readwkb: readwkb.c printwkb.c

clean:
	@rm -f readwkb
