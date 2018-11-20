LIBPQ_INCLUDE = $(shell pg_config --includedir)
LIBPQ_LIB = $(shell pg_config --libdir)

.PHONY: check

all: extended

extended:
	$(CC) extended.c -I$(LIBPQ_INCLUDE) -L$(LIBPQ_LIB) -lpq -o $@

test: all
	py.test -v

clean:
	rm -rf ./.pdb*
	rm -rf ./__pycache__
	rm -f extended
	rm -rf test_binary_upgrade_data_dir*
	rm -rf test_tablespace
	rm -rf pg_upgrade*

