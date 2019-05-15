MODULE_big = pipelinedb
SOURCES = $(shell find src -type f -name '*.c' -not -path 'src/test/*')
OBJS = $(patsubst %.c,%.o,$(SOURCES))

PG_CONFIG := pg_config

EXTENSION = pipelinedb
REGRESS = $(EXTENSION)

DATA = $(shell find . -type f -name 'pipelinedb--*.sql')
EXTRA_CLEAN = src/test/regress/expected/$(REGRESS).out src/test/regress/sql/$(REGRESS).sql
SHLIB_LINK += -lzmq -lstdc++

ifdef USE_PGXS
PG_CPPFLAGS += -I./include -I$(shell $(PG_CONFIG) --includedir)

ifdef PIPELINE_VERSION_STR
PG_CPPFLAGS += -DPIPELINE_VERSION_STR=\"$(PIPELINE_VERSION_STR)\"
endif
ifdef PIPELINE_REVISION_STR
PG_CPPFLAGS += -DPIPELINE_REVISION_STR=\"$(PIPELINE_REVISION_STR)\"
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else

$(shell touch src/test/regress/sql/$(REGRESS).sql)
$(shell touch src/test/regress/expected/$(REGRESS).out)

bindir = $(shell $(PG_CONFIG) --bindir)
REGRESS_OPTS = --schedule=./src/test/regress/parallel_schedule \
  --host=localhost \
  --inputdir=./src/test/regress \
  --outputdir=./src/test/regress \
  --load-extension=pipelinedb \
  --temp-config=./src/test/regress/pipelinedb.conf \
  --bindir=$(bindir)

NO_GENERATED_HEADERS = 1
NO_PGXS = 1
NO_TEMP_INSTALL = 1
top_builddir = $(shell $(PG_CONFIG) --pkglibdir)/pgxs

include $(shell $(PG_CONFIG) --pkglibdir)/pgxs/src/Makefile.global
include $(shell $(PG_CONFIG) --pgxs)

endif

bin_dir = ./bin

headers_dir = $(shell $(PG_CONFIG) --includedir-server)/../pipelinedb

# Headers for other extensions to build against
install-headers:
	$(MKDIR_P) $(headers_dir)
	$(INSTALL_DATA) $(CURDIR)/include/*.h '$(headers_dir)'

install: install-headers

bootstrap:
	$(bin_dir)/bootstrap

run:
	$(bin_dir)/run-dev

test:
	make check
	make -C src/test/py test
