MODULE_big = pipelinedb
SOURCES = $(shell find src -type f -name '*.c' -not -path 'src/test/*')
OBJS = $(patsubst %.c,%.o,$(SOURCES))
BASE_DIR=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

PG_CONFIG := pg_config

EXTENSION = pipelinedb
REGRESS = $(EXTENSION)

DATA = $(shell find . -type f -name 'pipelinedb--*.sql')
EXTRA_CLEAN = src/test/regress/expected/$(REGRESS).out src/test/regress/sql/$(REGRESS).sql
SHLIB_LINK += /usr/lib/libzmq.a -lstdc++

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

build-deb:
	@rm -rf install/deb/*.deb && \
	mkdir -p $(BASE_DIR)/install/deb/pipelinedb/include && \
	mkdir -p $(BASE_DIR)/install/deb/pipelinedb/lib && \
	mkdir -p $(BASE_DIR)/install/deb/pipelinedb/share && \
	cp -r $(BASE_DIR)/include/* $(BASE_DIR)/install/deb/pipelinedb/include  && \
	cp -r $(BASE_DIR)/pipelinedb.so $(BASE_DIR)/install/deb/pipelinedb/lib  && \
	cp -r $(BASE_DIR)/pipelinedb.control $(BASE_DIR)/install/deb/pipelinedb/share  && \
	cp -r $(BASE_DIR)/pipelinedb*.sql $(BASE_DIR)/install/deb/pipelinedb/share  

	@cd $(BASE_DIR)/install/deb/pipelinedb && \
	rm -rf debian/changelog ; \
	dch --create --distribution stable --package "pipelinedb" --newversion 1.0.0-15 "Release" ; \
	dpkg-buildpackage -us -uc  

build-rpm:
		@RPM="pipelinedb" ; \
		for dir_path in $${RPM}; do \
				rm -rf install/$${dir_path}/RPMS/ ; \
				mkdir -p install/$${dir_path}/{BUILD,BUILDROOT,RPMS,SOURCES,SRPMS} ; \
		done ; \
		for dir_path in $${RPM}; do \
			rpmbuild --bb -D "_topdir $(BASE_DIR)/install/$${dir_path}" install/$${dir_path}/SPECS/$${dir_path}.specs || exit 1 ; \
		done