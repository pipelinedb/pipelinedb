PACKAGE = pipelinedb
PREFIX = /usr/lib/$(PACKAGE)

TOP_BUILDDIR = ../../
BINDIR = $(PREFIX)/bin
LIBDIR = $(PREFIX)/lib
BUILDROOT = $(CURDIR)/build

all: spec conf pipeline pipeline-debug clean-libs set-perms
	mkdir $(CURDIR)/build
	cp -p -r --parents $(PREFIX) $(BUILDROOT)
	rpmbuild -bb -vv \
		--define "_topdir $(CURDIR)/rpmbuild" \
		--define "_tmppath $(CURDIR)/tmp" \
		 --buildroot $(BUILDROOT) pipelinedb.spec
	mv $(CURDIR)/rpmbuild/RPMS/x86_64/*.rpm .

spec:
	sed -e 's/__VERSION__/$(PIPELINE_VERSION)/g' \
		-e 's:__PREFIX__:$(PREFIX):g' $(CURDIR)/pipelinedb.spec.in > pipelinedb.spec

set-perms:
	find $(PREFIX) -type d -exec chmod 755 {} +

conf:
	(cd $(TOP_BUILDDIR) && ./configure)

pipeline:
	(cd $(TOP_BUILDDIR) && ./configure PIPELINE_VERSION=$(PIPELINE_VERSION) PIPELINE_REVISION=$(PIPELINE_REVISION) --prefix=$(PREFIX) --with-system-tzdata=/usr/share/zoneinfo CFLAGS="$(PIPELINE_CFLAGS)" --disable-rpath)
	(cd $(TOP_BUILDDIR) && $(MAKE))
	(cd $(TOP_BUILDDIR) && $(MAKE) install)
	(cd $(TOP_BUILDDIR)/contrib/pipeline_kafka && ./configure && make && make install)

pipeline-debug:
	(cd $(TOP_BUILDDIR) && make clean)
	(cd $(TOP_BUILDDIR) && ./configure CFLAGS="-g -O0" PIPELINE_VERSION=$(PIPELINE_VERSION) PIPELINE_REVISION=$(PIPELINE_REVISION) --prefix=$(PREFIX) --with-system-tzdata=/usr/share/zoneinfo --enable-cassert --disable-rpath)
	(cd $(TOP_BUILDDIR) && $(MAKE) PIPELINE_SERVER_BINARY=pipeline-server-debug)
	(cd $(TOP_BUILDDIR)/src/backend && $(MAKE) PIPELINE_SERVER_BINARY=pipeline-server-debug install-server-bin )

clean-libs:
	find $(LIBDIR) -name "*.la" -exec rm {} \;

clean:
	rm -rf $(GEOS)* $(GDAL)* $(PROJ)* rpmbuild tmp pipelinedb.spec
