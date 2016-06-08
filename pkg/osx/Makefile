PACKAGE = pipelinedb
PREFIX = /usr/local/lib/$(PACKAGE)

TOP_BUILDDIR = ../../
DESTDIR = $(CURDIR)/build
INSTALL_DIR = $(DESTDIR)$(PREFIX)
BINDIR = $(PREFIX)/bin
LIBDIR = $(PREFIX)/lib
SCRIPTS = $(CURDIR)/scripts

all: conf pipeline pipeline-debug
	find $(PREFIX) -type d -exec chmod 775 {} +
	find $(PREFIX)/share/pipelinedb/timezone -type f -exec chmod 644 {} +
	mkdir $(SCRIPTS)
	cp postinstall $(SCRIPTS)
	pkgbuild --identifier $(PACKAGE) --version $(PIPELINE_VERSION) \
		--ownership recommended --root $(PREFIX) --install-location $(PREFIX) \
		--scripts $(SCRIPTS) pipelinedb-$(PIPELINE_VERSION).pkg
	cp $(CURDIR)/*.pkg ..

conf:
	(cd $(TOP_BUILDDIR) && ./configure)

pipeline:
	(cd $(TOP_BUILDDIR) && ./configure PIPELINE_VERSION=$(PIPELINE_VERSION) PIPELINE_REVISION=$(PIPELINE_REVISION) --prefix=$(PREFIX) CFLAGS="$(PIPELINE_CFLAGS)")
	(cd $(TOP_BUILDDIR) && $(MAKE))
	(cd $(TOP_BUILDDIR) && $(MAKE) install)

pipeline-debug:
	(cd $(TOP_BUILDDIR) && make clean)
	(cd $(TOP_BUILDDIR) && ./configure CFLAGS="-g -O0 -DDUMP_CORE" PIPELINE_VERSION=$(PIPELINE_VERSION) PIPELINE_REVISION=$(PIPELINE_REVISION) --prefix=$(PREFIX) --with-system-tzdata=/usr/share/zoneinfo --enable-cassert --disable-rpath)
	(cd $(TOP_BUILDDIR) && $(MAKE) PIPELINE_SERVER_BINARY=pipelinedb-debug)
	(cd $(TOP_BUILDDIR)/src/backend && $(MAKE) PIPELINE_SERVER_BINARY=pipelinedb-debug install-server-bin )

clean-libs:
	find $(LIBDIR) -name "*.la" -exec rm {} \;

clean:
	rm -rf $(DESTDIR) $(SCRIPTS)
