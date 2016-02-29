PACKAGE = pipelinedb
PREFIX = /usr/local/lib/$(PACKAGE)

TOP_BUILDDIR = ../../
DESTDIR = $(CURDIR)/build
INSTALL_DIR = $(DESTDIR)$(PREFIX)
BINDIR = $(PREFIX)/bin
LIBDIR = $(PREFIX)/lib
SCRIPTS = $(CURDIR)/scripts

all: conf pipeline
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
	(cd $(TOP_BUILDDIR)/contrib/pipeline_kafka && ./configure && make LIB_RDKAFKA_STATIC=/usr/local/lib/librdkafka.a && make install)

clean-libs:
	find $(LIBDIR) -name "*.la" -exec rm {} \;

clean:
	rm -rf $(DESTDIR) $(SCRIPTS)
