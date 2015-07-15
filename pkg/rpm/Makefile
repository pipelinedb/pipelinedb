GEOS = geos-3.4.2
GDAL = gdal-1.11.2
PROJ = proj-4.8.0

PACKAGE = pipelinedb
PREFIX = /usr/lib/$(PACKAGE)

TOP_BUILDDIR = ../../
BINDIR = $(PREFIX)/bin
LIBDIR = $(PREFIX)/lib
BUILDROOT = $(CURDIR)/build

all: spec conf geos gdal proj pipeline gis clean-libs set-perms
	mkdir $(CURDIR)/build
	cp -r --parents $(PREFIX) $(BUILDROOT)
	rpmbuild -bb -vv \
		--define "_topdir $(CURDIR)/rpmbuild" \
		--define "_tmppath $(CURDIR)/tmp" \
		 --buildroot $(BUILDROOT) pipelinedb.spec
	mv $(CURDIR)/rpmbuild/RPMS/x86_64/*.rpm .

spec:
	sed -e 's/__VERSION__/$(VERSION)/g' \
		-e 's:__PREFIX__:$(PREFIX):g' $(CURDIR)/pipelinedb.spec.in > pipelinedb.spec

set-perms:
	find $(PREFIX) -type d -exec chmod 755 {} +
	
conf:
	(cd $(TOP_BUILDDIR) && ./configure)

pipeline:
	(cd $(TOP_BUILDDIR) && ./configure --prefix=$(PREFIX) --with-system-tzdata=/usr/share/zoneinfo CFLAGS="$(PIPELINE_CFLAGS)" --disable-rpath)
	(cd $(TOP_BUILDDIR) && $(MAKE) VERSION=$(VERSION))
	(cd $(TOP_BUILDDIR) && $(MAKE) VERSION=$(VERSION) install)

geos:
	wget http://download.pipelinedb.com/dep/$(GEOS).tar.bz2
	tar -xf $(GEOS).tar.bz2
	(cd $(GEOS) && ./configure --prefix=$(PREFIX))
	(cd $(GEOS) && $(MAKE) && $(MAKE) install)

gdal:
	wget http://download.pipelinedb.com/dep/$(GDAL).tar.gz
	tar -xf $(GDAL).tar.gz
	(cd $(GDAL) && ./configure --prefix=$(PREFIX) \
            --with-threads \
            --with-ogr \
            --with-geos \
            --without-libtool \
            --with-libz=no \
            --without-gif \
            --without-pg \
            --without-grass \
            --without-libgrass \
            --without-cfitsio \
            --without-pcraster \
            --without-netcdf \
            --without-png \
            --without-jpeg \
            --without-gif \
            --without-ogdi \
            --without-fme \
            --without-hdf4 \
            --without-hdf5 \
            --without-jasper \
            --without-ecw \
            --without-kakadu \
            --without-mrsid \
            --without-jp2mrsid \
            --without-bsb \
            --without-grib \
            --without-mysql \
            --without-ingres \
            --without-xerces \
            --without-expat \
            --without-odbc \
            --without-curl \
            --without-sqlite3 \
            --without-dwgdirect \
            --without-idb \
            --without-sde \
            --without-perl \
            --without-php \
            --without-ruby \
            --without-python \
            --with-hide-internal-symbols \
						--disable-rpath)
	(cd $(GDAL) && $(MAKE) && $(MAKE) install)
	
proj:
	wget http://download.pipelinedb.com/dep/$(PROJ).tar.gz
	tar -xf $(PROJ).tar.gz
	(cd $(PROJ) && ./configure --prefix=$(PREFIX))
	(cd $(PROJ) && $(MAKE) && $(MAKE) install)

gis:
	(cd $(TOP_BUILDDIR)/src/gis && ./autogen.sh)
	(cd $(TOP_BUILDDIR)/src/gis && ./configure \
		--with-pgconfig=$(BINDIR)/pipeline-config \
		--with-geosconfig=$(BINDIR)/geos-config \
		--with-gdalconfig=$(BINDIR)/gdal-config \
		--with-projdir=$(BINDIR)/../ \
		--disable-rpath)
	(cd $(TOP_BUILDDIR)/src/gis && $(MAKE) && $(MAKE) install)	

clean-libs:
	chrpath -d $(BINDIR)/raster2pgsql
	chrpath -d $(BINDIR)/geod
	chrpath -d $(BINDIR)/cs2cs
	chrpath -d $(BINDIR)/nad2bin
	chrpath -d $(BINDIR)/proj
	chrpath -d $(LIBDIR)/libgeos_c.so.1.8.2
	chrpath -d $(LIBDIR)/liblwgeom-2.2.0dev.so
	rm -rf $(PREFIX)/share/gdal
	rm -rf $(PREFIX)/share/proj
	find $(LIBDIR) -name "*.la" -exec rm {} \;
	
clean:
	rm -rf $(GEOS)* $(GDAL)* $(PROJ)* rpmbuild tmp pipelinedb.spec
