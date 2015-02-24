# Copyright (c) 2013-2015 PipelineDB
VERSION = 0.1a

TMP_BUILD = ./tmp_build
GEOS = geos-3.4.2
GDAL = gdal-1.11.2
PROJ = proj-4.8.0
PREFIX = $(shell pwd)/staging
TOP_BUILDDIR = ../../
FPM = fpm -s dir -t rpm -n pipelinedb -v $(VERSION) --epoch 0 --prefix=/opt/pipelinedb --after-install post-install . 

geos:
	mkdir -p $(TMP_BUILD)
	wget -P $(TMP_BUILD) http://download.osgeo.org/geos/$(GEOS).tar.bz2
	(cd $(TMP_BUILD) && tar -xf $(GEOS).tar.bz2)
	(cd $(TMP_BUILD)/$(GEOS) && ./configure --prefix=$(PREFIX))
	(cd $(TMP_BUILD)/$(GEOS) && make && make install)

gdal:
	mkdir -p $(TMP_BUILD)
	wget -P $(TMP_BUILD) ftp://ftp.remotesensing.org/gdal/1.11.2/$(GDAL).tar.gz
	(cd $(TMP_BUILD) && tar -xf $(GDAL).tar.gz)
	(cd $(TMP_BUILD)/$(GDAL) && ./configure --prefix=$(PREFIX))
	(cd $(TMP_BUILD)/$(GDAL) && make && make install)

proj:
	mkdir -p $(TMP_BUILD)
	wget -P $(TMP_BUILD) http://download.osgeo.org/proj/$(PROJ).tar.gz
	(cd $(TMP_BUILD) && tar -xf $(PROJ).tar.gz)
	(cd $(TMP_BUILD)/$(PROJ) && ./configure --prefix=$(PREFIX))
	(cd $(TMP_BUILD)/$(PROJ) && make && make install)

pipeline:
	(cd $(TOP_BUILDDIR) && ./configure --prefix=$(PREFIX) CFLAGS="-O0")
	(cd $(TOP_BUILDDIR) && make && make install)

gis:
	(cd $(TOP_BUILDDIR)/src/gis && ./autogen.sh)
	(cd $(TOP_BUILDDIR)/src/gis && ./configure \
		--with-pgconfig=$(PREFIX)/bin/pipeline-config \
		--with-geosconfig=$(PREFIX)/bin/geos-config \
		--with-gdalconfig=$(PREFIX)/bin/gdal-config \
		--with-projdir=$(PREFIX))
	(cd $(TOP_BUILDDIR)/src/gis && make && make install)	

fpm:
	cp post-install $(PREFIX)/
	(cd $(PREFIX) && $(FPM))
	mv $(PREFIX)/*.rpm .

all: geos gdal proj pipeline gis fpm	
	echo "Successfully built RPM package `ls *.rpm`"
	
clean:
	rm -rf $(GEOS)* $(GDAL)* $(PROJ)*
	rm -rf $(TMP_BUILD)
	rm -rf $(PREFIX)
	rm -rf *.rpm