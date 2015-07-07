# Copyright (c) 2013-2015 PipelineDB
changelog = $(CURDIR)/debian/changelog

changelog:
	echo "pipelinedb ($(VERSION)-1) UNRELEASED; urgency=low" > $(changelog)
	echo "" >> $(changelog)
	echo "`git log --oneline $(PREV_VERSION)..$(VERSION) | sed -e 's/^/  * /'`" >> $(changelog)
	echo "" >> $(changelog)
	echo " -- PipelineDB Packaging <packaging@pipelinedb.com>  `date +"%a, %d %b %Y %T %z"`" >> $(changelog)

deb: changelog
	dpkg-buildpackage -us -uc -b
	mv $(CURDIR)/../*.deb .
	rm $(CURDIR)/../*.changes

rpm:
	$(MAKE) -C $@
	mv $(CURDIR)/rpm/*.rpm .

osx:
	$(MAKE) -C $@

clean:
	$(MAKE) -f debian/rules clean
	$(MAKE) -C rpm clean
	$(MAKE) -C osx clean

.PHONY: deb rpm
