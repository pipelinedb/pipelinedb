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

osxpkg:
	cp postinst-osxpkg $(PREFIX)
	find $(PREFIX) -type d -exec chmod 775 {} +
	find $(PREFIX)/share/pipelinedb/timezone -type f -exec chmod 644 {} +
	(cd $(PREFIX) && fpm -s dir -t osxpkg -n pipelinedb -v $(VERSION) --epoch 0 --prefix=/opt/pipelinedb --after-install postinst-osxpkg .)
	cp $(PREFIX)/*.pkg .

clean:
	$(MAKE) -f debian/rules clean
	$(MAKE) -C rpm clean
	$(MAKE) -C osx clean

.PHONY: deb rpm osx
