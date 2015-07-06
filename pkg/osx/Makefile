PACKAGE = pipelinedb
STAGING = $(shell pwd)/staging
PREFIX = /usr/lib/$(PACKAGE)

all:
	cp postinst-osxpkg $(STAGING)
	find $(STAGING) -type d -exec chmod 775 {} +
	find $(STAGING)/share/pipelinedb/timezone -type f -exec chmod 644 {} +
	(cd $(STAGING) && fpm -s dir -t osxpkg -n $(PACKAGE) -v $(VERSION) --epoch 0 --prefix=$(PREFIX) --after-install postinst-osx .)
	cp $(STAGING)/*.pkg .
	
clean:
	rm -f *.pkg
