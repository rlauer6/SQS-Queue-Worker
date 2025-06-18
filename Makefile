#-*- mode: makefile; -*-

SHELL := /bin/bash
.SHELLFLAGS := -ec

MODULE_NAME := SQS::Queue::Worker
MODULE_PATH := $(subst ::,/,$(MODULE_NAME)).pm

PERL_MODULES = \
    lib/$(MODULE_PATH).in \
    lib/SQS/Queue/Constants.pm.in

GPERL_MODULES = $(PERL_MODULES:.pm.in=.pm)

VERSION := $(shell cat VERSION)

%.pm: %.pm.in
	sed "s/[@]PACKAGE_VERSION[@]/$(VERSION)/g" $< > $@

TARBALL = $(subst ::,-,$(MODULE_NAME))-$(VERSION).tar.gz

all: $(TARBALL)

$(GPERL_MODULES): $(PERL_MODULES)


$(TARBALL): buildspec.yml $(GPERL_MODULES) VERSION requires test-requires bin/sqs-queue-processor.pl README.md
	for a in $(GPERL_MODULES); do \
	  perl -I lib -wc $$a; \
	done; \
	make-cpan-dist.pl -b $<

README.md: lib/$(MODULE_PATH)
	pod2markdown $< > $@

clean:
	rm -f *.tar.gz
