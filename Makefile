#-*- mode: makefile; -*-

SHELL := /bin/bash
.SHELLFLAGS := -ec

MODULE_NAME := SQS::Queue::Worder
MODULE_PATH := $(subst ::,/,$(MODULE_NAME)).pm

PERL_MODULES = \
    lib/$(MODULE_PATH)

VERSION := $(shell perl -I lib -M$(MODULE_NAME) -e 'print $$$(MODULE_NAME)::VERSION;')

TARBALL = $(subst ::,-,$(MODULE_NAME))-$(VERSION).tar.gz

$(TARBALL): buildspec.yml $(PERL_MODULES) requires test-requires bin/sqs-queue-processor.pl README.md
	make-cpan-dist.pl -b $<

README.md: lib/$(MODULE_PATH)
	pod2markdown $< > $@

all: $(TARBALL)

clean:
	rm -f *.tar.gz
