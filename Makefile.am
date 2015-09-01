ACLOCAL_AMFLAGS = -I libltdl/m4

SUBDIRS = libltdl src bindings .

AM_CPPFLAGS = $(LTDLINCL)

EXTRA_DIST = contrib version-gen.sh debian

install-exec-hook:
	$(mkinstalldirs) $(DESTDIR)$(localstatedir)/run
	$(mkinstalldirs) $(DESTDIR)$(localstatedir)/lib/$(PACKAGE_NAME)
	$(mkinstalldirs) $(DESTDIR)$(localstatedir)/log
	if test -e $(DESTDIR)$(sysconfdir)/collectd-td.conf; \
	then \
		$(INSTALL) -m 0644 contrib/netperf/collectd-td.conf $(DESTDIR)$(sysconfdir)/collectd-td.conf.pkg-orig; \
	else \
		$(INSTALL) -m 0644 contrib/netperf/collectd-td.conf $(DESTDIR)$(sysconfdir)/collectd-td.conf; \
	fi;

dist-hook:
	sed -i 's/git/native/' $(distdir)/debian/source/format

maintainer-clean-local:
	-rm -f -r libltdl
	-rm -f INSTALL
	-rm -f aclocal.m4
