SUBDIRS = \
	src/utility \
	src/knowledge \
	src/liftover \
	src/biofilter \
	BioUpdater

ACLOCAL_AMFLAGS = -I ./make/m4

# This is here to prevent installation of LOKI during a distcheck
DISTCHECK_CONFIGURE_FLAGS=--disable-loki
