SUBDIRS = \
	src/utility \
	src/knowledge \
	src/ldspline \
	src/liftover \
	src/biofilter \
	BioUpdater

ACLOCAL_AMFLAGS = -I ./make/m4

# This is here to prevent installation of LOKI during a distcheck
AM_DISTCHECK_CONFIGURE_FLAGS="--enable-loki=no"