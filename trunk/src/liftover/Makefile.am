noinst_LTLIBRARIES = libliftover.la

libknowledge_la_LIBADD = \
	../utility/libutility.la \
	$(SOCI_LIB) $(SOCI_SQLITE_LIB)

AM_CPPFLAGS = \
	-I$(top_srcdir)/src \
	$(BOOST_CPPFLAGS) $(SOCI_CPPFLAGS) $(SOCI_SQLITE_CPPFLAGS)
	
AM_LDFLAGS = \
	$(BOOST_LDFLAGS) $(SOCI_LDFLAGS) $(SOCI_SQLITE_LDFLAGS)

libliftover_la_SOURCES = \
	chain.h \
	converter.h \
	converterdb.h \
	region.h \
	snp.h \
	chain.cpp \
	converter.cpp \
	converterdb.cpp \
	region.cpp
