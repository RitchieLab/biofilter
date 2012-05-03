noinst_LTLIBRARIES = libutility.la

libknowledge_la_LIBADD = \
	../utility/libutility.la \
	$(BOOST_REGEX_LIB)

AM_CPPFLAGS = \
	$(BOOST_CPPFLAGS)
	
AM_LDFLAGS = \
	$(BOOST_LDFLAGS)

libutility_la_SOURCES = \
	filetools.h \
	lineparser.h \
	locus.h \
	strings.h \
	types.h \
	exception.h \
	filetools.cpp \
	lineparser.cpp \
	locus.cpp \
	strings.cpp \
	exception.cpp
