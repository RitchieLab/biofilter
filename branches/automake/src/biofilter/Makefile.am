bin_PROGRAMS = $(top_srcdir)/bin/biofilter

__top_srcdir__bin_biofilter_LDADD = \
	$(top_srcdir)/src/utility/libutility.la \
	$(top_srcdir)/src/knowledge/libknowledge.la \
	$(top_srcdir)/src/ldspline/libldspline.la \
	$(top_srcdir)/src/liftover/libliftover.la \
	$(BOOST_LIB) $(BOOST_FILESYSTEM_LIB) $(BOOST_SYSTEM_LIB) $(BOOST_REGEX_LIB) \
	$(SOCI_LIB) $(SOCI_SQLITE_LIB)

AM_CPPFLAGS = \
	-I $(top_srcdir)/src \
	$(BOOST_CPPFLAGS) $(BOOST_FILESYSTEM_CPPFLAGS) $(BOOST_SYSTEM_CPPFLAGS) $(BOOST_REGEX_CPPFLAGS) \
	$(SOCI_CPPFLAGS) $(SOCI_SQLITE_CPPFLAGS)

AM_LDFLAGS = \
	$(BOOST_LDFLAGS) $(BOOST_FILESYSTEM_LDFLAGS) $(BOOST_SYSTEM_LDFLAGS) $(BOOST_REGEX_LDFLAGS) \
	$(SOCI_LDFLAGS) $(SOCI_SQLITE_LDFLAGS)

__top_srcdir__bin_biofilter_SOURCES = \
	application.h \
	configuration.h \
	ldsplineimporter.h \
	main.h \
	task.h \
	taskgenecoverage.h \
	application.cpp \
	configuration.cpp \
	ldsplineimporter.cpp \
	main.cpp \
	task.cpp \
	taskgenecoverage.cpp
