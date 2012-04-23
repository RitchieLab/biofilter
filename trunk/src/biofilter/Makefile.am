bin_PROGRAMS = biofilter

biofilter_LDADD = \
	../utility/libutility.la \
	../knowledge/libknowledge.la \
	../liftover/libliftover.la \
	$(BOOST_LIB) $(BOOST_FILESYSTEM_LIB) $(BOOST_SYSTEM_LIB) $(BOOST_REGEX_LIB) \
	$(SOCI_LIB) $(SOCI_SQLITE_LIB)

AM_CPPFLAGS = \
	-I$(top_srcdir)/src \
	$(BOOST_CPPFLAGS) $(BOOST_FILESYSTEM_CPPFLAGS) $(BOOST_SYSTEM_CPPFLAGS) $(BOOST_REGEX_CPPFLAGS) \
	$(SOCI_CPPFLAGS) $(SOCI_SQLITE_CPPFLAGS) -DDATA_DIR='"$(datadir)"'

AM_LDFLAGS = \
	$(BOOST_LDFLAGS) $(BOOST_FILESYSTEM_LDFLAGS) $(BOOST_SYSTEM_LDFLAGS) $(BOOST_REGEX_LDFLAGS) \
	$(SOCI_LDFLAGS) $(SOCI_SQLITE_LDFLAGS)

biofilter_SOURCES = \
	application.h \
	configuration.h \
	ldsplineimporter.h \
	main.h \
	task.h \
	taskgenecoverage.h \
	taskgenereport.h \
	taskmarkerinfo.h \
	tasksnpgenemap.h \
	taskgenegenemodelreport.h \
	timestamp.h \
	tasksnpsnpmodelarchive.h \
	taskgenebased.h \
	tasksnpreport.h \
	application.cpp \
	configuration.cpp \
	ldsplineimporter.cpp \
	main.cpp \
	task.cpp \
	taskgenecoverage.cpp

EXTRA_DIST=\
	$(top_srcdir)/doc/manual.odt \
	$(top_srcdir)/doc/manual.pdf \
	$(top_srcdir)/examples/affy50-na28.txt \
	$(top_srcdir)/examples/affy-dmet.txt \
	$(top_srcdir)/examples/Illumina-1MDuo.txt \
	$(top_srcdir)/examples/Illumina-660Quad.txt
