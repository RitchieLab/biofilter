noinst_LTLIBRARIES = libknowledge.la

libknowledge_la_LIBADD = \
	../utility/libutility.la \
	$(SOCI_LIB) $(SOCI_SQLITE_LIB)

AM_CPPFLAGS = \
	-I$(top_srcdir)/src \
	$(BOOST_CPPFLAGS) $(SOCI_CPPFLAGS) $(SOCI_SQLITE_CPPFLAGS)
	
AM_LDFLAGS = \
	$(BOOST_LDFLAGS) $(SOCI_LDFLAGS) $(SOCI_SQLITE_LDFLAGS)

libknowledge_la_SOURCES = \
	biomodel.h \
	chromosome.h \
	def.h \
	genegenemodel.h \
	genegenemodelarchive.h \
	group.h \
	groupmanager.h \
	groupmanagerdb.h \
	region.h \
	regioncontainer.h \
	regionmanager.h \
	regionmanagerdb.h \
	snpdataset.h \
	snpsnpmodel.h \
	biomodel.cpp \
	chromosome.cpp \
	def.cpp \
	genegenemodel.cpp \
	genegenemodelarchive.cpp \
	group.cpp \
	groupmanager.cpp \
	groupmanagerdb.cpp \
	region.cpp \
	regioncontainer.cpp \
	regionmanager.cpp \
	regionmanagerdb.cpp \
	snpdataset.cpp \
	snpsnpmodel.cpp
