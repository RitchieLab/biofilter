#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT(biofilter, 2, bugs@ritchielab.psu.edu)

AM_INIT_AUTOMAKE([1.8 foreign])

AC_CONFIG_SRCDIR([src/biofilter/main.cpp])
AC_CONFIG_HEADER([config.h])

RL_CONFIGURE([1.46], [HAVE_BOOST HAVE_BOOST_FILESYSTEM HAVE_BOOST_SYSTEM HAVE_BOOST_REGEX HAVE_SOCI HAVE_SOCI_SQLITE])

AC_CONFIG_FILES([
		Makefile
		src/utility/Makefile
		src/knowledge/Makefile
		src/ldspline/Makefile
		src/liftover/Makefile
		src/biofilter/Makefile
])

AC_OUTPUT
