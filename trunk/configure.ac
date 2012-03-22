#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT(biofilter, 1.1.0, software@ritchielab.psu.edu)
AM_INIT_AUTOMAKE([1.8 -Wall subdir-objects foreign])
AC_CONFIG_SRCDIR([src/biofilter/main.cpp])
AC_CONFIG_HEADER([config.h])

RL_CONFIGURE([1.46],[HAVE_BOOST HAVE_BOOST_FILESYSTEM HAVE_BOOST_SYSTEM HAVE_BOOST_REGEX HAVE_SQLITE HAVE_SOCI HAVE_SOCI_SQLITE])

AC_ARG_WITH(mpi, [AS_HELP_STRING([--with-mpi], [
	compile with MPI (parallelization) support. If none is found, MPI is not used.
	Default: auto
])],,[with_mpi=auto])


# Checks for programs.
AX_PROG_CXX_MPI([test x"$with_mpi" != xno],[
	AC_DEFINE(HAVE_MPI,1,[Define to 1 if you have the mpi library (-lmpi).])
	AC_DEFINE(USE_MPI,1,[Define to 1 to enable parallelization support using MPI.])
],[
	if test x"$with_mpi" = xyes; then
		AC_MSG_ERROR([MPI was requested but could not be found])
	else
		AC_MSG_WARN([MPI compiler could not be found; parallelization support will be disabled])
	fi
])


AC_CONFIG_FILES([
		Makefile
		src/utility/Makefile
		src/knowledge/Makefile
		src/ldspline/Makefile
		src/liftover/Makefile
		src/biofilter/Makefile
])

AC_OUTPUT
