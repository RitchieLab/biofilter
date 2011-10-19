#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT(biofilter, 1.0.0, bugs@ritchielab.psu.edu)
AM_INIT_AUTOMAKE([1.8 -Wall subdir-objects foreign])
AC_CONFIG_SRCDIR([src/biofilter/main.cpp])
#AC_CONFIG_HEADER([config.h])


# Checks for configuration options.

AX_CHECK_ENABLE_DEBUG

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
AC_PROG_LIBTOOL


# Checks for libraries.

AX_BOOST_BASE(1.46, [], AC_MSG_ERROR(boost >= 1.46 is required))
AX_BOOST_REGEX
AX_BOOST_SYSTEM
AX_BOOST_FILESYSTEM
PKG_CHECK_MODULES([SQLITE],[sqlite3 >= 3.3], [AC_DEFINE([HAVE_SQLITE],[],[SQLite is available])])
AX_SOCI_CORE
AX_SOCI_SQLITE


# Checks for header files.

AC_HEADER_STDC
AC_HEADER_STDBOOL
AC_CHECK_HEADERS(dlfcn.h getopt.h inttypes.h math.h memory.h stdbool.h stdint.h stdlib.h string.h strings.h sys/stat.h sys/time.h sys/types.h unistd.h)


# Checks for typedefs, structures, and compiler characteristics.

AC_C_CONST
AC_C_INLINE
AC_TYPE_SIZE_T


# Checks for library functions.

AC_CHECK_FUNCS(floor getopt_long memset pow round sqrt)


AC_CONFIG_FILES([
		Makefile
		src/utility/Makefile
		src/knowledge/Makefile
		src/ldspline/Makefile
		src/liftover/Makefile
		src/biofilter/Makefile
])

AC_OUTPUT
