# ===========================================================================
#       http://www.gnu.org/software/autoconf-archive/ax_soci_base.html
# ===========================================================================
#
# SYNOPSIS
#
#   AX_SOCI_SQLITE([ACTION-IF-FOUND], [ACTION-IF-NOT-FOUND])
#
# DESCRIPTION
#
#   Test for the Boost C++ libraries of a particular version (or newer)
#
#   If no path to the installed soci library is given the macro searchs
#   under /usr, /usr/local, /opt and /opt/local and evaluates the
#   $SOCI_ROOT environment variable. Further documentation is available at
#   <http://randspringer.de/soci/index.html>.
#
#   This macro calls:
#
#     AC_SUBST(SOCI_SQLITE_CPPFLAGS) / AC_SUBST(SOCI_SQLITE_LDFLAGS)
#
#   And sets:
#
#     HAVE_SOCI_SQLITE
#
# LICENSE
#
#   Copyright (c) 2008 Thomas Porschberg <thomas@randspringer.de>
#   Copyright (c) 2009 Peter Adolphs
#
#   Copying and distribution of this file, with or without modification, are
#   permitted in any medium without royalty provided the copyright notice
#   and this notice are preserved. This file is offered as-is, without any
#   warranty.

#serial 20

AC_DEFUN([AX_SOCI_SQLITE],
[
AC_ARG_WITH([soci-sqlite],
  [AS_HELP_STRING([--with-soci-sqlite@<:@=ARG@:>@],
    [use SOCI SQLite backend from a standard location (ARG=yes),
     from the specified location (ARG=<path>),
     or disable it (ARG=no)
     @<:@ARG=yes@:>@ ])],
    [
    if test "$withval" = "no"; then
        want_soci_sqlite="no"
    elif test "$withval" = "yes"; then
        want_soci_sqlite="yes"
        ac_soci_sqlite_path=""
    else
        want_soci_sqlite="yes"
        ac_soci_sqlite_path="$withval"
    fi
    ],
    [want_soci_sqlite="yes"])


AC_ARG_WITH([soci-sqlite-libdir],
        AS_HELP_STRING([--with-soci-sqlite-libdir=LIB_DIR],
        [Force given directory for soci libraries. Note that this will override library path detection, so use this parameter only if default library detection fails and you know exactly where your soci libraries are located.]),
        [
        if test -d "$withval"
        then
                ac_soci_sqlite_lib_path="$withval"
        else
                AC_MSG_ERROR(--with-soci-sqlite-libdir expected directory name)
        fi
        ],
        [ac_soci_sqlite_lib_path=""]
)


if test "x$want_soci_sqlite" = "xyes"; then

	PKG_CHECK_MODULES([SQLITE],[sqlite3 >= 3.3],[],[])

    AC_MSG_CHECKING(for SOCI SQLite backend)
    AC_REQUIRE([AC_PROG_CXX])
    AC_LANG_PUSH(C++)
    include_succeed=no
    link_succeed=no

	dnl Check for lib64 in appropriate systems
	
    libsubdirs="lib"
    ax_arch=`uname -m`
    if test $ax_arch = x86_64 -o $ax_arch = ppc64 -o $ax_arch = s390x -o $ax_arch = sparc64; then
        libsubdirs="lib64 lib lib64"
    fi

	dnl List all the standard search paths here
	std_paths="/usr /usr/local /opt /opt/local"
	
	if test "$ac_soci_sqlite_path" != ""; then
		if test -d "$ac_soci_sqlite_path/include/soci/sqlite3" && test -r "$ac_soci_sqlite_path/include/soci/sqlite3"; then
			SOCI_SQLITE_CPPFLAGS="-I$ac_soci_sqlite_path/include/soci/sqlite3"
		else
			SOCI_SQLITE_CPPFLAGS="-I$ac_soci_sqlite_path/include"
		fi
	else
		
		SOCI_SQLITE_CPPFLAGS=""
		
		dnl Check for location relative to SOCI_CPPFLAGS
		if test "$SOCI_CPPFLAGS" != ""; then
			ac_soci_incl_dir="`echo $SOCI_CPPFLAGS | sed 's/^-I//g'`"
			if test -r "$ac_soci_incl_dir/sqlite3/soci-sqlite3.h"; then
				SOCI_SQLITE_CPPFLAGS="-I$ac_soci_incl_dir/sqlite3/soci-sqlite3.h"
			elif test -r "$ac_soci_incl_dir/soci-sqlite3.h"; then
				SOCI_SQLITE_CPPFLAGS="-I$ac_soci_incl_dir"
			fi
		fi
		
	   	dnl Check for the location now
		CPPFLAGS_SAVED="$CPPFLAGS"
    	CPPFLAGS="$CPPFLAGS $SQLITE_CPPFLAGS $SOCI_CPPFLAGS $SOCI_SQLITE_CPPFLAGS"
		export CPPFLAGS

		AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[@%:@include <soci.h>
											 @%:@include <soci-sqlite3.h>
										   ]], [[]])],[include_succeed=yes],[])

        CPPFLAGS="$CPPFLAGS_SAVED"
	    
	    if test "x$include_succeed" != "xyes"; then
    	   	dnl Check the CPLUS_INCLUDE_PATH as well as /usr /usr/local /opt /opt/local                
       		for ac_soci_sqlite_path_tmp in `echo $CPLUS_INCLUDE_PATH | sed 's/:/ /g' | sed 's/\/include\/* */ /g'` "$std_paths" ; do
           		if test -r "$ac_soci_sqlite_path_tmp/include/soci/sqlite3/soci-sqlite3.h"; then
   		        	SOCI_SQLITE_CPPFLAGS="-I$ac_soci_sqlite_path_tmp/include/soci/sqlite3"
   		        	break;
   		        elif test -r "$ac_soci_sqlite_path_tmp/include/soci-sqlite3.h"; then
   		        	SOCI_SQLITE_CPPFLAGS="-I$ac_soci_sqlite_path_tmp/include"
	           		break;
    	        fi
       		done
			CPPFLAGS_SAVED="$CPPFLAGS"
    		CPPFLAGS="$CPPFLAGS $SQLITE_CPPFLAGS $SOCI_CPPFLAGS $SOCI_SQLITE_CPPFLAGS"
			export CPPFLAGS       		
		AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[@%:@include <soci.h>
											 @%:@include <soci-sqlite3.h>
										   ]], [[]])],[include_succeed=yes],[])
       	fi
    fi
    
    SOCI_SQLITE_LDFLAGS=""
    SOCI_SQLITE_LIB="-lsoci_sqlite3"
        
    dnl overwrite ld flags if we have required special directory with
    dnl --with-soci-libdir parameter
    if test "$ac_soci_sqlite_lib_path" != ""; then
       SOCI_SQLITE_LDFLAGS="-L$ac_soci_sqlite_lib_path"
    else
    	if test "$ac_soci_sqlite_path" != ""; then
	    	for libsubdir in $libsubdirs ; do
    	   		if ls "$ac_soci_path/$libsubdir/libsoci_sqlite3"* >/dev/null 2>&1 ; then 
        			SOCI_SQLITE_LDFLAGS="-L$ac_soci_path/$libsubdir"
        			break;
        		fi
            done
        else

		    LDFLAGS_SAVED="$LDFLAGS"
		    LDFLAGS="$LDFLAGS $SQLITE_LDFLAGS $SOCI_LDFLAGS $SOCI_LIB $SOCI_SQLITE_LDFLAGS $SOCI_SQLITE_LIB"
		    export LDFLAGS
    

			dnl Check if we can link against the SOCI libraries
			AC_LINK_IFELSE(
				[AC_LANG_PROGRAM([#include <soci.h>
								  #include <soci-sqlite3.h>],
		    		[soci::session dummy(soci::sqlite3,"")])],
  				[link_succeed=yes],
  				[]
		  	)
		  	
		  	LDFLAGS="$LDFLAGS_SAVED"
  	
		  	if test "x$link_succeed" != "xyes"; then
  		
  				dnl Check the LPATH as well as the standards above
			  	for ac_soci_sqlite_path_tmp in `echo $LPATH | sed 's/:/ /g' | sed 's/\/lib\(64\)*\/* */ /g'` "$std_paths" ; do
					for libsubdir in $libsubdirs ; do
        				if ls "$ac_soci_sqlite_path_tmp/$libsubdir/libsoci_sqlite3"* >/dev/null 2>&1 ; then 
		        			SOCI_SQLITE_LDFLAGS="-L$ac_soci_sqlite_path_tmp/$libsubdir"
        					found_lib="yes"
        					break;
		        		fi
        		    done
		            if test "x$found_lib" = "xyes"; then break; fi
        		done
        		
       		    LDFLAGS="$LDFLAGS $SQLITE_LDFLAGS $SOCI_LDFLAGS $SOCI_LIB $SOCI_SQLITE_LDFLAGS $SOCI_SQLITE_LIB"
				export LDFLAGS
				dnl Check if we can link against the SOCI libraries
				AC_LINK_IFELSE(
					[AC_LANG_PROGRAM([#include <soci.h>
									  #include <soci-sqlite3.h>],
    					[soci::session dummy(soci::sqlite3,"")])],
	  				[link_succeed=yes],
  		[]
	)
  	
        	fi
        fi
	fi
	

    if test "x$include_succeed" = "xyes" && test "x$link_succeed" = "xyes"; then
    	AC_MSG_RESULT(yes)
    	AC_SUBST(SOCI_SQLITE_CPPFLAGS)
        AC_SUBST(SOCI_SQLITE_LDFLAGS)
        AC_SUBST(SOCI_SQLITE_LIB)
        AC_DEFINE(HAVE_SOCI_SQLITE,,[define if the soci SQLite backend is available])
        ifelse([$1], , :, [$1])
    else
    	AC_MSG_RESULT(no)
    	ifelse([$2], , :, [$2])
    fi
    
    AC_LANG_POP([C++])
    CPPFLAGS="$CPPFLAGS_SAVED"
    LDFLAGS="$LDFLAGS_SAVED"
fi

])
