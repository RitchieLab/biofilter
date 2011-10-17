###############################################################################
# CMake module to search for SQLite 3 library
#
# On success, the macro sets the following variables:
# SQLITE3_FOUND = if the library found
# SQLITE3_LIBRARY = full path to the library
# SSQLITE3_INCLUDE_DIR = where to find the library headers
#
# Copyright (c) 2009 Mateusz Loskot <mateusz@loskot.net>
#
# Redistribution and use is allowed according to the terms of the BSD license.
# For details see the accompanying COPYING-CMAKE-SCRIPTS file.
#
###############################################################################
#MESSAGE(STATUS "Searching for SpatialIndex ${SpatialIndex_FIND_VERSION}+ library")
#MESSAGE(STATUS " NOTE: Required version is not checked - to be implemented")
 
IF(SQLITE3_INCLUDE_DIR AND SQLITE3_LIBRARY)
    # Already in cache, be silent
    SET(SQLITE3_FIND_QUIETLY TRUE)
ELSE()
 
  IF(WIN32)
    SET(OSGEO4W_IMPORT_LIBRARY sqlite3_i)
    IF(DEFINED ENV{OSGEO4W_ROOT})
        SET(OSGEO4W_ROOT_DIR $ENV{OSGEO4W_ROOT})
        MESSAGE(STATUS "Trying OSGeo4W using environment variable OSGEO4W_ROOT=$ENV{OSGEO4W_ROOT}")
    ELSE()
        SET(OSGEO4W_ROOT_DIR c:/OSGeo4W)
        MESSAGE(STATUS "Trying OSGeo4W using default location OSGEO4W_ROOT=${OSGEO4W_ROOT_DIR}")
    ENDIF()
  ENDIF()
 
  FIND_PATH(SQLITE3_INCLUDE_DIR
    NAMES sqlite3.h
    PATH_PREFIXES sqlite sqlite3
    PATHS
    /usr/include
    /usr/local/include
    $ENV{LIB_DIR}/include
    $ENV{LIB_DIR}/include/sqlite
    $ENV{LIB_DIR}/include/sqlite3
    $ENV{ProgramFiles}/SQLite/*/include
    $ENV{ProgramFiles}/SQLite3/*/include
    $ENV{SystemDrive}/SQLite/*/include
    $ENV{SystemDrive}/SQLite3/*/include
    ${OSGEO4W_ROOT_DIR}/include)
 
  SET(SQLITE3_NAMES ${OSGEO4W_IMPORT_LIBRARY} sqlite3)
  FIND_LIBRARY(SQLITE3_LIBRARY
    NAMES sqlite3
    PATHS
    /usr/lib
    /usr/local/lib
    $ENV{LIB_DIR}/lib
    $ENV{ProgramFiles}/SQLite/*/lib
    $ENV{ProgramFiles}/SQLite3/*/lib
    $ENV{SystemDrive}/SQLite/*/lib
    $ENV{SystemDrive}/SQLite3/*/lib
    ${OSGEO4W_ROOT_DIR}/lib)
 
  message(STATUS ${SQLITE3_LIBRARY})
  # Handle the QUIETLY and REQUIRED arguments and set SQLITE3_FOUND to TRUE
  # if all listed variables are TRUE
  INCLUDE(FindPackageHandleStandardArgs)
  FIND_PACKAGE_HANDLE_STANDARD_ARGS(SQLITE3 DEFAULT_MSG SQLITE3_LIBRARY SQLITE3_INCLUDE_DIR)
 
  # TODO: Do we want to mark these as advanced? --mloskot
  # http://www.cmake.org/cmake/help/cmake2.6docs.html#command:mark_as_advanced
  #MARK_AS_ADVANCED(SQLITE3_LIBRARY SQLITE3_INCLUDE_DIR)

ENDIF()
