###########################################################################
# Genetics Library used for Ritchie Lab software 
#
# This library contains various generic classes and tools used by several 
# applications produced at the Ritchie Lab. 
#
#
# 
project (Random)
include_directories (${Random_SOURCE_DIR}/..)
set (RandomFiles 
	mersenne.cpp 
)

add_library (random ${RandomFiles})
SET_TARGET_PROPERTIES( random PROPERTIES DEBUG_POSTFIX d)
