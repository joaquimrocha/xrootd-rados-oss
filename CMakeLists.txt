#-------------------------------------------------------------------------------
# Project description
#-------------------------------------------------------------------------------
set( VERSION_MAJOR "0" )
set( VERSION_MINOR "1" )
set( VERSION_PATCH "0")

cmake_minimum_required( VERSION 2.6 )
project( RadosOss )

set( CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/src ${PROJECT_SOURCE_DIR}/cmake )

if( MacOSX )
  set( LIB_INSTALL_DIR lib )
else( MacOSX )
  set( LIB_INSTALL_DIR lib64 )
endif( MacOSX )

add_subdirectory( src )
