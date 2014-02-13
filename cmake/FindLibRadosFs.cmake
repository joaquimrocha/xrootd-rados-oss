FIND_PATH(RADOS_FS_INCLUDE_DIR radosfs.hh
  HINTS
  $ENV{XROOTD_DIR}
  /usr
  /opt/radosfs/
  PATH_SUFFIXES include/radosfs
  PATHS /opt/radosfs
)

FIND_LIBRARY(RADOS_FS_LIB libradosfs radosfs
  HINTS
  /usr
  /opt/radosfs/
  PATH_SUFFIXES lib
  lib64
/usr/lib64
  lib/radosfs/
  lib64/radosfs/
)

# GET_FILENAME_COMPONENT( XROOTD_LIB_DIR ${XROOTD_UTILS} PATH )

INCLUDE( FindPackageHandleStandardArgs )
FIND_PACKAGE_HANDLE_STANDARD_ARGS( RadosFs DEFAULT_MSG
                                        RADOS_FS_LIB
                                        RADOS_FS_INCLUDE_DIR )
