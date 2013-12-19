find_package( XRootD REQUIRED )
find_package( LibRados REQUIRED )

add_library( RadosOss SHARED
             RadosOss.cc RadosOss.hh
             RadosOssFile.cc RadosOssFile.hh
             DirInfo.cc DirInfo.hh
             RadosOssDefines.hh
             hash64.c hash64.h
)

include_directories( ${XROOTD_INCLUDE_DIR} ${RADOS_INCLUDE_DIR} )

add_definitions( -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 )

target_link_libraries( RadosOss ${RADOS_LIB} )

if( Linux )
  set_target_properties( RadosOss PROPERTIES
    VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}
    SOVERSION ${VERSION_MAJOR}
    CLEAN_DIRECT_OUTPUT 1
  )
endif( Linux )

install( TARGETS RadosOss LIBRARY DESTINATION ${LIB_INSTALL_DIR} )
