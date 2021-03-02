set(ZLIBEXTERNAL_VERSION "4a94dad2771bfb1fe5a169878623b4632ee0a134")
set(ZLIBEXTERNAL_GIT_URL "https://github.com/cloudflare/zlib.git")

include(ExternalProject)
find_package(Git REQUIRED)


set(ZLIBEXTERNAL_BASE zlib_ep)
set(ZLIBEXTERNAL_PREFIX ${DEPS_PREFIX}/${ZLIBEXTERNAL_BASE})
set(ZLIBEXTERNAL_BASE_DIR ${CMAKE_CURRENT_BINARY_DIR}/${ZLIBEXTERNAL_PREFIX})
set(ZLIBEXTERNAL_INSTALL_DIR ${ZLIBEXTERNAL_BASE_DIR}/install)
set(ZLIBEXTERNAL_INCLUDE_DIR ${ZLIBEXTERNAL_INSTALL_DIR}/include)
set(ZLIBEXTERNAL_LIB_DIR ${ZLIBEXTERNAL_INSTALL_DIR}/lib)

ExternalProject_Add(${ZLIBEXTERNAL_BASE}
        PREFIX ${ZLIBEXTERNAL_PREFIX}
        GIT_REPOSITORY ${ZLIBEXTERNAL_GIT_URL}
        GIT_TAG ${ZLIBEXTERNAL_VERSION}
        GIT_PROGRESS ON
        BUILD_IN_SOURCE 1
        CONFIGURE_COMMAND ${zlib_PREFIX}/src/zlib/configure --prefix=${ZLIBEXTERNAL_INSTALL_DIR} --static
        INSTALL_DIR ${ZLIBEXTERNAL_INSTALL_DIR}
##        INSTALL_DIR ${ZLIBEXTERNAL_INSTALL_DIR}
#        BUILD_COMMAND
#        ./configure --prefix=${ZLIBEXTERNAL_PREFIX} &&
#        make &&
#        make install
#        INSTALL_COMMAND ""
        )

file(MAKE_DIRECTORY ${ZLIBEXTERNAL_INSTALL_DIR}) # Include directory needs to exist to run configure step

add_executable(zlib_external IMPORTED)
set_target_properties(zlib_external PROPERTIES IMPORTED_LOCATION ${ZLIBEXTERNAL_LIB_DIR}/libz.a)
target_include_directories(zlib_external INTERFACE ${ZLIBEXTERNAL_INCLUDE_DIR})
add_dependencies(zlib_external ${ZLIBEXTERNAL_BASE})

showTargetProps(zlib_external)