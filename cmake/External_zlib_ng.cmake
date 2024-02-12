# zlib_ng

set(ZLIB_NG_VERSION "2.0.5")
set(ZLIB_NG_GIT_URL "https://github.com/zlib-ng/zlib-ng")

include(ExternalProject)
find_package(Git REQUIRED)


set(ZLIB_NG_BASE zlib_ng_ep)
set(ZLIB_NG_PREFIX ${DEPS_PREFIX}/${ZLIB_NG_BASE})
set(ZLIB_NG_BASE_DIR ${CMAKE_CURRENT_BINARY_DIR}/${ZLIB_NG_PREFIX})
set(ZLIB_NG_INSTALL_DIR ${ZLIB_NG_BASE_DIR}/install)
set(ZLIB_NG_INCLUDE_DIR ${ZLIB_NG_INSTALL_DIR}/include)
set(ZLIB_NG_LIB_DIR ${ZLIB_NG_INSTALL_DIR}/lib)

set(ZLIB_NG_STATIC_LIB ${ZLIB_NG_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}z-ng${CMAKE_STATIC_LIBRARY_SUFFIX})

ExternalProject_Add(${ZLIB_NG_BASE}
        PREFIX ${ZLIB_NG_BASE_DIR}
        INSTALL_DIR ${ZLIB_NG_INSTALL_DIR}
        GIT_REPOSITORY ${ZLIB_NG_GIT_URL}
        GIT_TAG ${ZLIB_NG_VERSION}
        GIT_PROGRESS ON
        GIT_SHALLOW ON
        UPDATE_DISCONNECTED TRUE
        BUILD_BYPRODUCTS ${ZLIB_NG_STATIC_LIB}
        CMAKE_ARGS
        -DCMAKE_INSTALL_MESSAGE=NEVER
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX=${ZLIB_NG_INSTALL_DIR}
        -DZLIB_ENABLE_TESTS=OFF
        )


file(MAKE_DIRECTORY ${ZLIB_NG_INCLUDE_DIR}) # Include directory needs to exist to run configure step

add_library(zlibstatic STATIC IMPORTED)
set_target_properties(zlibstatic PROPERTIES IMPORTED_LOCATION ${ZLIB_NG_STATIC_LIB})
target_include_directories(zlibstatic INTERFACE ${ZLIB_NG_INCLUDE_DIR})
add_dependencies(zlibstatic ${ZLIB_NG_BASE})


#showTargetProps(zlib_ng::zlib_ng)
