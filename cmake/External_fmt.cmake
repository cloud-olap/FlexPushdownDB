# fmt

set(FMT_VERSION "6.2.0")
set(FMT_GIT_URL "https://github.com/fmtlib/fmt")

include(ExternalProject)
find_package(Git REQUIRED)


set(FMT_BASE fmt_ep)
set(FMT_PREFIX ${DEPS_PREFIX}/${FMT_BASE})
set(FMT_BASE_DIR ${CMAKE_CURRENT_BINARY_DIR}/${FMT_PREFIX})
set(FMT_INSTALL_DIR ${FMT_BASE_DIR}/install)
set(FMT_INCLUDE_DIR ${FMT_INSTALL_DIR}/include)
set(FMT_LIB_DIR ${FMT_INSTALL_DIR}/lib)
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(FMT_STATIC_LIB ${FMT_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}fmtd${CMAKE_STATIC_LIBRARY_SUFFIX})
else()
    set(FMT_STATIC_LIB ${FMT_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}fmt${CMAKE_STATIC_LIBRARY_SUFFIX})
endif()


ExternalProject_Add(${FMT_BASE}
        PREFIX ${FMT_BASE_DIR}
        INSTALL_DIR ${FMT_INSTALL_DIR}
        GIT_REPOSITORY ${FMT_GIT_URL}
        GIT_TAG ${FMT_VERSION}
        GIT_PROGRESS ON
        GIT_SHALLOW ON
        UPDATE_DISCONNECTED TRUE
        BUILD_BYPRODUCTS ${FMT_STATIC_LIB}
        CMAKE_ARGS
        -DCMAKE_POSITION_INDEPENDENT_CODE=TRUE
        -DCMAKE_INSTALL_MESSAGE=NEVER
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX=${FMT_INSTALL_DIR}
        )


file(MAKE_DIRECTORY ${FMT_INCLUDE_DIR}) # Include directory needs to exist to run configure step


add_library(fmt::fmt STATIC IMPORTED)
set_target_properties(fmt::fmt PROPERTIES IMPORTED_LOCATION ${FMT_STATIC_LIB})
target_include_directories(fmt::fmt INTERFACE ${FMT_INCLUDE_DIR})
add_dependencies(fmt::fmt ${FMT_BASE})


#showTargetProps(fmt::fmt)
