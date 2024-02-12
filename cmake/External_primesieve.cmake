# Primesieve

set(PRIMESIEVE_VERSION "v7.5")
set(PRIMESIEVE_GIT_URL "https://github.com/kimwalisch/primesieve.git")

include(ExternalProject)
find_package(Git REQUIRED)


set(PRIMESIEVE_BASE primesieve_ep)
set(PRIMESIEVE_PREFIX ${DEPS_PREFIX}/${PRIMESIEVE_BASE})
set(PRIMESIEVE_BASE_DIR ${CMAKE_CURRENT_BINARY_DIR}/${PRIMESIEVE_PREFIX})
set(PRIMESIEVE_INSTALL_DIR ${PRIMESIEVE_BASE_DIR}/install)
set(PRIMESIEVE_INCLUDE_DIR ${PRIMESIEVE_INSTALL_DIR}/include)
set(PRIMESIEVE_LIB_DIR ${PRIMESIEVE_INSTALL_DIR}/lib)
set(PRIMESIEVE_STATIC_LIB ${PRIMESIEVE_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}primesieve${CMAKE_STATIC_LIBRARY_SUFFIX})
set(PRIMESIEVE_SHARED_LIB ${PRIMESIEVE_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}primesieve${CMAKE_SHARED_LIBRARY_SUFFIX})


ExternalProject_Add(${PRIMESIEVE_BASE}
        PREFIX ${PRIMESIEVE_BASE_DIR}
        INSTALL_DIR ${PRIMESIEVE_INSTALL_DIR}
        GIT_REPOSITORY ${PRIMESIEVE_GIT_URL}
        GIT_TAG ${PRIMESIEVE_VERSION}
        GIT_PROGRESS ON
        GIT_SHALLOW ON
        UPDATE_DISCONNECTED TRUE
        BUILD_BYPRODUCTS ${PRIMESIEVE_SHARED_LIB} ${PRIMESIEVE_STATIC_LIB}
        CMAKE_ARGS
        -DBUILD_PRIMESIEVE=OFF
        -DBUILD_SHARED_LIBS=ON
        -DBUILD_STATIC_LIBS=ON
        -DBUILD_DOC=OFF
        -DBUILD_MANPAGE=OFF
        -DBUILD_EXAMPLES=OFF
        -DBUILD_TESTS=OFF
        -DCMAKE_INSTALL_MESSAGE=NEVER
#        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX=${PRIMESIEVE_INSTALL_DIR}
        )


file(MAKE_DIRECTORY ${PRIMESIEVE_INCLUDE_DIR}) # Include directory needs to exist to run configure step

add_library(primesieve_shared SHARED IMPORTED)
set_target_properties(primesieve_shared PROPERTIES IMPORTED_LOCATION "${PRIMESIEVE_SHARED_LIB}")
target_include_directories(primesieve_shared INTERFACE "${PRIMESIEVE_INCLUDE_DIR}")
target_link_libraries(primesieve_shared INTERFACE pthread)
add_dependencies(primesieve_shared ${PRIMESIEVE_BASE})

add_library(primesieve_static STATIC IMPORTED)
set_target_properties(primesieve_static PROPERTIES IMPORTED_LOCATION "${PRIMESIEVE_STATIC_LIB}")
target_include_directories(primesieve_static INTERFACE "${PRIMESIEVE_INCLUDE_DIR}")
target_link_libraries(primesieve_static INTERFACE pthread)
add_dependencies(primesieve_static ${PRIMESIEVE_BASE})


#showTargetProps(primesieve_static)
