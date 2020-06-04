# ANTLR

set(ANTLR_VERSION "4.8")
set(ANTLR_GIT_URL "https://github.com/antlr/antlr4.git")


include(ExternalProject)
find_package(Git REQUIRED)


set(ANTLR_BASE antlr_ep)
set(ANTLR_PREFIX ${DEPS_PREFIX}/${ANTLR_BASE})
set(ANTLR_BASE_DIR ${CMAKE_CURRENT_BINARY_DIR}/${ANTLR_PREFIX})
set(ANTLR_INSTALL_DIR ${ANTLR_BASE_DIR}/install)
set(ANTLR_INCLUDE_DIR ${ANTLR_INSTALL_DIR}/include/antlr4-runtime)
set(ANTLR_LIB_DIR ${ANTLR_INSTALL_DIR}/lib)
set(ANTLR_RUNTIME_SHARED_LIB ${ANTLR_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}antlr4-runtime${CMAKE_SHARED_LIBRARY_SUFFIX})
set(ANTLR_RUNTIME_STATIC_LIB ${ANTLR_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}antlr4-runtime${CMAKE_STATIC_LIBRARY_SUFFIX})


ExternalProject_Add(${ANTLR_BASE}
        PREFIX ${ANTLR_BASE_DIR}
        GIT_REPOSITORY ${ANTLR_GIT_URL}
        GIT_TAG ${ANTLR_VERSION}
        GIT_PROGRESS ON
        GIT_SHALLOW ON
        UPDATE_DISCONNECTED TRUE
        SOURCE_SUBDIR runtime/Cpp
        INSTALL_DIR ${ANTLR_INSTALL_DIR}
        BUILD_BYPRODUCTS ${ANTLR_RUNTIME_SHARED_LIB} ${ANTLR_RUNTIME_STATIC_LIB}
        CMAKE_ARGS
        -DWITH_LIBCXX:BOOL=OFF
        -DCMAKE_INSTALL_MESSAGE=NEVER
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX=${ANTLR_INSTALL_DIR}
        )


file(MAKE_DIRECTORY ${ANTLR_INCLUDE_DIR}) # Include directory needs to exist to run configure step


add_library(antlr4_shared SHARED IMPORTED)
set_target_properties(antlr4_shared PROPERTIES IMPORTED_LOCATION ${ANTLR_RUNTIME_SHARED_LIB})
target_include_directories(antlr4_shared INTERFACE ${ANTLR_INCLUDE_DIR})
add_dependencies(antlr4_shared ${ANTLR_BASE})

add_library(antlr4_static STATIC IMPORTED)
set_target_properties(antlr4_static PROPERTIES IMPORTED_LOCATION ${ANTLR_RUNTIME_STATIC_LIB})
target_include_directories(antlr4_static INTERFACE ${ANTLR_INCLUDE_DIR})
add_dependencies(antlr4_static ${ANTLR_BASE})


#showTargetProps(antlr4_shared)
#showTargetProps(antlr4_static)
