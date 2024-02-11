# Arrow
set(ARROW_VERSION "release-4.0.0")
set(ARROW_GIT_URL "https://github.com/Yifei-yang7/arrow.git")


include(ExternalProject)
find_package(Git REQUIRED)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)

set(ARROW_BASE arrow_ep)
set(ARROW_PREFIX ${DEPS_PREFIX}/${ARROW_BASE})
set(ARROW_BASE_DIR ${CMAKE_CURRENT_BINARY_DIR}/${ARROW_PREFIX})
set(ARROW_INSTALL_DIR ${ARROW_BASE_DIR}/install)
set(ARROW_INCLUDE_DIR ${ARROW_INSTALL_DIR}/include)
set(ARROW_LIB_DIR ${ARROW_INSTALL_DIR}/lib)
set(ARROW_CORE_SHARED_LIBS ${ARROW_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}arrow${CMAKE_SHARED_LIBRARY_SUFFIX})
set(ARROW_CORE_STATIC_LIBS ${ARROW_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}arrow${CMAKE_STATIC_LIBRARY_SUFFIX})
#set(ARROW_RE2_BASE_DIR ${ARROW_BASE_DIR}/src/${ARROW_BASE}-build/re2_ep-install)
#set(ARROW_RE2_INCLUDE_DIR ${ARROW_RE2_BASE_DIR}/include)
#set(ARROW_RE2_STATIC_LIB ${ARROW_RE2_BASE_DIR}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}re2${CMAKE_STATIC_LIBRARY_SUFFIX})
set(ARROW_GANDIVA_SHARED_LIB ${ARROW_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}gandiva${CMAKE_SHARED_LIBRARY_SUFFIX})
set(ARROW_GANDIVA_STATIC_LIB ${ARROW_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}gandiva${CMAKE_STATIC_LIBRARY_SUFFIX})
#set(ARROW_THRIFT_BASE_DIR ${ARROW_BASE_DIR}/src/${ARROW_BASE}-build/thrift_ep/src/thrift_ep-install)
#set(ARROW_THRIFT_INCLUDE_DIR ${ARROW_THRIFT_BASE_DIR}/include)
#if(${CMAKE_BUILD_TYPE} STREQUAL "Debug")
#    set(ARROW_THRIFT_STATIC_LIB ${ARROW_THRIFT_BASE_DIR}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}thriftd${CMAKE_STATIC_LIBRARY_SUFFIX})
#else()
#    set(ARROW_THRIFT_STATIC_LIB ${ARROW_THRIFT_BASE_DIR}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}thrift${CMAKE_STATIC_LIBRARY_SUFFIX})
#endif()
#set(ARROW_SNAPPY_BASE_DIR ${ARROW_BASE_DIR}/src/${ARROW_BASE}-build/snappy_ep/src/snappy_ep-install)
#set(ARROW_SNAPPY_INCLUDE_DIR ${ARROW_SNAPPY_BASE_DIR}/include)
#set(ARROW_SNAPPY_STATIC_LIB ${ARROW_SNAPPY_BASE_DIR}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}snappy${CMAKE_STATIC_LIBRARY_SUFFIX})
set(ARROW_PARQUET_SHARED_LIB ${ARROW_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}parquet${CMAKE_SHARED_LIBRARY_SUFFIX})
set(ARROW_PARQUET_STATIC_LIB ${ARROW_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}parquet${CMAKE_STATIC_LIBRARY_SUFFIX})
#set(ARROW_JEMALLOC_BASE_DIR ${ARROW_BASE_DIR}/src/${ARROW_BASE}-build/jemalloc_ep-prefix/src/jemalloc_ep)
#set(ARROW_JEMALLOC_INCLUDE_DIR ${ARROW_JEMALLOC_BASE_DIR}/include)
#set(ARROW_JEMALLOC_STATIC_LIB ${ARROW_JEMALLOC_BASE_DIR}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}jemalloc${CMAKE_STATIC_LIBRARY_SUFFIX})
set(ARROW_DEPENDENCIES_SHARED_LIBS ${ARROW_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}arrow_bundled_dependencies${CMAKE_SHARED_LIBRARY_SUFFIX})
set(ARROW_DEPENDENCIES_STATIC_LIBS ${ARROW_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}arrow_bundled_dependencies${CMAKE_STATIC_LIBRARY_SUFFIX})

# need to set llvm dir on mac, making it a duplicated arg as default
# TODO: probably can do more elegantly? but didn't find a way to set to empty string...
set(arg_llvm_dir -DARROW_USE_CCACHE:BOOL=ON)
if (${APPLE})
  set(arg_llvm_dir -DLLVM_DIR=/usr/local/opt/llvm@11)
endif()
ExternalProject_Add(${ARROW_BASE}
        PREFIX ${ARROW_PREFIX}
        GIT_REPOSITORY ${ARROW_GIT_URL}
        GIT_TAG ${ARROW_VERSION}
        GIT_PROGRESS ON
        GIT_SHALLOW ON
#        UPDATE_DISCONNECTED TRUE
        SOURCE_SUBDIR cpp
        INSTALL_DIR ${ARROW_INSTALL_DIR}
        BUILD_BYPRODUCTS
        ${ARROW_CORE_SHARED_LIBS}
        ${ARROW_CORE_STATIC_LIBS}
#        ${ARROW_JEMALLOC_STATIC_LIB}
#        ${ARROW_RE2_STATIC_LIB}
        ${ARROW_GANDIVA_SHARED_LIB}
        ${ARROW_GANDIVA_STATIC_LIB}
#        ${ARROW_THRIFT_STATIC_LIB}
#        ${ARROW_SNAPPY_STATIC_LIB}
        ${ARROW_PARQUET_SHARED_LIB}
        ${ARROW_PARQUET_STATIC_LIB}
        ${ARROW_DEPENDENCIES_SHARED_LIBS}
        ${ARROW_DEPENDENCIES_STATIC_LIBS}
        CMAKE_ARGS
        ${arg_llvm_dir}
        -DARROW_USE_CCACHE:BOOL=ON
        -DARROW_CSV:BOOL=ON
        -DARROW_DATASET:BOOL=OFF
        -DARROW_FLIGHT:BOOL=OFF
        -DARROW_IPC:BOOL=OFF
        -DARROW_PARQUET:BOOL=ON
        -DARROW_WITH_SNAPPY:BOOL=ON
        -DARROW_WITH_ZLIB:BOOL=ON
        -DARROW_JEMALLOC:BOOL=ON
        -DARROW_GANDIVA:BOOL=ON
        -DARROW_DEPENDENCY_SOURCE=BUNDLED
        -DCMAKE_INSTALL_MESSAGE=NEVER
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        -DCMAKE_CXX_EXTENSIONS=OFF
        -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX=${ARROW_INSTALL_DIR}
        )


#file(MAKE_DIRECTORY ${ARROW_JEMALLOC_INCLUDE_DIR}) # Include directory needs to exist to run configure step
#file(MAKE_DIRECTORY ${ARROW_RE2_INCLUDE_DIR}) # Include directory needs to exist to run configure step
#file(MAKE_DIRECTORY ${ARROW_THRIFT_INCLUDE_DIR}) # Include directory needs to exist to run configure step
#file(MAKE_DIRECTORY ${ARROW_SNAPPY_INCLUDE_DIR}) # Include directory needs to exist to run configure step
file(MAKE_DIRECTORY ${ARROW_INCLUDE_DIR}) # Include directory needs to exist to run configure step

## Needed by the re2 find_package module
#include(FindPkgConfig)
#
## Add the installed arrow find_package modules
#set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${ARROW_LIB_DIR}/cmake/arrow)
#
## Add the arrow source find_package modules
#set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${ARROW_BASE_DIR}/src/arrow_ep/cpp/cmake_modules)
#
## Add the path to the re2 install dir needed by the re2 find_package module
#set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${ARROW_BASE_DIR}/src/arrow_ep-build/re2_ep-install)
#
## Find the arrow and gandiva packages
#find_package(Threads REQUIRED)
#find_package(LLVM REQUIRED)
#find_package(RE2 REQUIRED)
#find_package(Arrow REQUIRED)
#find_package(Gandiva REQUIRED)

add_library(arrow_bundled_dependencies_static STATIC IMPORTED)
set_target_properties(arrow_bundled_dependencies_static PROPERTIES IMPORTED_LOCATION ${ARROW_DEPENDENCIES_STATIC_LIBS})
add_dependencies(arrow_bundled_dependencies_static ${ARROW_BASE})

#add_library(jemalloc_static STATIC IMPORTED)
#set_target_properties(jemalloc_static PROPERTIES IMPORTED_LOCATION ${ARROW_JEMALLOC_STATIC_LIB})
#target_include_directories(jemalloc_static INTERFACE ${ARROW_JEMALLOC_INCLUDE_DIR})
#target_link_libraries(jemalloc_static INTERFACE pthread)
#add_dependencies(jemalloc_static ${ARROW_BASE})

#add_library(re2_static STATIC IMPORTED)
#set_target_properties(re2_static PROPERTIES IMPORTED_LOCATION ${ARROW_RE2_STATIC_LIB})
#target_include_directories(re2_static INTERFACE ${ARROW_RE2_INCLUDE_DIR})
#add_dependencies(re2_static ${ARROW_BASE})
#
#add_library(thrift_static STATIC IMPORTED)
#set_target_properties(thrift_static PROPERTIES IMPORTED_LOCATION ${ARROW_THRIFT_STATIC_LIB})
#target_include_directories(thrift_static INTERFACE ${ARROW_THRIFT_INCLUDE_DIR})
#add_dependencies(thrift_static ${ARROW_BASE})

#add_library(snappy_static STATIC IMPORTED)
#set_target_properties(snappy_static PROPERTIES IMPORTED_LOCATION ${ARROW_SNAPPY_STATIC_LIB})
#target_include_directories(snappy_static INTERFACE ${ARROW_SNAPPY_INCLUDE_DIR})
#add_dependencies(snappy_static ${ARROW_BASE})

add_library(arrow_static STATIC IMPORTED)
set_target_properties(arrow_static PROPERTIES IMPORTED_LOCATION ${ARROW_CORE_STATIC_LIBS})
target_include_directories(arrow_static INTERFACE ${ARROW_INCLUDE_DIR})
target_link_libraries(arrow_static INTERFACE arrow_bundled_dependencies_static)
#target_link_libraries(arrow_static INTERFACE jemalloc_static)
#target_link_libraries(arrow_static INTERFACE re2_static)
#target_link_libraries(arrow_static INTERFACE snappy_static)
target_link_libraries(arrow_static INTERFACE OpenSSL::SSL)
target_link_libraries(arrow_static INTERFACE OpenSSL::Crypto)
target_link_libraries(arrow_static INTERFACE Threads::Threads)
target_link_libraries(arrow_static INTERFACE dl)
target_link_libraries(arrow_static INTERFACE rt)
target_link_libraries(arrow_static INTERFACE z)
add_dependencies(arrow_static ${ARROW_BASE})

add_library(arrow_shared SHARED IMPORTED)
set_target_properties(arrow_shared PROPERTIES IMPORTED_LOCATION ${ARROW_CORE_SHARED_LIBS})
target_include_directories(arrow_shared INTERFACE ${ARROW_INCLUDE_DIR})
target_link_libraries(arrow_shared INTERFACE arrow_bundled_dependencies_static)
#target_link_libraries(arrow_shared INTERFACE jemalloc_static)
#target_link_libraries(arrow_shared INTERFACE re2_static)
#target_link_libraries(arrow_shared INTERFACE snappy_static)
target_link_libraries(arrow_shared INTERFACE pthread)
target_link_libraries(arrow_shared INTERFACE z)
add_dependencies(arrow_shared ${ARROW_BASE})

# Gandiva needs LLVM version 7 or 7.1
find_package(LLVM)

add_library(gandiva_static STATIC IMPORTED)
set_target_properties(gandiva_static PROPERTIES IMPORTED_LOCATION ${ARROW_GANDIVA_STATIC_LIB})
target_include_directories(gandiva_static INTERFACE ${ARROW_INCLUDE_DIR})
target_link_libraries(gandiva_static INTERFACE arrow_static)
target_link_libraries(gandiva_static INTERFACE LLVM)
#target_link_libraries(gandiva_static INTERFACE arrow_static)
#target_link_libraries(gandiva_static INTERFACE re2_static)
add_dependencies(gandiva_static ${ARROW_BASE})

add_library(gandiva_shared SHARED IMPORTED)
set_target_properties(gandiva_shared PROPERTIES IMPORTED_LOCATION ${ARROW_GANDIVA_SHARED_LIB})
target_include_directories(gandiva_shared INTERFACE ${ARROW_INCLUDE_DIR})
target_link_libraries(gandiva_shared INTERFACE arrow_static)
target_link_libraries(gandiva_shared INTERFACE LLVM)
#target_link_libraries(gandiva_shared INTERFACE arrow_static)
#target_link_libraries(gandiva_shared INTERFACE re2_static)
add_dependencies(gandiva_shared ${ARROW_BASE})


add_library(parquet_static STATIC IMPORTED)
set_target_properties(parquet_static PROPERTIES IMPORTED_LOCATION ${ARROW_PARQUET_STATIC_LIB})
target_include_directories(parquet_static INTERFACE ${ARROW_INCLUDE_DIR})
target_link_libraries(parquet_static INTERFACE arrow_static)
#target_link_libraries(parquet_static INTERFACE thrift_static)
#target_link_libraries(parquet_static INTERFACE snappy_static)
add_dependencies(parquet_static ${ARROW_BASE})

add_library(parquet_shared SHARED IMPORTED)
set_target_properties(parquet_shared PROPERTIES IMPORTED_LOCATION ${ARROW_PARQUET_SHARED_LIB})
target_include_directories(parquet_shared INTERFACE ${ARROW_INCLUDE_DIR})
target_link_libraries(parquet_shared INTERFACE arrow_static)
#target_link_libraries(parquet_shared INTERFACE thrift_static)
#target_link_libraries(parquet_shared INTERFACE snappy_static)
add_dependencies(parquet_shared ${ARROW_BASE})

#showTargetProps(arrow_static)
#showTargetProps(arrow_dataset_static)
