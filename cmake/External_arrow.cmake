include(GNUInstallDirs)

# This repo is forked from Arrow repo, with a fix for exec engine that sticks it into a fixed thread index
# in serial execution
set(ARROW_VERSION "release-6.0.0-for-fpdb")
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
#set(ARROW_SNAPPY_BASE_DIR ${ARROW_BASE_DIR}/src/${ARROW_BASE}-build/snappy_ep/src/snappy_ep-install)
#set(ARROW_SNAPPY_INCLUDE_DIR ${ARROW_SNAPPY_BASE_DIR}/include)
#set(ARROW_SNAPPY_STATIC_LIB ${ARROW_SNAPPY_BASE_DIR}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}snappy${CMAKE_STATIC_LIBRARY_SUFFIX})
set(ARROW_GRPC_BASE_DIR ${ARROW_BASE_DIR}/src/${ARROW_BASE}-build/grpc_ep-install)
set(ARROW_GRPC_INCLUDE_DIR ${ARROW_GRPC_BASE_DIR}/include)
set(ARROW_GRPC_STATIC_LIB ${ARROW_GRPC_BASE_DIR}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}grpc${CMAKE_STATIC_LIBRARY_SUFFIX})
set(ARROW_GRPCPP_REFLECTION_STATIC_LIB ${ARROW_GRPC_BASE_DIR}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}grpc++_reflection${CMAKE_STATIC_LIBRARY_SUFFIX})
set(ARROW_GRPC_BIN_DIR ${ARROW_GRPC_BASE_DIR}/bin)
set(ARROW_GRPC_CPP_PLUGIN ${ARROW_GRPC_BIN_DIR}/grpc_cpp_plugin)
set(ARROW_PROTOBUF_BASE_DIR ${ARROW_BASE_DIR}/src/${ARROW_BASE}-build/protobuf_ep-install)
set(ARROW_PROTOBUF_INCLUDE_DIR ${ARROW_PROTOBUF_BASE_DIR}/include)
set(ARROW_PROTOBUF_STATIC_LIB ${ARROW_PROTOBUF_BASE_DIR}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}protobuf${CMAKE_STATIC_LIBRARY_SUFFIX})
set(ARROW_PROTOBUF_BIN_DIR ${ARROW_PROTOBUF_BASE_DIR}/bin)
set(ARROW_PROTOC ${ARROW_PROTOBUF_BIN_DIR}/protoc)
set(ARROW_THRIFT_BASE_DIR ${ARROW_BASE_DIR}/src/${ARROW_BASE}-build/thrift_ep-install)
set(ARROW_THRIFT_INCLUDE_DIR ${ARROW_THRIFT_BASE_DIR}/include)
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(ARROW_THRIFT_STATIC_LIB ${ARROW_THRIFT_BASE_DIR}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}thriftd${CMAKE_STATIC_LIBRARY_SUFFIX})
else()
    set(ARROW_THRIFT_STATIC_LIB ${ARROW_THRIFT_BASE_DIR}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}thrift${CMAKE_STATIC_LIBRARY_SUFFIX})
endif()
set(ARROW_PARQUET_SHARED_LIB ${ARROW_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}parquet${CMAKE_SHARED_LIBRARY_SUFFIX})
set(ARROW_PARQUET_STATIC_LIB ${ARROW_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}parquet${CMAKE_STATIC_LIBRARY_SUFFIX})
set(ARROW_FLIGHT_SHARED_LIB ${ARROW_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}arrow_flight${CMAKE_SHARED_LIBRARY_SUFFIX})
set(ARROW_FLIGHT_STATIC_LIB ${ARROW_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}arrow_flight${CMAKE_STATIC_LIBRARY_SUFFIX})
#set(ARROW_JEMALLOC_BASE_DIR ${ARROW_BASE_DIR}/src/${ARROW_BASE}-build/jemalloc_ep-prefix/src/jemalloc_ep)
#set(ARROW_JEMALLOC_INCLUDE_DIR ${ARROW_JEMALLOC_BASE_DIR}/include)
#set(ARROW_JEMALLOC_STATIC_LIB ${ARROW_JEMALLOC_BASE_DIR}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}jemalloc${CMAKE_STATIC_LIBRARY_SUFFIX})
set(ARROW_DEPENDENCIES_SHARED_LIBS ${ARROW_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}arrow_bundled_dependencies${CMAKE_SHARED_LIBRARY_SUFFIX})
set(ARROW_DEPENDENCIES_STATIC_LIBS ${ARROW_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}arrow_bundled_dependencies${CMAKE_STATIC_LIBRARY_SUFFIX})


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
        ${ARROW_FLIGHT_SHARED_LIB}
        ${ARROW_FLIGHT_STATIC_LIB}
        ${ARROW_DEPENDENCIES_SHARED_LIBS}
        ${ARROW_DEPENDENCIES_STATIC_LIBS}
#        CMAKE_COMMAND
#        "${CMAKE_COMMAND}" -E env LDFLAGS="${CMAKE_SHARED_LINKER_FLAGS}" "${CMAKE_COMMAND}"
        CMAKE_ARGS
        -DARROW_USE_CCACHE=ON
        -DARROW_CSV=ON
        -DARROW_DATASET=OFF
        -DARROW_FLIGHT=ON
        -DARROW_IPC=OFF
        -DARROW_PARQUET=ON
        -DARROW_WITH_SNAPPY=ON
        -DARROW_WITH_ZLIB=ON
        -DARROW_JEMALLOC=ON
        -DARROW_GANDIVA=ON
        -DARROW_GRPC_USE_SHARED=OFF
        -DARROW_DEPENDENCY_SOURCE=BUNDLED
        -DCMAKE_INSTALL_MESSAGE=NEVER
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        -DCMAKE_CXX_EXTENSIONS=OFF
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX=${ARROW_INSTALL_DIR}
        -DOPENSSL_ROOT_DIR=${OPENSSL_ROOT_DIR}
        )

# Include directory needs to exist to run configure step
file(MAKE_DIRECTORY ${ARROW_INCLUDE_DIR})
file(MAKE_DIRECTORY ${ARROW_PROTOBUF_INCLUDE_DIR})
file(MAKE_DIRECTORY ${ARROW_GRPC_INCLUDE_DIR})
file(MAKE_DIRECTORY ${ARROW_THRIFT_INCLUDE_DIR})
#file(MAKE_DIRECTORY ${ARROW_JEMALLOC_INCLUDE_DIR})
#file(MAKE_DIRECTORY ${ARROW_RE2_INCLUDE_DIR})
#file(MAKE_DIRECTORY ${ARROW_SNAPPY_INCLUDE_DIR})

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

# Arrow does not add absl to the arrow_bundled_dependencies static library so we need to add it ourselves
# See: https://issues.apache.org/jira/browse/ARROW-14708
set(_ABSL_EP_INSTALL_DIR ${ARROW_BASE_DIR}/src/${ARROW_BASE}-build/absl_ep-install)
file(MAKE_DIRECTORY ${_ABSL_EP_INSTALL_DIR}/${CMAKE_INSTALL_INCLUDEDIR})

# Absl's lib structure is very complicated so use the targets exported by cmake
set(_IMPORT_PREFIX ${_ABSL_EP_INSTALL_DIR})
if("${CMAKE_SYSTEM}" MATCHES "Linux")
    include(${CMAKE_CURRENT_LIST_DIR}/absl/abslTargets-linux.cmake)
elseif("${CMAKE_SYSTEM}" MATCHES "Darwin")
    include(${CMAKE_CURRENT_LIST_DIR}/absl/abslTargets-apple.cmake)
endif()
set(_IMPORT_PREFIX)

add_dependencies(absl::base ${ARROW_BASE})
add_dependencies(absl::str_format_internal ${ARROW_BASE})
add_dependencies(absl::time ${ARROW_BASE})
add_dependencies(absl::optional ${ARROW_BASE})
add_dependencies(absl::synchronization ${ARROW_BASE})

add_library(arrow_bundled_dependencies_static STATIC IMPORTED)
set_target_properties(arrow_bundled_dependencies_static PROPERTIES IMPORTED_LOCATION ${ARROW_DEPENDENCIES_STATIC_LIBS})
add_dependencies(arrow_bundled_dependencies_static ${ARROW_BASE})
target_link_libraries(arrow_bundled_dependencies_static INTERFACE OpenSSL::SSL)
target_link_libraries(arrow_bundled_dependencies_static INTERFACE OpenSSL::Crypto)
target_link_libraries(arrow_bundled_dependencies_static INTERFACE absl::base)
target_link_libraries(arrow_bundled_dependencies_static INTERFACE absl::str_format_internal)
target_link_libraries(arrow_bundled_dependencies_static INTERFACE absl::time)
target_link_libraries(arrow_bundled_dependencies_static INTERFACE absl::optional)
target_link_libraries(arrow_bundled_dependencies_static INTERFACE absl::synchronization)
# additional libs needed on mac
if (${APPLE})
  find_library(FoundationLib CoreFoundation)
  target_link_libraries(arrow_bundled_dependencies_static INTERFACE ${FoundationLib})
  target_link_libraries(arrow_bundled_dependencies_static INTERFACE /usr/lib/libresolv.dylib)
endif()

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
if(LINUX)
    target_link_libraries(arrow_static INTERFACE rt)
endif()
#target_link_libraries(arrow_static INTERFACE z)
add_dependencies(arrow_static ${ARROW_BASE})

add_library(arrow_shared SHARED IMPORTED)
set_target_properties(arrow_shared PROPERTIES IMPORTED_LOCATION ${ARROW_CORE_SHARED_LIBS})
target_include_directories(arrow_shared INTERFACE ${ARROW_INCLUDE_DIR})
target_link_libraries(arrow_shared INTERFACE arrow_bundled_dependencies_static)
#target_link_libraries(arrow_shared INTERFACE jemalloc_static)
#target_link_libraries(arrow_shared INTERFACE re2_static)
#target_link_libraries(arrow_shared INTERFACE snappy_static)
target_link_libraries(arrow_shared INTERFACE Threads::Threads)
#target_link_libraries(arrow_shared INTERFACE z)
add_dependencies(arrow_shared ${ARROW_BASE})

# Gandiva needs LLVM
#find_package(LLVM)

add_library(gandiva_static STATIC IMPORTED)
set_target_properties(gandiva_static PROPERTIES IMPORTED_LOCATION ${ARROW_GANDIVA_STATIC_LIB})
target_include_directories(gandiva_static INTERFACE ${ARROW_INCLUDE_DIR})
target_link_libraries(gandiva_static INTERFACE arrow_static)
target_link_libraries(gandiva_static INTERFACE LLVM::LLVM_INTERFACE)
#target_link_libraries(gandiva_static INTERFACE arrow_static)
#target_link_libraries(gandiva_static INTERFACE re2_static)
add_dependencies(gandiva_static ${ARROW_BASE})

add_library(gandiva_shared SHARED IMPORTED)
set_target_properties(gandiva_shared PROPERTIES IMPORTED_LOCATION ${ARROW_GANDIVA_SHARED_LIB})
target_include_directories(gandiva_shared INTERFACE ${ARROW_INCLUDE_DIR})
target_link_libraries(gandiva_shared INTERFACE arrow_shared)
#target_link_libraries(gandiva_shared INTERFACE LLVM)
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
target_link_libraries(parquet_shared INTERFACE arrow_shared)
#target_link_libraries(parquet_shared INTERFACE thrift_static)
#target_link_libraries(parquet_shared INTERFACE snappy_static)
add_dependencies(parquet_shared ${ARROW_BASE})

add_library(arrow_flight_static STATIC IMPORTED)
set_target_properties(arrow_flight_static PROPERTIES IMPORTED_LOCATION ${ARROW_FLIGHT_STATIC_LIB})
target_include_directories(arrow_flight_static INTERFACE ${ARROW_INCLUDE_DIR})
target_link_libraries(arrow_flight_static INTERFACE arrow_static)
add_dependencies(arrow_flight_static ${ARROW_BASE})

add_library(arrow_flight_shared SHARED IMPORTED)
set_target_properties(arrow_flight_shared PROPERTIES IMPORTED_LOCATION ${ARROW_FLIGHT_SHARED_LIB})
target_include_directories(arrow_flight_shared INTERFACE ${ARROW_INCLUDE_DIR})
target_link_libraries(arrow_flight_shared INTERFACE arrow_shared)
add_dependencies(arrow_flight_shared ${ARROW_BASE})


# third-party of arrow

#add_library(jemalloc_static STATIC IMPORTED)
#set_target_properties(jemalloc_static PROPERTIES IMPORTED_LOCATION ${ARROW_JEMALLOC_STATIC_LIB})
#target_include_directories(jemalloc_static INTERFACE ${ARROW_JEMALLOC_INCLUDE_DIR})
#target_link_libraries(jemalloc_static INTERFACE pthread)
#add_dependencies(jemalloc_static ${ARROW_BASE})

#add_library(re2_static STATIC IMPORTED)
#set_target_properties(re2_static PROPERTIES IMPORTED_LOCATION ${ARROW_RE2_STATIC_LIB})
#target_include_directories(re2_static INTERFACE ${ARROW_RE2_INCLUDE_DIR})
#add_dependencies(re2_static ${ARROW_BASE})

#add_library(snappy_static STATIC IMPORTED)
#set_target_properties(snappy_static PROPERTIES IMPORTED_LOCATION ${ARROW_SNAPPY_STATIC_LIB})
#target_include_directories(snappy_static INTERFACE ${ARROW_SNAPPY_INCLUDE_DIR})
#add_dependencies(snappy_static ${ARROW_BASE})

add_library(protobuf_static STATIC IMPORTED)
set_target_properties(protobuf_static PROPERTIES IMPORTED_LOCATION ${ARROW_PROTOBUF_STATIC_LIB})
target_include_directories(protobuf_static INTERFACE ${ARROW_PROTOBUF_INCLUDE_DIR})
add_dependencies(protobuf_static ${ARROW_BASE})

add_library(grpcpp_reflection_static STATIC IMPORTED)
set_target_properties(grpcpp_reflection_static PROPERTIES IMPORTED_LOCATION ${ARROW_GRPCPP_REFLECTION_STATIC_LIB})
target_include_directories(grpcpp_reflection_static INTERFACE ${ARROW_GRPC_INCLUDE_DIR})
add_dependencies(grpcpp_reflection_static ${ARROW_BASE})

add_library(grpc_static STATIC IMPORTED)
set_target_properties(grpc_static PROPERTIES IMPORTED_LOCATION ${ARROW_GRPC_STATIC_LIB})
target_include_directories(grpc_static INTERFACE ${ARROW_GRPC_INCLUDE_DIR})
add_dependencies(grpc_static ${ARROW_BASE})
target_link_libraries(grpc_static INTERFACE protobuf_static)
target_link_libraries(grpc_static INTERFACE grpcpp_reflection_static)
target_link_libraries(grpc_static INTERFACE arrow_bundled_dependencies_static)

add_library(thrift_static STATIC IMPORTED)
set_target_properties(thrift_static PROPERTIES IMPORTED_LOCATION ${ARROW_THRIFT_STATIC_LIB})
target_include_directories(thrift_static INTERFACE ${ARROW_THRIFT_INCLUDE_DIR})
add_dependencies(thrift_static ${ARROW_BASE})

#showTargetProps(arrow_static)
#showTargetProps(arrow_dataset_static)

