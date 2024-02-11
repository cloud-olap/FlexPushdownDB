cmake_minimum_required(VERSION 3.15)
project("normal" VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${CMAKE_CURRENT_LIST_DIR}/cmake")

include(CMakeUtils)

setDefaults()

# Gandiva requires LLVM 7 and anything linking to it must use the same version
#if ((NOT CMAKE_CXX_COMPILER_ID STREQUAL "Clang") OR (NOT CMAKE_CXX_COMPILER_VERSION MATCHES "7.*"))
#    message(FATAL_ERROR "Illegal compiler - '${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}'.
#     Gandiva requires Clang version 7")
#endif()


include(Configure_LLVM)


#-----------------------------------------------------------------------------------------------------------------------
# Dependencies
#-----------------------------------------------------------------------------------------------------------------------

set(DEPS_PREFIX "_deps")

include(External_spdlog)
include(External_doctest)
include(External_nanobench)
include(External_caf)
include(External_arrow)
include(External_AWSCPPSDK)
include(External_ANTLR)
include(External_graphviz)
include(External_expected)
include(External_fmt)
include(External_ssb_dbgen)
include(External_primesieve)
include(Dependency_SQLite3)
include(External_Backward)
include(External_CallableTraits)
include(External_zlib_ng)

#-----------------------------------------------------------------------------------------------------------------------
# Modules
#-----------------------------------------------------------------------------------------------------------------------

add_subdirectory(normal-util)
add_subdirectory(normal-core)
add_subdirectory(normal-tuple)
add_subdirectory(normal-connector)
add_subdirectory(normal-expression)
add_subdirectory(normal-expression-gandiva)
add_subdirectory(ATTIC/normal-expression-simple)
add_subdirectory(ATTIC/normal-expression-benchmark)
add_subdirectory(normal-plan)
add_subdirectory(normal-pushdown)
add_subdirectory(normal-sql)
add_subdirectory(ATTIC/normal-test)
add_subdirectory(normal-ssb)
add_subdirectory(normal-cache)
add_subdirectory(normal-frontend)

#-----------------------------------------------------------------------------------------------------------------------
# Resource files
#-----------------------------------------------------------------------------------------------------------------------

file(COPY resources DESTINATION ${CMAKE_CURRENT_BINARY_DIR})