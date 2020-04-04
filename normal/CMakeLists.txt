cmake_minimum_required(VERSION 3.15)
project("normal" VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${CMAKE_CURRENT_LIST_DIR}/cmake")

include(CMakeUtils)

setDefaults()

# Gandiva requires LLVM 7 and anything linking to it must use the same version
if ((NOT CMAKE_CXX_COMPILER_ID STREQUAL "Clang") OR (NOT CMAKE_CXX_COMPILER_VERSION MATCHES "7.*"))
    message(FATAL_ERROR "Illegal compiler - '${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}'. Compiler must be Clang version 7")
endif()

# On MacOSX packages installed via homebrew are installed to a location cmake doesn't search for packages
#list(APPEND CMAKE_PREFIX_PATH /usr/local/opt)
set(CMAKE_PREFIX_PATH /usr/local/opt CACHE INTERNAL "")
# Hack to make sure clang can be found, FIXME: am sure there is a proper way to do this
find_package(LLVM REQUIRED CONFIG)
get_filename_component(CLAMG_CMAKE_DIR ${LLVM_CMAKE_DIR}/../clang REALPATH)
set(clang_DIR ${CLAMG_CMAKE_DIR} CACHE INTERNAL "")
find_package(Clang REQUIRED CONFio8uIG)

message("${Clang_DIR}")
message("${CMAKE_PREFIX_PATH}")

#-----------------------------------------------------------------------------------------------------------------------
# Dependencies
#-----------------------------------------------------------------------------------------------------------------------

set(DEPS_PREFIX "_deps")

include(External_spdlog)
include(External_doctest)
include(External_caf)
include(External_arrow)
include(External_AWSCPPSDK)
include(External_ANTLR)
include(External_graphviz)
include(External_expected)

include(External_iwyu)


#-----------------------------------------------------------------------------------------------------------------------
# Modules
#-----------------------------------------------------------------------------------------------------------------------

add_subdirectory("normal-core")
add_subdirectory("normal-pushdown")
add_subdirectory("normal-sql")
add_subdirectory("normal-test")
