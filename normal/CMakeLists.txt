cmake_minimum_required(VERSION 3.15)
project("normal" VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${CMAKE_CURRENT_LIST_DIR}/cmake")

#set(CMAKE_CXX_INCLUDE_WHAT_YOU_USE "/usr/bin/iwyu;")

include(CMakeUtils)
include(ExternalProject)
find_package(Git REQUIRED)


#-----------------------------------------------------------------------------------------------------------------------
# Dependencies
#-----------------------------------------------------------------------------------------------------------------------

set(DEPENDENCIES_BASE_DIR "${CMAKE_BINARY_DIR}/contrib")

find_package(spdlog REQUIRED)
find_package(doctest REQUIRED)
find_package(AWSCPPSDK REQUIRED)
find_package(arrow REQUIRED)
find_package(caf REQUIRED)


#-----------------------------------------------------------------------------------------------------------------------
# Modules
#-----------------------------------------------------------------------------------------------------------------------

add_subdirectory("normal-core")
add_subdirectory("normal-pushdown")
add_subdirectory("normal-test")
