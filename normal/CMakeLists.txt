cmake_minimum_required(VERSION 3.15)
project("normal" VERSION "${PROJECT_VERSION}" LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)

include(cmake/CMakeUtils.cmake)
include(ExternalProject)
find_package(Git REQUIRED)

#-----------------------------------------------------------------------------------------------------------------------
# Dependencies
#-----------------------------------------------------------------------------------------------------------------------

set(DEPENDENCIES_BASE_DIR "${CMAKE_BINARY_DIR}/contrib")

# SPDLog
set(SPDLOG_VERSION "v1.4.2")
set(SPDLOG_GIT_URL "https://github.com/gabime/spdlog.git")

ExternalProject_Add(spdlog-project
        PREFIX ${DEPENDENCIES_BASE_DIR}/spdlog
        INSTALL_DIR ${DEPENDENCIES_BASE_DIR}/spdlog
        GIT_REPOSITORY ${SPDLOG_GIT_URL}
        GIT_TAG ${SPDLOG_VERSION}
        CMAKE_ARGS
        -DSPDLOG_BUILD_EXAMPLE=NO
        -DSPDLOG_BUILD_TESTS=NO
        -DSPDLOG_BUILD_BENCH=NO
        -DCMAKE_INSTALL_PREFIX=${DEPENDENCIES_BASE_DIR}/spdlog
        )

ExternalProject_Get_Property(spdlog-project INSTALL_DIR)
set(SPDLOG_INSTALL_DIR ${INSTALL_DIR})

file(MAKE_DIRECTORY ${SPDLOG_INSTALL_DIR}/include) # Include directory needs to exist to run configure step

add_library("spdlog" STATIC IMPORTED)
set_target_properties("spdlog" PROPERTIES IMPORTED_LOCATION ${SPDLOG_INSTALL_DIR}/lib/libspdlog.a)
set_target_properties("spdlog" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${SPDLOG_INSTALL_DIR}/include)


#-----------------------------------------------------------------------------------------------------------------------
# Modules
#-----------------------------------------------------------------------------------------------------------------------

add_subdirectory("normal-core")
add_subdirectory("normal-pushdown")
add_subdirectory("normal-test")
