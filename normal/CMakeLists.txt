cmake_minimum_required(VERSION 3.15)
project("normal" VERSION "${PROJECT_VERSION}" LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)

#-----------------------------------------------------------------------------------------------------------------------
# External dependencies
#-----------------------------------------------------------------------------------------------------------------------

include(ExternalProject)
find_package(Git REQUIRED)

set(DEPENDENCIES_BASE_DIR "${CMAKE_BINARY_DIR}/contrib")

ExternalProject_Add(
        spdlog
        PREFIX ${DEPENDENCIES_BASE_DIR}/spdlog
        INSTALL_DIR ${DEPENDENCIES_BASE_DIR}/spdlog
        GIT_REPOSITORY https://github.com/gabime/spdlog.git
        GIT_TAG v1.4.2
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${DEPENDENCIES_BASE_DIR}/spdlog
        GIT_PROGRESS TRUE
        LOG_DOWNLOAD ON
        LOG_CONFIGURE ON
        LOG_BUILD ON
)

ExternalProject_Get_Property(spdlog INSTALL_DIR)
include_directories(${INSTALL_DIR}/include)

add_subdirectory("normal-core")
add_subdirectory("normal-pushdown")
add_subdirectory("normal-test")
