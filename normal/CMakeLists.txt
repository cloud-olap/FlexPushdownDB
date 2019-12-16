cmake_minimum_required(VERSION 3.15)
project("normal" VERSION "${PROJECT_VERSION}" LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_INCLUDE_WHAT_YOU_USE "/usr/bin/iwyu;")

include(cmake/CMakeUtils.cmake)
include(ExternalProject)
find_package(Git REQUIRED)


#-----------------------------------------------------------------------------------------------------------------------
# Dependencies
#-----------------------------------------------------------------------------------------------------------------------

set(DEPENDENCIES_BASE_DIR "${CMAKE_BINARY_DIR}/contrib")


include(cmake/CAF.cmake)


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
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_INSTALL_PREFIX=${DEPENDENCIES_BASE_DIR}/spdlog
        )

ExternalProject_Get_Property(spdlog-project INSTALL_DIR)
set(SPDLOG_INSTALL_DIR ${INSTALL_DIR})

file(MAKE_DIRECTORY ${SPDLOG_INSTALL_DIR}/include) # Include directory needs to exist to run configure step

add_library("spdlog" STATIC IMPORTED)
set_target_properties("spdlog" PROPERTIES IMPORTED_LOCATION ${SPDLOG_INSTALL_DIR}/lib/libspdlog.a)
set_target_properties("spdlog" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${SPDLOG_INSTALL_DIR}/include)
add_dependencies("spdlog" "spdlog-project")


# Arrow
set(ARROW_VERSION "cb2647a")
set(ARROW_GIT_URL "https://github.com/apache/arrow.git")

ExternalProject_Add("arrow-project"
        GIT_REPOSITORY ${ARROW_GIT_URL}
        GIT_TAG ${ARROW_VERSION}
        PREFIX ${DEPENDENCIES_BASE_DIR}/arrow
        SOURCE_SUBDIR cpp
        CMAKE_ARGS
        -DARROW_USE_CCACHE=ON
        -DARROW_CSV=ON
        -DARROW_DATASET=ON
        -DARROW_FLIGHT=OFF
        -DARROW_IPC=OFF
        -DARROW_PARQUET=OFF
        -DARROW_WITH_SNAPPY=ON
        -DARROW_JEMALLOC=ON
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_INSTALL_PREFIX=${DEPENDENCIES_BASE_DIR}/arrow
        )

ExternalProject_get_property("arrow-project" INSTALL_DIR)
set(ARROW_LIB_DIR ${INSTALL_DIR}/lib)
set(ARROW_INCLUDE_DIR ${INSTALL_DIR}/include)

file(MAKE_DIRECTORY ${ARROW_INCLUDE_DIR}) # Include directory needs to exist to run configure step

add_library("arrow" STATIC IMPORTED)
set_target_properties("arrow" PROPERTIES IMPORTED_LOCATION ${ARROW_LIB_DIR}/libarrow.a)
set_target_properties("arrow" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_DIR})
add_dependencies("arrow" "arrow-project")

add_library("arrow-dataset" STATIC IMPORTED)
set_target_properties("arrow-dataset" PROPERTIES IMPORTED_LOCATION ${ARROW_LIB_DIR}/libarrow_dataset.a)
set_target_properties("arrow-dataset" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_DIR})
add_dependencies("arrow-dataset" "arrow-project")

add_library("jemalloc" STATIC IMPORTED)
set_target_properties("jemalloc" PROPERTIES IMPORTED_LOCATION ${ARROW_LIB_DIR}/../src/arrow-project-build/jemalloc_ep-prefix/src/jemalloc_ep/lib/libjemalloc.a)
set_target_properties("jemalloc" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_DIR})
add_dependencies("jemalloc" "arrow-project")


#add_library("arrow-flight" STATIC IMPORTED)
#set_target_properties("arrow-flight" PROPERTIES IMPORTED_LOCATION ${ARROW_LIB_DIR}/libarrow_flight.a)
#set_target_properties("arrow-flight" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_DIR})
#add_dependencies("arrow-flight" "arrow-project")

#add_library("arrow-parquet" STATIC IMPORTED)
#set_target_properties("arrow-parquet" PROPERTIES IMPORTED_LOCATION ${ARROW_LIB_DIR}/libparquet.a)
#set_target_properties("arrow-parquet" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_DIR})
#add_dependencies("arrow-parquet" "arrow-project")


#-----------------------------------------------------------------------------------------------------------------------
# Modules
#-----------------------------------------------------------------------------------------------------------------------

add_subdirectory("normal-core")
add_subdirectory("normal-pushdown")
add_subdirectory("normal-test")
