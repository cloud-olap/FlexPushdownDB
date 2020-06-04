# Nanobench

set(NANOBENCH_VERSION "v3.1.0")
set(NANOBENCH_GIT_URL "https://github.com/martinus/nanobench")


include(ExternalProject)
find_package(Git REQUIRED)


set(NANOBENCH_BASE nanobench_ep)
set(NANOBENCH_PREFIX ${DEPS_PREFIX}/${NANOBENCH_BASE})
set(NANOBENCH_BASE_DIR ${CMAKE_CURRENT_BINARY_DIR}/${NANOBENCH_PREFIX})
set(NANOBENCH_SOURCE_DIR ${NANOBENCH_BASE_DIR}/src/${NANOBENCH_BASE})
set(NANOBENCH_INCLUDE_DIR ${NANOBENCH_SOURCE_DIR}/src/include)


ExternalProject_Add(${NANOBENCH_BASE}
        PREFIX ${NANOBENCH_PREFIX}
        GIT_REPOSITORY ${NANOBENCH_GIT_URL}
        GIT_TAG ${NANOBENCH_VERSION}
        GIT_PROGRESS ON
        GIT_SHALLOW ON
        UPDATE_DISCONNECTED TRUE
        CMAKE_ARGS
        -DCMAKE_C_COMPILER:STRING=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER:STRING=${CMAKE_CXX_COMPILER}
        -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
        )

file(MAKE_DIRECTORY ${NANOBENCH_INCLUDE_DIR}) # Include directory needs to exist to run configure step

add_library(nanobench::nanobench INTERFACE IMPORTED)
target_include_directories(nanobench::nanobench INTERFACE ${NANOBENCH_INCLUDE_DIR})
add_dependencies(nanobench::nanobench ${NANOBENCH_BASE})
