# iwyu

set(IWYU_VERSION "clang_7.0")
set(IWYU_GIT_URL "https://github.com/include-what-you-use/include-what-you-use.git")

include(ExternalProject)
find_package(Git REQUIRED)

set(IWYU_BASE iwyu_ep)
set(IWYU_PREFIX ${DEPS_PREFIX}/${IWYU_BASE})
set(IWYU_BASE_DIR ${CMAKE_CURRENT_BINARY_DIR}/${IWYU_PREFIX})
set(IWYU_INSTALL_DIR ${IWYU_BASE_DIR}/install)
set(IWYU_BIN_DIR ${IWYU_INSTALL_DIR}/bin)
set(IWYU_EXECUTABLE ${IWYU_BIN_DIR}/iwyu.sh)


ExternalProject_Add(${IWYU_BASE}
        PREFIX ${IWYU_BASE_DIR}
        INSTALL_DIR ${IWYU_INSTALL_DIR}
        GIT_REPOSITORY ${IWYU_GIT_URL}
        GIT_TAG ${IWYU_VERSION}
        GIT_PROGRESS ON
        GIT_SHALLOW ON
        BUILD_BYPRODUCTS ${IWYU_EXECUTABLE}
        CMAKE_ARGS
        -DCMAKE_INSTALL_MESSAGE=NEVER
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX=${IWYU_INSTALL_DIR}
        -DCMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH}
        )

add_custom_target(iwyu ALL)
add_dependencies(iwyu ${IWYU_BASE})


set(CMAKE_CXX_INCLUDE_WHAT_YOU_USE ${IWYU_EXECUTABLE})
set(CMAKE_C_INCLUDE_WHAT_YOU_USE ${IWYU_EXECUTABLE})

#showTargetProps(iwyu)
