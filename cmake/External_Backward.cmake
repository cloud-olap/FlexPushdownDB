# Backward

set(BACKWARD_VERSION "v1.5")
set(BACKWARD_GIT_URL "https://github.com/bombela/backward-cpp.git")


include(ExternalProject)
find_package(Git REQUIRED)
find_package (binutils REQUIRED)


set(BACKWARD_BASE backward_ep)
set(BACKWARD_PREFIX ${DEPS_PREFIX}/${BACKWARD_BASE})
set(BACKWARD_BASE_DIR ${CMAKE_CURRENT_BINARY_DIR}/${BACKWARD_PREFIX})
set(BACKWARD_INSTALL_DIR ${BACKWARD_BASE_DIR}/install)
set(BACKWARD_INCLUDE_DIR ${BACKWARD_INSTALL_DIR}/include)


ExternalProject_Add(${BACKWARD_BASE}
        PREFIX ${BACKWARD_BASE_DIR}
        GIT_REPOSITORY ${BACKWARD_GIT_URL}
        GIT_TAG ${BACKWARD_VERSION}
        GIT_PROGRESS ON
        GIT_SHALLOW ON
        UPDATE_DISCONNECTED TRUE
        INSTALL_DIR ${BACKWARD_INSTALL_DIR}
        CMAKE_ARGS
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        -DCMAKE_INSTALL_PREFIX=${BACKWARD_INSTALL_DIR}
        )

file(MAKE_DIRECTORY ${BACKWARD_INCLUDE_DIR}) # Include directory needs to exist to run configure step

add_library(Backward::Backward INTERFACE IMPORTED)
target_include_directories(Backward::Backward INTERFACE ${BACKWARD_INCLUDE_DIR})
add_dependencies(Backward::Backward ${BACKWARD_BASE})
