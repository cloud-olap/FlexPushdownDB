# Boost

set(BOOST_VERSION "boost-1.75.0")
set(BOOST_GIT_URL "https://github.com/boostorg/iostreams.git")


include(ExternalProject)
find_package(Git REQUIRED)


set(BOOST_BASE boost_ep)
set(BOOST_PREFIX ${DEPS_PREFIX}/${BOOST_BASE})
set(BOOST_BASE_DIR ${CMAKE_CURRENT_BINARY_DIR}/${BOOST_PREFIX})
set(BOOST_INSTALL_DIR ${BOOST_BASE_DIR}/install)
set(BOOST_INCLUDE_DIR ${BOOST_INSTALL_DIR}/include)


ExternalProject_Add(${BOOST_BASE}
        PREFIX ${BOOST_BASE_DIR}
        GIT_REPOSITORY ${BOOST_GIT_URL}
        GIT_TAG ${BOOST_VERSION}
        GIT_PROGRESS ON
        GIT_SHALLOW ON
        UPDATE_DISCONNECTED TRUE
        INSTALL_DIR ${BOOST_INSTALL_DIR}
        CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${BOOST_INSTALL_DIR}
        )

file(MAKE_DIRECTORY ${BOOST_INCLUDE_DIR}) # Include directory needs to exist to run configure step

add_library(boost INTERFACE IMPORTED)
target_include_directories(boost INTERFACE ${BOOST_INCLUDE_DIR})
add_dependencies(boost ${BOOST_BASE})
