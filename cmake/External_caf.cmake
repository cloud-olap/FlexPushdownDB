# CAF

# This repo is forked from CAF repo, with support added for remote spawning actors with spawn_options
# A pull request will be made, and after it's get merged I will switch back to CAF repo
set(CAF_VERSION "0.18.5-remote_spawn_option")
set(CAF_GIT_URL "https://github.com/Yifei-yang7/actor-framework.git")


include(ExternalProject)
find_package(Git REQUIRED)
find_package(OpenSSL REQUIRED)

set(CAF_BASE caf_ep)
set(CAF_PREFIX ${DEPS_PREFIX}/${CAF_BASE})
set(CAF_BASE_DIR ${CMAKE_CURRENT_BINARY_DIR}/${CAF_PREFIX})
set(CAF_INSTALL_DIR ${CAF_BASE_DIR}/install)
set(CAF_INCLUDE_DIR ${CAF_INSTALL_DIR}/include)
set(CAF_LIB_DIR ${CAF_INSTALL_DIR}/lib)
set(CAF_CORE_SHARED_LIBS ${CAF_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}caf_core${CMAKE_SHARED_LIBRARY_SUFFIX})
#set(CAF_CORE_STATIC_LIBS ${CAF_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}caf_core_static${CMAKE_STATIC_LIBRARY_SUFFIX})
set(CAF_IO_SHARED_LIBS ${CAF_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}caf_io${CMAKE_SHARED_LIBRARY_SUFFIX})
#set(CAF_IO_STATIC_LIBS ${CAF_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}caf_io_static${CMAKE_STATIC_LIBRARY_SUFFIX})
set(CAF_OPENSSL_SHARED_LIBS ${CAF_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}caf_openssl${CMAKE_SHARED_LIBRARY_SUFFIX})
#set(CAF_OPENSSL_STATIC_LIBS ${CAF_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}caf_openssl_static${CMAKE_STATIC_LIBRARY_SUFFIX})


ExternalProject_Add(${CAF_BASE}
        PREFIX ${CAF_BASE_DIR}
        INSTALL_DIR ${CAF_INSTALL_DIR}
        GIT_REPOSITORY ${CAF_GIT_URL}
        GIT_TAG ${CAF_VERSION}
        GIT_PROGRESS ON
        GIT_SHALLOW ON
        UPDATE_DISCONNECTED TRUE
        BUILD_BYPRODUCTS ${CAF_CORE_SHARED_LIBS} ${CAF_IO_SHARED_LIBS} ${CAF_OPENSSL_SHARED_LIBS}
        CMAKE_ARGS
        -DBUILD_SHARED_LIBS=ON
        -DCAF_NO_EXAMPLES=ON
        -DCAF_NO_UNIT_TESTS=ON
        -DCAF_NO_PYTHON=ON
        -DCAF_NO_OPENCL=ON
        -DCAF_NO_TOOLS=ON
        -DCAF_NO_AUTO_LIBCPP=ON
        -DCAF_ENABLE_RUNTIME_CHECKS=ON
        -DCMAKE_INSTALL_MESSAGE=NEVER
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX=${CAF_INSTALL_DIR}
        -DOPENSSL_ROOT_DIR=${OPENSSL_ROOT_DIR}
        )


file(MAKE_DIRECTORY ${CAF_INCLUDE_DIR}) # Include directory needs to exist to run configure step

find_package(Threads REQUIRED)

add_library(caf::libcaf_core_shared SHARED IMPORTED)
set_target_properties(caf::libcaf_core_shared PROPERTIES IMPORTED_LOCATION ${CAF_CORE_SHARED_LIBS})
target_include_directories(caf::libcaf_core_shared INTERFACE ${CAF_INCLUDE_DIR})
add_dependencies(caf::libcaf_core_shared ${CAF_BASE})

#add_library(caf::libcaf_core_static STATIC IMPORTED)
#set_target_properties(caf::libcaf_core_static PROPERTIES IMPORTED_LOCATION ${CAF_CORE_STATIC_LIBS})
#target_include_directories(caf::libcaf_core_static INTERFACE ${CAF_INCLUDE_DIR})
#add_dependencies(caf::libcaf_core_static ${CAF_BASE})

add_library(caf::libcaf_io_shared SHARED IMPORTED)
set_target_properties(caf::libcaf_io_shared PROPERTIES IMPORTED_LOCATION ${CAF_IO_SHARED_LIBS})
target_include_directories(caf::libcaf_io_shared INTERFACE ${CAF_INCLUDE_DIR})
target_link_libraries(caf::libcaf_io_shared INTERFACE Threads::Threads)
add_dependencies(caf::libcaf_io_shared ${CAF_BASE})

#add_library(caf::libcaf_io_static STATIC IMPORTED)
#set_target_properties(caf::libcaf_io_static PROPERTIES IMPORTED_LOCATION ${CAF_IO_STATIC_LIBS})
#target_include_directories(caf::libcaf_io_static INTERFACE ${CAF_INCLUDE_DIR})
#target_link_libraries(caf::libcaf_io_static INTERFACE Threads::Threads)
#add_dependencies(caf::libcaf_io_static ${CAF_BASE})

#add_library(caf::libcaf_openssl_static STATIC IMPORTED)
#set_target_properties(caf::libcaf_openssl_static PROPERTIES IMPORTED_LOCATION ${CAF_OPENSSL_STATIC_LIBS})
#target_include_directories(caf::libcaf_openssl_static INTERFACE ${CAF_INCLUDE_DIR})
#add_dependencies(caf::libcaf_openssl_static ${CAF_BASE})

add_library(caf::libcaf_openssl_shared SHARED IMPORTED)
set_target_properties(caf::libcaf_openssl_shared PROPERTIES IMPORTED_LOCATION ${CAF_OPENSSL_SHARED_LIBS})
target_include_directories(caf::libcaf_openssl_shared INTERFACE ${CAF_INCLUDE_DIR})
add_dependencies(caf::libcaf_openssl_shared ${CAF_BASE})


#showTargetProps(caf::libcaf_core_shared)
#showTargetProps(caf::libcaf_core_static)
#showTargetProps(caf::libcaf_io_shared)
#showTargetProps(caf::libcaf_io_static)
#showTargetProps(caf::libcaf_openssl_static)
#showTargetProps(caf::libcaf_openssl_shared)
