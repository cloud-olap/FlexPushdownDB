# Boost

set(BOOST_VERSION "boost-1.75.0")
set(BOOST_GIT_URL "https://github.com/boostorg/boost.git")

include(GNUInstallDirs)
include(ExternalProject)
find_package(Git REQUIRED)


set(BOOST_BASE boost_ep)
set(BOOST_PREFIX ${DEPS_PREFIX}/${BOOST_BASE})
set(BOOST_BASE_DIR ${CMAKE_CURRENT_BINARY_DIR}/${BOOST_PREFIX})
set(BOOST_BUILD_DIR ${BOOST_BASE_DIR}/src/${BOOST_BASE}-build)
set(BOOST_INSTALL_DIR ${BOOST_BASE_DIR}/install)
set(BOOST_INCLUDE_DIR ${BOOST_INSTALL_DIR}/include)
set(BOOST_LIB_DIR ${BOOST_INSTALL_DIR}/${CMAKE_INSTALL_LIBDIR})

# Determine Boost libraries to compile (these need only be the non header only libs)
set(_BOOST_INCLUDE_LIBRARIES "")
list(APPEND _BOOST_INCLUDE_LIBRARIES "locale")
string(REPLACE ";" "," _BOOST_CONFIGURE_LIBRARIES "${_BOOST_INCLUDE_LIBRARIES}")

# Libraries
set(BOOST_SYSTEM_STATIC_LIB ${BOOST_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}boost_system${CMAKE_STATIC_LIBRARY_SUFFIX})
set(BOOST_SYSTEM_SHARED_LIB ${BOOST_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}boost_system${CMAKE_SHARED_LIBRARY_SUFFIX})
set(BOOST_LOCALE_STATIC_LIB ${BOOST_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}boost_locale${CMAKE_STATIC_LIBRARY_SUFFIX})
set(BOOST_LOCALE_SHARED_LIB ${BOOST_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}boost_locale${CMAKE_SHARED_LIBRARY_SUFFIX})
set(BOOST_CHRONO_STATIC_LIB ${BOOST_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}boost_chrono${CMAKE_STATIC_LIBRARY_SUFFIX})
set(BOOST_CHRONO_SHARED_LIB ${BOOST_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}boost_chrono${CMAKE_SHARED_LIBRARY_SUFFIX})
set(BOOST_THREAD_STATIC_LIB ${BOOST_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}boost_thread${CMAKE_STATIC_LIBRARY_SUFFIX})
set(BOOST_THREAD_SHARED_LIB ${BOOST_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}boost_thread${CMAKE_SHARED_LIBRARY_SUFFIX})

# Determine ByProducts
set(_BOOST_BYPRODUCTS "")
list(APPEND _BOOST_BYPRODUCTS ${BOOST_SYSTEM_STATIC_LIB})
list(APPEND _BOOST_BYPRODUCTS ${BOOST_SYSTEM_SHARED_LIB})
list(APPEND _BOOST_BYPRODUCTS ${BOOST_LOCALE_STATIC_LIB})
list(APPEND _BOOST_BYPRODUCTS ${BOOST_LOCALE_SHARED_LIB})
list(APPEND _BOOST_BYPRODUCTS ${BOOST_CHRONO_STATIC_LIB})
list(APPEND _BOOST_BYPRODUCTS ${BOOST_CHRONO_SHARED_LIB})
list(APPEND _BOOST_BYPRODUCTS ${BOOST_THREAD_STATIC_LIB})
list(APPEND _BOOST_BYPRODUCTS ${BOOST_THREAD_SHARED_LIB})

# Determine Boost toolset
set(_BOOST_TOOLSET)
if (CMAKE_CXX_COMPILER_ID MATCHES "GNU")
    if (APPLE)
        set(_BOOST_TOOLSET "darwin")
    else ()
        set(_BOOST_TOOLSET "gcc")
    endif ()
elseif (CMAKE_CXX_COMPILER_ID MATCHES ".*Clang")
#    string(REGEX REPLACE "([0-9]*)\\..*" "\\1" _BOOST_TOOLSET_VERSION
#            ${CMAKE_CXX_COMPILER_VERSION})
#    set(_BOOST_TOOLSET "clang-${_BOOST_TOOLSET_VERSION}")
set(_BOOST_TOOLSET "clang")
elseif (CMAKE_CXX_COMPILER_ID MATCHES "Intel")
    set(_BOOST_TOOLSET "intel")
elseif (CMAKE_CXX_COMPILER_ID MATCHES "MSVC")
    set(_BOOST_TOOLSET "msvc")
endif ()

# Determine Boost CXX standard flags
set(_BOOST_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
set(_BOOST_LINK_FLAGS "${CMAKE_STATIC_LINKER_FLAGS}")

# Write build Jam file (only way to specify location of compiler)
set(_BOOST_USER_CONFIG_JAMFILE "${BOOST_BUILD_DIR}/user-config.jam")
file(WRITE "${_BOOST_USER_CONFIG_JAMFILE}" "using ${_BOOST_TOOLSET} : : ${CMAKE_CXX_COMPILER} : <cxxflags>\"${_BOOST_CXX_FLAGS}\" <linkflags>\"${_BOOST_LINK_FLAGS}\" ;")

# Determine variant
if("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
    set(_BOOST_BUILD_VARIANT "debug")
else()
    set(_BOOST_BUILD_VARIANT "release")
endif()

set(_CONFIGURE_COMMAND)
string(APPEND _CONFIGURE_COMMAND "CXX=${CMAKE_CXX_COMPILER} ")
string(APPEND _CONFIGURE_COMMAND "${BOOST_BASE_DIR}/src/${BOOST_BASE}/bootstrap.sh ")
string(APPEND _CONFIGURE_COMMAND "--with-toolset=${_BOOST_TOOLSET} ")
string(APPEND _CONFIGURE_COMMAND "--with-libraries=${_BOOST_CONFIGURE_LIBRARIES} ")
string(APPEND _CONFIGURE_COMMAND "--prefix=${BOOST_INSTALL_DIR} ")

set(_BUILD_COMMAND)
string(APPEND _BUILD_COMMAND "${BOOST_BASE_DIR}/src/${BOOST_BASE}/b2 ")
string(APPEND _BUILD_COMMAND "-j${NPROC} ")
string(APPEND _BUILD_COMMAND "--build-dir=${BOOST_BUILD_DIR} ")
string(APPEND _BUILD_COMMAND "variant=${_BOOST_BUILD_VARIANT} ")
string(APPEND _BUILD_COMMAND "threading=multi ")
string(APPEND _BUILD_COMMAND "--user-config=${_BOOST_USER_CONFIG_JAMFILE} ")

set(_INSTALL_COMMAND)
string(APPEND _INSTALL_COMMAND "${BOOST_BASE_DIR}/src/${BOOST_BASE}/b2 ")
string(APPEND _INSTALL_COMMAND "-j${NPROC} ")
string(APPEND _INSTALL_COMMAND "install ")
string(APPEND _INSTALL_COMMAND "--build-dir=${BOOST_BUILD_DIR} ")

ExternalProject_Add(${BOOST_BASE}
        PREFIX ${BOOST_BASE_DIR}
        GIT_REPOSITORY ${BOOST_GIT_URL}
        GIT_TAG ${BOOST_VERSION}
        GIT_PROGRESS ON
        GIT_SHALLOW ON
        UPDATE_DISCONNECTED TRUE
        BUILD_BYPRODUCTS ${_BOOST_BYPRODUCTS}
        EXCLUDE_FROM_ALL ON
        ${EP_LOG_OPTIONS}
        BUILD_IN_SOURCE TRUE
        CONFIGURE_COMMAND sh -c "${_CONFIGURE_COMMAND}"
        BUILD_COMMAND sh -c "${_BUILD_COMMAND}"
        INSTALL_COMMAND sh -c "${_INSTALL_COMMAND}"
        )

file(MAKE_DIRECTORY ${BOOST_INCLUDE_DIR}) # Include directory needs to exist to run configure step

add_library(boost_headers INTERFACE IMPORTED)
target_include_directories(boost_headers INTERFACE ${BOOST_INCLUDE_DIR})
add_dependencies(boost_headers ${BOOST_BASE})

add_library(boost_system_static STATIC IMPORTED)
set_target_properties(boost_system_static PROPERTIES IMPORTED_LOCATION ${BOOST_SYSTEM_STATIC_LIB})
target_include_directories(boost_system_static INTERFACE ${BOOST_INCLUDE_DIR})
add_dependencies(boost_system_static ${BOOST_BASE})

add_library(boost_system_shared STATIC IMPORTED)
set_target_properties(boost_system_shared PROPERTIES IMPORTED_LOCATION ${BOOST_SYSTEM_SHARED_LIB})
target_include_directories(boost_system_shared INTERFACE ${BOOST_INCLUDE_DIR})
add_dependencies(boost_system_shared ${BOOST_BASE})

add_library(boost_locale_static STATIC IMPORTED)
set_target_properties(boost_locale_static PROPERTIES IMPORTED_LOCATION ${BOOST_LOCALE_STATIC_LIB})
target_include_directories(boost_locale_static INTERFACE ${BOOST_INCLUDE_DIR})
add_dependencies(boost_locale_static ${BOOST_BASE})

add_library(boost_chrono_static STATIC IMPORTED)
set_target_properties(boost_chrono_static PROPERTIES IMPORTED_LOCATION ${BOOST_CHRONO_STATIC_LIB})
target_include_directories(boost_chrono_static INTERFACE ${BOOST_INCLUDE_DIR})
add_dependencies(boost_chrono_static ${BOOST_BASE})

add_library(boost_thread_static STATIC IMPORTED)
set_target_properties(boost_thread_static PROPERTIES IMPORTED_LOCATION ${BOOST_THREAD_STATIC_LIB})
target_include_directories(boost_thread_static INTERFACE ${BOOST_INCLUDE_DIR})
add_dependencies(boost_thread_static ${BOOST_BASE})
