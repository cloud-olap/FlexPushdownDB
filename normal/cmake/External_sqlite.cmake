# sqlite

set(GIT_TAG "version-3.32.2")
set(GIT_URL "https://github.com/sqlite/sqlite.git")


include(ExternalProject)
find_package(Git REQUIRED)


set(EP_BASE sqlite_ep)
set(EP_PREFIX ${DEPS_PREFIX}/${EP_BASE})
set(EP_BASE_DIR ${CMAKE_CURRENT_BINARY_DIR}/${EP_PREFIX})
set(EP_SOURCE_DIR ${EP_BASE_DIR}/src/${EP_BASE})
set(EP_BUILD_DIR ${EP_BASE_DIR}/src/${EP_BASE}-build)
set(EP_INSTALL_DIR ${EP_BASE_DIR}/install)
set(EP_BIN_DIR ${EP_INSTALL_DIR}/bin)
set(EP_INCLUDE_DIR ${EP_INSTALL_DIR}/include)
set(EP_LIB_DIR ${EP_INSTALL_DIR}/lib)

set(SQLITE_STATIC_LIB ${EP_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}sqlite3${CMAKE_STATIC_LIBRARY_SUFFIX})
set(SQLITE_SHARED_LIB ${EP_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}sqlite3${CMAKE_SHARED_LIBRARY_SUFFIX})
set(SQLITE_CSV_STATIC_LIB_NAME ${CMAKE_STATIC_LIBRARY_PREFIX}csv${CMAKE_STATIC_LIBRARY_SUFFIX})
set(SQLITE_CSV_STATIC_LIB ${EP_LIB_DIR}/${SQLITE_CSV_STATIC_LIB_NAME})
set(SQLITE_CSV_SHARED_LIB_NAME ${CMAKE_SHARED_LIBRARY_PREFIX}csv${CMAKE_SHARED_LIBRARY_SUFFIX})
set(SQLITE_CSV_SHARED_LIB ${EP_LIB_DIR}/${SQLITE_CSV_SHARED_LIB_NAME})

ExternalProject_Add(${EP_BASE}
        PREFIX ${EP_PREFIX}
        GIT_REPOSITORY ${GIT_URL}
        GIT_TAG ${GIT_TAG}
        GIT_PROGRESS ON
        GIT_SHALLOW ON
        UPDATE_DISCONNECTED TRUE
        CMAKE_ARGS
        -DCMAKE_C_COMPILER:STRING=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER:STRING=${CMAKE_CXX_COMPILER}
        -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        CONFIGURE_COMMAND
            cd ${EP_BUILD_DIR} &&
            CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} ${EP_SOURCE_DIR}/configure --prefix=${EP_INSTALL_DIR}
        BUILD_COMMAND
            cd ${EP_BUILD_DIR} &&
            make --silent &&
            ${CMAKE_C_COMPILER} -g -fPIC -shared -I. ${EP_SOURCE_DIR}/ext/misc/csv.c -o ${EP_BUILD_DIR}/${SQLITE_CSV_SHARED_LIB_NAME} &&
            ${CMAKE_C_COMPILER} -g -c -I. -DSQLITE_CORE ${EP_SOURCE_DIR}/ext/misc/csv.c -o ${EP_BUILD_DIR}/csv.o &&
            ar rc ${EP_BUILD_DIR}/${SQLITE_CSV_STATIC_LIB_NAME} ${EP_BUILD_DIR}/csv.o
        INSTALL_COMMAND
            cd ${EP_BUILD_DIR} &&
            make install --silent &&
            install ${EP_BUILD_DIR}/${SQLITE_CSV_SHARED_LIB_NAME} ${EP_LIB_DIR}/${SQLITE_CSV_SHARED_LIB_NAME} &&
            install ${EP_BUILD_DIR}/${SQLITE_CSV_STATIC_LIB_NAME} ${EP_LIB_DIR}/${SQLITE_CSV_STATIC_LIB_NAME}
        BUILD_BYPRODUCTS ${SQLITE_STATIC_LIB} ${SQLITE_SHARED_LIB} ${SQLITE_CSV_SHARED_LIB} ${SQLITE_CSV_STATIC_LIB}
        )

file(MAKE_DIRECTORY ${EP_INCLUDE_DIR}) # Include directory needs to exist to run configure step

add_library(sqlite3_static STATIC IMPORTED)
set_target_properties(sqlite3_static PROPERTIES IMPORTED_LOCATION ${SQLITE_STATIC_LIB})
target_include_directories(sqlite3_static INTERFACE ${EP_INCLUDE_DIR})
target_link_libraries(sqlite3_static INTERFACE ${CMAKE_DL_LIBS})
add_dependencies(sqlite3_static ${EP_BASE})

add_library(sqlite3_shared SHARED IMPORTED)
set_target_properties(sqlite3_shared PROPERTIES IMPORTED_LOCATION ${SQLITE_SHARED_LIB})
target_include_directories(sqlite3_shared INTERFACE ${EP_INCLUDE_DIR})
target_link_libraries(sqlite3_shared INTERFACE ${CMAKE_DL_LIBS})
add_dependencies(sqlite3_shared ${EP_BASE})

# FIXME: This currently only works with the Makefile generator not Ninja. Ninja wont pick up the rpath so the csv
#  extension isn't found. Need to link with the static lib (below) instead.
add_library(sqlite3_csv_shared SHARED IMPORTED)
set_target_properties(sqlite3_csv_shared PROPERTIES IMPORTED_LOCATION ${SQLITE_CSV_SHARED_LIB})
target_include_directories(sqlite3_csv_shared INTERFACE ${EP_INCLUDE_DIR})
set_target_properties(sqlite3_csv_shared PROPERTIES INSTALL_RPATH ${EP_LIB_DIR})
add_dependencies(sqlite3_csv_shared ${EP_BASE})

add_library(sqlite3_csv_static STATIC IMPORTED)
set_target_properties(sqlite3_csv_static PROPERTIES IMPORTED_LOCATION ${SQLITE_CSV_STATIC_LIB})
target_include_directories(sqlite3_csv_static INTERFACE ${EP_INCLUDE_DIR})
add_dependencies(sqlite3_csv_static ${EP_BASE})
