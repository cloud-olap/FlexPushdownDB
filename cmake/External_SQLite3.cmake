# sqlite

set(GIT_TAG "version-3.32.2")
set(GIT_URL "https://github.com/sqlite/sqlite.git")

include(ExternalProject)
find_package(Git REQUIRED)
include(Paths_SQLite3)

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
        # This seems to prevent cmake from building sqlite on every build. Basically configure is only done once when
        # the sources are downloaded and not on each build. A bit of a hack but mostly works.
        PATCH_COMMAND
        cd ${EP_BUILD_DIR} &&
        CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} ${EP_SOURCE_DIR}/configure --prefix=${EP_INSTALL_DIR}
        CONFIGURE_COMMAND ""
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

include(Targets_SQLite3)
