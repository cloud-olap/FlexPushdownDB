#
# Find module for SQLite3
#

include(FindPackageHandleStandardArgs)
include(Paths_SQLite3)

unset(SQLite3_INCLUDE_DIR CACHE)

find_path(
        SQLite3_INCLUDE_DIR
        sqlite3.h
        HINTS ${EP_INCLUDE_DIR}
        REQUIRED
        NO_DEFAULT_PATH)

find_library(
        SQLite3_STATIC_LIBRARY
        NAMES ${SQLITE_STATIC_LIB_NAME}
        HINTS ${EP_LIB_DIR}
        REQUIRED
        NO_DEFAULT_PATH)

find_library(
        SQLite3_SHARED_LIBRARY
        NAMES ${SQLITE_SHARED_LIB_NAME}
        HINTS ${EP_LIB_DIR}
        REQUIRED
        NO_DEFAULT_PATH)

find_library(
        SQLite3_CSV_STATIC_LIBRARY
        NAMES ${SQLITE_CSV_STATIC_LIB_NAME}
        HINTS ${EP_LIB_DIR}
        REQUIRED
        NO_DEFAULT_PATH)

find_library(
        SQLite3_CSV_SHARED_LIBRARY
        NAMES ${SQLITE_CSV_SHARED_LIB_NAME}
        HINTS ${EP_LIB_DIR}
        REQUIRED
        NO_DEFAULT_PATH)

find_package_handle_standard_args(
        SQLite3
        DEFAULT_MSG
        SQLite3_INCLUDE_DIR
        SQLite3_STATIC_LIBRARY
        SQLite3_SHARED_LIBRARY
        SQLite3_CSV_STATIC_LIBRARY
        SQLite3_CSV_SHARED_LIBRARY)

if (SQLite3_FOUND)
    include(Targets_SQLite3)
endif ()
