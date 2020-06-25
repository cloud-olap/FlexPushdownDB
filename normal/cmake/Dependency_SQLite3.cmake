# SQLite3

find_package(SQLite3)
if (NOT SQLite3_FOUND)
    message(STATUS "SQLite3 not found, will build from source")
    include(External_SQLite3)
endif ()
