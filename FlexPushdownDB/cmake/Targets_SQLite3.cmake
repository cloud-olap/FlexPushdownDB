#
# Target definitions for SQLite3
#

include(Paths_SQLite3)

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
