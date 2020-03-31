project(normal-test VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-test
        src/Globals.h
        src/MainTest.cpp
        src/FileScanTest.cpp
        src/S3SelectScanTest.cpp
        src/CacheTest.cpp
        src/MultiQueryTest.cpp
        src/SQLTest.cpp)

target_include_directories(normal-test PRIVATE include)

target_link_libraries(normal-test PUBLIC normal-core)
target_link_libraries(normal-test PUBLIC normal-pushdown)
target_link_libraries(normal-test PUBLIC normal-sql)
target_link_libraries(normal-test PRIVATE spdlog::spdlog)
target_link_libraries(normal-test PRIVATE doctest::doctest)
target_link_libraries(normal-test PRIVATE antlr4_static)

# Test data
configure_file(data/data-file-simple/test.csv ${CMAKE_CURRENT_BINARY_DIR}/data/data-file-simple/test.csv COPYONLY)
configure_file(data/data-file-sharded/test01.csv ${CMAKE_CURRENT_BINARY_DIR}/data/data-file-sharded/test01.csv COPYONLY)
configure_file(data/data-file-sharded/test02.csv ${CMAKE_CURRENT_BINARY_DIR}/data/data-file-sharded/test02.csv COPYONLY)
configure_file(data/data-file-sharded/test03.csv ${CMAKE_CURRENT_BINARY_DIR}/data/data-file-sharded/test03.csv COPYONLY)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

showTargetProps(normal-test)
