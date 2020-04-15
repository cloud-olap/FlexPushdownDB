project(normal-test VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

function(link_target target)
    target_include_directories(${target} PRIVATE include)

    target_link_libraries(${target} PRIVATE normal-core)
    target_link_libraries(${target} PRIVATE normal-pushdown)
    target_link_libraries(${target} PRIVATE normal-sql)
    target_link_libraries(${target} PRIVATE spdlog::spdlog)
    target_link_libraries(${target} PRIVATE doctest::doctest)
    target_link_libraries(${target} PRIVATE antlr4_static)
    target_link_libraries(${target} PRIVATE arrow_static)
    target_link_libraries(${target} PRIVATE gandiva_static)
    target_link_libraries(${target} PRIVATE LLVM-filesystem)
endfunction()


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-test
        include/normal/test/Globals.h
        src/TestUtil.cpp include/normal/test/TestUtil.h
        src/MainTest.cpp
        src/FileScanTest.cpp
        src/S3SelectScanTest.cpp
        src/CacheTest.cpp
        src/MultiQueryTest.cpp
        src/SQLTest.cpp
        src/TupleTest.cpp)

link_target(normal-test)

add_executable(normal-test-file
        include/normal/test/Globals.h
        src/TestUtil.cpp include/normal/test/TestUtil.h
        src/MainTest.cpp
        src/FileScanTest.cpp)

link_target(normal-test-file)


add_executable(normal-test-s3
        include/normal/test/Globals.h
        src/TestUtil.cpp include/normal/test/TestUtil.h
        src/MainTest.cpp
        src/S3SelectScanTest.cpp)

link_target(normal-test-s3)


add_executable(normal-test-cache
        include/normal/test/Globals.h
        src/TestUtil.cpp include/normal/test/TestUtil.h
        src/MainTest.cpp
        src/CacheTest.cpp)

link_target(normal-test-cache)


add_executable(normal-test-sql
        include/normal/test/Globals.h
        src/TestUtil.cpp include/normal/test/TestUtil.h
        src/MainTest.cpp
        src/SQLTest.cpp)

link_target(normal-test-sql)

add_executable(normal-test-tuple
        include/normal/test/Globals.h
        src/TestUtil.cpp include/normal/test/TestUtil.h
        src/MainTest.cpp
        src/TupleTest.cpp)

link_target(normal-test-tuple)

# Test data
configure_file(data/data-file-simple/test.csv ${CMAKE_CURRENT_BINARY_DIR}/data/data-file-simple/test.csv COPYONLY)
configure_file(data/data-file-sharded/test01.csv ${CMAKE_CURRENT_BINARY_DIR}/data/data-file-sharded/test01.csv COPYONLY)
configure_file(data/data-file-sharded/test02.csv ${CMAKE_CURRENT_BINARY_DIR}/data/data-file-sharded/test02.csv COPYONLY)
configure_file(data/data-file-sharded/test03.csv ${CMAKE_CURRENT_BINARY_DIR}/data/data-file-sharded/test03.csv COPYONLY)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-test)
