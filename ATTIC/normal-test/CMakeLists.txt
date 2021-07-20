project(normal-test VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

function(link_target target)
    target_include_directories(${target} PRIVATE include)

    target_link_libraries(${target} PRIVATE spdlog::spdlog)
    target_link_libraries(${target} PRIVATE doctest::doctest)
    target_link_libraries(${target} PRIVATE antlr4_static)
    target_link_libraries(${target} PRIVATE arrow_static)
    target_link_libraries(${target} PRIVATE gandiva_static)
    target_link_libraries(${target} PRIVATE LLVM-filesystem)
#    target_link_libraries(${target} PRIVATE normal-core)
    target_link_libraries(${target} PRIVATE normal-pushdown)
    target_link_libraries(${target} PRIVATE normal-sql)
    target_link_libraries(${target} PRIVATE normal-expression)
    target_link_libraries(${target} PRIVATE normal-expression-gandiva)
endfunction()


#-----------------------------------------------------------------------------------------------------------------------
# Targets
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-test-suite
        src/Globals.h
        src/TestUtil.cpp src/TestUtil.h
        src/MainTest.cpp
        src/CacheTest.cpp
        src/SQLTest.cpp
)

link_target(normal-test-suite)

add_executable(normal-test-cache
        src/Globals.h
        src/TestUtil.cpp src/TestUtil.h
        src/MainTest.cpp
        src/CacheTest.cpp)

link_target(normal-test-cache)

add_executable(normal-test-sql
        src/Globals.h
        src/TestUtil.cpp src/TestUtil.h
        src/MainTest.cpp
        src/SQLTest.cpp)

link_target(normal-test-sql)


# Test data
configure_file(data/data-file-simple/test.csv ${CMAKE_CURRENT_BINARY_DIR}/data/data-file-simple/test.csv COPYONLY)
configure_file(data/data-file-sharded/test01.csv ${CMAKE_CURRENT_BINARY_DIR}/data/data-file-sharded/test01.csv COPYONLY)
configure_file(data/data-file-sharded/test02.csv ${CMAKE_CURRENT_BINARY_DIR}/data/data-file-sharded/test02.csv COPYONLY)
configure_file(data/data-file-sharded/test03.csv ${CMAKE_CURRENT_BINARY_DIR}/data/data-file-sharded/test03.csv COPYONLY)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-test)
