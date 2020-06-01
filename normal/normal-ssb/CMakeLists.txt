project(normal-ssb VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Benchmark
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-ssb-benchmark
        include/normal/ssb/Globals.h
        src/TestUtil.cpp include/normal/ssb/TestUtil.h
        src/Queries.cpp include/normal/ssb/Queries.h
        bench/Main.cpp
        bench/SSBBenchmark.cpp)

target_include_directories(normal-ssb-benchmark PUBLIC include)

target_link_libraries(normal-ssb-benchmark PRIVATE spdlog::spdlog)
target_link_libraries(normal-ssb-benchmark PRIVATE doctest::doctest)
target_link_libraries(normal-ssb-benchmark PRIVATE nanobench::nanobench)
target_link_libraries(normal-ssb-benchmark PRIVATE LLVM-filesystem)
target_link_libraries(normal-ssb-benchmark PRIVATE fmt::fmt)
target_link_libraries(normal-ssb-benchmark PRIVATE normal-plan)
target_link_libraries(normal-ssb-benchmark PRIVATE normal-sql)


#-----------------------------------------------------------------------------------------------------------------------
# Test
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-ssb-test
        include/normal/ssb/Globals.h
        src/TestUtil.cpp include/normal/ssb/TestUtil.h
        test/Main.cpp
        test/SSBTest.cpp
        src/Queries.cpp include/normal/ssb/Queries.h)

target_include_directories(normal-ssb-test PUBLIC include)

target_link_libraries(normal-ssb-test PRIVATE spdlog::spdlog)
target_link_libraries(normal-ssb-test PRIVATE doctest::doctest)
target_link_libraries(normal-ssb-test PRIVATE fmt::fmt)
target_link_libraries(normal-ssb-test PRIVATE normal-plan)
target_link_libraries(normal-ssb-test PRIVATE normal-sql)


#-----------------------------------------------------------------------------------------------------------------------
# Data
#-----------------------------------------------------------------------------------------------------------------------

configure_file(data/lineorder.csv ${CMAKE_CURRENT_BINARY_DIR}/data/lineorder.csv COPYONLY)
configure_file(data/date.csv ${CMAKE_CURRENT_BINARY_DIR}/data/date.csv COPYONLY)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-ssb-test)
#showTargetProps(normal-ssb-benchmark)
