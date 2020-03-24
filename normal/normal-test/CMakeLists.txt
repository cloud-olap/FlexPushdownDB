project(normal-test VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

setDefaults()


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-test
        src/Globals.h
        src/MainTest.cpp
        src/FileScanTest.cpp
        src/S3SelectScanTest.cpp
        src/CacheTest.cpp
        src/MultiQueryTest.cpp)

target_compile_features(normal-test PRIVATE cxx_std_17)
target_compile_options(normal-test PRIVATE -Wall -Wextra -pedantic)
set_target_properties(normal-test PROPERTIES LINKER_LANGUAGE CXX)

target_include_directories(normal-test PRIVATE include)

target_link_libraries(normal-test PUBLIC normal-core)
target_link_libraries(normal-test PUBLIC normal-pushdown)
target_link_libraries(normal-test PUBLIC normal-sql)
target_link_libraries(normal-test PRIVATE spdlog::spdlog)
target_link_libraries(normal-test PRIVATE doctest::doctest)

# Test data
configure_file(data/test.csv ${CMAKE_CURRENT_BINARY_DIR}/data/test.csv COPYONLY)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-test)
