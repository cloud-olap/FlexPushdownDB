project(normal-expression-benchmark VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-expression-benchmark
        test/Globals.h
        test/Main.cpp
        test/ExpressionBenchmark.cpp)

target_link_libraries(normal-expression-benchmark spdlog::spdlog)
target_link_libraries(normal-expression-benchmark doctest::doctest)
target_link_libraries(normal-expression-benchmark Backward::Backward)
target_link_libraries(normal-expression-benchmark nanobench::nanobench)
target_link_libraries(normal-expression-benchmark normal-expression)
target_link_libraries(normal-expression-benchmark normal-expression-simple)
target_link_libraries(normal-expression-benchmark normal-expression-gandiva)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-expression-benchmark)
