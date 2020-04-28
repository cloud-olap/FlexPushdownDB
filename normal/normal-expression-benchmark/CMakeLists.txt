project(normal-expression-benchmark VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-expression-benchmark
        test/Globals.h
        test/Main.cpp
        test/ExpressionBenchmark.cpp)

target_link_libraries(normal-expression-benchmark PRIVATE spdlog::spdlog)
target_link_libraries(normal-expression-benchmark PRIVATE doctest::doctest)
target_link_libraries(normal-expression-benchmark PRIVATE nanobench::nanobench)
target_link_libraries(normal-expression-benchmark PRIVATE normal-expression)
target_link_libraries(normal-expression-benchmark PRIVATE normal-expression-simple)
target_link_libraries(normal-expression-benchmark PRIVATE normal-expression-gandiva)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

showTargetProps(normal-expression-benchmark)
