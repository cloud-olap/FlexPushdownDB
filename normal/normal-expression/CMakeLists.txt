project(normal-expression VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-expression STATIC
        include/normal/expression/Globals.h
        src/expression/Expression.cpp include/normal/expression/Expression.h
        src/expression/IProjector.cpp include/normal/expression/IProjector.h)

target_include_directories(normal-expression PUBLIC include)

target_link_libraries(normal-expression PRIVATE spdlog::spdlog)
target_link_libraries(normal-expression PUBLIC arrow_static)
target_link_libraries(normal-expression PUBLIC expected)
target_link_libraries(normal-expression PUBLIC normal-core)


#-----------------------------------------------------------------------------------------------------------------------
# Tests
#-----------------------------------------------------------------------------------------------------------------------

#add_executable(normal-expression-benchmark
#        test/Globals.h
#        test/Main.cpp
#        test/ExpressionBenchmark.cpp)
#
#target_link_libraries(normal-expression-benchmark PRIVATE spdlog::spdlog)
#target_link_libraries(normal-expression-benchmark PRIVATE doctest::doctest)
#target_link_libraries(normal-expression-benchmark PRIVATE nanobench::nanobench)
#target_link_libraries(normal-expression-benchmark PRIVATE normal-core)
#target_link_libraries(normal-expression-benchmark PRIVATE normal-expression)
#target_link_libraries(normal-expression-benchmark PRIVATE normal-expression-simple)
#target_link_libraries(normal-expression-benchmark PRIVATE normal-expression-gandiva)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

showTargetProps(normal-expression)
#showTargetProps(normal-expression-benchmark)
