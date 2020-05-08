project(normal-expression-gandiva VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-expression-gandiva STATIC
        src/Globals.h
        src/Expression.cpp include/normal/expression/gandiva/Expression.h
        src/Column.cpp include/normal/expression/gandiva/Column.h
        src/Cast.cpp include/normal/expression/gandiva/Cast.h
        src/Projector.cpp include/normal/expression/gandiva/Projector.h
        src/Add.cpp include/normal/expression/gandiva/Add.h
        src/Divide.cpp include/normal/expression/gandiva/Divide.h
        src/Literal.cpp include/normal/expression/gandiva/Literal.h
        src/Multiply.cpp include/normal/expression/gandiva/Multiply.h
        src/Subtract.cpp include/normal/expression/gandiva/Subtract.h
        src/LessThan.cpp include/normal/expression/gandiva/LessThan.h
        src/Filter.cpp include/normal/expression/gandiva/Filter.h)

target_include_directories(normal-expression-gandiva PUBLIC include)

target_link_libraries(normal-expression-gandiva PRIVATE spdlog::spdlog)
target_link_libraries(normal-expression-gandiva PUBLIC arrow_static)
target_link_libraries(normal-expression-gandiva PUBLIC gandiva_static)
target_link_libraries(normal-expression-gandiva PUBLIC expected)
target_link_libraries(normal-expression-gandiva PUBLIC normal-core)
target_link_libraries(normal-expression-gandiva PUBLIC normal-expression)


#-----------------------------------------------------------------------------------------------------------------------
# Test
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-expression-gandiva-test
        test/Globals.h
        test/MainTest.cpp
        test/CastTest.cpp
        test/TestUtil.cpp test/TestUtil.h
        test/BinaryExpressionTest.cpp
        test/GandivaFunctionRegistryTest.cpp)

target_include_directories(normal-expression-gandiva-test PRIVATE include)

target_link_libraries(normal-expression-gandiva-test PRIVATE spdlog::spdlog)
target_link_libraries(normal-expression-gandiva-test PRIVATE doctest::doctest)
target_link_libraries(normal-expression-gandiva-test PRIVATE normal-test)
target_link_libraries(normal-expression-gandiva-test PRIVATE normal-expression-gandiva)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

showTargetProps(normal-expression-gandiva)
