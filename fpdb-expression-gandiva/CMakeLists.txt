
#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(fpdb-expression-gandiva STATIC
        src/Globals.cpp include/fpdb/expression/gandiva/Globals.h
        src/Expression.cpp include/fpdb/expression/gandiva/Expression.h
        include/fpdb/expression/gandiva/ExpressionType.h
        src/Column.cpp include/fpdb/expression/gandiva/Column.h
        src/Cast.cpp include/fpdb/expression/gandiva/Cast.h
        src/Projector.cpp include/fpdb/expression/gandiva/Projector.h
        src/Add.cpp include/fpdb/expression/gandiva/Add.h
        src/Divide.cpp include/fpdb/expression/gandiva/Divide.h
        src/NumericLiteral.cpp include/fpdb/expression/gandiva/NumericLiteral.h
        src/Multiply.cpp include/fpdb/expression/gandiva/Multiply.h
        src/Subtract.cpp include/fpdb/expression/gandiva/Subtract.h
        src/LessThan.cpp include/fpdb/expression/gandiva/LessThan.h
        src/Filter.cpp include/fpdb/expression/gandiva/Filter.h
        src/EqualTo.cpp include/fpdb/expression/gandiva/EqualTo.h
        src/NotEqualTo.cpp include/fpdb/expression/gandiva/NotEqualTo.h
        src/LessThanOrEqualTo.cpp include/fpdb/expression/gandiva/LessThanOrEqualTo.h
        src/GreaterThanOrEqualTo.cpp include/fpdb/expression/gandiva/GreaterThanOrEqualTo.h
        src/GreaterThan.cpp include/fpdb/expression/gandiva/GreaterThan.h
        src/And.cpp include/fpdb/expression/gandiva/And.h
        src/DateIntervalType.cpp include/fpdb/expression/gandiva/DateIntervalType.h
        src/DateAdd.cpp include/fpdb/expression/gandiva/DateAdd.h
        src/Or.cpp include/fpdb/expression/gandiva/Or.h
        src/StringLiteral.cpp include/fpdb/expression/gandiva/StringLiteral.h
        src/In.cpp include/fpdb/expression/gandiva/In.h
        src/Not.cpp include/fpdb/expression/gandiva/Not.h
        src/If.cpp include/fpdb/expression/gandiva/If.h
        src/Like.cpp include/fpdb/expression/gandiva/Like.h
        src/DateExtract.cpp include/fpdb/expression/gandiva/DateExtract.h
        src/IsNull.cpp include/fpdb/expression/gandiva/IsNull.h
        src/Substr.cpp include/fpdb/expression/gandiva/Substr.h
        src/BinaryExpression.cpp include/fpdb/expression/gandiva/BinaryExpression.h
        src/Canonicalizer.cpp include/fpdb/expression/gandiva/Canonicalizer.h
        src/Util.cpp include/fpdb/expression/gandiva/Util.h
        include/fpdb/expression/gandiva/caf-serialization/CAFExpressionSerializer.h)

target_include_directories(fpdb-expression-gandiva PUBLIC include)

set_target_properties(fpdb-expression-gandiva PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_link_libraries(fpdb-expression-gandiva PUBLIC gandiva_static)
target_link_libraries(fpdb-expression-gandiva PUBLIC fpdb-expression)
target_link_libraries(fpdb-expression-gandiva PUBLIC fpdb-caf)
target_link_libraries(fpdb-expression-gandiva PUBLIC fpdb-util)


#-----------------------------------------------------------------------------------------------------------------------
# Test
#-----------------------------------------------------------------------------------------------------------------------

add_executable(fpdb-expression-gandiva-test
        test/Globals.h
        test/MainTest.cpp
        test/CastTest.cpp
        test/TestUtil.cpp test/TestUtil.h
        test/BinaryExpressionTest.cpp
        test/GandivaFunctionRegistryTest.cpp
        test/MultiplyTest.cpp test/ThreadTest.cpp)

target_include_directories(fpdb-expression-gandiva-test PRIVATE include)

target_link_libraries(fpdb-expression-gandiva-test spdlog::spdlog)
target_link_libraries(fpdb-expression-gandiva-test doctest::doctest)
target_link_libraries(fpdb-expression-gandiva-test Backward::Backward)
target_link_libraries(fpdb-expression-gandiva-test fpdb-expression-gandiva)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(fpdb-expression-gandiva)
