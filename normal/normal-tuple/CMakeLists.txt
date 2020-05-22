project(normal-tuple VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-tuple STATIC
        include/normal/tuple/Globals.h
        src/TupleSet.cpp include/normal/tuple/TupleSet.h
        src/TupleSet2.cpp include/normal/tuple/TupleSet2.h
        src/Scalar.cpp include/normal/tuple/Scalar.h
        src/Column.cpp include/normal/tuple/Column.h
        src/Schema.cpp include/normal/tuple/Schema.h
        src/ColumnIterator.cpp include/normal/tuple/ColumnIterator.h
        src/ColumnIndex.cpp include/normal/tuple/ColumnIndex.h
        src/TupleSetShowOptions.cpp include/normal/tuple/TupleSetShowOptions.h
        src/ColumnName.cpp include/normal/tuple/ColumnName.h
        src/ColumnBuilder.cpp include/normal/tuple/ColumnBuilder.h
        src/Sample.cpp include/normal/tuple/Sample.h
        src/arrow/Arrays.cpp include/normal/tuple/arrow/Arrays.h
        include/normal/tuple/arrow/ArrayHelper.h
        include/normal/tuple/arrow/TableHelper.h
        include/normal/tuple/arrow/ScalarHelperImpl.h
        include/normal/tuple/arrow/ScalarHelperBuilder.h
        include/normal/tuple/arrow/ScalarHelper.h
        src/arrow/SchemaHelper.cpp include/normal/tuple/arrow/SchemaHelper.h)

target_include_directories(normal-tuple PUBLIC include)

target_link_libraries(normal-tuple PRIVATE spdlog::spdlog)
target_link_libraries(normal-tuple PRIVATE fmt::fmt)
target_link_libraries(normal-tuple PUBLIC expected)
target_link_libraries(normal-tuple PUBLIC arrow_static)


#-----------------------------------------------------------------------------------------------------------------------
# Test
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-tuple-test
        test/Globals.h
        test/MainTest.cpp
        test/TupleTest.cpp)


target_include_directories(normal-tuple-test PRIVATE include)

target_link_libraries(normal-tuple-test spdlog::spdlog)
target_link_libraries(normal-tuple-test doctest::doctest)
target_link_libraries(normal-tuple-test normal-test)
target_link_libraries(normal-tuple-test normal-tuple)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-sql)
