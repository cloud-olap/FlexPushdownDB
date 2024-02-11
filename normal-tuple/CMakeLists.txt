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
        src/arrow/SchemaHelper.cpp include/normal/tuple/arrow/SchemaHelper.h
        src/csv/CSVParser.cpp include/normal/tuple/csv/CSVParser.h
        src/ArrayAppender.cpp include/normal/tuple/ArrayAppender.h
        src/ArrayHasher.cpp include/normal/tuple/ArrayHasher.h
        src/TupleSetIndex.cpp include/normal/tuple/TupleSetIndex.h
        src/TupleSetIndexWrapper.cpp include/normal/tuple/TupleSetIndexWrapper.h
        src/TupleSetIndexFinder.cpp include/normal/tuple/TupleSetIndexFinder.h
        src/TupleSetIndexFinderWrapper.cpp include/normal/tuple/TupleSetIndexFinderWrapper.h
        src/TupleSetIndexFinderWrapper.cpp include/normal/tuple/TupleSetIndexFinderWrapper.h
        src/Converter.cpp include/normal/tuple/Converter.h
        src/parquet/ParquetReader.cpp include/normal/tuple/parquet/ParquetReader.h
        src/Util.cpp include/normal/tuple/Util.h
        src/FileReader.cpp include/normal/tuple/FileReader.h
        src/FileType.cpp include/normal/tuple/FileType.h
        src/FileReaderBuilder.cpp include/normal/tuple/FileReaderBuilder.h
        src/csv/CSVReader.cpp include/normal/tuple/csv/CSVReader.h
        src/ArrayAppenderWrapper.cpp include/normal/tuple/ArrayAppenderWrapper.h
        src/arrow/ArrowAWSInputStream.cpp include/normal/tuple/arrow/ArrowAWSInputStream.h
        src/arrow/ArrowAWSGZIPInputStream.cpp include/normal/tuple/arrow/ArrowAWSGZIPInputStream.h
        src/arrow/ArrowAWSGZIPInputStream2.cpp include/normal/tuple/arrow/ArrowAWSGZIPInputStream2.h
        src/arrow/CSVToArrowSIMDStreamParser.cpp include/normal/tuple/arrow/CSVToArrowSIMDStreamParser.h
        src/arrow/SIMDParserHelpers.cpp include/normal/tuple/arrow/SIMDParserHelpers.h
        src/arrow/CSVToArrowSIMDChunkParser.cpp include/normal/tuple/arrow/CSVToArrowSIMDChunkParser.h)

target_include_directories(normal-tuple PUBLIC include)


target_link_libraries(normal-tuple PRIVATE spdlog::spdlog)
target_link_libraries(normal-tuple PRIVATE fmt::fmt)
target_link_libraries(normal-tuple PRIVATE LLVM-filesystem)
target_link_libraries(normal-tuple PUBLIC expected)
target_link_libraries(normal-tuple PUBLIC arrow_static)
target_link_libraries(normal-tuple PUBLIC parquet_static)
target_link_libraries(normal-tuple PUBLIC deflate)


## Ideally use cloudflare instead of z-ng, however don't have CMake working yet
## Follow download instructions here: https://github.com/zlib-ng/zlib-ng to get linking working
#target_link_libraries(normal-tuple PUBLIC z-ng)
## need to include zlib-ng library on mac
#if (${APPLE})
#  target_include_directories(normal-tuple PUBLIC /usr/local/include)
#endif()
target_link_libraries(normal-tuple PUBLIC zlibstatic)


#-----------------------------------------------------------------------------------------------------------------------
# Test
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-tuple-test
        test/Globals.h
        test/MainTest.cpp
        test/TupleTest.cpp
        test/CSVTest.cpp
        test/ParquetTest.cpp
        test/ArrayAppenderTest.cpp
        test/TupleSetIndexTest.cpp)


target_include_directories(normal-tuple-test PRIVATE include)

target_link_libraries(normal-tuple-test spdlog::spdlog)
target_link_libraries(normal-tuple-test doctest::doctest)
target_link_libraries(normal-tuple-test Backward::Backward)
target_link_libraries(normal-tuple-test fmt::fmt)
target_link_libraries(normal-tuple-test normal-tuple)


#-----------------------------------------------------------------------------------------------------------------------
# Benchmark
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-tuple-benchmark
        bench/Globals.h
        bench/Main.cpp
        bench/ArrayAppenderBench.cpp)


target_include_directories(normal-tuple-benchmark PRIVATE include)

target_link_libraries(normal-tuple-benchmark spdlog::spdlog)
target_link_libraries(normal-tuple-benchmark doctest::doctest)
target_link_libraries(normal-tuple-benchmark Backward::Backward)
target_link_libraries(normal-tuple-benchmark nanobench::nanobench)
target_link_libraries(normal-tuple-benchmark fmt::fmt)
target_link_libraries(normal-tuple-benchmark normal-tuple)


#-----------------------------------------------------------------------------------------------------------------------
# Test Data
#-----------------------------------------------------------------------------------------------------------------------

configure_file(data/csv/test.csv ${CMAKE_CURRENT_BINARY_DIR}/data/csv/test.csv COPYONLY)
configure_file(data/csv/test3x10000.csv ${CMAKE_CURRENT_BINARY_DIR}/data/csv/test3x10000.csv COPYONLY)

#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-sql)
