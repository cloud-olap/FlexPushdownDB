#-------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(fpdb-tuple STATIC
        include/fpdb/tuple/Globals.h
        src/TupleSet.cpp include/fpdb/tuple/TupleSet.h
        src/Scalar.cpp include/fpdb/tuple/Scalar.h
        src/Column.cpp include/fpdb/tuple/Column.h
        src/Schema.cpp include/fpdb/tuple/Schema.h
        src/ColumnIterator.cpp include/fpdb/tuple/ColumnIterator.h
        src/ColumnIndex.cpp include/fpdb/tuple/ColumnIndex.h
        src/TupleSetShowOptions.cpp include/fpdb/tuple/TupleSetShowOptions.h
        src/ColumnName.cpp include/fpdb/tuple/ColumnName.h
        src/ColumnBuilder.cpp include/fpdb/tuple/ColumnBuilder.h
        src/ArrayAppender.cpp include/fpdb/tuple/ArrayAppender.h
        src/ArrayAppenderWrapper.cpp include/fpdb/tuple/ArrayAppenderWrapper.h
        src/ArrayHasher.cpp include/fpdb/tuple/ArrayHasher.h
        src/RecordBatchHasher.cpp include/fpdb/tuple/RecordBatchHasher.h
        include/fpdb/tuple/TupleKey.h
        src/TupleSetIndex.cpp include/fpdb/tuple/TupleSetIndex.h
        src/TupleSetIndexFinder.cpp include/fpdb/tuple/TupleSetIndexFinder.h
        src/Converter.cpp include/fpdb/tuple/Converter.h
        src/FileReader.cpp include/fpdb/tuple/FileReader.h
        src/LocalFileReader.cpp include/fpdb/tuple/LocalFileReader.h
        src/LocalFileReaderBuilder.cpp include/fpdb/tuple/LocalFileReaderBuilder.h
        src/FileFormat.cpp include/fpdb/tuple/FileFormat.h
        include/fpdb/tuple/FileFormatType.h
        src/util/Util.cpp include/fpdb/tuple/util/Util.h
        src/util/Sample.cpp include/fpdb/tuple/util/Sample.h
        src/csv/CSVParser.cpp include/fpdb/tuple/csv/CSVParser.h
        src/csv/CSVFormat.cpp include/fpdb/tuple/csv/CSVFormat.h
        src/csv/CSVReader.cpp include/fpdb/tuple/csv/CSVReader.h
        src/csv/LocalCSVReader.cpp include/fpdb/tuple/csv/LocalCSVReader.h
        src/parquet/ParquetFormat.cpp include/fpdb/tuple/parquet/ParquetFormat.h
        src/parquet/ParquetReader.cpp include/fpdb/tuple/parquet/ParquetReader.h
        src/parquet/LocalParquetReader.cpp include/fpdb/tuple/parquet/LocalParquetReader.h
        src/arrow/Arrays.cpp include/fpdb/tuple/arrow/Arrays.h
        src/arrow/SchemaHelper.cpp include/fpdb/tuple/arrow/SchemaHelper.h
        include/fpdb/tuple/arrow/ArrayHelper.h
        include/fpdb/tuple/arrow/TableHelper.h
        include/fpdb/tuple/arrow/ScalarHelperImpl.h
        include/fpdb/tuple/arrow/ScalarHelperBuilder.h
        include/fpdb/tuple/arrow/ScalarHelper.h
        src/arrow/ArrowInputStream.cpp include/fpdb/tuple/arrow/ArrowInputStream.h
        src/arrow/ArrowGzipInputStream.cpp include/fpdb/tuple/arrow/ArrowGzipInputStream.h
        src/arrow/ArrowGzipInputStream2.cpp include/fpdb/tuple/arrow/ArrowGzipInputStream2.h
        src/arrow/CSVToArrowSIMDStreamParser.cpp include/fpdb/tuple/arrow/CSVToArrowSIMDStreamParser.h
        src/arrow/SIMDParserHelpers.cpp include/fpdb/tuple/arrow/SIMDParserHelpers.h
        src/arrow/CSVToArrowSIMDChunkParser.cpp include/fpdb/tuple/arrow/CSVToArrowSIMDChunkParser.h
        src/arrow/exec/DummyNode.cpp include/fpdb/tuple/arrow/exec/DummyNode.h
        src/arrow/exec/BufferedSinkNode.cpp include/fpdb/tuple/arrow/exec/BufferedSinkNode.h
        include/fpdb/tuple/arrow/exec/Util.h
        src/arrow/exec/BloomFilter.cpp include/fpdb/tuple/arrow/exec/BloomFilter.h
        src/arrow/exec/PartitionUtil.cpp include/fpdb/tuple/arrow/exec/PartitionUtil.h
        src/serialization/ArrowSerializer.cpp include/fpdb/tuple/serialization/ArrowSerializer.h
        include/fpdb/tuple/caf-serialization/CAFTupleKeyElementSerializer.h
        include/fpdb/tuple/caf-serialization/CAFFileFormatSerializer.h)

target_include_directories(fpdb-tuple PUBLIC include)

set_target_properties(fpdb-tuple PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_link_libraries(fpdb-tuple PUBLIC fpdb-util)
target_link_libraries(fpdb-tuple PUBLIC fpdb-caf)
target_link_libraries(fpdb-tuple PUBLIC spdlog::spdlog)
target_link_libraries(fpdb-tuple PUBLIC fmt::fmt)
target_link_libraries(fpdb-tuple PUBLIC expected)
target_link_libraries(fpdb-tuple PUBLIC arrow_static)
target_link_libraries(fpdb-tuple PUBLIC parquet_static)
target_link_libraries(fpdb-tuple PUBLIC zlibstatic)
target_link_libraries(fpdb-tuple PUBLIC nlohmann_json)


#-----------------------------------------------------------------------------------------------------------------------
# Test
#-----------------------------------------------------------------------------------------------------------------------

add_library(fpdb-file-read-test-util
        src/util/FileReaderTestUtil.cpp include/fpdb/tuple/util/FileReaderTestUtil.h)

target_include_directories(fpdb-file-read-test-util PUBLIC include)

target_link_libraries(fpdb-file-read-test-util fpdb-tuple)
target_link_libraries(fpdb-file-read-test-util doctest::doctest)


add_executable(fpdb-tuple-test
        test/Globals.h
        test/MainTest.cpp
        test/TupleTest.cpp
        test/CSVTest.cpp
        test/ParquetTest.cpp
        test/ArrayAppenderTest.cpp
        test/TupleSetIndexTest.cpp)

target_include_directories(fpdb-tuple-test PRIVATE include)

target_link_libraries(fpdb-tuple-test fpdb-tuple)
target_link_libraries(fpdb-tuple-test fpdb-file-read-test-util)
target_link_libraries(fpdb-tuple-test spdlog::spdlog)
target_link_libraries(fpdb-tuple-test doctest::doctest)
target_link_libraries(fpdb-tuple-test fmt::fmt)


#-----------------------------------------------------------------------------------------------------------------------
# Microbenchmark
#-----------------------------------------------------------------------------------------------------------------------

add_executable(fpdb-tuple-bench
        bench/Globals.h
        bench/Main.cpp
        bench/ArrayAppenderBench.cpp)


target_include_directories(fpdb-tuple-bench PRIVATE include)

target_link_libraries(fpdb-tuple-bench spdlog::spdlog)
target_link_libraries(fpdb-tuple-bench doctest::doctest)
target_link_libraries(fpdb-tuple-bench nanobench::nanobench)
target_link_libraries(fpdb-tuple-bench fmt::fmt)
target_link_libraries(fpdb-tuple-bench fpdb-tuple)
target_link_libraries(fpdb-tuple-bench Backward::Backward)


#-----------------------------------------------------------------------------------------------------------------------
# Executable
#-----------------------------------------------------------------------------------------------------------------------

add_executable(fpdb-tuple-converter-exec
        executable/ConverterMain.cpp)

target_include_directories(fpdb-tuple-converter-exec PRIVATE include)

target_link_libraries(fpdb-tuple-converter-exec fpdb-tuple)



#-----------------------------------------------------------------------------------------------------------------------
# Test Data
#-----------------------------------------------------------------------------------------------------------------------

configure_file(data/csv/test.csv ${CMAKE_CURRENT_BINARY_DIR}/data/csv/test.csv COPYONLY)
configure_file(data/csv/test3x10000.csv ${CMAKE_CURRENT_BINARY_DIR}/data/csv/test3x10000.csv COPYONLY)

#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(fpdb-sql)
