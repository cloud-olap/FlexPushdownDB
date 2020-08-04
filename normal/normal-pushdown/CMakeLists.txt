project("normal-pushdown" VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-pushdown STATIC
        include/normal/pushdown/Globals.h
        src/Collate.cpp include/normal/pushdown/Collate.h
        src/s3/S3SelectScan.cpp include/normal/pushdown/s3/S3SelectScan.h
        src/file/FileScan.cpp include/normal/pushdown/file/FileScan.h
        src/Aggregate.cpp include/normal/pushdown/Aggregate.h
        src/s3/S3SelectParser.cpp src/s3/S3SelectParser.h
        src/aggregate/AggregationResult.cpp include/normal/pushdown/aggregate/AggregationResult.h
        src/aggregate/Sum.cpp include/normal/pushdown/aggregate/Sum.h
        src/aggregate/AggregationFunction.cpp include/normal/pushdown/aggregate/AggregationFunction.h
        src/AWSClient.cpp include/normal/pushdown/AWSClient.h
        src/Project.cpp include/normal/pushdown/Project.h
        src/join/JoinPredicate.cpp include/normal/pushdown/join/JoinPredicate.h
        src/join/HashJoinProbe.cpp include/normal/pushdown/join/HashJoinProbe.h
        src/join/HashJoinBuild.cpp include/normal/pushdown/join/HashJoinBuild.h
        src/join/HashTableMessage.cpp include/normal/pushdown/join/HashTableMessage.h
        src/join/HashTable.cpp include/normal/pushdown/join/HashTable.h
        src/join/Joiner.cpp include/normal/pushdown/join/Joiner.h
        src/filter/Filter.cpp include/normal/pushdown/filter/Filter.h
        src/filter/FilterPredicate.cpp include/normal/pushdown/filter/FilterPredicate.h
        src/Sort.cpp include/normal/pushdown/Sort.h
        src/cacheScan.cpp include/normal/pushdown/cacheScan.h
        src/group/Group.cpp include/normal/pushdown/group/Group.h
        src/group/GroupKey.cpp include/normal/pushdown/group/GroupKey.h
        src/TupleMessage.cpp include/normal/pushdown/TupleMessage.h
        src/cache/CacheHelper.cpp include/normal/pushdown/cache/CacheHelper.h
        src/s3/S3SelectCSVParseOptions.cpp include/normal/pushdown/s3/S3SelectCSVParseOptions.h
        src/Util.cpp include/normal/pushdown/Util.h
        src/shuffle/Shuffle.cpp include/normal/pushdown/shuffle/Shuffle.h
        src/shuffle/ATTIC/Shuffler.cpp include/normal/pushdown/shuffle/ATTIC/Shuffler.h
        src/cache/CacheLoad.cpp include/normal/pushdown/cache/CacheLoad.h
        src/merge/MergeOperator.cpp include/normal/pushdown/merge/MergeOperator.h
        src/merge/MergeKernel.cpp include/normal/pushdown/merge/MergeKernel.h
        include/normal/pushdown/scan/ScanMessage.cpp include/normal/pushdown/scan/ScanMessage.h
        src/shuffle/ATTIC/ShuffleKernel.cpp include/normal/pushdown/shuffle/ATTIC/ShuffleKernel.h
        src/shuffle/RecordBatchShuffler.cpp include/normal/pushdown/shuffle/RecordBatchShuffler.h
        src/shuffle/ShuffleKernel2.cpp include/normal/pushdown/shuffle/ShuffleKernel2.h
        src/shuffle/ArrayAppender.cpp include/normal/pushdown/shuffle/ArrayAppender.h
        src/shuffle/StringArrayAppender.cpp include/normal/pushdown/shuffle/StringArrayAppender.h
        src/shuffle/Int64ArrayAppender.cpp include/normal/pushdown/shuffle/Int64ArrayAppender.h
        src/shuffle/ArrayHasher.cpp include/normal/pushdown/shuffle/ArrayHasher.h
        src/shuffle/StringArrayHasher.cpp include/normal/pushdown/shuffle/StringArrayHasher.h
        src/shuffle/Int64ArrayHasher.cpp include/normal/pushdown/shuffle/Int64ArrayHasher.h
        include/normal/pushdown/join/HashJoinBuildKernel.cpp include/normal/pushdown/join/HashJoinBuildKernel.h
        include/normal/pushdown/join/HashJoinProbeKernel.cpp include/normal/pushdown/join/HashJoinProbeKernel.h
        include/normal/pushdown/join/HashJoinProbeKernel2.cpp include/normal/pushdown/join/HashJoinProbeKernel2.h
        include/normal/pushdown/join/HashJoinBuildKernel2.cpp include/normal/pushdown/join/HashJoinBuildKernel2.h
        include/normal/pushdown/join/TupleSetIndex.cpp include/normal/pushdown/join/TupleSetIndex.h
        include/normal/pushdown/join/TupleSetIndexWrapper.cpp include/normal/pushdown/join/TupleSetIndexWrapper.h
        include/normal/pushdown/join/RecordBatchJoiner.cpp include/normal/pushdown/join/RecordBatchJoiner.h
        include/normal/pushdown/join/TupleSetIndexFinder.cpp include/normal/pushdown/join/TupleSetIndexFinder.h
        include/normal/pushdown/join/TupleSetIndexFinderWrapper.cpp include/normal/pushdown/join/TupleSetIndexFinderWrapper.h)

target_include_directories(normal-pushdown PUBLIC include)

target_link_libraries(normal-pushdown PUBLIC pthread)
target_link_libraries(normal-pushdown PRIVATE spdlog::spdlog)
target_link_libraries(normal-pushdown PUBLIC aws-cpp-sdk-core)
target_link_libraries(normal-pushdown PUBLIC aws-cpp-sdk-s3)
target_link_libraries(normal-pushdown PUBLIC arrow_static)
target_link_libraries(normal-pushdown PUBLIC gandiva_static)
target_link_libraries(normal-pushdown PUBLIC fmt::fmt)
target_link_libraries(normal-pushdown PUBLIC normal-core)
target_link_libraries(normal-pushdown PUBLIC normal-tuple)
target_link_libraries(normal-pushdown PUBLIC normal-expression)
target_link_libraries(normal-pushdown PUBLIC normal-expression-gandiva)


#-----------------------------------------------------------------------------------------------------------------------
# Test
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-pushdown-test
        test/Globals.h
        test/TestUtil.cpp test/TestUtil.h
        test/MainTest.cpp
        test/JoinTest.cpp
        test/FilterTest.cpp
        test/GroupTest.cpp
        test/AggregateTest.cpp
        test/SortTest.cpp
        test/FileScanTest.cpp
        test/CacheTest.cpp
        test/S3SelectScanTest.cpp
        test/ShuffleTest.cpp test/MergeTest.cpp)


target_include_directories(normal-pushdown-test PRIVATE include)

target_link_libraries(normal-pushdown-test PRIVATE spdlog::spdlog)
target_link_libraries(normal-pushdown-test PRIVATE doctest::doctest)
target_link_libraries(normal-pushdown-test PRIVATE normal-pushdown)


#-----------------------------------------------------------------------------------------------------------------------
# Micro Benchmark
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-pushdown-benchmark
        bench/Globals.h
        bench/MainBenchmark.cpp
        bench/ShuffleBenchmark.cpp bench/HashJoinBenchmark.cpp)


target_include_directories(normal-pushdown-benchmark PRIVATE include)

target_link_libraries(normal-pushdown-benchmark PRIVATE spdlog::spdlog)
target_link_libraries(normal-pushdown-benchmark PRIVATE doctest::doctest)
target_link_libraries(normal-pushdown-benchmark PRIVATE nanobench::nanobench)
target_link_libraries(normal-pushdown-benchmark PRIVATE normal-tuple)
target_link_libraries(normal-pushdown-benchmark PRIVATE normal-pushdown)


#-----------------------------------------------------------------------------------------------------------------------
# Test Data
#-----------------------------------------------------------------------------------------------------------------------

configure_file(data/filter/a.csv ${CMAKE_CURRENT_BINARY_DIR}/data/filter/a.csv COPYONLY)

configure_file(data/join/a.csv ${CMAKE_CURRENT_BINARY_DIR}/data/join/a.csv COPYONLY)
configure_file(data/join/b.csv ${CMAKE_CURRENT_BINARY_DIR}/data/join/b.csv COPYONLY)

configure_file(data/group/a.csv ${CMAKE_CURRENT_BINARY_DIR}/data/group/a.csv COPYONLY)

configure_file(data/aggregate/a.csv ${CMAKE_CURRENT_BINARY_DIR}/data/aggregate/a.csv COPYONLY)

configure_file(data/filescan/single-partition/test.csv ${CMAKE_CURRENT_BINARY_DIR}/data/filescan/single-partition/test.csv COPYONLY)
configure_file(data/filescan/multi-partition/test01.csv ${CMAKE_CURRENT_BINARY_DIR}/data/filescan/multi-partition/test01.csv COPYONLY)
configure_file(data/filescan/multi-partition/test02.csv ${CMAKE_CURRENT_BINARY_DIR}/data/filescan/multi-partition/test02.csv COPYONLY)
configure_file(data/filescan/multi-partition/test03.csv ${CMAKE_CURRENT_BINARY_DIR}/data/filescan/multi-partition/test03.csv COPYONLY)

configure_file(data/cache/test.csv ${CMAKE_CURRENT_BINARY_DIR}/data/cache/test.csv COPYONLY)

#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-pushdown)
