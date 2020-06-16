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
        src/Util.cpp include/normal/pushdown/Util.h)

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
        test/S3SelectScanTest.cpp)


target_include_directories(normal-pushdown-test PRIVATE include)

target_link_libraries(normal-pushdown-test PRIVATE spdlog::spdlog)
target_link_libraries(normal-pushdown-test PRIVATE doctest::doctest)
target_link_libraries(normal-pushdown-test PRIVATE normal-pushdown)


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
