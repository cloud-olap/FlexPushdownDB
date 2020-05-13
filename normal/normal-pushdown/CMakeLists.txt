project("normal-pushdown" VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-pushdown STATIC
        src/io/CSVParser.cpp
        src/io/CSVParser.h
        include/normal/pushdown/Globals.h
        src/Collate.cpp include/normal/pushdown/Collate.h
        src/S3SelectScan.cpp include/normal/pushdown/S3SelectScan.h
        src/FileScan.cpp include/normal/pushdown/FileScan.h
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
        src/group/Group.cpp include/normal/pushdown/group/Group.h)

target_include_directories(normal-pushdown PUBLIC include)

target_link_libraries(normal-pushdown PUBLIC pthread)
target_link_libraries(normal-pushdown PRIVATE spdlog::spdlog)
target_link_libraries(normal-pushdown PUBLIC aws-cpp-sdk-core)
target_link_libraries(normal-pushdown PUBLIC aws-cpp-sdk-s3)
target_link_libraries(normal-pushdown PUBLIC arrow_static)
target_link_libraries(normal-pushdown PUBLIC gandiva_static)
target_link_libraries(normal-pushdown PUBLIC normal-core)
target_link_libraries(normal-pushdown PUBLIC normal-tuple)
target_link_libraries(normal-pushdown PUBLIC normal-expression)
target_link_libraries(normal-pushdown PUBLIC normal-expression-gandiva)


#-----------------------------------------------------------------------------------------------------------------------
# Test
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-pushdown-test
        test/Globals.h
        test/MainTest.cpp
        test/JoinTest.cpp
        test/FilterTest.cpp test/GroupTest.cpp)

target_include_directories(normal-pushdown-test PRIVATE include)

target_link_libraries(normal-pushdown-test PRIVATE spdlog::spdlog)
target_link_libraries(normal-pushdown-test PRIVATE doctest::doctest)
target_link_libraries(normal-pushdown-test PRIVATE normal-test)
target_link_libraries(normal-pushdown-test PRIVATE normal-pushdown)


#-----------------------------------------------------------------------------------------------------------------------
# Test Data
#-----------------------------------------------------------------------------------------------------------------------

configure_file(data/filter/a.csv ${CMAKE_CURRENT_BINARY_DIR}/data/filter/a.csv COPYONLY)

configure_file(data/join/a.csv ${CMAKE_CURRENT_BINARY_DIR}/data/join/a.csv COPYONLY)
configure_file(data/join/b.csv ${CMAKE_CURRENT_BINARY_DIR}/data/join/b.csv COPYONLY)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-pushdown)
