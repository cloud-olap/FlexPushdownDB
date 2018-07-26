cmake_minimum_required (VERSION 2.8)
set (CMAKE_CXX_STANDARD 11)
set (CMAKE_CXX_FLAGS -pthread)
set (CMAKE_PREFIX_PATH  "/home/matt/Work/aws-sdk-cpp")

project (cpp-s3filter)

find_package(aws-sdk-cpp)

add_definitions(-DUSE_IMPORT_EXPORT)

#include_directories(/opt/boost_1_67_0)

add_executable(cpp-s3filter cpp-s3filter.cpp)

#target_link_libraries(cpp-s3filter pthread /opt/boost_1_67_0/stage/lib/libboost_system.so aws-cpp-sdk-core aws-cpp-sdk-s3)

target_link_libraries(cpp-s3filter pthread aws-cpp-sdk-core aws-cpp-sdk-s3)
