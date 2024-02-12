project(fpdb-aws VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# AWS
#-----------------------------------------------------------------------------------------------------------------------

add_library(fpdb-aws
        include/fpdb/aws/S3ClientType.h
        src/AWSClient.cpp include/fpdb/aws/AWSClient.h
        src/AWSConfig.cpp include/fpdb/aws/AWSConfig.h
        src/S3Util.cpp include/fpdb/aws/S3Util.h
        src/ProfileAWSCredentialsProviderChain.cpp include/fpdb/aws/ProfileAWSCredentialsProviderChain.h
        src/AirMettleClientAuthHandler.cpp include/fpdb/aws/AirMettleClientAuthHandler.hpp)

target_include_directories(fpdb-aws PUBLIC include)

target_link_libraries(fpdb-aws PUBLIC arrow_flight_static)
target_link_libraries(fpdb-aws PUBLIC fpdb-util)
target_link_libraries(fpdb-aws PUBLIC spdlog::spdlog)
target_link_libraries(fpdb-aws PUBLIC aws-cpp-sdk-core)
target_link_libraries(fpdb-aws PUBLIC aws-cpp-sdk-s3)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(fpdb-aws)
