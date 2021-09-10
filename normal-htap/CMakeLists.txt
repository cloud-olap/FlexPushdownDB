cmake_policy(SET CMP0048 NEW)

project("normal-htap" VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

# need to link libraries on mac
LINK_DIRECTORIES(/usr/local/opt/llvm@11/lib)
LINK_DIRECTORIES(/usr/local/opt/binutils/lib)

#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-htap STATIC
        test/S3AvroTest.cpp
        )

target_include_directories(normal-htap PUBLIC include)

target_link_libraries(normal-htap PUBLIC normal-avro-tuple)
target_link_libraries(normal-htap PUBLIC aws-cpp-sdk-core)
target_link_libraries(normal-htap PUBLIC aws-cpp-sdk-s3)
target_link_libraries(normal-htap PUBLIC normal-pushdown)
target_link_libraries(normal-htap PUBLIC /usr/local/lib/libavrocpp.so)

#-----------------------------------------------------------------------------------------------------------------------
# Test
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-htap-test
        test/S3AvroTest.cpp
        )
target_include_directories(normal-htap-test PUBLIC include)

target_link_libraries(normal-htap-test PUBLIC normal-htap)