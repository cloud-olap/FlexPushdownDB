cmake_minimum_required(VERSION 3.15)
project(normal)

include(ExternalProject)

set(CMAKE_CXX_STANDARD 17)

#-----------------------------------------------------------------------------------------------------------------------
# Dependencies
#-----------------------------------------------------------------------------------------------------------------------

file(GLOB HEADERS
        "include/*.h"
        )

file(GLOB SOURCE
        "src/*.cpp"
        )

file(GLOB ALL
        ${HEADERS}
        ${SOURCE}
        )

add_executable(${PROJECT_NAME} ${ALL})

#-----------------------------------------------------------------------------------------------------------------------
# Dependencies
#-----------------------------------------------------------------------------------------------------------------------

set(DEPENDENCIES_BASE_DIR "${CMAKE_BINARY_DIR}/contrib")

# AWS SDK
set(AWS_CPP_SDK_VERSION "1.7.227")
set(AWS_CPP_SDK_GIT_URL "https://github.com/aws/aws-sdk-cpp.git")

ExternalProject_Add(awscppsdk
        GIT_REPOSITORY ${AWS_CPP_SDK_GIT_URL}
        GIT_TAG ${AWS_CPP_SDK_VERSION}
        PREFIX ${DEPENDENCIES_BASE_DIR}/awscppsdk
        CMAKE_ARGS -DBUILD_ONLY=s3 -DCMAKE_INSTALL_PREFIX=${DEPENDENCIES_BASE_DIR}/awscppsdk
        )
add_dependencies(normal awscppsdk)

ExternalProject_get_property(awscppsdk INSTALL_DIR)
MESSAGE("AWS SDK installation directory: '${INSTALL_DIR}'")

target_include_directories(${PROJECT_NAME} PUBLIC
        ${INSTALL_DIR}/include)
target_link_directories(${PROJECT_NAME} PRIVATE
        ${INSTALL_DIR}/lib/)
target_link_libraries(${PROJECT_NAME} PRIVATE
        aws-cpp-sdk-core
        aws-cpp-sdk-s3)
