project("normal-pushdown" VERSION "${PROJECT_VERSION}" LANGUAGES CXX)

include(ExternalProject)
find_package(Git REQUIRED)

#-----------------------------------------------------------------------------------------------------------------------
# Dependencies
#-----------------------------------------------------------------------------------------------------------------------

# AWS SDK
set(AWS_CPP_SDK_VERSION "1.7.227")
set(AWS_CPP_SDK_GIT_URL "https://github.com/aws/aws-sdk-cpp.git")

ExternalProject_Add("aws-cpp-sdk-project"
        GIT_REPOSITORY ${AWS_CPP_SDK_GIT_URL}
        GIT_TAG ${AWS_CPP_SDK_VERSION}
        PREFIX ${DEPENDENCIES_BASE_DIR}/aws-cpp-sdk
        CMAKE_ARGS
        -DBUILD_ONLY=s3
        -DCPP_STANDARD=17
        -DENABLE_TESTING=OFF
        -DUSE_IMPORT_EXPORT=ON
        -DCMAKE_INSTALL_PREFIX=${DEPENDENCIES_BASE_DIR}/aws-cpp-sdk
        )

ExternalProject_get_property("aws-cpp-sdk-project" INSTALL_DIR)
set(AWS_SDK_INSTALL_DIR ${INSTALL_DIR})

file(MAKE_DIRECTORY ${AWS_SDK_INSTALL_DIR}/include) # Include directory needs to exist to run configure step

add_library("aws-cpp-sdk-core" SHARED IMPORTED)
set_target_properties("aws-cpp-sdk-core" PROPERTIES IMPORTED_LOCATION ${AWS_SDK_INSTALL_DIR}/lib/libaws-cpp-sdk-core.so)
set_target_properties("aws-cpp-sdk-core" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${AWS_SDK_INSTALL_DIR}/include)

add_library("aws-cpp-sdk-s3" SHARED IMPORTED)
set_target_properties("aws-cpp-sdk-s3" PROPERTIES IMPORTED_LOCATION ${AWS_SDK_INSTALL_DIR}/lib/libaws-cpp-sdk-s3.so)
set_target_properties("aws-cpp-sdk-s3" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${AWS_SDK_INSTALL_DIR}/include)


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library("normal-pushdown" STATIC)

target_sources("normal-pushdown" PRIVATE
        src/Collate.cpp include/normal/pushdown/Collate.h
        src/S3SelectScan.cpp include/normal/pushdown/S3SelectScan.h)

target_compile_features("normal-pushdown" PRIVATE cxx_std_17)
set_target_properties("normal-pushdown" PROPERTIES LINKER_LANGUAGE CXX)

target_include_directories("normal-pushdown" PUBLIC include)

target_link_libraries("normal-pushdown" PUBLIC "normal-core")
target_link_libraries("normal-pushdown" PRIVATE spdlog)
target_link_libraries("normal-pushdown" PRIVATE aws-cpp-sdk-core)
target_link_libraries("normal-pushdown" PRIVATE aws-cpp-sdk-s3)


#-----------------------------------------------------------------------------------------------------------------------
# Dependency graph
#-----------------------------------------------------------------------------------------------------------------------

add_dependencies("normal-pushdown" "normal-core")
add_dependencies("normal-pushdown" "spdlog-project")
add_dependencies("normal-pushdown" "aws-cpp-sdk-project")


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

showTargetProps("normal-pushdown")
