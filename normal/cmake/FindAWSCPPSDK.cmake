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
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_INSTALL_PREFIX=${DEPENDENCIES_BASE_DIR}/aws-cpp-sdk
        )

ExternalProject_get_property("aws-cpp-sdk-project" INSTALL_DIR)
set(AWS_SDK_INSTALL_DIR ${INSTALL_DIR})

file(MAKE_DIRECTORY ${AWS_SDK_INSTALL_DIR}/include) # Include directory needs to exist to run configure step

add_library("aws-cpp-sdk-core" SHARED IMPORTED)
set_target_properties("aws-cpp-sdk-core" PROPERTIES IMPORTED_LOCATION ${AWS_SDK_INSTALL_DIR}/lib/libaws-cpp-sdk-core${CMAKE_SHARED_LIBRARY_SUFFIX})
set_target_properties("aws-cpp-sdk-core" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${AWS_SDK_INSTALL_DIR}/include)
add_dependencies("aws-cpp-sdk-core" "aws-cpp-sdk-project")
showTargetProps("aws-cpp-sdk-core")


add_library("aws-cpp-sdk-s3" SHARED IMPORTED)
set_target_properties("aws-cpp-sdk-s3" PROPERTIES IMPORTED_LOCATION ${AWS_SDK_INSTALL_DIR}/lib/libaws-cpp-sdk-s3${CMAKE_SHARED_LIBRARY_SUFFIX})
set_target_properties("aws-cpp-sdk-s3" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${AWS_SDK_INSTALL_DIR}/include)
add_dependencies("aws-cpp-sdk-s3" "aws-cpp-sdk-project")
showTargetProps("aws-cpp-sdk-s3")
