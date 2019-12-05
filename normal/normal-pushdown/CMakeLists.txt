project("normal-pushdown")

# AWS SDK
set(AWS_CPP_SDK_VERSION "1.7.227")
set(AWS_CPP_SDK_GIT_URL "https://github.com/aws/aws-sdk-cpp.git")

ExternalProject_Add(awscppsdk
        GIT_REPOSITORY ${AWS_CPP_SDK_GIT_URL}
        GIT_TAG ${AWS_CPP_SDK_VERSION}
        PREFIX ${DEPENDENCIES_BASE_DIR}/awscppsdk
        CMAKE_ARGS -DBUILD_ONLY=s3 -DCMAKE_INSTALL_PREFIX=${DEPENDENCIES_BASE_DIR}/awscppsdk
        GIT_PROGRESS TRUE
        LOG_DOWNLOAD ON
        LOG_CONFIGURE ON
        LOG_BUILD ON
        )

ExternalProject_get_property(awscppsdk INSTALL_DIR)
set(AWS_SDK_INSTALL_DIR ${INSTALL_DIR})
MESSAGE("AWS SDK installation directory: '${AWS_SDK_INSTALL_DIR}'")

include_directories("${AWS_SDK_INSTALL_DIR}/include")


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library("normal-pushdown" STATIC src/S3SelectScan.cpp include/normal/pushdown/S3SelectScan.h src/Collate.cpp include/normal/pushdown/Collate.h)

# PUBLIC will expose public headers internally and externally
target_include_directories("normal-pushdown" PUBLIC include)

target_link_libraries("normal-pushdown" PUBLIC "normal-core")

#link_directories(STATIC ${AWS_SDK_INSTALL_DIR}/lib)
link_directories(awscppsdk)


#-----------------------------------------------------------------------------------------------------------------------
# Dependency graph
#-----------------------------------------------------------------------------------------------------------------------

add_dependencies("normal-pushdown"
        "normal-core"
        "spdlog"
        awscppsdk)
