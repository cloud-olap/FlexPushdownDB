project(fpdb-caf VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(fpdb-caf STATIC
        src/CAFUtil.cpp include/fpdb/caf/CAFUtil.h)

target_include_directories(fpdb-caf PUBLIC include)

target_link_libraries(fpdb-caf PUBLIC caf::libcaf_core_shared)
target_link_libraries(fpdb-caf PUBLIC caf::libcaf_io_shared)
target_link_libraries(fpdb-caf PUBLIC caf::libcaf_openssl_shared)