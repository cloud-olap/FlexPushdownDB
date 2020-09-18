project(normal-cache VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-cache STATIC
        include/normal/cache/Globals.h
        src/SegmentCache.cpp include/normal/cache/SegmentCache.h
        src/SegmentKey.cpp include/normal/cache/SegmentKey.h
        src/SegmentData.cpp include/normal/cache/SegmentData.h
        src/SegmentRange.cpp include/normal/cache/SegmentRange.h
        src/LRUCachingPolicy.cpp include/normal/cache/LRUCachingPolicy.h src/CachingPolicy.cpp include/normal/cache/CachingPolicy.h src/SegmentMetadata.cpp include/normal/cache/SegmentMetadata.h src/FBRCachingPolicy.cpp include/normal/cache/FBRCachingPolicy.h src/Globals.cpp)

target_include_directories(normal-cache PUBLIC include)

target_link_libraries(normal-cache PRIVATE spdlog::spdlog)
target_link_libraries(normal-cache PRIVATE fmt::fmt)
target_link_libraries(normal-cache PUBLIC arrow_static)
target_link_libraries(normal-cache PUBLIC normal-connector)
target_link_libraries(normal-cache PUBLIC normal-tuple)


#-----------------------------------------------------------------------------------------------------------------------
# Test
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-cache-test
        test/Globals.h
        test/MainTest.cpp
        test/CacheTest.cpp)


target_include_directories(normal-cache-test PRIVATE include)

target_link_libraries(normal-cache-test spdlog::spdlog)
target_link_libraries(normal-cache-test doctest::doctest)
target_link_libraries(normal-cache-test Backward::Backward)
target_link_libraries(normal-cache-test normal-cache)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-cache)
