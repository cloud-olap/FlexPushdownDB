#-------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(fpdb-cache STATIC
        src/Globals.cpp include/fpdb/cache/Globals.h
        src/Util.cpp include/fpdb/cache/Util.h
        src/SegmentCache.cpp include/fpdb/cache/SegmentCache.h
        src/SegmentKey.cpp include/fpdb/cache/SegmentKey.h
        src/SegmentData.cpp include/fpdb/cache/SegmentData.h
        src/SegmentRange.cpp include/fpdb/cache/SegmentRange.h
        src/SegmentMetadata.cpp include/fpdb/cache/SegmentMetadata.h
        src/policy/CachingPolicy.cpp include/fpdb/cache/policy/CachingPolicy.h
        src/policy/LRUCachingPolicy.cpp include/fpdb/cache/policy/LRUCachingPolicy.h
        src/policy/LFUCachingPolicy.cpp include/fpdb/cache/policy/LFUCachingPolicy.h
        src/policy/LFUSCachingPolicy.cpp include/fpdb/cache/policy/LFUSCachingPolicy.h
        src/policy/WLFUCachingPolicy.cpp include/fpdb/cache/policy/WLFUCachingPolicy.h
        src/policy/BeladyCachingPolicyHelper.cpp include/fpdb/cache/policy/BeladyCachingPolicyHelper.h
        src/policy/BeladyCachingPolicy.cpp include/fpdb/cache/policy/BeladyCachingPolicy.h
        include/fpdb/cache/caf-serialization/CAFCachingPolicySerializer.h)

target_include_directories(fpdb-cache PUBLIC include)

target_link_libraries(fpdb-cache PUBLIC fpdb-catalogue)


#-----------------------------------------------------------------------------------------------------------------------
# Test
#-----------------------------------------------------------------------------------------------------------------------

add_executable(fpdb-cache-test
        test/Globals.h
        test/MainTest.cpp
        test/CacheTest.cpp)


target_include_directories(fpdb-cache-test PRIVATE include)

target_link_libraries(fpdb-cache-test doctest::doctest)
target_link_libraries(fpdb-cache-test Backward::Backward)
target_link_libraries(fpdb-cache-test fpdb-cache)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(fpdb-cache)
