project(normal-util VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Util
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-util
        src/Globals.cpp include/normal/util/Globals.h
        include/normal/util/Traits.h
        include/normal/util/Util.h src/Util.cpp)

target_include_directories(normal-util PUBLIC include)

target_link_libraries(normal-util PRIVATE LLVM-filesystem)
target_link_libraries(normal-util PRIVATE fmt::fmt)
target_link_libraries(normal-util PRIVATE spdlog::spdlog)
target_link_libraries(normal-util PUBLIC boost_callable_traits)
target_link_libraries(normal-util PUBLIC caf::libcaf_core_shared)
target_link_libraries(normal-util PUBLIC caf::libcaf_io_shared)


#-----------------------------------------------------------------------------------------------------------------------
# Test
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-util-test
        test/MainTest.cpp
        test/Globals.h
        test/TraitsTest.cpp)

target_link_libraries(normal-util-test fmt::fmt)
target_link_libraries(normal-util-test spdlog::spdlog)
target_link_libraries(normal-util-test doctest::doctest)
target_link_libraries(normal-util-test Backward::Backward)
target_link_libraries(normal-util-test normal-util)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-ssb-test)
#showTargetProps(normal-ssb-benchmark)
