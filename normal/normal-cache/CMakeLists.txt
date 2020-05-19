project(normal-cache VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-cache STATIC
        include/normal/cache/Globals.h)

target_include_directories(normal-cache PUBLIC include)

target_link_libraries(normal-cache PRIVATE spdlog::spdlog)
target_link_libraries(normal-cache PRIVATE fmt::fmt)
target_link_libraries(normal-cache PUBLIC arrow_static)
target_link_libraries(normal-cache PUBLIC normal-core)


#-----------------------------------------------------------------------------------------------------------------------
# Test
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-cache-test
        test/Globals.h
        test/MainTest.cpp)


target_include_directories(normal-cache-test PRIVATE include)

target_link_libraries(normal-cache-test PRIVATE spdlog::spdlog)
target_link_libraries(normal-cache-test PRIVATE doctest::doctest)
target_link_libraries(normal-cache-test PRIVATE normal-test)
target_link_libraries(normal-cache-test PRIVATE normal-cache)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-cache)
