project(normal-ssb VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-ssb-test
        include/normal/ssb/Globals.h
        test/Main.cpp
        test/SSBTest.cpp
        src/Queries.cpp include/normal/ssb/Queries.h)

target_include_directories(normal-ssb-test PUBLIC include)

target_link_libraries(normal-ssb-test PRIVATE spdlog::spdlog)
target_link_libraries(normal-ssb-test PRIVATE doctest::doctest)
target_link_libraries(normal-ssb-test PRIVATE nanobench::nanobench)
target_link_libraries(normal-ssb-test PRIVATE fmt::fmt)
target_link_libraries(normal-ssb-test PRIVATE normal-test)
target_link_libraries(normal-ssb-test PRIVATE normal-sql)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-ssb)
