project("normal-test" VERSION "${PROJECT_VERSION}" LANGUAGES CXX)

include(ExternalProject)
find_package(Git REQUIRED)


#-----------------------------------------------------------------------------------------------------------------------
# Dependencies
#-----------------------------------------------------------------------------------------------------------------------




#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_executable("normal-test")

target_sources("normal-test" PRIVATE
        src/MainTest.cpp)

target_compile_features("normal-test" PRIVATE cxx_std_17)
target_compile_options("normal-test" PRIVATE -Wall -Wextra -pedantic)
set_target_properties("normal-test" PROPERTIES LINKER_LANGUAGE CXX)

target_include_directories("normal-test" PRIVATE include)

target_link_libraries(normal-test PRIVATE normal-core)
target_link_libraries("normal-test" PRIVATE "normal-pushdown")
target_link_libraries("normal-test" PRIVATE spdlog)
target_link_libraries("normal-test" PRIVATE doctest)

# Test data
configure_file(data/test.csv ${CMAKE_CURRENT_BINARY_DIR}/data/test.csv COPYONLY)


#-----------------------------------------------------------------------------------------------------------------------
# Dependency graph
#-----------------------------------------------------------------------------------------------------------------------

add_dependencies("normal-test" "spdlog")
add_dependencies("normal-test" "doctest")
add_dependencies("normal-test" "normal-core")
add_dependencies("normal-test" "normal-pushdown")


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

showTargetProps("normal-test")
