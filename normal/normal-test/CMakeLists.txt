project("normal-test" VERSION "${PROJECT_VERSION}" LANGUAGES CXX)

include(ExternalProject)
find_package(Git REQUIRED)


#-----------------------------------------------------------------------------------------------------------------------
# Dependencies
#-----------------------------------------------------------------------------------------------------------------------

# Doctest
set(DOCTEST_VERSION "2.3.5")
set(DOCTEST_GIT_URL "https://github.com/onqtam/doctest.git")

ExternalProject_Add(doctest-project
        PREFIX ${DEPENDENCIES_BASE_DIR}/doctest
        GIT_REPOSITORY ${DOCTEST_GIT_URL}
        GIT_TAG ${DOCTEST_VERSION}
        CMAKE_ARGS
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_INSTALL_PREFIX=${DEPENDENCIES_BASE_DIR}/doctest
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
        )

ExternalProject_Get_Property(doctest-project SOURCE_DIR)
set(DOCTEST_INCLUDE_DIR ${SOURCE_DIR} CACHE INTERNAL "Doctest include dir")

add_library("doctest" INTERFACE IMPORTED)
set_target_properties("doctest" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${DOCTEST_INCLUDE_DIR})


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

add_dependencies("normal-test" "spdlog-project")
add_dependencies("normal-test" "doctest-project")
add_dependencies("normal-test" "normal-core")
add_dependencies("normal-test" "normal-pushdown")


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

showTargetProps("normal-test")
