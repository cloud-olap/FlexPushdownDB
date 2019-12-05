include(ExternalProject)
find_package(Git REQUIRED)


#-----------------------------------------------------------------------------------------------------------------------
# External dependencies
#-----------------------------------------------------------------------------------------------------------------------

ExternalProject_Add(
        doctest
        PREFIX ${DEPENDENCIES_BASE_DIR}/doctest
        GIT_REPOSITORY https://github.com/onqtam/doctest.git
        TIMEOUT 10
        UPDATE_COMMAND ${GIT_EXECUTABLE} pull
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
        GIT_PROGRESS TRUE
        LOG_DOWNLOAD ON
        LOG_CONFIGURE ON
        LOG_BUILD ON
)

# Expose required variable (DOCTEST_INCLUDE_DIR) to parent scope
ExternalProject_Get_Property(doctest source_dir)
set(DOCTEST_INCLUDE_DIR ${source_dir}/doctest CACHE INTERNAL "Doctest include dir")


#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_executable("normal-test" src/MainTest.cpp)

target_include_directories("normal-test" PUBLIC ${DOCTEST_INCLUDE_DIR})

target_link_libraries("normal-test" PUBLIC "normal-core" "normal-pushdown")


#-----------------------------------------------------------------------------------------------------------------------
# Dependency graph
#-----------------------------------------------------------------------------------------------------------------------

add_dependencies("normal-test"
        "doctest"
        "normal-core"
        "normal-pushdown")
