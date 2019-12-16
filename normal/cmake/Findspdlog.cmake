# SPDLog
set(SPDLOG_VERSION "v1.4.2")
set(SPDLOG_GIT_URL "https://github.com/gabime/spdlog.git")

ExternalProject_Add(spdlog-project
        PREFIX ${DEPENDENCIES_BASE_DIR}/spdlog
        INSTALL_DIR ${DEPENDENCIES_BASE_DIR}/spdlog
        GIT_REPOSITORY ${SPDLOG_GIT_URL}
        GIT_TAG ${SPDLOG_VERSION}
        CMAKE_ARGS
        -DSPDLOG_BUILD_EXAMPLE:BOOL=NO
        -DSPDLOG_BUILD_TESTS:BOOL=NO
        -DSPDLOG_BUILD_BENCH:BOOL=NO
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_CXX_STANDARD:STRING=${CMAKE_CXX_STANDARD}
        -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX=${DEPENDENCIES_BASE_DIR}/spdlog
        )

ExternalProject_Get_Property(spdlog-project INSTALL_DIR)
set(SPDLOG_INSTALL_DIR ${INSTALL_DIR})

file(MAKE_DIRECTORY ${SPDLOG_INSTALL_DIR}/include) # Include directory needs to exist to run configure step

add_library("spdlog" STATIC IMPORTED)
set_target_properties("spdlog" PROPERTIES IMPORTED_LOCATION ${SPDLOG_INSTALL_DIR}/lib/libspdlogd.a)
set_target_properties("spdlog" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${SPDLOG_INSTALL_DIR}/include)
add_dependencies("spdlog" "spdlog-project")
