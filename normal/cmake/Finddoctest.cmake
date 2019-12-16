# Doctest
set(DOCTEST_VERSION "2.3.5")
set(DOCTEST_GIT_URL "https://github.com/onqtam/doctest.git")

ExternalProject_Add(doctest-project
        PREFIX ${DEPENDENCIES_BASE_DIR}/doctest
        GIT_REPOSITORY ${DOCTEST_GIT_URL}
        GIT_TAG ${DOCTEST_VERSION}
        CMAKE_ARGS
        -DCMAKE_C_COMPILER:STRING=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER:STRING=${CMAKE_CXX_COMPILER}
        -DCMAKE_CXX_STANDARD:STRING=${CMAKE_CXX_STANDARD}
        -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX=${DEPENDENCIES_BASE_DIR}/doctest
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
        )

ExternalProject_Get_Property(doctest-project SOURCE_DIR)
set(DOCTEST_INCLUDE_DIR ${SOURCE_DIR} CACHE INTERNAL "Doctest include dir")

add_library("doctest" INTERFACE IMPORTED)
set_target_properties("doctest" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${DOCTEST_INCLUDE_DIR})
add_dependencies("doctest" "doctest-project")

