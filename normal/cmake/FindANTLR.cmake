# ANTLR
set(ANTLR_VERSION "4.8")
set(ANTLR_GIT_URL "https://github.com/antlr/antlr4.git")

ExternalProject_Add("antlr-project"
        PREFIX ${DEPENDENCIES_BASE_DIR}/antlr
        GIT_REPOSITORY ${ANTLR_GIT_URL}
        GIT_TAG ${ANTLR_VERSION}
        SOURCE_SUBDIR runtime/Cpp
        CMAKE_ARGS
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_CXX_STANDARD:STRING=${CMAKE_CXX_STANDARD}
        -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX=${DEPENDENCIES_BASE_DIR}/antlr
        )

ExternalProject_get_property("antlr-project" INSTALL_DIR)
set(ANTLR_LIB_DIR ${INSTALL_DIR}/lib)
set(ANTLR_INCLUDE_DIR ${INSTALL_DIR}/include/antlr4-runtime)


file(MAKE_DIRECTORY ${ANTLR_INCLUDE_DIR}) # Include directory needs to exist to run configure step


add_library("antlr" SHARED IMPORTED)
set_target_properties("antlr" PROPERTIES IMPORTED_LOCATION ${ANTLR_LIB_DIR}/libantlr4-runtime${CMAKE_SHARED_LIBRARY_SUFFIX})
set_target_properties("antlr" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${ANTLR_INCLUDE_DIR})
add_dependencies("antlr" "antlr-project")
showTargetProps("antlr")


add_library("antlr-static" STATIC IMPORTED)
set_target_properties("antlr-static" PROPERTIES IMPORTED_LOCATION ${ANTLR_LIB_DIR}/libantlr4-runtime${CMAKE_STATIC_LIBRARY_SUFFIX})
set_target_properties("antlr-static" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${ANTLR_INCLUDE_DIR})
add_dependencies("antlr-static" "antlr-project")
showTargetProps("antlr-static")
