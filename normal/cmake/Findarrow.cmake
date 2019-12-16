# Arrow
set(ARROW_VERSION "cb2647a")
set(ARROW_GIT_URL "https://github.com/apache/arrow.git")

ExternalProject_Add("arrow-project"
        PREFIX ${DEPENDENCIES_BASE_DIR}/arrow
        GIT_REPOSITORY ${ARROW_GIT_URL}
        GIT_TAG ${ARROW_VERSION}
        SOURCE_SUBDIR cpp
        CMAKE_ARGS
        -DARROW_USE_CCACHE:BOOL=yes
        -DARROW_CSV:BOOL=yes
        -DARROW_DATASET:BOOL=yes
        -DARROW_FLIGHT:BOOL=no
        -DARROW_IPC:BOOL=no
        -DARROW_PARQUET:BOOL=no
        -DARROW_WITH_SNAPPY:BOOL=yes
        -DARROW_JEMALLOC:BOOL=yes
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_CXX_STANDARD:STRING=${CMAKE_CXX_STANDARD}
        -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX=${DEPENDENCIES_BASE_DIR}/arrow
        )

ExternalProject_get_property("arrow-project" INSTALL_DIR)
set(ARROW_LIB_DIR ${INSTALL_DIR}/lib)
set(ARROW_INCLUDE_DIR ${INSTALL_DIR}/include)


file(MAKE_DIRECTORY ${ARROW_INCLUDE_DIR}) # Include directory needs to exist to run configure step


add_library("arrow" STATIC IMPORTED)
set_target_properties("arrow" PROPERTIES IMPORTED_LOCATION ${ARROW_LIB_DIR}/libarrow.a)
set_target_properties("arrow" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_DIR})
add_dependencies("arrow" "arrow-project")
showTargetProps("arrow")

add_library("arrow-dataset" STATIC IMPORTED)
set_target_properties("arrow-dataset" PROPERTIES IMPORTED_LOCATION ${ARROW_LIB_DIR}/libarrow_dataset.a)
set_target_properties("arrow-dataset" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_DIR})
add_dependencies("arrow-dataset" "arrow-project")
showTargetProps("arrow-dataset")

add_library("jemalloc" STATIC IMPORTED)
set_target_properties("jemalloc" PROPERTIES IMPORTED_LOCATION ${ARROW_LIB_DIR}/../src/arrow-project-build/jemalloc_ep-prefix/src/jemalloc_ep/lib/libjemalloc.a)
set_target_properties("jemalloc" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_DIR})
add_dependencies("jemalloc" "arrow-project")
showTargetProps("jemalloc")

#add_library("arrow-flight" STATIC IMPORTED)
#set_target_properties("arrow-flight" PROPERTIES IMPORTED_LOCATION ${ARROW_LIB_DIR}/libarrow_flight.a)
#set_target_properties("arrow-flight" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_DIR})
#add_dependencies("arrow-flight" "arrow-project")

#add_library("arrow-parquet" STATIC IMPORTED)
#set_target_properties("arrow-parquet" PROPERTIES IMPORTED_LOCATION ${ARROW_LIB_DIR}/libparquet.a)
#set_target_properties("arrow-parquet" PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_DIR})
#add_dependencies("arrow-parquet" "arrow-project")

