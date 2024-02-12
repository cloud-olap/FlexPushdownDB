# Graphviz

set(GRAPHVIZ_VERSION "4136626c") # master at 11/10 (cmake starts working at this commit)
set(GRAPHVIZ_GIT_URL "https://gitlab.com/graphviz/graphviz.git")


include(ExternalProject)
find_package(Git REQUIRED)


set(GRAPHVIZ_BASE graphviz_ep)
set(GRAPHVIZ_PREFIX ${DEPS_PREFIX}/${GRAPHVIZ_BASE})
set(GRAPHVIZ_BASE_DIR ${CMAKE_CURRENT_BINARY_DIR}/${GRAPHVIZ_PREFIX})
set(GRAPHVIZ_SOURCE_DIR ${GRAPHVIZ_BASE_DIR}/src/${GRAPHVIZ_BASE})
set(GRAPHVIZ_BUILD_DIR ${GRAPHVIZ_BASE_DIR}/src/${GRAPHVIZ_BASE}-build)
set(GRAPHVIZ_INSTALL_DIR ${GRAPHVIZ_BASE_DIR}/install)
set(GRAPHVIZ_INCLUDE_DIR ${GRAPHVIZ_INSTALL_DIR}/include)
set(GRAPHVIZ_LIB_DIR ${GRAPHVIZ_INSTALL_DIR}/lib)

set(GRAPHVIZ_GVC_SHARED_LIB ${GRAPHVIZ_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}gvc${CMAKE_SHARED_LIBRARY_SUFFIX})
set(GRAPHVIZ_CDT_SHARED_LIB ${GRAPHVIZ_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}cdt${CMAKE_SHARED_LIBRARY_SUFFIX})
set(GRAPHVIZ_XDOT_SHARED_LIB ${GRAPHVIZ_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}xdot${CMAKE_SHARED_LIBRARY_SUFFIX})
set(GRAPHVIZ_PATHPLAN_SHARED_LIB ${GRAPHVIZ_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}pathplan${CMAKE_SHARED_LIBRARY_SUFFIX})
set(GRAPHVIZ_CGRAPH_SHARED_LIB ${GRAPHVIZ_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}cgraph${CMAKE_SHARED_LIBRARY_SUFFIX})

set(GRAPHVIZ_PLUGIN_CORE_SHARED_LIB ${GRAPHVIZ_LIB_DIR}/graphviz/${CMAKE_SHARED_LIBRARY_PREFIX}gvplugin_core${CMAKE_SHARED_LIBRARY_SUFFIX})
set(GRAPHVIZ_DOT_LAYOUT_SHARED_LIB ${GRAPHVIZ_LIB_DIR}/graphviz/${CMAKE_SHARED_LIBRARY_PREFIX}gvplugin_dot_layout${CMAKE_SHARED_LIBRARY_SUFFIX})
set(GRAPHVIZ_NEATO_LAYOUT_SHARED_LIB ${GRAPHVIZ_LIB_DIR}/graphviz/${CMAKE_SHARED_LIBRARY_PREFIX}gvplugin_neato_layout${CMAKE_SHARED_LIBRARY_SUFFIX})

set(GRAPHVIZ_SPARSE_STATIC_LIB ${GRAPHVIZ_BUILD_DIR}/lib/sparse/${CMAKE_SHARED_LIBRARY_PREFIX}sparse${CMAKE_STATIC_LIBRARY_SUFFIX})
set(GRAPHVIZ_NEATOGEN_STATIC_LIB ${GRAPHVIZ_BUILD_DIR}/lib/neatogen/${CMAKE_SHARED_LIBRARY_PREFIX}neatogen${CMAKE_STATIC_LIBRARY_SUFFIX})


# Note the version cloned from git, does not built properly, and nor do the cmake scripts. Need to use autoconf.

find_program(BISON_COMMAND bison PATHS /usr/bin /usr/local/opt)
get_filename_component(BISON_ROOT ${BISON_COMMAND} DIRECTORY)

ExternalProject_Add(${GRAPHVIZ_BASE}
        PREFIX ${GRAPHVIZ_PREFIX}
        GIT_REPOSITORY ${GRAPHVIZ_GIT_URL}
        GIT_TAG ${GRAPHVIZ_VERSION}
        UPDATE_DISCONNECTED TRUE
        INSTALL_DIR ${GRAPHVIZ_INSTALL_DIR}
        BUILD_BYPRODUCTS
        ${GRAPHVIZ_GVC_SHARED_LIB}
        ${GRAPHVIZ_CDT_SHARED_LIB}
        ${GRAPHVIZ_XDOT_SHARED_LIB}
        ${GRAPHVIZ_PATHPLAN_SHARED_LIB}
        ${GRAPHVIZ_CGRAPH_SHARED_LIB}
        ${GRAPHVIZ_PLUGIN_CORE_SHARED_LIB}
        ${GRAPHVIZ_DOT_LAYOUT_SHARED_LIB}
        ${GRAPHVIZ_NEATO_LAYOUT_SHARED_LIB}
        ${GRAPHVIZ_SPARSE_STATIC_LIB}
        ${GRAPHVIZ_NEATOGEN_STATIC_LIB}
        CMAKE_ARGS
        -DCMAKE_INSTALL_MESSAGE=NEVER
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX=${GRAPHVIZ_INSTALL_DIR}
        -DCMAKE_POLICY_DEFAULT_CMP0074=NEW
        -DCMAKE_POLICY_DEFAULT_CMP0042=NEW
        -DBISON_ROOT=${BISON_ROOT}
        )


file(MAKE_DIRECTORY ${GRAPHVIZ_INCLUDE_DIR}) # Include directory needs to exist to run configure step

add_library(graphviz_cgraph_shared SHARED IMPORTED)
set_target_properties(graphviz_cgraph_shared PROPERTIES IMPORTED_LOCATION ${GRAPHVIZ_CGRAPH_SHARED_LIB})
target_include_directories(graphviz_cgraph_shared INTERFACE ${GRAPHVIZ_INCLUDE_DIR})
add_dependencies(graphviz_cgraph_shared ${GRAPHVIZ_BASE})

add_library(graphviz_cdt_shared SHARED IMPORTED)
set_target_properties(graphviz_cdt_shared PROPERTIES IMPORTED_LOCATION ${GRAPHVIZ_CDT_SHARED_LIB})
target_include_directories(graphviz_cdt_shared INTERFACE ${GRAPHVIZ_INCLUDE_DIR})
add_dependencies(graphviz_cdt_shared ${GRAPHVIZ_BASE})

add_library(graphviz_xdot_shared SHARED IMPORTED)
set_target_properties(graphviz_xdot_shared PROPERTIES IMPORTED_LOCATION ${GRAPHVIZ_XDOT_SHARED_LIB})
target_include_directories(graphviz_xdot_shared INTERFACE ${GRAPHVIZ_INCLUDE_DIR})
add_dependencies(graphviz_xdot_shared ${GRAPHVIZ_BASE})

add_library(graphviz_pathplan_shared SHARED IMPORTED)
set_target_properties(graphviz_pathplan_shared PROPERTIES IMPORTED_LOCATION ${GRAPHVIZ_PATHPLAN_SHARED_LIB})
target_include_directories(graphviz_pathplan_shared INTERFACE ${GRAPHVIZ_INCLUDE_DIR})
add_dependencies(graphviz_pathplan_shared ${GRAPHVIZ_BASE})

add_library(graphviz_plugin_core_shared SHARED IMPORTED)
set_target_properties(graphviz_plugin_core_shared PROPERTIES IMPORTED_LOCATION ${GRAPHVIZ_PLUGIN_CORE_SHARED_LIB})
target_include_directories(graphviz_plugin_core_shared INTERFACE ${GRAPHVIZ_INCLUDE_DIR})
add_dependencies(graphviz_plugin_core_shared ${GRAPHVIZ_BASE})

add_library(graphviz_gvc_shared SHARED IMPORTED)
set_target_properties(graphviz_gvc_shared PROPERTIES IMPORTED_LOCATION ${GRAPHVIZ_GVC_SHARED_LIB})
target_include_directories(graphviz_gvc_shared INTERFACE ${GRAPHVIZ_INCLUDE_DIR})
target_link_libraries(graphviz_gvc_shared INTERFACE graphviz_cgraph_shared)
target_link_libraries(graphviz_gvc_shared INTERFACE graphviz_pathplan_shared)
target_link_libraries(graphviz_gvc_shared INTERFACE graphviz_xdot_shared)
target_link_libraries(graphviz_gvc_shared INTERFACE graphviz_cdt_shared)
target_link_libraries(graphviz_gvc_shared INTERFACE graphviz_plugin_core_shared)
add_dependencies(graphviz_gvc_shared ${GRAPHVIZ_BASE})

add_library(graphviz_dot_layout_shared SHARED IMPORTED)
set_target_properties(graphviz_dot_layout_shared PROPERTIES IMPORTED_LOCATION ${GRAPHVIZ_NEATO_LAYOUT_SHARED_LIB})
target_include_directories(graphviz_dot_layout_shared INTERFACE ${GRAPHVIZ_INCLUDE_DIR})
target_link_libraries(graphviz_dot_layout_shared INTERFACE graphviz_gvc_shared)
target_link_libraries(graphviz_dot_layout_shared INTERFACE graphviz_pathplan_shared)
target_link_libraries(graphviz_dot_layout_shared INTERFACE graphviz_cgraph_shared)
target_link_libraries(graphviz_dot_layout_shared INTERFACE graphviz_cdt_shared)
target_link_libraries(graphviz_dot_layout_shared INTERFACE graphviz_xdot_shared)
add_dependencies(graphviz_dot_layout_shared ${GRAPHVIZ_BASE})

add_library(graphviz_sparse_static STATIC IMPORTED)
set_target_properties(graphviz_sparse_static PROPERTIES IMPORTED_LOCATION ${GRAPHVIZ_SPARSE_STATIC_LIB})
add_dependencies(graphviz_sparse_static ${GRAPHVIZ_BASE})

add_library(graphviz_neatogen_static STATIC IMPORTED)
set_target_properties(graphviz_neatogen_static PROPERTIES IMPORTED_LOCATION ${GRAPHVIZ_NEATOGEN_STATIC_LIB})
add_dependencies(graphviz_neatogen_static ${GRAPHVIZ_BASE})

add_library(graphviz_neato_layout_shared SHARED IMPORTED)
set_target_properties(graphviz_neato_layout_shared PROPERTIES IMPORTED_LOCATION ${GRAPHVIZ_NEATO_LAYOUT_SHARED_LIB})
target_include_directories(graphviz_neato_layout_shared INTERFACE ${GRAPHVIZ_INCLUDE_DIR})
target_link_libraries(graphviz_neato_layout_shared INTERFACE graphviz_sparse_static)
target_link_libraries(graphviz_neato_layout_shared INTERFACE graphviz_neatogen_static)
target_link_libraries(graphviz_neato_layout_shared INTERFACE graphviz_gvc_shared)
target_link_libraries(graphviz_neato_layout_shared INTERFACE graphviz_pathplan_shared)
target_link_libraries(graphviz_neato_layout_shared INTERFACE graphviz_cgraph_shared)
target_link_libraries(graphviz_neato_layout_shared INTERFACE graphviz_cdt_shared)
target_link_libraries(graphviz_neato_layout_shared INTERFACE graphviz_xdot_shared)
add_dependencies(graphviz_neato_layout_shared ${GRAPHVIZ_BASE})



#showTargetProps(graphviz_neato_layout_shared)
#showTargetProps(arrow_dataset_static)
