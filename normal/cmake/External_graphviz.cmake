# Graphviz
set(GRAPHVIZ_VERSION "stable_release_2.42.0")
set(GRAPHVIZ_GIT_URL "https://gitlab.com/graphviz/graphviz.git")
set(GRAPHVIZ_SOURCE_URL "https://graphviz.gitlab.io/pub/graphviz/stable/SOURCES/graphviz.tar.gz")


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


# Note the version cloned from git, does not built properly, and nor do the cmake scripts. Need to use autoconf.

ExternalProject_Add(${GRAPHVIZ_BASE}
        PREFIX ${GRAPHVIZ_PREFIX}
        URL ${GRAPHVIZ_SOURCE_URL}
        UPDATE_DISCONNECTED TRUE
        INSTALL_DIR ${GRAPHVIZ_INSTALL_DIR}
        CONFIGURE_COMMAND
            cd ${GRAPHVIZ_BUILD_DIR} &&
            CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} ${GRAPHVIZ_SOURCE_DIR}/configure --enable-perl=no --prefix=${GRAPHVIZ_INSTALL_DIR}
        BUILD_COMMAND
            cd ${GRAPHVIZ_BUILD_DIR} &&
            make --silent
        INSTALL_COMMAND
            cd ${GRAPHVIZ_BUILD_DIR} &&
            make --silent install
        BUILD_BYPRODUCTS
        ${GRAPHVIZ_GVC_SHARED_LIB}
        ${GRAPHVIZ_CDT_SHARED_LIB}
        ${GRAPHVIZ_XDOT_SHARED_LIB}
        ${GRAPHVIZ_PATHPLAN_SHARED_LIB}
        ${GRAPHVIZ_CGRAPH_SHARED_LIB}
        ${GRAPHVIZ_PLUGIN_CORE_SHARED_LIB}
        ${GRAPHVIZ_DOT_LAYOUT_SHARED_LIB}
        ${GRAPHVIZ_NEATO_LAYOUT_SHARED_LIB}
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

add_library(graphviz_neato_layout_shared SHARED IMPORTED)
set_target_properties(graphviz_neato_layout_shared PROPERTIES IMPORTED_LOCATION ${GRAPHVIZ_NEATO_LAYOUT_SHARED_LIB})
target_include_directories(graphviz_neato_layout_shared INTERFACE ${GRAPHVIZ_INCLUDE_DIR})
target_link_libraries(graphviz_neato_layout_shared INTERFACE graphviz_gvc_shared)
target_link_libraries(graphviz_neato_layout_shared INTERFACE graphviz_pathplan_shared)
target_link_libraries(graphviz_neato_layout_shared INTERFACE graphviz_cgraph_shared)
target_link_libraries(graphviz_neato_layout_shared INTERFACE graphviz_cdt_shared)
target_link_libraries(graphviz_neato_layout_shared INTERFACE graphviz_xdot_shared)
add_dependencies(graphviz_neato_layout_shared ${GRAPHVIZ_BASE})



#showTargetProps(graphviz_neato_layout_shared)
#showTargetProps(arrow_dataset_static)
