include(FindPackageHandleStandardArgs)
include(GNUInstallDirs)

find_path(LibBFD_INCLUDE_DIR NAMES
        "bfd.h"
        PATHS /usr/${CMAKE_INSTALL_INCLUDEDIR} /usr/local/opt/binutils/${CMAKE_INSTALL_INCLUDEDIR})

find_library(LibBFD_LIBRARY
        bfd
        PATHS /usr/${CMAKE_INSTALL_LIBDIR} /usr/local/opt/binutils/${CMAKE_INSTALL_LIBDIR})

set(LibBFD_INCLUDE_DIRS ${LibBFD_INCLUDE_DIR})
set(LibBFD_LIBRARIES ${LibBFD_LIBRARY})

find_package_handle_standard_args(LibBFD
        REQUIRED_VARS LibBFD_LIBRARY LibBFD_INCLUDE_DIR)

if(LibBFD_FOUND)

    find_package(LibOpcodes REQUIRED)

    add_library(LibBFD::LibBFD UNKNOWN IMPORTED)
    set_target_properties(LibBFD::LibBFD PROPERTIES IMPORTED_LOCATION ${LibBFD_LIBRARY})
    target_include_directories(LibBFD::LibBFD INTERFACE ${LibBFD_INCLUDE_DIR})
    target_link_libraries(LibBFD::LibBFD INTERFACE LibOpcodes::LibOpcodes)
    target_link_libraries(LibBFD::LibBFD INTERFACE ${CMAKE_DL_LIBS})
else()
    message(FATAL_ERROR "BFD library not found")
endif()

#message(STATUS ${LibBFD_LIBRARY})
#message(STATUS ${LibBFD_INCLUDE_DIR})