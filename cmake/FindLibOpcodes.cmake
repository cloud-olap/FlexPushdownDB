include(FindPackageHandleStandardArgs)
include(GNUInstallDirs)

find_path(LibOpcodes_INCLUDE_DIR NAMES
        "dis-asm.h"
        PATHS /usr/${CMAKE_INSTALL_INCLUDEDIR} /usr/local/opt/binutils/${CMAKE_INSTALL_INCLUDEDIR})

find_library(LibOpcodes_LIBRARY
        opcodes
        PATHS /usr/${CMAKE_INSTALL_LIBDIR} /usr/local/opt/binutils/${CMAKE_INSTALL_LIBDIR})

find_package_handle_standard_args(LibOpcodes
        REQUIRED_VARS LibOpcodes_LIBRARY LibOpcodes_INCLUDE_DIR)

if(LibOpcodes_FOUND)
    add_library(LibOpcodes::LibOpcodes UNKNOWN IMPORTED)
    set_target_properties(LibOpcodes::LibOpcodes PROPERTIES IMPORTED_LOCATION ${LibOpcodes_LIBRARY})
    target_include_directories(LibOpcodes::LibOpcodes INTERFACE ${LibOpcodes_INCLUDE_DIR})
else()
    message(FATAL_ERROR "LibOpcodes library not found")
endif()

#message(STATUS ${LibOpcodes_LIBRARY})
#message(STATUS ${LibOpcodes_INCLUDE_DIR})