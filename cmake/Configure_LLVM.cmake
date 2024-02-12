# Search for LLVM
set(LLVM_HINTS ${LLVM_ROOT} ${LLVM_DIR} /usr/lib /usr/share)
if(_LLVM_DIR)
    list(APPEND LLVM_HINTS ${_LLVM_DIR})
endif()
foreach (FPDB_LLVM_VERSION ${FPDB_LLVM_VERSIONS})
    find_package(LLVM
            ${FPDB_LLVM_VERSION}
            QUIET
            CONFIG
            HINTS
            ${LLVM_HINTS})
    if (LLVM_FOUND)
        break()
    endif ()
endforeach ()


# Define LLVM targets
if (LLVM_FOUND)

    message(STATUS "Found acceptable LLVM version ${LLVM_VERSION} ${LLVM_DIR}")

    # Find the libraries that correspond to the LLVM components
    llvm_map_components_to_libnames(LLVM_LIBS
            core
            mcjit
            native
            ipo
            bitreader
            target
            linker
            analysis
            debuginfodwarf)

    find_program(LLVM_LINK_EXECUTABLE llvm-link HINTS ${LLVM_TOOLS_BINARY_DIR})

    find_program(CLANG_EXECUTABLE
            NAMES clang-${LLVM_PACKAGE_VERSION}
            clang-${LLVM_VERSION_MAJOR}.${LLVM_VERSION_MINOR}
            clang-${LLVM_VERSION_MAJOR} clang
            HINTS ${LLVM_TOOLS_BINARY_DIR})

    add_library(LLVM::LLVM_INTERFACE INTERFACE IMPORTED)

    set_target_properties(LLVM::LLVM_INTERFACE
            PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${LLVM_INCLUDE_DIRS}"
            INTERFACE_COMPILE_FLAGS "${LLVM_DEFINITIONS}"
            INTERFACE_LINK_LIBRARIES "${LLVM_LIBS}")
else ()
    message(FATAL_ERROR "Could not find acceptable LLVM version. Acceptable versions: ${FPDB_LLVM_VERSIONS}")
endif ()

mark_as_advanced(CLANG_EXECUTABLE LLVM_LINK_EXECUTABLE)

find_package_handle_standard_args(LLVMAlt
        REQUIRED_VARS # The first variable is used for display.
        LLVM_PACKAGE_VERSION
        CLANG_EXECUTABLE
        LLVM_FOUND
        LLVM_LINK_EXECUTABLE)
if (LLVMAlt_FOUND)
    message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
    message(STATUS "Found llvm-link ${LLVM_LINK_EXECUTABLE}")
    message(STATUS "Found clang ${CLANG_EXECUTABLE}")
endif ()
