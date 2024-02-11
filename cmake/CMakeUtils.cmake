function(showTargetProps target)

    get_target_property(TARGET_INCLUDE_DIRECTORIES ${target} INCLUDE_DIRECTORIES)
    get_target_property(TARGET_INTERFACE_INCLUDE_DIRECTORIES ${target} INTERFACE_INCLUDE_DIRECTORIES)
    get_target_property(TARGET_LINK_LIBRARIES ${target} LINK_LIBRARIES)
    get_target_property(TARGET_LINK_DIRECTORIES ${target} LINK_DIRECTORIES)
    get_target_property(TARGET_INTERFACE_LINK_LIBRARIES ${target} INTERFACE_LINK_LIBRARIES)
    get_target_property(TARGET_INTERFACE_LINK_DIRECTORIES ${target} INTERFACE_LINK_DIRECTORIES)
    get_target_property(TARGET_IMPORTED_LOCATION ${target} IMPORTED_LOCATION)

    message("Target '${target}'  |  Properties:")
    message("  INCLUDE_DIRECTORIES: '${TARGET_INCLUDE_DIRECTORIES}'")
    message("  INTERFACE_INCLUDE_DIRECTORIES: '${TARGET_INTERFACE_INCLUDE_DIRECTORIES}'")
    message("  LINK_LIBRARIES: '${TARGET_LINK_LIBRARIES}'")
    message("  LINK_DIRECTORIES: '${TARGET_LINK_DIRECTORIES}'")
    message("  INTERFACE_LINK_LIBRARIES: '${TARGET_INTERFACE_LINK_LIBRARIES}'")
    message("  INTERFACE_LINK_DIRECTORIES: '${TARGET_INTERFACE_LINK_DIRECTORIES}'")
    message("  IMPORTED_LOCATION: '${TARGET_IMPORTED_LOCATION}'")
    message("")

endfunction()

function(setDefaults)

    set(CMAKE_CXX_STANDARD 17 CACHE INTERNAL "CMAKE_CXX_STANDARD")
    set(CMAKE_CXX_STANDARD_REQUIRED ON CACHE INTERNAL "CMAKE_CXX_STANDARD_REQUIRED")
    set(CMAKE_CXX_EXTENSIONS OFF CACHE INTERNAL "CMAKE_CXX_EXTENSIONS")

#    set(CMAKE_VERBOSE_MAKEFILE ON CACHE INTERNAL "CMAKE_VERBOSE_MAKEFILE")

    add_compile_options(-march=native -Wall -Wextra -pedantic)

#    # Needed so we compile with CLang standard library, we use the CMake variable instead of the functions so
#    # we can pass the needed flags to external project cmake invocations
##    add_compile_options(-stdlib=libc++)
##    add_link_options(-stdlib=libc++)
#    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++" CACHE INTERNAL "CMAKE_CXX_FLAGS")
#    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -stdlib=libc++" CACHE INTERNAL "CMAKE_EXE_LINKER_FLAGS")

    #    add_compile_options(-fsanitize=undefined -fno-omit-frame-pointer)
    #    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)

    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        add_compile_options(-fdiagnostics-color=always)
    elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        add_compile_options(-fcolor-diagnostics)
    endif ()

    # These are needed so the profiler can show methods in stack frames
    # Not using makes inlined methods simply appear as "inline"
    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0")

        # Fix for strange issue with clang not producing debug info
        if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
            add_compile_options(-fstandalone-debug)
        endif()
    endif()

endfunction()