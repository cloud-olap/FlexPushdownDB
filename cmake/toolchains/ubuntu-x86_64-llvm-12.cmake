set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR x86_64)

find_program(_LLVM_CONFIG_COMMAND
        NAMES llvm-config-12
        REQUIRED)

if(NOT _LLVM_CONFIG_COMMAND)
    message(FATAL_ERROR "LLVM 12 not found")
endif()

find_program(_CLANG_COMMAND
        NAMES clang-12
        REQUIRED)

if(NOT _CLANG_COMMAND)
    message(FATAL_ERROR "clang 12 not found")
endif()

find_program(_CLANGPP_COMMAND
        NAMES clang++-12
        REQUIRED)

if(NOT _CLANGPP_COMMAND)
    message(FATAL_ERROR "clang++ 12 not found")
endif()

set(CMAKE_C_COMPILER ${_CLANG_COMMAND})
set(CMAKE_CXX_COMPILER ${_CLANGPP_COMMAND})

set(CMAKE_C_FLAGS "")
set(CMAKE_CXX_FLAGS "")

set(CMAKE_EXE_LINKER_FLAGS_INIT "")
set(CMAKE_SHARED_LINKER_FLAGS_INIT "")
