set(CMAKE_SYSTEM_NAME Darwin)
set(CMAKE_SYSTEM_PROCESSOR x86_64)

find_program(_BREW_COMMAND
        NAMES brew
        REQUIRED)

if(NOT _BREW_COMMAND)
    message(FATAL_ERROR "Homebrew not found")
endif()

execute_process(COMMAND "${_BREW_COMMAND}" "--prefix" "llvm@12"
        OUTPUT_VARIABLE _LLVM_DIR
        OUTPUT_STRIP_TRAILING_WHITESPACE
        )

if(NOT _LLVM_DIR)
    message(FATAL_ERROR "Homebrew LLVM installation not found")
endif()

message(STATUS "Found Homebrew LLVM installation at: ${_LLVM_DIR}")

set(CMAKE_C_COMPILER ${_LLVM_DIR}/bin/clang)
set(CMAKE_CXX_COMPILER ${_LLVM_DIR}/bin/clang++)

set(CMAKE_C_FLAGS "-I${_LLVM_DIR}/include")
set(CMAKE_CXX_FLAGS "-I${_LLVM_DIR}/include")

set(CMAKE_EXE_LINKER_FLAGS_INIT "-L${_LLVM_DIR}/lib -Wl,-rpath,${_LLVM_DIR}/lib")
set(CMAKE_SHARED_LINKER_FLAGS_INIT "-L${_LLVM_DIR}/lib -Wl,-rpath,${_LLVM_DIR}/lib")
