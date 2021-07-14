# Gandiva has a requirement on llvm 7 or 7.1 plus finding llvm on MacOS is tricky
# because it is often installed via homebrew. This find module restricts the LLVM
# version to 7 or 7.1, plus it adds the homebrew search paths.

#find_package(LLVM 7 CONFIG QUIET PATHS /usr/local/opt)
#if(NOT LLVM_FOUND)
#    find_package(LLVM 7.1 CONFIG REQUIRED PATHS /usr/local/opt)
#endif()
#
#if(NOT LLVM_FOUND)
#    message(FATAL ERROR "Required LLVM version 7 or 7.1 not found")
#else()
#    message(DEBUG "Found suitable LLVM (Version: ${LLVM_PACKAGE_VERSION})")
#endif()
