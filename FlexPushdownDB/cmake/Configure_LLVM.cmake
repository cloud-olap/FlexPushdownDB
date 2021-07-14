# To enable std::filesystem support programs need to link against extra libraries
# As std::filesystem is an emerging standard, this is different for different versions
# of LLVM and different platforms.
#
# Not entirely clear what is the correct way to do this, but below we create a dummy library for the library containing
# filesystem support. This works on mac. The linux version is linking to the gcc stdlib which contains filesystem
# support. FIXME: Get it linking properly to the LLVM lib.

find_package(LLVM)

if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    add_library(LLVM-filesystem STATIC IMPORTED)
    get_filename_component(LLVM_FS_LIB ${LLVM_DIR}/../../${CMAKE_STATIC_LIBRARY_PREFIX}c++fs${CMAKE_STATIC_LIBRARY_SUFFIX} REALPATH)
    set_target_properties(LLVM-filesystem PROPERTIES IMPORTED_LOCATION ${LLVM_FS_LIB})
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
#    add_library(LLVM-filesystem STATIC IMPORTED)
#    get_filename_component(LLVM_FS_LIB ${LLVM_DIR}/../lib/${CMAKE_STATIC_LIBRARY_PREFIX}c++fs${CMAKE_STATIC_LIBRARY_SUFFIX} REALPATH)
#    set_target_properties(LLVM-filesystem PROPERTIES IMPORTED_LOCATION ${LLVM_FS_LIB})

    add_library(LLVM-filesystem INTERFACE)
    target_link_libraries(LLVM-filesystem INTERFACE stdc++fs)
else()
    message(FATAL ERROR "Platform ${CMAKE_SYSTEM_NAME} not yet supported")
endif()

#showTargetProps(LLVM::fs)