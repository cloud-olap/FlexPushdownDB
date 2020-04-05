# To enable std::filesystem support programs need to link against extra libraries
# As std::filesystem is an emerging standard, this is different for different versions
# of LLVM

find_package(LLVM)

get_filename_component(LLVM_FS_LIB ${LLVM_DIR}/../../${CMAKE_STATIC_LIBRARY_PREFIX}c++fs${CMAKE_STATIC_LIBRARY_SUFFIX} REALPATH)

add_library(LLVM::fs STATIC IMPORTED)
set_target_properties(LLVM::fs PROPERTIES IMPORTED_LOCATION ${LLVM_FS_LIB})

#showTargetProps(LLVM::fs)