#
# Find module for binutils (needed for backward)
#

include(FindPackageHandleStandardArgs)

unset(Binutils_INCLUDE_DIR CACHE)

find_path (Binutils_INCLUDE_DIR
        bfd.h
        PATHS /usr/include /usr/local/opt/binutils/include
        NO_DEFAULT_PATH)

find_package_handle_standard_args(binutils DEFAULT_MSG Binutils_INCLUDE_DIR)
