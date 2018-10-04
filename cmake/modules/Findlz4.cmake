# - Find Lz4
# Find the lz4 compression library and includes
#
# LZ4_INCLUDE_DIR - where to find lz4.h, etc.
# LZ4_LIBRARIES - List of libraries when using lz4.
# LZ4_FOUND - True if lz4 found.

find_path(LZ4_INCLUDE_DIR
  NAMES lz4.h
  HINTS ${LZ4_ROOT_DIR}/include)

find_library(LZ4_LIBRARIES
  NAMES lz4
  HINTS ${LZ4_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(lz4 DEFAULT_MSG LZ4_LIBRARIES LZ4_INCLUDE_DIR)

mark_as_advanced(
  LZ4_LIBRARIES
  LZ4_INCLUDE_DIR)
