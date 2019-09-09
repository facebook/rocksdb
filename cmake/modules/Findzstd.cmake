# - Find zstd
# Find the zstd compression library and includes
#
# ZSTD_INCLUDE_DIR - where to find zstd.h, etc.
# ZSTD_LIBRARIES - List of libraries when using zstd.
# ZSTD_FOUND - True if zstd found.

find_path(ZSTD_INCLUDE_DIR
  NAMES zstd.h
  HINTS ${ZSTD_ROOT_DIR}/include)

find_library(ZSTD_LIBRARIES
  NAMES zstd
  HINTS ${ZSTD_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(zstd DEFAULT_MSG ZSTD_LIBRARIES ZSTD_INCLUDE_DIR)

mark_as_advanced(
  ZSTD_LIBRARIES
  ZSTD_INCLUDE_DIR)
