# - Find Bzip2
# Find the bzip2 compression library and includes
#
# BZIP2_INCLUDE_DIR - where to find bzlib.h, etc.
# BZIP2_LIBRARIES - List of libraries when using bzip2.
# BZIP2_FOUND - True if bzip2 found.

find_path(BZIP2_INCLUDE_DIR
  NAMES bzlib.h
  HINTS ${BZIP2_ROOT_DIR}/include)

find_library(BZIP2_LIBRARIES
  NAMES bz2
  HINTS ${BZIP2_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(bzip2 DEFAULT_MSG BZIP2_LIBRARIES BZIP2_INCLUDE_DIR)

mark_as_advanced(
  BZIP2_LIBRARIES
  BZIP2_INCLUDE_DIR)
