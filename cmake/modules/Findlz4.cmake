# - Find Lz4
# Find the lz4 compression library and includes
#
# lz4_INCLUDE_DIRS - where to find lz4.h, etc.
# lz4_LIBRARIES - List of libraries when using lz4.
# lz4_FOUND - True if lz4 found.

find_path(lz4_INCLUDE_DIRS
  NAMES lz4.h
  HINTS ${lz4_ROOT_DIR}/include)

find_library(lz4_LIBRARIES
  NAMES lz4
  HINTS ${lz4_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(lz4 DEFAULT_MSG lz4_LIBRARIES lz4_INCLUDE_DIRS)

mark_as_advanced(
  lz4_LIBRARIES
  lz4_INCLUDE_DIRS)

if(lz4_FOUND AND NOT (TARGET lz4::lz4))
  add_library(lz4::lz4 UNKNOWN IMPORTED)
  set_target_properties(lz4::lz4
    PROPERTIES
      IMPORTED_LOCATION ${lz4_LIBRARIES}
      INTERFACE_INCLUDE_DIRECTORIES ${lz4_INCLUDE_DIRS})
endif()
