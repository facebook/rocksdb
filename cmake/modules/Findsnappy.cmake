# - Find Snappy
# Find the snappy compression library and includes
#
# snappy_INCLUDE_DIRS - where to find snappy.h, etc.
# snappy_LIBRARIES - List of libraries when using snappy.
# snappy_FOUND - True if snappy found.

find_path(snappy_INCLUDE_DIRS
  NAMES snappy.h
  HINTS ${snappy_ROOT_DIR}/include)

find_library(snappy_LIBRARIES
  NAMES snappy
  HINTS ${snappy_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(snappy DEFAULT_MSG snappy_LIBRARIES snappy_INCLUDE_DIRS)

mark_as_advanced(
  snappy_LIBRARIES
  snappy_INCLUDE_DIRS)

if(snappy_FOUND AND NOT (TARGET snappy::snappy))
  add_library (snappy::snappy UNKNOWN IMPORTED)
  set_target_properties(snappy::snappy
    PROPERTIES
      IMPORTED_LOCATION ${snappy_LIBRARIES}
      INTERFACE_INCLUDE_DIRECTORIES ${snappy_INCLUDE_DIRS})
endif()
