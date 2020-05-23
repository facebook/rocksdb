# - Find Snappy
# Find the snappy compression library and includes
#
# Snappy_INCLUDE_DIRS - where to find snappy.h, etc.
# Snappy_LIBRARIES - List of libraries when using snappy.
# Snappy_FOUND - True if snappy found.

find_path(Snappy_INCLUDE_DIRS
  NAMES snappy.h
  HINTS ${snappy_ROOT_DIR}/include)

find_library(Snappy_LIBRARIES
  NAMES snappy
  HINTS ${snappy_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Snappy DEFAULT_MSG Snappy_LIBRARIES Snappy_INCLUDE_DIRS)

mark_as_advanced(
  Snappy_LIBRARIES
  Snappy_INCLUDE_DIRS)

if(Snappy_FOUND AND NOT (TARGET Snappy::snappy))
  add_library (Snappy::snappy UNKNOWN IMPORTED)
  set_target_properties(Snappy::snappy
    PROPERTIES
      IMPORTED_LOCATION ${Snappy_LIBRARIES}
      INTERFACE_INCLUDE_DIRECTORIES ${Snappy_INCLUDE_DIRS})
endif()
