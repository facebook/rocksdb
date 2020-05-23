# - Find gflags library
# Find the gflags includes and library
#
# gflags_INCLUDE_DIR - where to find gflags.h.
# gflags_LIBRARIES - List of libraries when using gflags.
# gflags_FOUND - True if gflags found.

find_path(GFLAGS_INCLUDE_DIR
  NAMES gflags/gflags.h)

find_library(GFLAGS_LIBRARIES
  NAMES gflags)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(gflags
  DEFAULT_MSG GFLAGS_LIBRARIES GFLAGS_INCLUDE_DIR)

mark_as_advanced(
  GFLAGS_LIBRARIES
  GFLAGS_INCLUDE_DIR)

if(gflags_FOUND AND NOT (TARGET gflags::gflags))
  add_library(gflags::gflags UNKNOWN IMPORTED)
  set_target_properties(gflags::gflags
    PROPERTIES
      IMPORTED_LOCATION ${GFLAGS_LIBRARIES}
      INTERFACE_INCLUDE_DIRECTORIES ${GFLAGS_INCLUDE_DIR}
      IMPORTED_LINK_INTERFACE_LANGUAGES "CXX")
endif()
