# - Find TBB
# Find the Thread Building Blocks library and includes
#
# TBB_INCLUDE_DIRS - where to find tbb.h, etc.
# TBB_LIBRARIES - List of libraries when using TBB.
# TBB_FOUND - True if TBB found.

if(NOT DEFINED TBB_ROOT_DIR)
  set(TBB_ROOT_DIR "$ENV{TBBROOT}")
endif()

find_path(TBB_INCLUDE_DIRS
  NAMES tbb/tbb.h
  HINTS ${TBB_ROOT_DIR}/include)

find_library(TBB_LIBRARIES
  NAMES tbb
  HINTS ${TBB_ROOT_DIR}/lib ENV LIBRARY_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(TBB DEFAULT_MSG TBB_LIBRARIES TBB_INCLUDE_DIRS)

mark_as_advanced(
  TBB_LIBRARIES
  TBB_INCLUDE_DIRS)

if(TBB_FOUND AND NOT (TARGET TBB::TBB))
  add_library (TBB::TBB UNKNOWN IMPORTED)
  set_target_properties(TBB::TBB
    PROPERTIES
      IMPORTED_LOCATION ${TBB_LIBRARIES}
      INTERFACE_INCLUDE_DIRECTORIES ${TBB_INCLUDE_DIRS})
endif()
