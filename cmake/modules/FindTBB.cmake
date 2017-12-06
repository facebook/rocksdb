# - Find TBB
# Find the Thread Building Blocks library and includes
#
# TBB_INCLUDE_DIR - where to find tbb.h, etc.
# TBB_LIBRARIES - List of libraries when using TBB.
# TBB_FOUND - True if TBB found.

find_path(TBB_INCLUDE_DIR
NAMES tbb/tbb.h
HINTS ${TBB_ROOT_DIR}/include)

find_library(TBB_LIBRARIES
NAMES tbb
HINTS ${TBB_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(TBB DEFAULT_MSG TBB_LIBRARIES TBB_INCLUDE_DIR)

mark_as_advanced(
TBB_LIBRARIES
TBB_INCLUDE_DIR)
