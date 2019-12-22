# Read rocksdb version from version.h header file.

function(get_rocksdb_version version_var)
  file(READ "${CMAKE_CURRENT_SOURCE_DIR}/include/rocksdb/version.h" version_header_file)
  foreach(component MAJOR MINOR PATCH)
    string(REGEX MATCH "#define ROCKSDB_${component} ([0-9]+)" _ ${version_header_file})
    set(ROCKSDB_VERSION_${component} ${CMAKE_MATCH_1})
  endforeach()
  set(${version_var} "${ROCKSDB_VERSION_MAJOR}.${ROCKSDB_VERSION_MINOR}.${ROCKSDB_VERSION_PATCH}" PARENT_SCOPE)
endfunction()
