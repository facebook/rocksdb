set(TOOLS
  sst_dump.cc
  db_sanity_test.cc
  write_stress.cc
  ldb.cc
  db_repl_stress.cc
  dump/rocksdb_dump.cc
  dump/rocksdb_undump.cc)
foreach(src ${TOOLS})
  get_filename_component(exename ${src} NAME_WE)
  add_executable(${exename}${ARTIFACT_SUFFIX}
    ${src})
  target_link_libraries(${exename}${ARTIFACT_SUFFIX} ${ROCKSDB_LIB})
  list(APPEND tool_deps ${exename})
endforeach()

list(APPEND tool_deps)

add_custom_target(ldb_tests
  COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/ldb_tests.py
  DEPENDS ldb)
