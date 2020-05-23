add_executable(db_stress${ARTIFACT_SUFFIX}
  db_stress.cc
  db_stress_tool.cc
  batched_ops_stress.cc
  cf_consistency_stress.cc
  db_stress_common.cc
  db_stress_driver.cc
  db_stress_test_base.cc
  db_stress_shared_state.cc
  db_stress_gflags.cc
  db_stress_tool.cc
  no_batched_ops_stress.cc)
target_link_libraries(db_stress${ARTIFACT_SUFFIX} ${ROCKSDB_LIB})
list(APPEND tool_deps db_stress)
