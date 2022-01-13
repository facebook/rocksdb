find_package(benchmark REQUIRED)
find_package(Threads REQUIRED)

file(GLOB_RECURSE ALL_BENCH_CPP *.cc)
foreach(ONE_BENCH_CPP ${ALL_BENCH_CPP})
  get_filename_component(TARGET_NAME ${ONE_BENCH_CPP} NAME_WE)
  add_executable(${TARGET_NAME} ${ONE_BENCH_CPP})
  target_link_libraries(${TARGET_NAME} ${ROCKSDB_LIB} benchmark::benchmark
          ${CMAKE_THREAD_LIBS_INIT})
  # run benchmark like a test, if added, the benchmark tests could be run by `ctest -R Bench_`
  # add_test(Bench_${TARGET_NAME} ${TARGET_NAME})
  list(APPEND ALL_BENCH_TARGETS ${TARGET_NAME})
endforeach()
add_custom_target(microbench
        COMMAND for t in ${ALL_BENCH_TARGETS}\; do \.\/$$t \|\| exit 1\; done
        DEPENDS ${ALL_BENCH_TARGETS})
