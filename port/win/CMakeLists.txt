set(SOURCES
        ../stack_trace.cc
        env_win.cc
        port_win.cc
        win_logger.cc
)

add_library(rocksdb_port_win ${SOURCES})

add_library(rocksdb_port_win_je ${SOURCES})
set_target_properties(rocksdb_port_win_je PROPERTIES COMPILE_FLAGS ${JEMALLOC_CXX_FLAGS})

