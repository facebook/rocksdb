@echo off
call :init
call :runtest arena_test.exe
call :runtest autovector_test.exe
call :runtest auto_roll_logger_test.exe
call :runtest backupable_db_test.exe
rem call :runtest benchharness_test.exe
call :runtest block_based_filter_block_test.exe
call :runtest block_hash_index_test.exe
call :runtest block_test.exe
call :runtest bloom_test.exe
call :runtest cache_test.exe
call :runtest coding_test.exe
call :runtest column_family_test.exe
call :runtest compaction_job_test.exe
call :runtest compaction_picker_test.exe
call :runtest comparator_db_test.exe
call :runtest corruption_test.exe
call :runtest crc32c_test.exe
call :runtest cuckoo_table_builder_test.exe
call :runtest cuckoo_table_db_test.exe
call :runtest cuckoo_table_reader_test.exe
call :runtest dbformat_test.exe
call :runtest db_iter_test.exe
call :runtest db_test.exe
call :runtest deletefile_test.exe
call :runtest dynamic_bloom_test.exe
call :runtest env_test.exe
call :runtest fault_injection_test.exe
call :runtest filelock_test.exe
call :runtest filename_test.exe
call :runtest file_indexer_test.exe
call :runtest full_filter_block_test.exe
call :runtest histogram_test.exe
call :runtest listener_test.exe
call :runtest log_test.exe
call :runtest manual_compaction_test.exe
call :runtest memenv_test.exe
call :runtest merger_test.exe
call :runtest merge_test.exe
call :runtest mock_env_test.exe
call :runtest options_test.exe
call :runtest perf_context_test.exe
call :runtest plain_table_db_test.exe
call :runtest prefix_test.exe
call :runtest rate_limiter_test.exe
call :runtest redis_lists_test.exe
rem call :runtest signal_test.exe
call :runtest skiplist_test.exe
call :runtest slice_transform_test.exe
call :runtest sst_dump_test.exe
call :runtest stringappend_test.exe
call :runtest table_properties_collector_test.exe
call :runtest table_test.exe
call :runtest thread_list_test.exe
call :runtest thread_local_test.exe
call :runtest ttl_test.exe
call :runtest version_builder_test.exe
call :runtest version_edit_test.exe
call :runtest version_set_test.exe
call :runtest wal_manager_test.exe
call :runtest write_batch_test.exe
rem call :runtest write_batch_with_index_test.exe
call :runtest write_controller_test.exe
call :stat
goto :eof

:init
set tests=0
set passed=0
set failed=0
goto :eof

:runtest
set /A tests=%tests% + 1
echo|set /p=Running %1... 
%1 > %1.log 2>&1
findstr /C:"PASSED" %1.log > nul 2>&1
IF ERRORLEVEL 1 (
    findstr /C:"Passed all tests" %1.log > nul 2>&1
    IF ERRORLEVEL 1 (
        echo ***FAILED***
        set /A failed=%failed% + 1
    ) ELSE (
        echo OK
        set /A passed=%passed% + 1
    )
) ELSE (
    echo OK
    set /A passed=%passed% + 1
)
goto :eof

:stat
echo =================
echo Total tests : %tests%
echo Passed      : %passed%
echo Failed      : %failed%
goto :eof
