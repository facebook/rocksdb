add_library(rocksdb_table
                adaptive_table_factory.cc
                block.cc
                block_based_filter_block.cc
                block_based_table_builder.cc
                block_based_table_factory.cc
                block_based_table_reader.cc
                block_builder.cc
                block_hash_index.cc
                block_prefix_index.cc
                bloom_block.cc
                cuckoo_table_builder.cc
                cuckoo_table_factory.cc
                cuckoo_table_reader.cc
                flush_block_policy.cc
                format.cc
                full_filter_block.cc
                get_context.cc
                iterator.cc
                merger.cc
                meta_blocks.cc
                mock_table.cc
                plain_table_builder.cc
                plain_table_factory.cc
                plain_table_index.cc
                plain_table_key_coding.cc
                plain_table_reader.cc
                table_properties.cc
                two_level_iterator.cc
            )

set(APPS )

set(TESTS
        block_based_filter_block_test.cc
        block_hash_index_test.cc
        block_test.cc
        cuckoo_table_builder_test.cc
        cuckoo_table_reader_test.cc
        full_filter_block_test.cc
        merger_test.cc
        table_test.cc
)

set(EXES ${APPS} ${TESTS})

foreach(sourcefile ${EXES})
    string(REPLACE ".cc" "" exename ${sourcefile})
    add_executable(${exename} ${sourcefile})
    target_link_libraries(${exename} ${LIBS})
endforeach(sourcefile ${EXES})
