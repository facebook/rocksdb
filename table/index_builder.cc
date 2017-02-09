#include <assert.h>
#include <inttypes.h>
#include <stdio.h>

#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "db/dbformat.h"

#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/table.h"

#include "table/block.h"
#include "table/block_based_table_reader.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/block_based_filter_block.h"
#include "table/block_based_table_factory.h"
#include "table/full_filter_block.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "table/table_builder.h"

#include "util/string_util.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/stop_watch.h"
#include "util/xxhash.h"

#include "table/index_builder.h"
#include "table/partitioned_filter_block.h"

// Without anonymous namespace here, we fail the warning -Wmissing-prototypes
namespace rocksdb {
//using namespace rocksdb;
// Create a index builder based on its type.
IndexBuilder* IndexBuilder::CreateIndexBuilder(BlockBasedTableOptions::IndexType index_type,
                                 const InternalKeyComparator* comparator,
                                 const SliceTransform* prefix_extractor,
                                 int index_block_restart_interval,
                                 uint64_t index_per_partition,
                                 const BlockBasedTableOptions& table_opt) {
  switch (index_type) {
    case BlockBasedTableOptions::kBinarySearch: {
      return new ShortenedIndexBuilder(comparator,
                                       index_block_restart_interval);
    }
    case BlockBasedTableOptions::kHashSearch: {
      return new HashIndexBuilder(comparator, prefix_extractor,
                                  index_block_restart_interval);
    }
    case BlockBasedTableOptions::kTwoLevelIndexSearch: {
      class DummyFilterBitsBuilder : public FilterBitsBuilder {
        public:
          virtual void AddKey(const Slice& key) { assert(0); }
          virtual Slice Finish(std::unique_ptr<const char[]>* buf) { assert(0); }
      };
      FilterBitsBuilder* filter_bits_builder =
          table_opt.filter_policy != nullptr
              ? table_opt.filter_policy->GetFilterBitsBuilder()
              : new DummyFilterBitsBuilder();
      assert(filter_bits_builder);
      return new PartitionIndexBuilder(
          comparator, prefix_extractor, index_per_partition,
          index_block_restart_interval, table_opt.whole_key_filtering,
          filter_bits_builder, table_opt);
    }
    default: {
      assert(!"Do not recognize the index type ");
      return nullptr;
    }
  }
  // impossible.
  assert(false);
  return nullptr;
}
}
