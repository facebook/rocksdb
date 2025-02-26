// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

// DB contents are stored in a set of blocks, each of which holds a
// sequence of key,value pairs.  Each block may be compressed before
// being stored in a file.  The following enum describes which
// compression method (if any) is used to compress a block.

enum CompressionType : unsigned char {
  // NOTE: do not change the values of existing entries, as these are
  // part of the persistent format on disk.
  kNoCompression = 0x0,
  kSnappyCompression = 0x1,
  kZlibCompression = 0x2,
  kBZip2Compression = 0x3,
  kLZ4Compression = 0x4,
  kLZ4HCCompression = 0x5,
  kXpressCompression = 0x6,
  kZSTD = 0x7,

  // kDisableCompressionOption is used to disable some compression options.
  kDisableCompressionOption = 0xff,
};

// Compression options for different compression algorithms like Zlib
struct CompressionOptions {
  // ==> BEGIN options that can be set by deprecated configuration syntax, <==
  // ==> e.g. compression_opts=5:6:7:8:9:10:true:11:false                  <==
  // ==> Please use compression_opts={level=6;strategy=7;} form instead.   <==

  // RocksDB's generic default compression level. Internally it'll be translated
  // to the default compression level specific to the library being used (see
  // comment above `ColumnFamilyOptions::compression`).
  //
  // The default value is the max 16-bit int as it'll be written out in OPTIONS
  // file, which should be portable.
  static constexpr int kDefaultCompressionLevel = 32767;

  // zlib only: windowBits parameter. See https://www.zlib.net/manual.html
  int window_bits = -14;

  // Compression "level" applicable to zstd, zlib, LZ4, and LZ4HC. Except for
  // kDefaultCompressionLevel (see above), the meaning of each value depends
  // on the compression algorithm. Decreasing across non-
  // `kDefaultCompressionLevel` values will either favor speed over
  // compression ratio or have no effect.
  //
  // In LZ4 specifically, the absolute value of a negative `level` internally
  // configures the `acceleration` parameter. For example, set `level=-10` for
  // `acceleration=10`. This negation is necessary to ensure decreasing `level`
  // values favor speed over compression ratio.
  int level = kDefaultCompressionLevel;

  // zlib only: strategy parameter. See https://www.zlib.net/manual.html
  int strategy = 0;

  // Maximum size of dictionaries used to prime the compression library.
  // Enabling dictionary can improve compression ratios when there are
  // repetitions across data blocks.
  //
  // The dictionary is created by sampling the SST file data. If
  // `zstd_max_train_bytes` is nonzero, the samples are passed through zstd's
  // dictionary generator (see comments for option `use_zstd_dict_trainer` for
  // detail on dictionary generator). If `zstd_max_train_bytes` is zero, the
  // random samples are used directly as the dictionary.
  //
  // When compression dictionary is disabled, we compress and write each block
  // before buffering data for the next one. When compression dictionary is
  // enabled, we buffer SST file data in-memory so we can sample it, as data
  // can only be compressed and written after the dictionary has been finalized.
  //
  // The amount of data buffered can be limited by `max_dict_buffer_bytes`. This
  // buffered memory is charged to the block cache when there is a block cache.
  // If block cache insertion fails with `Status::MemoryLimit` (i.e., it is
  // full), we finalize the dictionary with whatever data we have and then stop
  // buffering.
  uint32_t max_dict_bytes = 0;

  // Maximum size of training data passed to zstd's dictionary trainer. Using
  // zstd's dictionary trainer can achieve even better compression ratio
  // improvements than using `max_dict_bytes` alone.
  //
  // The training data will be used to generate a dictionary of max_dict_bytes.
  uint32_t zstd_max_train_bytes = 0;

  // Number of threads for parallel compression.
  // Parallel compression is enabled only if threads > 1.
  // THE FEATURE IS STILL EXPERIMENTAL
  //
  // This option is valid only when BlockBasedTable is used.
  //
  // When parallel compression is enabled, SST size file sizes might be
  // more inflated compared to the target size, because more data of unknown
  // compressed size is in flight when compression is parallelized. To be
  // reasonably accurate, this inflation is also estimated by using historical
  // compression ratio and current bytes inflight.
  uint32_t parallel_threads = 1;

  // When the compression options are set by the user, it will be set to "true".
  // For bottommost_compression_opts, to enable it, user must set enabled=true.
  // Otherwise, bottommost compression will use compression_opts as default
  // compression options.
  //
  // For compression_opts, if compression_opts.enabled=false, it is still
  // used as compression options for compression process.
  bool enabled = false;

  // Limit on data buffering when gathering samples to build a dictionary. Zero
  // means no limit. When dictionary is disabled (`max_dict_bytes == 0`),
  // enabling this limit (`max_dict_buffer_bytes != 0`) has no effect.
  //
  // In compaction, the buffering is limited to the target file size (see
  // `target_file_size_base` and `target_file_size_multiplier`) even if this
  // setting permits more buffering. Since we cannot determine where the file
  // should be cut until data blocks are compressed with dictionary, buffering
  // more than the target file size could lead to selecting samples that belong
  // to a later output SST.
  //
  // Limiting too strictly may harm dictionary effectiveness since it forces
  // RocksDB to pick samples from the initial portion of the output SST, which
  // may not be representative of the whole file. Configuring this limit below
  // `zstd_max_train_bytes` (when enabled) can restrict how many samples we can
  // pass to the dictionary trainer. Configuring it below `max_dict_bytes` can
  // restrict the size of the final dictionary.
  uint64_t max_dict_buffer_bytes = 0;

  // Use zstd trainer to generate dictionaries. When this option is set to true,
  // zstd_max_train_bytes of training data sampled from max_dict_buffer_bytes
  // buffered data will be passed to zstd dictionary trainer to generate a
  // dictionary of size max_dict_bytes.
  //
  // When this option is false, zstd's API ZDICT_finalizeDictionary() will be
  // called to generate dictionaries. zstd_max_train_bytes of training sampled
  // data will be passed to this API. Using this API should save CPU time on
  // dictionary training, but the compression ratio may not be as good as using
  // a dictionary trainer.
  bool use_zstd_dict_trainer = true;

  // ===> END options that can be set by deprecated configuration syntax <===
  // ===> Use compression_opts={level=6;strategy=7;} form for below opts <===

  // Essentially specifies a minimum acceptable compression ratio. A block is
  // stored uncompressed if the compressed block does not achieve this ratio,
  // because the downstream cost of decompression is not considered worth such
  // a small savings (if any).
  // However, the ratio is specified in a way that is efficient for checking.
  // An integer from 1 to 1024 indicates the maximum allowable compressed bytes
  // per 1KB of input, so the minimum acceptable ratio is 1024.0 / this value.
  // For example, for a minimum ratio of 1.5:1, set to 683. See SetMinRatio().
  // Default: abandon use of compression for a specific block or entry if
  // compressed by less than 12.5% (minimum ratio of 1.143:1).
  int max_compressed_bytes_per_kb = 1024 * 7 / 8;

  // ZSTD only.
  // Enable compression algorithm's checksum feature.
  // (https://github.com/facebook/zstd/blob/d857369028d997c92ff1f1861a4d7f679a125464/lib/zstd.h#L428)
  // Each compressed frame will have a 32-bit checksum attached. The checksum
  // computed from the uncompressed data and can be verified during
  // decompression.
  bool checksum = false;

  // A convenience function for setting max_compressed_bytes_per_kb based on a
  // minimum acceptable compression ratio (uncompressed size over compressed
  // size).
  void SetMinRatio(double min_ratio) {
    max_compressed_bytes_per_kb = static_cast<int>(1024.0 / min_ratio + 0.5);
  }

#if __cplusplus >= 202002L
  bool operator==(const CompressionOptions& rhs) const = default;
#endif
};

}  // namespace ROCKSDB_NAMESPACE
