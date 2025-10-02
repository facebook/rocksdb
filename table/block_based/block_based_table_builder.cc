//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/block_based/block_based_table_builder.h"

#include <atomic>
#include <cassert>
#include <cstdio>
#include <list>
#include <map>
#include <memory>
#include <numeric>
#include <string>
#include <unordered_map>
#include <utility>

#include "block_cache.h"
#include "cache/cache_entry_roles.h"
#include "cache/cache_helpers.h"
#include "cache/cache_key.h"
#include "cache/cache_reservation_manager.h"
#include "db/dbformat.h"
#include "index_builder.h"
#include "logging/logging.h"
#include "memory/memory_allocator_impl.h"
#include "options/options_helper.h"
#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/table.h"
#include "rocksdb/types.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/block_builder.h"
#include "table/block_based/filter_block.h"
#include "table/block_based/filter_policy_internal.h"
#include "table/block_based/full_filter_block.h"
#include "table/block_based/partitioned_filter_block.h"
#include "table/block_based/user_defined_index_wrapper.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "table/table_builder.h"
#include "util/bit_fields.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/defer.h"
#include "util/semaphore.h"
#include "util/stop_watch.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

extern const std::string kHashIndexPrefixesBlock;
extern const std::string kHashIndexPrefixesMetadataBlock;

// Without anonymous namespace here, we fail the warning -Wmissing-prototypes
namespace {

constexpr size_t kBlockTrailerSize = BlockBasedTable::kBlockTrailerSize;

// Create a filter block builder based on its type.
FilterBlockBuilder* CreateFilterBlockBuilder(
    const ImmutableCFOptions& /*opt*/, const MutableCFOptions& mopt,
    const FilterBuildingContext& context,
    const bool use_delta_encoding_for_index_values,
    PartitionedIndexBuilder* const p_index_builder, size_t ts_sz,
    const bool persist_user_defined_timestamps) {
  const BlockBasedTableOptions& table_opt = context.table_options;
  assert(table_opt.filter_policy);  // precondition

  FilterBitsBuilder* filter_bits_builder =
      BloomFilterPolicy::GetBuilderFromContext(context);
  if (filter_bits_builder == nullptr) {
    return nullptr;
  } else {
    if (table_opt.partition_filters) {
      assert(p_index_builder != nullptr);
      // Since after partition cut request from filter builder it takes time
      // until index builder actully cuts the partition, until the end of a
      // data block potentially with many keys, we take the lower bound as
      // partition size.
      assert(table_opt.block_size_deviation <= 100);
      auto partition_size =
          static_cast<uint32_t>(((table_opt.metadata_block_size *
                                  (100 - table_opt.block_size_deviation)) +
                                 99) /
                                100);
      partition_size = std::max(partition_size, static_cast<uint32_t>(1));
      return new PartitionedFilterBlockBuilder(
          mopt.prefix_extractor.get(), table_opt.whole_key_filtering,
          filter_bits_builder, table_opt.index_block_restart_interval,
          use_delta_encoding_for_index_values, p_index_builder, partition_size,
          ts_sz, persist_user_defined_timestamps,
          table_opt.decouple_partitioned_filters);
    } else {
      return new FullFilterBlockBuilder(mopt.prefix_extractor.get(),
                                        table_opt.whole_key_filtering,
                                        filter_bits_builder);
    }
  }
}

}  // namespace

// kBlockBasedTableMagicNumber was picked by running
//    echo rocksdb.table.block_based | sha1sum
// and taking the leading 64 bits.
// Please note that kBlockBasedTableMagicNumber may also be accessed by other
// .cc files
// for that reason we declare it extern in the header but to get the space
// allocated
// it must be not extern in one place.
const uint64_t kBlockBasedTableMagicNumber = 0x88e241b785f4cff7ull;
// We also support reading and writing legacy block based table format (for
// backwards compatibility)
const uint64_t kLegacyBlockBasedTableMagicNumber = 0xdb4775248b80fb57ull;

// A collector that collects properties of interest to block-based table.
// For now this class looks heavy-weight since we only write one additional
// property.
// But in the foreseeable future, we will add more and more properties that are
// specific to block-based table.
class BlockBasedTableBuilder::BlockBasedTablePropertiesCollector
    : public InternalTblPropColl {
 public:
  explicit BlockBasedTablePropertiesCollector(
      BlockBasedTableOptions::IndexType index_type, bool whole_key_filtering,
      bool prefix_filtering, bool decoupled_partitioned_filters)
      : index_type_(index_type),
        whole_key_filtering_(whole_key_filtering),
        prefix_filtering_(prefix_filtering),
        decoupled_partitioned_filters_(decoupled_partitioned_filters) {}

  Status InternalAdd(const Slice& /*key*/, const Slice& /*value*/,
                     uint64_t /*file_size*/) override {
    // Intentionally left blank. Have no interest in collecting stats for
    // individual key/value pairs.
    return Status::OK();
  }

  void BlockAdd(uint64_t /* block_uncomp_bytes */,
                uint64_t /* block_compressed_bytes_fast */,
                uint64_t /* block_compressed_bytes_slow */) override {
    // Intentionally left blank. No interest in collecting stats for
    // blocks.
  }

  Status Finish(UserCollectedProperties* properties) override {
    std::string val;
    PutFixed32(&val, static_cast<uint32_t>(index_type_));
    properties->insert({BlockBasedTablePropertyNames::kIndexType, val});
    properties->insert({BlockBasedTablePropertyNames::kWholeKeyFiltering,
                        whole_key_filtering_ ? kPropTrue : kPropFalse});
    properties->insert({BlockBasedTablePropertyNames::kPrefixFiltering,
                        prefix_filtering_ ? kPropTrue : kPropFalse});
    if (decoupled_partitioned_filters_) {
      properties->insert(
          {BlockBasedTablePropertyNames::kDecoupledPartitionedFilters,
           kPropTrue});
    }
    return Status::OK();
  }

  // The name of the properties collector can be used for debugging purpose.
  const char* Name() const override {
    return "BlockBasedTablePropertiesCollector";
  }

  UserCollectedProperties GetReadableProperties() const override {
    // Intentionally left blank.
    return UserCollectedProperties();
  }

 private:
  BlockBasedTableOptions::IndexType index_type_;
  bool whole_key_filtering_;
  bool prefix_filtering_;
  bool decoupled_partitioned_filters_;
};

struct BlockBasedTableBuilder::WorkingAreaPair {
  Compressor::ManagedWorkingArea compress;
  Decompressor::ManagedWorkingArea verify;
};

// ParallelCompressionRep essentially defines a framework for parallelizing
// block generation ("emit"), block compression, and block writing to storage.
// The synchronization is lock-free/wait-free, so thread waiting only happens
// when work-order dependencies are unsatisfied, though sleeping/idle threads
// might be kept idle when it seems unlikely they would improve throughput by
// waking them up (essentially auto-tuned parallelism). But because all threads
// are capable of 2 out of 3 kinds of work, in a quasi-work-stealing system,
// running threads can usually expect that compatible work is available.
//
// This is currently activated with CompressionOptions::parallel_threads > 1
// but that is a somewhat crude API that would ideally be adapted along with
// the implementation in the future to allow threads to serve multiple
// flush/compaction jobs, though the available improvement might be small.
// Even within the scope of a single file it might be nice to use a general
// framework for distributing work across threads, but (a) different threads
// are limited to which work they can do because of technical challenges, (b)
// being largely CPU bound on small work units means such a framework would
// likely have big overheads compared to this hand-optimized solution.
struct BlockBasedTableBuilder::ParallelCompressionRep {
  // The framework has two kinds of threads: the calling thread from
  // flush/compaction/SstFileWriter is called the "emit thread" (kEmitter).
  // Other threads cannot generally take over "emit" work because that is
  // largely happening up the call stack from BlockBasedTableBuilder.
  // The emit thread can also take on compression work in a quasi-work-stealing
  // manner when the buffer for emitting new blocks is full.
  //
  // When parallelism is enabled, there are also "worker" threads that
  // can handle compressing blocks and (one worker thread at a time) write them
  // to the SST file (and handle other single-threaded wrap-up of each block).
  //
  // NOTE: when parallelism is enabled, the emit thread is not permitted to
  // write to the SST file because that is the potential "output" bottleneck,
  // and it's generally bad for parallelism to allow the only thread that can
  // serve the "input" bottleneck (emit work) to also spend exclusive time on
  // the output bottleneck.
  enum class ThreadKind {
    kEmitter,
    kWorker,
  };

  // ThreadState allows each thread to track its work assignment. In addition to
  // the cases already mentioned, kEmitting, kCompressing, and kWriting to the
  // SST file writer,
  // * Threads can enter the kIdle state so that they can sleep when no work is
  // available for them, to be woken up when appropriate.
  // * The kEnd state means the thread is not doing any more work items, which
  // for worker threads means they will end soon.
  // * The kCompressingAndWriting state means a worker can compress and write a
  // block without additional state updates because the same block to be
  // compressed is the next to be written.
  enum class ThreadState {
    /* BEGIN Emitter only states */
    kEmitting,
    /* END Emitter only states */
    /* BEGIN states for emitter and worker */
    kIdle,
    kCompressing,
    kEnd,
    /* END states for emitter and worker */
    /* BEGIN Worker only states */
    kCompressingAndWriting,
    kWriting,
    /* END Worker only states */
  };

  // BlockRep instances are used and reused in a ring buffer (below), so that
  // many blocks can be in an intermediate state between serialized into
  // uncompressed bytes and written to the SST file. Notably, each block is
  // "emitted" in uncompressed form into a BlockRep, compressed (at least
  // attempted, when configured) for updated BlockRep, and then written from the
  // BlockRep to the writer for the SST file bytes.
  struct ALIGN_AS(CACHE_LINE_SIZE) BlockRep {
    // Uncompressed block contents
    std::string uncompressed;
    GrowableBuffer compressed;
    CompressionType compression_type = kNoCompression;
    std::unique_ptr<IndexBuilder::PreparedIndexEntry> prepared_index_entry;
  };

  // Ring buffer of emitted blocks that may or may not yet be compressed.
  std::unique_ptr<BlockRep[]> ring_buffer;
  // log_2(ring buffer size), where ring buffer size must be a power of two
  const int ring_buffer_nbits;
  // ring buffer size - 1, to function as a bit mask for ring buffer positions
  // (e.g. given the ordinal number of a block)
  const uint32_t ring_buffer_mask;
  // Number of threads in worker_threads. (Emit thread doesn't count)
  const uint32_t num_worker_threads;

  // Rough upper bound on the sst file size contribution from blocks emitted
  // into the parallel compression ring buffer but not yet written. Tracks
  // uncompressed size, with trailer, until a block is compressed, then
  // compressed size until the block is written. (TODO: does not currently
  // account for block_align)
  RelaxedAtomic<uint64_t> estimated_inflight_size{0};
  // Thread objects for worker threads
  std::vector<port::Thread> worker_threads;
  // Working areas for data_block_compressor for each worker thread
  std::vector<WorkingAreaPair> working_areas;

  // Semaphores for threads to sleep when there's no available work for them
  // and to wake back up when someone determines there is available work (most
  // likely). Split between worker threads and emit thread because they can do
  // different kinds of work.
  CountingSemaphore idle_worker_sem{0};
  BinarySemaphore idle_emit_sem{0};

  // Primary atomic state of parallel compression, which includes a number of
  // state fields that are best updated atomically to avoid locking and/or to
  // simplify the interesting interleavings that have to be considered and
  // accommodated.
  struct State : public BitFields<uint64_t, State> {};
  ALIGN_AS(CACHE_LINE_SIZE) AcqRelBitFieldsAtomic<State> atomic_state;

  // The first field is a bit for each ring buffer slot (max 32) for whether
  // that slot is ready to be claimed for writing by a worker thread. Because
  // compressions might finish out-of-order, we need to track individually
  // whether they are finished, though this field doesn't differentiate
  // "compression completed" from "compression not started" because that can be
  // inferred from NextToCompress. A block might not enter this state, because
  // the same thread that compresses it can also immediately write the block if
  // it notices that the block is next to write.
  using NeedsWriter = UnsignedBitField<State, 32, NoPrevBitField>;
  // Track how many worker threads are in an idle state because there was no
  // available work and haven't been selected to wake back up.
  using IdleWorkerCount = UnsignedBitField<State, 5, NeedsWriter>;
  // Track whether the emit thread is an idle state because there was no
  // available work and hasn't been triggered to wake back up. The nature of
  // available work and atomic CAS assignment of work ensures at least one
  // thread is kept out of the idle state.
  using IdleEmitFlag = BoolBitField<State, IdleWorkerCount>;
  // Track whether threads should end when they finish available work because no
  // more blocks will be emitted.
  using NoMoreToEmitFlag = BoolBitField<State, IdleEmitFlag>;
  // Track whether threads should abort ASAP because of an error.
  using AbortFlag = BoolBitField<State, NoMoreToEmitFlag>;
  // Track three "NextTo" counters for the positions of the next block to write,
  // to start compression, and to emit into the ring buffer. If these counters
  // never overflowed / wrapped around, we would have next_to_write <=
  // next_to_compress <= next_to_emit because a block must be emitted before
  // compressed, and compressed (at least attempted) before writing. We need to
  // track more than ring_buffer_nbits of these counters to be able to
  // distinguish an empty ring buffer (next_to_write == next_to_emit) from a
  // full ring buffer (next_to_write != next_to_emit but equal under
  // ring_buffer_mask).
  using NextToWrite = UnsignedBitField<State, 8, AbortFlag>;
  using NextToCompress = UnsignedBitField<State, 8, NextToWrite>;
  using NextToEmit = UnsignedBitField<State, 8, NextToCompress>;
  static_assert(NextToEmit::kEndBit == 64);

  // BEGIN fields for use by the emit thread only. These can't live on the stack
  // because the emit thread frequently returns out of BlockBasedTableBuilder.
  ALIGN_AS(CACHE_LINE_SIZE)
  ThreadState emit_thread_state = ThreadState::kEmitting;
  // Ring buffer index that emit thread is operating on (for emitting and
  // compressing states)
  uint32_t emit_slot = 0;
  // Including some data to inform when to wake up idle worker threads (see
  // implementation for details)
  int32_t emit_counter_toward_wake_up = 0;
  int32_t emit_counter_for_wake_up = 0;
  static constexpr int32_t kMaxWakeupInterval = 8;
  // END fields for use by the emit thread only

  // TSAN on GCC has bugs that report false positives on this watchdog code.
  // Other efforts to work around the bug have failed, so to avoid those false
  // positive reports, we simply disable the watchdog when running under GCC
  // TSAN.
#if !defined(NDEBUG) && !(defined(__GNUC__) && defined(__SANITIZE_THREAD__))
#define BBTB_PC_WATCHDOG 1
#endif
#ifdef BBTB_PC_WATCHDOG
  // These are for an extra "watchdog" thread in DEBUG builds that heuristically
  // checks for the most likely deadlock conditions. False positives and false
  // negatives are technically possible.
  std::thread watchdog_thread;
  std::mutex watchdog_mutex;
  std::condition_variable watchdog_cv;
  bool shutdown_watchdog = false;
  RelaxedAtomic<uint32_t> live_workers{0};
  RelaxedAtomic<uint32_t> idling_workers{0};
  RelaxedAtomic<bool> live_emit{0};
  RelaxedAtomic<bool> idling_emit{0};
#endif  // BBTB_PC_WATCHDOG

  int ComputeRingBufferNbits(uint32_t parallel_threads) {
    // Ring buffer size is a power of two not to exceed 32 but otherwise
    // at least twice the number of threads.
    if (parallel_threads >= 9) {
      return 5;
    } else if (parallel_threads >= 5) {
      return 4;
    } else if (parallel_threads >= 3) {
      return 3;
    } else {
      assert(parallel_threads > 1);
      return 2;
    }
  }

  explicit ParallelCompressionRep(uint32_t parallel_threads)
      : ring_buffer_nbits(ComputeRingBufferNbits(parallel_threads)),
        ring_buffer_mask((uint32_t{1} << ring_buffer_nbits) - 1),
        num_worker_threads(std::min(parallel_threads, ring_buffer_mask)) {
    assert(num_worker_threads <= IdleWorkerCount::kMask);

    ring_buffer = std::make_unique<BlockRep[]>(ring_buffer_mask + 1);

    // Start by aggressively waking up idle workers
    emit_counter_for_wake_up = -static_cast<int32_t>(num_worker_threads);
  }

  ~ParallelCompressionRep() {
#ifndef NDEBUG
    auto state = atomic_state.Load();
    if (state.Get<AbortFlag>() == false) {
      // Should be clear / cancelled out with normal shutdown
      assert(state.Get<NeedsWriter>() == 0);

      // Ring buffer reached empty state
      assert(state.Get<NextToWrite>() == state.Get<NextToCompress>());
      assert(state.Get<NextToCompress>() == state.Get<NextToEmit>());

      // Everything cancels out in inflight size
      assert(estimated_inflight_size.LoadRelaxed() == 0);
    }
    // All idling metadata cleaned up, properly tracked
    assert(state.Get<IdleWorkerCount>() == 0);
    assert(state.Get<IdleEmitFlag>() == false);

    // No excess in semaphores
    assert(!idle_emit_sem.TryAcquire());
    assert(!idle_worker_sem.TryAcquire());
#endif  // !NDEBUG
  }

  // The primary function for a thread transitioning from one state or work
  // assignment to the next. `slot` refers to a position in the ring buffer
  // for assigned emit, compression, or write work.
  //
  // Because both the emit thread and worker threads can work on compression,
  // this is a quasi-work-stealing parallel algorithm. (Enabling other threads
  // to do emit work would be quite challenging, and allowing the emit thread
  // to handle writes could create a bottle-neck.)
  //
  // This function is basically a CAS loop trying to pick the next piece of work
  // for this thread and retrying if CAS fails. This function also handles
  // thread idling when that's the appropriate assignment, continuing the loop
  // looking for productive work when woken from an idle state.
  //
  // Precondition: thread_state is appropriate for thread_kind and not kEnd. It
  // must match the previously returned state for that thread, and is only kIdle
  // for the thread on startup (though the kIdle state is used internal to the
  // function).
  //
  // Postcondition: thread_state is appropriate for thread_kind and not kIdle.
  // Except for kEnd state, the calling thread has exclusive access to
  // ring_buffer[slot] until next StateTransition().
  template <ThreadKind thread_kind>
  void StateTransition(
      /*in/out*/ ThreadState& thread_state,
      /*in/out*/ uint32_t& slot) {
    assert(slot <= ring_buffer_mask);
    // Last known value for atomic_state
    State seen_state = atomic_state.Load();

    for (;;) {
      if (seen_state.Get<AbortFlag>()) {
        thread_state = ThreadState::kEnd;
        return;
      }

      assert(static_cast<uint8_t>(seen_state.Get<NextToEmit>() -
                                  seen_state.Get<NextToCompress>()) <=
             ring_buffer_mask + 1);
      assert(static_cast<uint8_t>(seen_state.Get<NextToCompress>() -
                                  seen_state.Get<NextToWrite>()) <=
             ring_buffer_mask + 1);
      assert(static_cast<uint8_t>(seen_state.Get<NextToEmit>() -
                                  seen_state.Get<NextToWrite>()) <=
             ring_buffer_mask + 1);

      // Draft of the next proposed atomic_state. Start by marking completion of
      // the current thread's last work.
      State next_state = seen_state;
      bool wake_idle = false;
      switch (thread_state) {
        case ThreadState::kEmitting: {
          assert(thread_kind == ThreadKind::kEmitter);
          assert(slot == (next_state.Get<NextToEmit>() & ring_buffer_mask));
          next_state.Ref<NextToEmit>() += 1;
          // Check whether to wake up idle worker thread
          if (next_state.Get<IdleWorkerCount>() > 0 &&
              // The number of blocks for which compression hasn't started
              // is well over the number of active threads.
              static_cast<uint8_t>(next_state.Get<NextToEmit>() -
                                   next_state.Get<NextToCompress>()) >=
                  (ring_buffer_mask + 1) / 4 +
                      (num_worker_threads -
                       next_state.Get<IdleWorkerCount>())) {
            // At first, emit_counter_for_wake_up is negative to aggressively
            // wake up idle worker threads. Then it backs off the interval at
            // which we wake up, up to some maximum that attempts to balance
            // maximum throughput and minimum CPU overhead.
            if (emit_counter_toward_wake_up >= emit_counter_for_wake_up) {
              // We reached a threshold to justify a wake-up.
              wake_idle = true;
              // Adjust idle count assuming we are going to own waking it up,
              // so no one else can duplicate that. (The idle count is really
              // the number idling for which no one yet owns waking them up.)
              next_state.Ref<IdleWorkerCount>() -= 1;
              // Reset the counter toward the threshold for wake-up
              emit_counter_toward_wake_up = 0;
              // Raise the threshold (up to some limit) to stabilize the number
              // of active threads after some ramp-up period.
              emit_counter_for_wake_up =
                  std::min(emit_counter_for_wake_up + 1,
                           static_cast<int32_t>(num_worker_threads +
                                                kMaxWakeupInterval));
            } else {
              // Advance closer to the threshold for justifying a wake-up
              emit_counter_toward_wake_up++;
            }
          }
          break;
        }
        case ThreadState::kIdle:
          // NOTE: thread that signalled to wake up already updated idle count
          // or marker. This is required to avoid overflow on the semaphore,
          // especially the binary semaphore for idle_emit_sem, and likely
          // desirable to avoid spurious/extra Release().
          break;
        case ThreadState::kCompressing:
          next_state.Ref<NeedsWriter>() |= uint32_t{1} << slot;
          if constexpr (thread_kind == ThreadKind::kEmitter) {
            if (next_state.Get<IdleWorkerCount>() == num_worker_threads) {
              // Work is available for a worker thread and none are running
              wake_idle = true;
              // Adjust idle count assuming we are going to own waking it up
              next_state.Ref<IdleWorkerCount>() -= 1;
            }
          }
          break;
        case ThreadState::kEnd:
          // Should have already recognized the end state
          assert(thread_state != ThreadState::kEnd);
          return;
        case ThreadState::kCompressingAndWriting:
        case ThreadState::kWriting:
          assert(thread_kind == ThreadKind::kWorker);
          assert((next_state.Get<NextToWrite>() & ring_buffer_mask) == slot);
          assert(next_state.Get<NextToCompress>() !=
                 next_state.Get<NextToWrite>());
          assert(next_state.Get<NextToEmit>() != next_state.Get<NextToWrite>());
          assert((next_state.Get<NeedsWriter>() & (uint32_t{1} << slot)) == 0);
          next_state.Ref<NextToWrite>() += 1;
          if (next_state.Get<IdleEmitFlag>()) {
            wake_idle = true;
            // Clear idle emit flag assuming we are going to own waking it up
            next_state.Set<IdleEmitFlag>(false);
          }
          break;
      }

      // Find the next state, depending on the kind of thread
      ThreadState next_thread_state = ThreadState::kEnd;
      uint32_t next_slot = 0;
      if constexpr (thread_kind == ThreadKind::kEmitter) {
        // First priority is emitting more uncompressed blocks, if there's
        // room in the ring buffer.
        if (static_cast<uint8_t>(next_state.Get<NextToEmit>() -
                                 next_state.Get<NextToWrite>()) <=
            ring_buffer_mask) {
          // There is room
          next_thread_state = ThreadState::kEmitting;
          next_slot = next_state.Get<NextToEmit>() & ring_buffer_mask;
        }
      }
      if constexpr (thread_kind == ThreadKind::kWorker) {
        // First priority is writing next block to write, if it needs a writer
        // assigned to it
        uint32_t next_to_write_slot =
            next_state.Get<NextToWrite>() & ring_buffer_mask;
        uint32_t needs_writer_bit = uint32_t{1} << next_to_write_slot;
        if (next_state.Get<NeedsWriter>() & needs_writer_bit) {
          // Clear the "needs writer" marker on the slot
          next_state.Ref<NeedsWriter>() &= ~needs_writer_bit;
          // Take ownership of writing it
          next_thread_state = ThreadState::kWriting;
          next_slot = next_to_write_slot;
        }
      }

      // If didn't find higher priority work
      if (next_thread_state == ThreadState::kEnd) {
        if (next_state.Get<NextToCompress>() != next_state.Get<NextToEmit>()) {
          // Compression work is available, select that
          if (thread_kind == ThreadKind::kWorker &&
              next_state.Get<NextToCompress>() ==
                  next_state.Get<NextToWrite>()) {
            next_thread_state = ThreadState::kCompressingAndWriting;
          } else {
            next_thread_state = ThreadState::kCompressing;
          }
          next_slot = next_state.Get<NextToCompress>() & ring_buffer_mask;
          next_state.Ref<NextToCompress>() += 1;
        } else if constexpr (thread_kind == ThreadKind::kEmitter) {
          // Emitter thread goes idle
          next_thread_state = ThreadState::kIdle;
          assert(next_state.Get<IdleEmitFlag>() == false);
          assert(next_state.Get<NoMoreToEmitFlag>() == false);
          next_state.Set<IdleEmitFlag>(true);
        } else if (next_state.Get<NoMoreToEmitFlag>()) {
          // Worker thread shall not idle if we are done emitting. At least
          // one worker will remain unblocked to finish writing
          next_thread_state = ThreadState::kEnd;
        } else {
          // Worker thread goes idle
          next_thread_state = ThreadState::kIdle;
          assert(next_state.Get<IdleWorkerCount>() < IdleWorkerCount::kMask);
          next_state.Ref<IdleWorkerCount>() += 1;
        }
      }
      assert(thread_state != ThreadState::kEnd);

      // Attempt to atomically apply the desired/computed state transition
      if (atomic_state.CasWeak(seen_state, next_state)) {
        // Success
        thread_state = next_thread_state;
        slot = next_slot;
        seen_state = next_state;
        if (wake_idle) {
          if constexpr (thread_kind == ThreadKind::kEmitter) {
            idle_worker_sem.Release();
          } else {
            idle_emit_sem.Release();
          }
        }
        if (thread_state != ThreadState::kIdle) {
          // Successfully transitioned to another useful state
          return;
        }
        // Handle idle state
        if constexpr (thread_kind == ThreadKind::kEmitter) {
#ifdef BBTB_PC_WATCHDOG
          idling_emit.StoreRelaxed(true);
          Defer decr{[this]() { idling_emit.StoreRelaxed(false); }};
#endif  // BBTB_PC_WATCHDOG

          // Likely go to sleep
          idle_emit_sem.Acquire();
        } else {
#ifdef BBTB_PC_WATCHDOG
          // Tracking for watchdog
          idling_workers.FetchAddRelaxed(1);
          Defer decr{[this]() { idling_workers.FetchSubRelaxed(1); }};
#endif  // BBTB_PC_WATCHDOG

          // Likely go to sleep
          idle_worker_sem.Acquire();
        }
        // Update state after sleep
        seen_state = atomic_state.Load();
      }
      // else loop and try again
    }
  }

  void EmitterStateTransition(
      /*in/out*/ ThreadState& thread_state,
      /*in/out*/ uint32_t& slot) {
    StateTransition<ThreadKind::kEmitter>(thread_state, slot);
  }

  void WorkerStateTransition(
      /*in/out*/ ThreadState& thread_state,
      /*in/out*/ uint32_t& slot) {
    StateTransition<ThreadKind::kWorker>(thread_state, slot);
  }

  // Exactly wake all idling threads (for an end state)
  void WakeAllIdle() {
    State old_state, new_state;
    auto transform =
        IdleEmitFlag::ClearTransform() + IdleWorkerCount::ClearTransform();
    atomic_state.Apply(transform, &old_state, &new_state);
    assert(new_state.Get<IdleEmitFlag>() == false);
    assert(new_state.Get<IdleWorkerCount>() == 0);
    if (old_state.Get<IdleEmitFlag>()) {
      idle_emit_sem.Release();
    }
    idle_worker_sem.Release(old_state.Get<IdleWorkerCount>());
  }

  // Called by emit thread if it is decided no more blocks will be emitted into
  // this SST file.
  void SetNoMoreToEmit(/*in/out*/ ThreadState& thread_state,
                       /*in/out*/ uint32_t& slot) {
    (void)slot;
    State old_state;
    atomic_state.Apply(NoMoreToEmitFlag::SetTransform(), &old_state);
    assert(old_state.Get<NoMoreToEmitFlag>() == false);
    assert(slot == BitwiseAnd(old_state.Get<NextToEmit>(), ring_buffer_mask));
    assert(thread_state == ThreadState::kEmitting);
    thread_state = ThreadState::kEnd;
    WakeAllIdle();
  }

  // Called by any thread to abort parallel compression, etc. because of an
  // error.
  void SetAbort(/*in/out*/ ThreadState& thread_state) {
    State old_state;
    atomic_state.Apply(AbortFlag::SetTransform(), &old_state);
    if (old_state.Get<AbortFlag>() == false) {
      // First to set abort. Wake all workers and emitter
      WakeAllIdle();
    }
    thread_state = ThreadState::kEnd;
  }

#ifdef BBTB_PC_WATCHDOG
  // Logic for the extra "watchdog" thread in DEBUG builds that heuristically
  // checks for the most likely deadlock conditions.
  //
  // Some ways to manually validate the watchdog:
  // * Insert
  //      if (Random::GetTLSInstance()->OneIn(100)) {
  //        sleep(100);
  //      }
  //   after either of the calls to semaphore Acquire above.
  // * Miss some Release()s in WakeAllIdle()
  //
  // and run table_test unit tests.
  void BGWatchdog() {
    int count_toward_deadlock_judgment = 0;
    for (;;) {
      // Check for termination condition: All workers and emit thread have
      // completed.
      if (live_workers.LoadRelaxed() == 0 && live_emit.LoadRelaxed() == false) {
        return;
      }

      // Check for potential deadlock condition
      if (idling_workers.LoadRelaxed() < live_workers.LoadRelaxed() ||
          (live_emit.LoadRelaxed() && !idling_emit.LoadRelaxed())) {
        // Someone is working, all good
        count_toward_deadlock_judgment = 0;
      } else {
        // Could be a deadlock state, but could also be a transient
        // state where someone has woken up but not cleared their idling flag.
        // Give it plenty of time and watchdog thread wake-ups before
        // declaring deadlock.
        count_toward_deadlock_judgment++;
        if (count_toward_deadlock_judgment >= 70) {
          fprintf(stderr,
                  "Error: apparent deadlock in parallel compression. "
                  "Aborting. %u / %u, %d / %d, %llx\n",
                  (unsigned)idling_workers.LoadRelaxed(),
                  (unsigned)live_workers.LoadRelaxed(),
                  (int)idling_emit.LoadRelaxed(), (int)live_emit.LoadRelaxed(),
                  (long long)atomic_state.Load().underlying);
          std::terminate();
        }
      }

      // Sleep for 1s at a time unless we are woken up because other threads
      // ended.
      std::unique_lock<std::mutex> lock(watchdog_mutex);
      if (!shutdown_watchdog) {
        watchdog_cv.wait_for(lock, std::chrono::seconds{1});
      }
    }
  }
#endif  // BBTB_PC_WATCHDOG
};

struct BlockBasedTableBuilder::Rep {
  const ImmutableOptions ioptions;
  // BEGIN from MutableCFOptions
  std::shared_ptr<const SliceTransform> prefix_extractor;
  // END from MutableCFOptions
  const WriteOptions write_options;
  const BlockBasedTableOptions table_options;
  const InternalKeyComparator& internal_comparator;
  // Size in bytes for the user-defined timestamps.
  size_t ts_sz;
  // When `ts_sz` > 0 and this flag is false, the user-defined timestamp in the
  // user key will be stripped when creating the block based table. This
  // stripping happens for all user keys, including the keys in data block,
  // index block for data block, index block for index block (if index type is
  // `kTwoLevelIndexSearch`), index for filter blocks (if using partitioned
  // filters), the `first_internal_key` in `IndexValue`, the `end_key` for range
  // deletion entries.
  // As long as the user keys are sorted when added via `Add` API, their logic
  // ordering won't change after timestamps are stripped. However, for each user
  // key to be logically equivalent before and after timestamp is stripped, the
  // user key should contain the minimum timestamp.
  bool persist_user_defined_timestamps;
  WritableFileWriter* file;
  // The current offset is only written by the current designated writer thread
  // but can be read by other threads to estimate current file size
  RelaxedAtomic<uint64_t> offset{0};
  size_t alignment;
  BlockBuilder data_block;
  // Buffers uncompressed data blocks to replay later. Needed when
  // compression dictionary is enabled so we can finalize the dictionary before
  // compressing any data blocks.
  std::vector<std::string> data_block_buffers;
  BlockBuilder range_del_block;

  InternalKeySliceTransform internal_prefix_transform;
  std::unique_ptr<IndexBuilder> index_builder;
  std::string index_separator_scratch;
  PartitionedIndexBuilder* p_index_builder_ = nullptr;

  std::string last_ikey;  // Internal key or empty (unset)
  bool warm_cache = false;
  bool uses_explicit_compression_manager = false;

  uint64_t sample_for_compression;
  RelaxedAtomic<uint64_t> compressible_input_data_bytes{0};
  RelaxedAtomic<uint64_t> uncompressible_input_data_bytes{0};
  RelaxedAtomic<uint64_t> sampled_input_data_bytes{0};
  RelaxedAtomic<uint64_t> sampled_output_slow_data_bytes{0};
  RelaxedAtomic<uint64_t> sampled_output_fast_data_bytes{0};
  uint32_t compression_parallel_threads;
  int max_compressed_bytes_per_kb;
  size_t max_dict_sample_bytes = 0;

  // *** Compressors & decompressors - Yes, it seems like a lot here but ***
  // *** these are distinct fields to minimize extra conditionals and    ***
  // *** field reads on hot code paths.                                  ***

  // A compressor for blocks in general, without dictionary compression
  std::unique_ptr<Compressor> basic_compressor;
  // A compressor using dictionary compression (when applicable)
  std::unique_ptr<Compressor> compressor_with_dict;
  // Once configured/determined, points to one of the above Compressors to
  // use on data blocks.
  Compressor* data_block_compressor = nullptr;
  // A decompressor corresponding to basic_compressor (when non-nullptr).
  // Used for verification and cache warming.
  std::shared_ptr<Decompressor> basic_decompressor;
  // When needed, a decompressor for verifying compression using a
  // dictionary sampled/trained from this file.
  std::unique_ptr<Decompressor> verify_decompressor_with_dict;
  // When non-nullptr, compression should be verified with this corresponding
  // decompressor, except for data blocks. (Points to same as basic_decompressor
  // when verify_compression is set.)
  UnownedPtr<Decompressor> verify_decompressor;
  // Once configured/determined, points to one of the above Decompressors to use
  // in verifying data blocks.
  UnownedPtr<Decompressor> data_block_verify_decompressor;

  // Set of compression types used for blocks in this file (mixing compression
  // algorithms in a single file is allowed, using a CompressionManager)
  SmallEnumSet<CompressionType, kDisableCompressionOption>
      compression_types_used;

  // Working area for basic_compressor when compression_parallel_threads==1
  WorkingAreaPair basic_working_area;
  // Working area for data_block_compressor, for emit/compaction thread
  WorkingAreaPair data_block_working_area;

  size_t data_begin_offset = 0;

  TableProperties props;

  // States of the builder.
  //
  // - `kBuffered`: This is the initial state where zero or more data blocks are
  //   accumulated uncompressed in-memory. From this state, call
  //   `EnterUnbuffered()` to finalize the compression dictionary if enabled,
  //   compress/write out any buffered blocks, and proceed to the `kUnbuffered`
  //   state.
  //
  // - `kUnbuffered`: This is the state when compression dictionary is finalized
  //   either because it wasn't enabled in the first place or it's been created
  //   from sampling previously buffered data. In this state, blocks are simply
  //   compressed/written out as they fill up. From this state, call `Finish()`
  //   to complete the file (write meta-blocks, etc.), or `Abandon()` to delete
  //   the partially created file.
  //
  // - `kClosed`: This indicates either `Finish()` or `Abandon()` has been
  //   called, so the table builder is no longer usable. We must be in this
  //   state by the time the destructor runs.
  enum class State {
    kBuffered,
    kUnbuffered,
    kClosed,
  };
  State state = State::kUnbuffered;
  // `kBuffered` state is allowed only as long as the buffering of uncompressed
  // data blocks (see `data_block_buffers`) does not exceed `buffer_limit`.
  uint64_t buffer_limit = 0;
  std::shared_ptr<CacheReservationManager>
      compression_dict_buffer_cache_res_mgr;
  const bool use_delta_encoding_for_index_values;
  std::unique_ptr<FilterBlockBuilder> filter_builder;
  OffsetableCacheKey base_cache_key;
  const TableFileCreationReason reason;

  BlockHandle pending_handle;  // Handle to add to index block

  GrowableBuffer single_threaded_compressed_output;
  std::unique_ptr<FlushBlockPolicy> flush_block_policy;

  std::vector<std::unique_ptr<InternalTblPropColl>> table_properties_collectors;

  std::unique_ptr<ParallelCompressionRep> pc_rep;
  RelaxedAtomic<uint64_t> worker_cpu_micros{0};
  BlockCreateContext create_context;

  // The size of the "tail" part of a SST file. "Tail" refers to
  // all blocks after data blocks till the end of the SST file.
  uint64_t tail_size;

  // The total size of all blocks in this file before they are compressed.
  // This is used for logging compaction stats.
  uint64_t pre_compression_size = 0;

  // See class Footer
  uint32_t base_context_checksum;

  uint64_t get_offset() { return offset.LoadRelaxed(); }
  void set_offset(uint64_t o) { offset.StoreRelaxed(o); }

  bool IsParallelCompressionActive() const { return pc_rep != nullptr; }

  Status GetStatus() { return GetIOStatus(); }

  bool StatusOk() {
    // The OK case is optimized with an atomic. Relaxed is sufficient because
    // if a thread other than the emit/compaction thread sets to non-OK it
    // will synchronize that in aborting parallel compression.
    bool ok = io_status_ok.LoadRelaxed();
#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
    if (ok) {
      std::lock_guard<std::mutex> lock(io_status_mutex);
      // Double-check
      if (io_status_ok.LoadRelaxed()) {
        io_status.PermitUncheckedError();
        assert(io_status.ok());
      } else {
        ok = false;
      }
    }
#endif  // ROCKSDB_ASSERT_STATUS_CHECKED
    return ok;
  }

  IOStatus GetIOStatus() {
    // See StatusOk, which is optimized to avoid Status object copies
    if (LIKELY(io_status_ok.LoadRelaxed())) {
#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
      std::lock_guard<std::mutex> lock(io_status_mutex);
      // Double-check
      if (io_status_ok.LoadRelaxed()) {
        io_status.PermitUncheckedError();
        assert(io_status.ok());
      } else {
        return io_status;
      }
#endif  // ROCKSDB_ASSERT_STATUS_CHECKED
      return IOStatus::OK();
    } else {
      std::lock_guard<std::mutex> lock(io_status_mutex);
      return io_status;
    }
  }

  // Avoid copying Status and IOStatus objects as much as possible.
  // Never erase an existing I/O status that is not OK.
  void SetStatus(Status&& s) {
    if (UNLIKELY(!s.ok()) && io_status_ok.LoadRelaxed()) {
      SetFailedIOStatus(status_to_io_status(std::move(s)));
    }
  }
  void SetStatus(const Status& s) {
    if (UNLIKELY(!s.ok()) && io_status_ok.LoadRelaxed()) {
      SetFailedIOStatus(status_to_io_status(Status(s)));
    }
  }
  void SetIOStatus(IOStatus&& ios) {
    if (UNLIKELY(!ios.ok()) && io_status_ok.LoadRelaxed()) {
      SetFailedIOStatus(std::move(ios));
    }
  }
  void SetIOStatus(const IOStatus& ios) {
    if (UNLIKELY(!ios.ok()) && io_status_ok.LoadRelaxed()) {
      SetFailedIOStatus(IOStatus(ios));
    }
  }

  void SetFailedIOStatus(IOStatus&& ios) {
    assert(!ios.ok());
    // Because !s.ok() is rare, locking is acceptable even in non-parallel case.
    std::lock_guard<std::mutex> lock(io_status_mutex);
    // Double-check
    if (io_status.ok()) {
      io_status = std::move(ios);
      io_status_ok.StoreRelaxed(false);
    }
  }

  Rep(const BlockBasedTableOptions& table_opt, const TableBuilderOptions& tbo,
      WritableFileWriter* f)
      : ioptions(tbo.ioptions),
        prefix_extractor(tbo.moptions.prefix_extractor),
        write_options(tbo.write_options),
        table_options(table_opt),
        internal_comparator(tbo.internal_comparator),
        ts_sz(tbo.internal_comparator.user_comparator()->timestamp_size()),
        persist_user_defined_timestamps(
            tbo.ioptions.persist_user_defined_timestamps),
        file(f),
        alignment(table_options.block_align
                      ? std::min(static_cast<size_t>(table_options.block_size),
                                 kDefaultPageSize)
                      : 0),
        data_block(table_options.block_restart_interval,
                   table_options.use_delta_encoding,
                   false /* use_value_delta_encoding */,
                   tbo.internal_comparator.user_comparator()
                           ->CanKeysWithDifferentByteContentsBeEqual()
                       ? BlockBasedTableOptions::kDataBlockBinarySearch
                       : table_options.data_block_index_type,
                   table_options.data_block_hash_table_util_ratio, ts_sz,
                   persist_user_defined_timestamps),
        range_del_block(
            1 /* block_restart_interval */, true /* use_delta_encoding */,
            false /* use_value_delta_encoding */,
            BlockBasedTableOptions::kDataBlockBinarySearch /* index_type */,
            0.75 /* data_block_hash_table_util_ratio */, ts_sz,
            persist_user_defined_timestamps),
        internal_prefix_transform(prefix_extractor.get()),
        sample_for_compression(tbo.moptions.sample_for_compression),
        compression_parallel_threads(
            ((table_opt.partition_filters &&
              !table_opt.decouple_partitioned_filters) ||
             table_options.user_defined_index_factory)
                ? uint32_t{1}
                : tbo.compression_opts.parallel_threads),
        max_compressed_bytes_per_kb(
            tbo.compression_opts.max_compressed_bytes_per_kb),
        use_delta_encoding_for_index_values(table_opt.format_version >= 4 &&
                                            !table_opt.block_align),
        reason(tbo.reason),
        flush_block_policy(
            table_options.flush_block_policy_factory->NewFlushBlockPolicy(
                table_options, data_block)),
        create_context(&table_options, &ioptions, ioptions.stats,
                       /*decompressor=*/nullptr,
                       tbo.moptions.block_protection_bytes_per_key,
                       tbo.internal_comparator.user_comparator(),
                       !use_delta_encoding_for_index_values,
                       table_opt.index_type ==
                           BlockBasedTableOptions::kBinarySearchWithFirstKey),
        tail_size(0) {
    FilterBuildingContext filter_context(table_options);

    filter_context.info_log = ioptions.logger;
    filter_context.column_family_name = tbo.column_family_name;
    filter_context.reason = reason;

    // Only populate other fields if known to be in LSM rather than
    // generating external SST file
    if (reason != TableFileCreationReason::kMisc) {
      filter_context.compaction_style = ioptions.compaction_style;
      filter_context.num_levels = ioptions.num_levels;
      filter_context.level_at_creation = tbo.level_at_creation;
      filter_context.is_bottommost = tbo.is_bottommost;
      assert(filter_context.level_at_creation < filter_context.num_levels);
    }

    props.compression_options =
        CompressionOptionsToString(tbo.compression_opts);

    auto* mgr = tbo.moptions.compression_manager.get();
    if (mgr == nullptr) {
      uses_explicit_compression_manager = false;
      mgr = GetBuiltinCompressionManager(
                GetCompressFormatForVersion(
                    static_cast<uint32_t>(table_opt.format_version)))
                .get();
    } else {
      uses_explicit_compression_manager = true;

      // Stuff some extra debugging info as extra pseudo-options. Using
      // underscore prefix to indicate they are special.
      props.compression_options.append("_compression_manager=");
      props.compression_options.append(mgr->GetId());
      props.compression_options.append("; ");
    }

    // Sanitize to only allowing compression when it saves space.
    max_compressed_bytes_per_kb =
        std::min(int{1023}, tbo.compression_opts.max_compressed_bytes_per_kb);

    basic_compressor = mgr->GetCompressorForSST(
        filter_context, tbo.compression_opts, tbo.compression_type);
    if (basic_compressor) {
      if (table_options.enable_index_compression) {
        basic_working_area.compress = basic_compressor->ObtainWorkingArea();
      }
      max_dict_sample_bytes = basic_compressor->GetMaxSampleSizeIfWantDict(
          CacheEntryRole::kDataBlock);
      if (max_dict_sample_bytes > 0) {
        state = State::kBuffered;
        if (tbo.target_file_size == 0) {
          buffer_limit = tbo.compression_opts.max_dict_buffer_bytes;
        } else if (tbo.compression_opts.max_dict_buffer_bytes == 0) {
          buffer_limit = tbo.target_file_size;
        } else {
          buffer_limit = std::min(tbo.target_file_size,
                                  tbo.compression_opts.max_dict_buffer_bytes);
        }
      } else {
        // No distinct data block compressor using dictionary
        data_block_compressor = basic_compressor.get();
        data_block_working_area.compress =
            data_block_compressor->ObtainWorkingArea();
      }
      basic_decompressor = basic_compressor->GetOptimizedDecompressor();
      if (basic_decompressor == nullptr) {
        // Optimized version not available
        basic_decompressor = mgr->GetDecompressor();
      }
      create_context.decompressor = basic_decompressor.get();

      if (table_options.verify_compression) {
        verify_decompressor = basic_decompressor.get();
        if (table_options.enable_index_compression) {
          basic_working_area.verify = verify_decompressor->ObtainWorkingArea(
              basic_compressor->GetPreferredCompressionType());
        }
        if (state == State::kUnbuffered) {
          assert(data_block_compressor);
          data_block_verify_decompressor = verify_decompressor.get();
          data_block_working_area.verify =
              data_block_verify_decompressor->ObtainWorkingArea(
                  data_block_compressor->GetPreferredCompressionType());
        }
      }
    }

    switch (table_options.prepopulate_block_cache) {
      case BlockBasedTableOptions::PrepopulateBlockCache::kFlushOnly:
        warm_cache = (reason == TableFileCreationReason::kFlush);
        break;
      case BlockBasedTableOptions::PrepopulateBlockCache::kDisable:
        warm_cache = false;
        break;
      default:
        // missing case
        assert(false);
        warm_cache = false;
    }

    const auto compress_dict_build_buffer_charged =
        table_options.cache_usage_options.options_overrides
            .at(CacheEntryRole::kCompressionDictionaryBuildingBuffer)
            .charged;
    if (table_options.block_cache &&
        (compress_dict_build_buffer_charged ==
             CacheEntryRoleOptions::Decision::kEnabled ||
         compress_dict_build_buffer_charged ==
             CacheEntryRoleOptions::Decision::kFallback)) {
      compression_dict_buffer_cache_res_mgr =
          std::make_shared<CacheReservationManagerImpl<
              CacheEntryRole::kCompressionDictionaryBuildingBuffer>>(
              table_options.block_cache);
    } else {
      compression_dict_buffer_cache_res_mgr = nullptr;
    }

    if (table_options.index_type ==
        BlockBasedTableOptions::kTwoLevelIndexSearch) {
      p_index_builder_ = PartitionedIndexBuilder::CreateIndexBuilder(
          &internal_comparator, use_delta_encoding_for_index_values,
          table_options, ts_sz, persist_user_defined_timestamps);
      index_builder.reset(p_index_builder_);
    } else {
      index_builder.reset(IndexBuilder::CreateIndexBuilder(
          table_options.index_type, &internal_comparator,
          &this->internal_prefix_transform, use_delta_encoding_for_index_values,
          table_options, ts_sz, persist_user_defined_timestamps));
    }

    // If user_defined_index_factory is provided, wrap the index builder with
    // UserDefinedIndexWrapper
    if (table_options.user_defined_index_factory != nullptr) {
      if (tbo.moptions.compression_opts.parallel_threads > 1 ||
          tbo.moptions.bottommost_compression_opts.parallel_threads > 1) {
        SetStatus(
            Status::InvalidArgument("user_defined_index_factory not supported "
                                    "with parallel compression"));
      } else {
        std::unique_ptr<UserDefinedIndexBuilder> user_defined_index_builder;
        UserDefinedIndexOption udi_options;
        udi_options.comparator = internal_comparator.user_comparator();
        auto s = table_options.user_defined_index_factory->NewBuilder(
            udi_options, user_defined_index_builder);
        if (!s.ok()) {
          SetStatus(s);
        } else {
          if (user_defined_index_builder != nullptr) {
            index_builder = std::make_unique<UserDefinedIndexBuilderWrapper>(
                std::string(table_options.user_defined_index_factory->Name()),
                std::move(index_builder), std::move(user_defined_index_builder),
                &internal_comparator, ts_sz, persist_user_defined_timestamps);
          }
        }
      }
    }

    if (ioptions.optimize_filters_for_hits && tbo.is_bottommost) {
      // Apply optimize_filters_for_hits setting here when applicable by
      // skipping filter generation
      filter_builder.reset();
    } else if (tbo.skip_filters) {
      // For SstFileWriter skip_filters
      filter_builder.reset();
    } else if (!table_options.filter_policy) {
      // Null filter_policy -> no filter
      filter_builder.reset();
    } else {
      filter_builder.reset(CreateFilterBlockBuilder(
          ioptions, tbo.moptions, filter_context,
          use_delta_encoding_for_index_values, p_index_builder_, ts_sz,
          persist_user_defined_timestamps));
    }

    assert(tbo.internal_tbl_prop_coll_factories);
    for (auto& factory : *tbo.internal_tbl_prop_coll_factories) {
      assert(factory);

      std::unique_ptr<InternalTblPropColl> collector{
          factory->CreateInternalTblPropColl(
              tbo.column_family_id, tbo.level_at_creation,
              tbo.ioptions.num_levels,
              tbo.last_level_inclusive_max_seqno_threshold)};
      if (collector) {
        table_properties_collectors.emplace_back(std::move(collector));
      }
    }
    table_properties_collectors.emplace_back(
        std::make_unique<BlockBasedTablePropertiesCollector>(
            table_options.index_type, table_options.whole_key_filtering,
            prefix_extractor != nullptr,
            table_options.decouple_partitioned_filters));
    if (ts_sz > 0 && persist_user_defined_timestamps) {
      table_properties_collectors.emplace_back(
          std::make_unique<TimestampTablePropertiesCollector>(
              tbo.internal_comparator.user_comparator()));
    }

    // These are only needed for populating table properties
    props.column_family_id = tbo.column_family_id;
    props.column_family_name = tbo.column_family_name;
    props.oldest_key_time = tbo.oldest_key_time;
    props.newest_key_time = tbo.newest_key_time;
    props.file_creation_time = tbo.file_creation_time;
    props.orig_file_number = tbo.cur_file_num;
    props.db_id = tbo.db_id;
    props.db_session_id = tbo.db_session_id;
    props.db_host_id = ioptions.db_host_id;
    props.format_version = table_options.format_version;
    if (!ReifyDbHostIdProperty(ioptions.env, &props.db_host_id).ok()) {
      ROCKS_LOG_INFO(ioptions.logger, "db_host_id property will not be set");
    }
    // Default is UINT64_MAX for unknown. Setting it to 0 here
    // to allow updating it by taking max in BlockBasedTableBuilder::Add().
    props.key_largest_seqno = 0;
    // Default is UINT64_MAX for unknown.
    props.key_smallest_seqno = UINT64_MAX;
    PrePopulateCompressionProperties(mgr);

    if (FormatVersionUsesContextChecksum(table_options.format_version)) {
      // Must be non-zero and semi- or quasi-random
      // TODO: ideally guaranteed different for related files (e.g. use file
      // number and db_session, for benefit of SstFileWriter)
      do {
        base_context_checksum = Random::GetTLSInstance()->Next();
      } while (UNLIKELY(base_context_checksum == 0));
    } else {
      base_context_checksum = 0;
    }

    if (alignment > 0 && basic_compressor) {
      // With better sanitization in `CompactionPicker::CompactFiles()`, we
      // would not need to handle this case here and could change it to an
      // assertion instead.
      SetStatus(Status::InvalidArgument(
          "Enable block_align, but compression enabled"));
    }
  }

  ~Rep() {
    // Must have been cleaned up by StopParallelCompression
    assert(pc_rep == nullptr);
  }

  Rep(const Rep&) = delete;
  Rep& operator=(const Rep&) = delete;

  void PrePopulateCompressionProperties(UnownedPtr<CompressionManager> mgr) {
    if (FormatVersionUsesCompressionManagerName(table_options.format_version)) {
      assert(mgr);
      // Use newer compression_name property
      props.compression_name.reserve(32);
      // If compression is disabled, use empty manager name
      if (basic_compressor) {
        props.compression_name.append(mgr->CompatibilityName());
      }
      props.compression_name.push_back(';');
      // Rest of property to be filled out at the end of building the file
    } else {
      // Use legacy compression_name property, populated at the end of
      // building the file. Not compatible with compression managers using
      // custom algorithms / compression types.
      assert(Slice(mgr->CompatibilityName())
                 .compare(GetBuiltinCompressionManager(
                              GetCompressFormatForVersion(
                                  static_cast<uint32_t>(props.format_version)))
                              ->CompatibilityName()) == 0);
    }
  }
  void PostPopulateCompressionProperties() {
    // Do not include "no compression" in the set. It's not really useful
    // information whether there are any uncompressed blocks. Some kinds of
    // blocks are never compressed anyway.
    compression_types_used.Remove(kNoCompression);
    size_t ctype_count = compression_types_used.count();

    if (uses_explicit_compression_manager) {
      // Stuff some extra debugging info as extra pseudo-options. Using
      // underscore prefix to indicate they are special.
      std::string& compression_options = props.compression_options;
      compression_options.append("_compressor=");
      compression_options.append(data_block_compressor
                                     ? data_block_compressor->GetId()
                                     : std::string{});
      compression_options.append("; ");
    } else {
      // No explicit compression manager
      assert(compression_types_used.count() <= 1);
    }

    std::string& compression_name = props.compression_name;
    if (FormatVersionUsesCompressionManagerName(table_options.format_version)) {
      // Fill in extended field of "compression name" property, which is the
      // set of compression types used, sorted by unsigned byte and then hex
      // encoded with two digits each (so that table properties are human
      // readable).
      assert(*compression_name.rbegin() == ';');
      size_t pos = compression_name.size();
      // Make space for the field contents
      compression_name.append(ctype_count * 2, '\0');
      char* ptr = compression_name.data() + pos;
      // Populate the field contents
      for (CompressionType t : compression_types_used) {
        PutBaseChars<16>(&ptr, /*n=*/2, static_cast<unsigned char>(t),
                         /*uppercase=*/true);
      }
      assert(ptr == compression_name.data() + pos + ctype_count * 2);
      // Allow additional fields in the future
      compression_name.push_back(';');
    } else {
      // Use legacy compression naming. To adhere to requirements described in
      // TableProperties::compression_name, we might have to replace the name
      // based on the legacy configured compression type.
      assert(compression_name.empty());
      if (ctype_count == 0) {
        // We could get a slight performance boost in the reader by marking
        // the file as "no compression" if compression is configured but
        // consistently rejected, but that would give misleading info for
        // debugging purposes. So instead we record the configured compression
        // type, matching the historical behavior.
        if (data_block_compressor) {
          compression_name = CompressionTypeToString(
              data_block_compressor->GetPreferredCompressionType());
        } else {
          assert(basic_compressor == nullptr);
          compression_name = CompressionTypeToString(kNoCompression);
        }
      } else if (compression_types_used.Contains(kZSTD)) {
        compression_name = CompressionTypeToString(kZSTD);
      } else {
        compression_name =
            CompressionTypeToString(*compression_types_used.begin());
      }
    }
  }

 private:
  // Synchronize io_status to be readable/writable across threads, but
  // optimize for the OK case
  std::mutex io_status_mutex;
  RelaxedAtomic<bool> io_status_ok{true};
  IOStatus io_status;
};

BlockBasedTableBuilder::BlockBasedTableBuilder(
    const BlockBasedTableOptions& table_options, const TableBuilderOptions& tbo,
    WritableFileWriter* file) {
  BlockBasedTableOptions sanitized_table_options(table_options);
  auto ucmp = tbo.internal_comparator.user_comparator();
  assert(ucmp);
  (void)ucmp;  // avoids unused variable error.
  rep_ = std::make_unique<Rep>(sanitized_table_options, tbo, file);

  TEST_SYNC_POINT_CALLBACK(
      "BlockBasedTableBuilder::BlockBasedTableBuilder:PreSetupBaseCacheKey",
      const_cast<TableProperties*>(&rep_->props));

  BlockBasedTable::SetupBaseCacheKey(&rep_->props, tbo.db_session_id,
                                     tbo.cur_file_num, &rep_->base_cache_key);

  MaybeStartParallelCompression();
  if (!rep_->IsParallelCompressionActive() && rep_->basic_compressor) {
    rep_->single_threaded_compressed_output.ResetForSize(
        table_options.block_size);
  }
}

BlockBasedTableBuilder::~BlockBasedTableBuilder() {
  // Catch errors where caller forgot to call Finish()
  assert(rep_->state == Rep::State::kClosed);
}

void BlockBasedTableBuilder::Add(const Slice& ikey, const Slice& value) {
  Rep* r = rep_.get();
  assert(rep_->state != Rep::State::kClosed);
  if (UNLIKELY(!ok())) {
    return;
  }
  ValueType value_type;
  SequenceNumber seq;
  UnPackSequenceAndType(ExtractInternalKeyFooter(ikey), &seq, &value_type);
  r->props.key_largest_seqno = std::max(r->props.key_largest_seqno, seq);
  r->props.key_smallest_seqno = std::min(r->props.key_smallest_seqno, seq);
  if (IsValueType(value_type)) {
#ifndef NDEBUG
    if (r->props.num_entries > r->props.num_range_deletions) {
      assert(r->internal_comparator.Compare(ikey, Slice(r->last_ikey)) > 0);
    }
    bool skip = false;
    TEST_SYNC_POINT_CALLBACK("BlockBasedTableBuilder::Add::skip", (void*)&skip);
    if (skip) {
      return;
    }
#endif  // !NDEBUG

    auto should_flush = r->flush_block_policy->Update(ikey, value);
    if (should_flush) {
      assert(!r->data_block.empty());
      Flush(/*first_key_in_next_block=*/&ikey);
    }

    // Note: PartitionedFilterBlockBuilder with
    // decouple_partitioned_filters=false requires key being added to filter
    // builder after being added to and "finished" in the index builder, so
    // forces no parallel compression (logic in Rep constructor).
    if (r->state == Rep::State::kUnbuffered) {
      if (r->filter_builder != nullptr) {
        r->filter_builder->AddWithPrevKey(
            ExtractUserKeyAndStripTimestamp(ikey, r->ts_sz),
            r->last_ikey.empty()
                ? Slice{}
                : ExtractUserKeyAndStripTimestamp(r->last_ikey, r->ts_sz));
      }
    }

    r->data_block.AddWithLastKey(ikey, value, r->last_ikey);
    r->last_ikey.assign(ikey.data(), ikey.size());
    assert(!r->last_ikey.empty());
    if (r->state == Rep::State::kBuffered) {
      // Buffered keys will be replayed from data_block_buffers during
      // `Finish()` once compression dictionary has been finalized.
    } else {
      r->index_builder->OnKeyAdded(ikey, value);
    }
    // TODO offset passed in is not accurate for parallel compression case
    NotifyCollectTableCollectorsOnAdd(ikey, value, r->get_offset(),
                                      r->table_properties_collectors,
                                      r->ioptions.logger);

  } else if (value_type == kTypeRangeDeletion) {
    Slice persisted_end = value;
    // When timestamps should not be persisted, we physically strip away range
    // tombstone end key's user timestamp before passing it along to block
    // builder. Physically stripping away start key's user timestamp is
    // handled at the block builder level in the same way as the other data
    // blocks.
    if (r->ts_sz > 0 && !r->persist_user_defined_timestamps) {
      persisted_end = StripTimestampFromUserKey(value, r->ts_sz);
    }
    r->range_del_block.Add(ikey, persisted_end);
    // TODO offset passed in is not accurate for parallel compression case
    NotifyCollectTableCollectorsOnAdd(ikey, value, r->get_offset(),
                                      r->table_properties_collectors,
                                      r->ioptions.logger);
  } else {
    assert(false);
    r->SetStatus(Status::InvalidArgument(
        "BlockBasedBuilder::Add() received a key with invalid value type " +
        std::to_string(static_cast<unsigned int>(value_type))));
    return;
  }

  r->props.num_entries++;
  r->props.raw_key_size += ikey.size();
  if (!r->persist_user_defined_timestamps) {
    r->props.raw_key_size -= r->ts_sz;
  }
  r->props.raw_value_size += value.size();
  if (value_type == kTypeDeletion || value_type == kTypeSingleDeletion ||
      value_type == kTypeDeletionWithTimestamp) {
    r->props.num_deletions++;
  } else if (value_type == kTypeRangeDeletion) {
    r->props.num_deletions++;
    r->props.num_range_deletions++;
  } else if (value_type == kTypeMerge) {
    r->props.num_merge_operands++;
  }
}

void BlockBasedTableBuilder::Flush(const Slice* first_key_in_next_block) {
  Rep* r = rep_.get();
  assert(rep_->state != Rep::State::kClosed);
  if (UNLIKELY(!ok())) {
    return;
  }
  if (r->data_block.empty()) {
    return;
  }
  Slice uncompressed_block_data = r->data_block.Finish();

  // NOTE: compression sampling is done here in the same thread as building
  // the uncompressed block because of the requirements to call table
  // property collectors:
  // * BlockAdd function expects block_compressed_bytes_{fast,slow} for
  //   historical reasons. Probably a hassle to remove.
  // * Collector is not thread safe so calls need to be
  // serialized/synchronized.
  // * Ideally, AddUserKey and BlockAdd calls need to line up such that a
  //   reported block corresponds to all the keys reported since the previous
  //   block.

  // If requested, we sample one in every N block with a
  // fast and slow compression algorithm and report the stats.
  // The users can use these stats to decide if it is worthwhile
  // enabling compression and they also get a hint about which
  // compression algorithm wil be beneficial.
  if (r->sample_for_compression > 0 &&
      Random::GetTLSInstance()->OneIn(
          static_cast<int>(r->sample_for_compression))) {
    std::string sampled_output_fast;
    std::string sampled_output_slow;

    // Sampling with a fast compression algorithm
    if (LZ4_Supported() || Snappy_Supported()) {
      CompressionType c =
          LZ4_Supported() ? kLZ4Compression : kSnappyCompression;
      CompressionOptions options;
      CompressionContext context(c, options);
      CompressionInfo info_tmp(options, context,
                               CompressionDict::GetEmptyDict(), c);

      OLD_CompressData(
          uncompressed_block_data, info_tmp,
          GetCompressFormatForVersion(r->table_options.format_version),
          &sampled_output_fast);
    }

    // Sampling with a slow but high-compression algorithm
    if (ZSTD_Supported() || Zlib_Supported()) {
      CompressionType c = ZSTD_Supported() ? kZSTD : kZlibCompression;
      CompressionOptions options;
      CompressionContext context(c, options);
      CompressionInfo info_tmp(options, context,
                               CompressionDict::GetEmptyDict(), c);

      OLD_CompressData(
          uncompressed_block_data, info_tmp,
          GetCompressFormatForVersion(r->table_options.format_version),
          &sampled_output_slow);
    }

    if (sampled_output_slow.size() > 0 || sampled_output_fast.size() > 0) {
      // Currently compression sampling is only enabled for data block.
      r->sampled_input_data_bytes.FetchAddRelaxed(
          uncompressed_block_data.size());
      r->sampled_output_slow_data_bytes.FetchAddRelaxed(
          sampled_output_slow.size());
      r->sampled_output_fast_data_bytes.FetchAddRelaxed(
          sampled_output_fast.size());
    }

    NotifyCollectTableCollectorsOnBlockAdd(
        r->table_properties_collectors, uncompressed_block_data.size(),
        sampled_output_slow.size(), sampled_output_fast.size());
  } else {
    NotifyCollectTableCollectorsOnBlockAdd(
        r->table_properties_collectors, uncompressed_block_data.size(),
        0 /*block_compressed_bytes_slow*/, 0 /*block_compressed_bytes_fast*/);
  }

  if (rep_->state == Rep::State::kBuffered) {
    std::string uncompressed_block_holder;
    uncompressed_block_holder.reserve(rep_->table_options.block_size);
    r->data_block.SwapAndReset(uncompressed_block_holder);
    assert(uncompressed_block_data.size() == uncompressed_block_holder.size());
    rep_->data_block_buffers.emplace_back(std::move(uncompressed_block_holder));
    rep_->data_begin_offset += uncompressed_block_data.size();
    MaybeEnterUnbuffered(first_key_in_next_block);
  } else {
    if (r->IsParallelCompressionActive()) {
      EmitBlockForParallel(r->data_block.MutableBuffer(), r->last_ikey,
                           first_key_in_next_block);
    } else {
      EmitBlock(r->data_block.MutableBuffer(), r->last_ikey,
                first_key_in_next_block);
    }
    r->data_block.Reset();
  }
}

void BlockBasedTableBuilder::EmitBlockForParallel(
    std::string& uncompressed, const Slice& last_key_in_current_block,
    const Slice* first_key_in_next_block) {
  Rep* r = rep_.get();
  assert(r->state == Rep::State::kUnbuffered);
  assert(uncompressed.size() > 0);
  auto& pc_rep = *r->pc_rep;
  // Can emit the uncompressed block into the ring buffer
  assert(pc_rep.emit_thread_state ==
         ParallelCompressionRep::ThreadState::kEmitting);
  auto* block_rep = &pc_rep.ring_buffer[pc_rep.emit_slot];
  pc_rep.estimated_inflight_size.FetchAddRelaxed(uncompressed.size() +
                                                 kBlockTrailerSize);
  std::swap(uncompressed, block_rep->uncompressed);
  r->index_builder->PrepareIndexEntry(last_key_in_current_block,
                                      first_key_in_next_block,
                                      block_rep->prepared_index_entry.get());
  block_rep->compressed.Reset();
  block_rep->compression_type = kNoCompression;

  // Might need to take up some compression work before we are able to
  // resume emitting the next uncompressed block.
  for (;;) {
    pc_rep.EmitterStateTransition(pc_rep.emit_thread_state, pc_rep.emit_slot);

    if (pc_rep.emit_thread_state ==
        ParallelCompressionRep::ThreadState::kCompressing) {
      // Took up some compression work to help unblock ourself
      block_rep = &pc_rep.ring_buffer[pc_rep.emit_slot];
      Status s = CompressAndVerifyBlock(
          block_rep->uncompressed, /*is_data_block=*/true,
          r->data_block_working_area, &block_rep->compressed,
          &block_rep->compression_type);
      if (UNLIKELY(!s.ok())) {
        r->SetStatus(s);
        pc_rep.SetAbort(pc_rep.emit_thread_state);
        break;
      }
    } else {
      assert(pc_rep.emit_thread_state !=
             ParallelCompressionRep::ThreadState::kCompressingAndWriting);
      assert(pc_rep.emit_thread_state !=
             ParallelCompressionRep::ThreadState::kWriting);
      assert(pc_rep.emit_thread_state !=
             ParallelCompressionRep::ThreadState::kIdle);
      // Either emitting or end state.
      // Detect nothing more to emit and set if so.
      if (first_key_in_next_block == nullptr &&
          pc_rep.emit_thread_state ==
              ParallelCompressionRep::ThreadState::kEmitting) {
        pc_rep.SetNoMoreToEmit(pc_rep.emit_thread_state, pc_rep.emit_slot);
      }
      break;
    }
  }
}
void BlockBasedTableBuilder::EmitBlock(std::string& uncompressed,
                                       const Slice& last_key_in_current_block,
                                       const Slice* first_key_in_next_block) {
  Rep* r = rep_.get();
  assert(r->state == Rep::State::kUnbuffered);
  // Single-threaded context only
  assert(!r->IsParallelCompressionActive());
  assert(uncompressed.size() > 0);
  // When data blocks are aligned with super block alignment, delta encoding
  // needs to be skipped for the first block after padding.
  bool skip_delta_encoding = false;
  WriteBlock(uncompressed, &r->pending_handle, BlockType::kData,
             &skip_delta_encoding);
  if (LIKELY(ok())) {
    // We do not emit the index entry for a block until we have seen the
    // first key for the next data block.  This allows us to use shorter
    // keys in the index block.  For example, consider a block boundary
    // between the keys "the quick brown fox" and "the who".  We can use
    // "the r" as the key for the index block entry since it is >= all
    // entries in the first block and < all entries in subsequent
    // blocks.
    r->index_builder->AddIndexEntry(
        last_key_in_current_block, first_key_in_next_block, r->pending_handle,
        &r->index_separator_scratch, skip_delta_encoding);
  }
}

void BlockBasedTableBuilder::WriteBlock(const Slice& uncompressed_block_data,
                                        BlockHandle* handle,
                                        BlockType block_type,
                                        bool* skip_delta_encoding) {
  Rep* r = rep_.get();
  assert(r->state == Rep::State::kUnbuffered);
  // Single-threaded context only
  assert(!r->IsParallelCompressionActive());
  CompressionType type;
  bool is_data_block = block_type == BlockType::kData;
  Status compress_status = CompressAndVerifyBlock(
      uncompressed_block_data, is_data_block,
      is_data_block ? r->data_block_working_area : r->basic_working_area,
      &r->single_threaded_compressed_output, &type);
  r->SetStatus(compress_status);
  if (UNLIKELY(!ok())) {
    return;
  }

  TEST_SYNC_POINT_CALLBACK(
      "BlockBasedTableBuilder::WriteBlock:TamperWithCompressedData",
      &r->single_threaded_compressed_output);
  WriteMaybeCompressedBlock(
      type == kNoCompression ? uncompressed_block_data
                             : Slice(r->single_threaded_compressed_output),
      type, handle, block_type, &uncompressed_block_data, skip_delta_encoding);
  r->single_threaded_compressed_output.Reset();
  if (is_data_block) {
    r->props.data_size = r->get_offset();
    r->props.uncompressed_data_size += uncompressed_block_data.size();
    ++r->props.num_data_blocks;
  }
}

uint64_t BlockBasedTableBuilder::GetWorkerCPUMicros() const {
  return rep_->worker_cpu_micros.LoadRelaxed();
}

void BlockBasedTableBuilder::BGWorker(WorkingAreaPair& working_area) {
  // Record CPU usage of this thread
  const uint64_t start_cpu_micros =
      rep_->ioptions.env->GetSystemClock()->CPUMicros();
  Defer log_cpu{[this, start_cpu_micros]() {
    rep_->worker_cpu_micros.FetchAddRelaxed(
        rep_->ioptions.env->GetSystemClock()->CPUMicros() - start_cpu_micros);
  }};

  auto& pc_rep = *rep_->pc_rep;
#ifdef BBTB_PC_WATCHDOG
  pc_rep.live_workers.FetchAddRelaxed(1);
  Defer decr{[&pc_rep]() { pc_rep.live_workers.FetchSubRelaxed(1); }};
#endif  // BBTB_PC_WATCHDOG
  ParallelCompressionRep::ThreadState thread_state =
      ParallelCompressionRep::ThreadState::kIdle;
  uint32_t slot = 0;
  // Workers should avoid checking the shared status (e.g. ok()) to minimize
  // potential data dependencies across threads. If another thread hits an
  // error, we will pick up the kEnd state from the abort.
  IOStatus ios;
  do {
    pc_rep.WorkerStateTransition(thread_state, slot);
    ParallelCompressionRep::BlockRep* block_rep = &pc_rep.ring_buffer[slot];
    auto compress_fn = [this, block_rep, &ios, &working_area]() {
      ios = status_to_io_status(CompressAndVerifyBlock(
          block_rep->uncompressed, /*is_data_block=*/true, working_area,
          &block_rep->compressed, &block_rep->compression_type));
    };
    auto write_fn = [this, block_rep, &ios]() {
      Slice compressed = block_rep->compressed;
      Slice uncompressed = block_rep->uncompressed;
      bool skip_delta_encoding = false;
      ios = WriteMaybeCompressedBlockImpl(
          block_rep->compression_type == kNoCompression ? uncompressed
                                                        : compressed,
          block_rep->compression_type, &rep_->pending_handle, BlockType::kData,
          &uncompressed, &skip_delta_encoding);
      if (LIKELY(ios.ok())) {
        rep_->props.data_size = rep_->get_offset();
        rep_->props.uncompressed_data_size += block_rep->uncompressed.size();
        ++rep_->props.num_data_blocks;

        rep_->index_builder->FinishIndexEntry(
            rep_->pending_handle, block_rep->prepared_index_entry.get(),
            skip_delta_encoding);
      }
    };
    switch (thread_state) {
      case ParallelCompressionRep::ThreadState::kEnd:
        // All done
        assert(ios.ok());
        return;
      case ParallelCompressionRep::ThreadState::kCompressing:
        compress_fn();
        break;
      case ParallelCompressionRep::ThreadState::kCompressingAndWriting:
        compress_fn();
        if (LIKELY(ios.ok())) {
          write_fn();
        }
        break;
      case ParallelCompressionRep::ThreadState::kWriting:
        write_fn();
        break;
      case ParallelCompressionRep::ThreadState::kEmitting:
        // Shouldn't happen
        assert(thread_state != ParallelCompressionRep::ThreadState::kEmitting);
        break;
      case ParallelCompressionRep::ThreadState::kIdle:
        // Shouldn't happen
        assert(thread_state != ParallelCompressionRep::ThreadState::kIdle);
        break;
      default:
        assert(false);
        break;
    }
  } while (LIKELY(ios.ok()));
  // Hit an error, so abort
  rep_->SetIOStatus(ios);
  pc_rep.SetAbort(thread_state);
}

Status BlockBasedTableBuilder::CompressAndVerifyBlock(
    const Slice& uncompressed_block_data, bool is_data_block,
    WorkingAreaPair& working_area, GrowableBuffer* compressed_output,
    CompressionType* result_compression_type) {
  Rep* r = rep_.get();
  Status status;

  Compressor* compressor = nullptr;
  Decompressor* verify_decomp = nullptr;
  if (is_data_block) {
    compressor = r->data_block_compressor;
    verify_decomp = r->data_block_verify_decompressor.get();
  } else {
    compressor = r->basic_compressor.get();
    verify_decomp = r->verify_decompressor.get();
  }

  compressed_output->Reset();
  CompressionType type = kNoCompression;
  if (LIKELY(uncompressed_block_data.size() < kCompressionSizeLimit)) {
    if (compressor) {
      StopWatchNano timer(
          r->ioptions.clock,
          ShouldReportDetailedTime(r->ioptions.env, r->ioptions.stats));

      size_t max_compressed_size = static_cast<size_t>(
          (static_cast<uint64_t>(r->max_compressed_bytes_per_kb) *
           uncompressed_block_data.size()) >>
          10);
      compressed_output->ResetForSize(max_compressed_size);
      status = compressor->CompressBlock(
          uncompressed_block_data, compressed_output->data(),
          &compressed_output->MutableSize(), &type, &working_area.compress);

      // Post-condition of Compressor::CompressBlock
      assert(type == kNoCompression || status.ok());
      assert(type == kNoCompression ||
             r->table_options.verify_compression == (verify_decomp != nullptr));

      // Some of the compression algorithms are known to be unreliable. If
      // the verify_compression flag is set then try to de-compress the
      // compressed data and compare to the input.
      if (verify_decomp && type != kNoCompression) {
        BlockContents contents;
        Status uncompress_status = DecompressBlockData(
            compressed_output->data(), compressed_output->size(), type,
            *verify_decomp, &contents, r->ioptions,
            /*allocator=*/nullptr, &working_area.verify);

        if (LIKELY(uncompress_status.ok())) {
          bool data_match = contents.data.compare(uncompressed_block_data) == 0;
          if (!data_match) {
            // The result of the compression was invalid. abort.
            const char* const msg =
                "Decompressed block did not match pre-compression block";
            ROCKS_LOG_ERROR(r->ioptions.logger, "%s", msg);
            status = Status::Corruption(msg);
            type = kNoCompression;
          }
        } else {
          // Decompression reported an error. abort.
          status = Status::Corruption(std::string("Could not decompress: ") +
                                      uncompress_status.getState());
          type = kNoCompression;
        }
      }
      if (timer.IsStarted()) {
        RecordTimeToHistogram(r->ioptions.stats, COMPRESSION_TIMES_NANOS,
                              timer.ElapsedNanos());
      }
    }
    if (is_data_block) {
      r->compressible_input_data_bytes.FetchAddRelaxed(
          uncompressed_block_data.size());
      r->uncompressible_input_data_bytes.FetchAddRelaxed(kBlockTrailerSize);
    }
  } else {
    // Status is not OK, or block is too big to be compressed.
    if (is_data_block) {
      r->uncompressible_input_data_bytes.FetchAddRelaxed(
          uncompressed_block_data.size() + kBlockTrailerSize);
    }
  }

  // Abort compression if the block is too big, or did not pass
  // verification.
  if (type == kNoCompression) {
    bool compression_attempted = !compressed_output->empty();
    RecordTick(r->ioptions.stats, compression_attempted
                                      ? NUMBER_BLOCK_COMPRESSION_REJECTED
                                      : NUMBER_BLOCK_COMPRESSION_BYPASSED);
    RecordTick(r->ioptions.stats,
               compression_attempted ? BYTES_COMPRESSION_REJECTED
                                     : BYTES_COMPRESSION_BYPASSED,
               uncompressed_block_data.size());
  } else {
    RecordTick(r->ioptions.stats, NUMBER_BLOCK_COMPRESSED);
    RecordTick(r->ioptions.stats, BYTES_COMPRESSED_FROM,
               uncompressed_block_data.size());
    RecordTick(r->ioptions.stats, BYTES_COMPRESSED_TO,
               compressed_output->size());
    if (r->IsParallelCompressionActive() && is_data_block) {
      r->pc_rep->estimated_inflight_size.FetchSubRelaxed(
          uncompressed_block_data.size() - compressed_output->size());
    }
  }
  *result_compression_type = type;
  return status;
}

void BlockBasedTableBuilder::WriteMaybeCompressedBlock(
    const Slice& block_contents, CompressionType comp_type, BlockHandle* handle,
    BlockType block_type, const Slice* uncompressed_block_data,
    bool* skip_delta_encoding) {
  rep_->SetIOStatus(WriteMaybeCompressedBlockImpl(
      block_contents, comp_type, handle, block_type, uncompressed_block_data,
      skip_delta_encoding));
}

IOStatus BlockBasedTableBuilder::WriteMaybeCompressedBlockImpl(
    const Slice& block_contents, CompressionType comp_type, BlockHandle* handle,
    BlockType block_type, const Slice* uncompressed_block_data,
    bool* skip_delta_encoding) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    compression_type: uint8
  //    checksum: uint32
  Rep* r = rep_.get();
  bool is_data_block = block_type == BlockType::kData;
  // For data block, skip_delta_encoding must be non null
  if (is_data_block) {
    assert(skip_delta_encoding != nullptr);
  }
  if (skip_delta_encoding != nullptr) {
    *skip_delta_encoding = false;
  }
  IOOptions io_options;
  // Always return io_s for NRVO
  IOStatus io_s =
      WritableFileWriter::PrepareIOOptions(r->write_options, io_options);
  if (UNLIKELY(!io_s.ok())) {
    return io_s;
  }
  // Old, misleading name of this function: WriteRawBlock
  StopWatch sw(r->ioptions.clock, r->ioptions.stats, WRITE_RAW_BLOCK_MICROS);

  auto offset = r->get_offset();
  // try to align the data block page to the super alignment size, if enabled
  if ((r->table_options.super_block_alignment_size != 0) && is_data_block) {
    auto super_block_alignment_mask =
        r->table_options.super_block_alignment_size - 1;
    if ((r->table_options.super_block_alignment_space_overhead_ratio != 0) &&
        (offset & (~super_block_alignment_mask)) !=
            ((offset + block_contents.size()) &
             (~super_block_alignment_mask))) {
      auto allowed_max_padding_size =
          r->table_options.super_block_alignment_size /
          r->table_options.super_block_alignment_space_overhead_ratio;
      // new block would cross the super block boundary
      auto pad_bytes = r->table_options.super_block_alignment_size -
                       (offset & super_block_alignment_mask);
      if (pad_bytes < allowed_max_padding_size) {
        io_s = r->file->Pad(io_options, pad_bytes, allowed_max_padding_size);
        if (UNLIKELY(!io_s.ok())) {
          r->SetIOStatus(io_s);
          return io_s;
        }
        r->pre_compression_size += pad_bytes;
        offset += pad_bytes;
        r->set_offset(offset);
        if (skip_delta_encoding != nullptr) {
          // Skip delta encoding in index block builder when a super block
          // alignment padding is added for data block.
          *skip_delta_encoding = true;
        }
        TEST_SYNC_POINT(
            "BlockBasedTableBuilder::WriteMaybeCompressedBlock:"
            "SuperBlockAlignment");
      } else {
        TEST_SYNC_POINT(
            "BlockBasedTableBuilder::WriteMaybeCompressedBlock:"
            "SuperBlockAlignmentPaddingBytesExceedLimit");
      }
    }
  }

  handle->set_offset(offset);
  handle->set_size(block_contents.size());
  assert(status().ok());
  assert(io_status().ok());
  if (uncompressed_block_data == nullptr) {
    uncompressed_block_data = &block_contents;
    assert(comp_type == kNoCompression);
  }

  // TODO: consider a variant of this function that puts the trailer after
  // block_contents (if it comes from a std::string) so we only need one
  // r->file->Append call
  {
    io_s = r->file->Append(io_options, block_contents);
    if (UNLIKELY(!io_s.ok())) {
      return io_s;
    }
  }

  r->compression_types_used.Add(comp_type);
  std::array<char, kBlockTrailerSize> trailer;
  trailer[0] = comp_type;
  uint32_t checksum = ComputeBuiltinChecksumWithLastByte(
      r->table_options.checksum, block_contents.data(), block_contents.size(),
      /*last_byte*/ comp_type);
  checksum += ChecksumModifierForContext(r->base_context_checksum, offset);

  if (block_type == BlockType::kFilter) {
    io_s = status_to_io_status(
        r->filter_builder->MaybePostVerifyFilter(block_contents));
    if (UNLIKELY(!io_s.ok())) {
      return io_s;
    }
  }

  EncodeFixed32(trailer.data() + 1, checksum);
  TEST_SYNC_POINT_CALLBACK(
      "BlockBasedTableBuilder::WriteMaybeCompressedBlock:TamperWithChecksum",
      trailer.data());
  {
    io_s = r->file->Append(io_options, Slice(trailer.data(), trailer.size()));
    if UNLIKELY (!io_s.ok()) {
      return io_s;
    }
  }

  if (r->warm_cache) {
    io_s = status_to_io_status(
        InsertBlockInCacheHelper(*uncompressed_block_data, handle, block_type));
    if (UNLIKELY(!io_s.ok())) {
      return io_s;
    }
  }

  r->pre_compression_size +=
      uncompressed_block_data->size() + kBlockTrailerSize;
  r->set_offset(r->get_offset() + block_contents.size() + kBlockTrailerSize);
  if (r->table_options.block_align && is_data_block) {
    size_t pad_bytes =
        (r->alignment -
         ((block_contents.size() + kBlockTrailerSize) & (r->alignment - 1))) &
        (r->alignment - 1);

    io_s = r->file->Pad(io_options, pad_bytes, kDefaultPageSize);
    if (LIKELY(io_s.ok())) {
      r->pre_compression_size += pad_bytes;
      r->set_offset(r->get_offset() + pad_bytes);
    } else {
      return io_s;
    }
  }

  if (r->IsParallelCompressionActive() && is_data_block) {
    r->pc_rep->estimated_inflight_size.FetchSubRelaxed(block_contents.size() +
                                                       kBlockTrailerSize);
  }
  return io_s;
}

void BlockBasedTableBuilder::MaybeStartParallelCompression() {
  if (rep_->compression_parallel_threads <= 1) {
    return;
  }
  // Although in theory having a separate thread for writing to the SST file
  // could help to hide the latency associated with writing, it is more often
  // the case that the latency comes in large units for rare calls to write that
  // flush downstream buffers, including in WritableFileWriter. The buffering
  // provided by the compression ring buffer is almost negligible for hiding
  // that latency. So even with some optimizations, turning on the parallel
  // framework when compression is disabled just eats more CPU with little-to-no
  // improvement in throughput.
  if (rep_->data_block_compressor == nullptr) {
    // Force the generally best configuration for no compression: no parallelism
    return;
  }
  rep_->pc_rep = std::make_unique<ParallelCompressionRep>(
      rep_->compression_parallel_threads);
  auto& pc_rep = *rep_->pc_rep;
  for (uint32_t i = 0; i <= pc_rep.ring_buffer_mask; i++) {
    pc_rep.ring_buffer[i].prepared_index_entry =
        rep_->index_builder->CreatePreparedIndexEntry();
  }
  pc_rep.worker_threads.reserve(pc_rep.num_worker_threads);
  pc_rep.working_areas.resize(pc_rep.num_worker_threads);
  for (uint32_t i = 0; i < pc_rep.num_worker_threads; i++) {
    auto& wa = pc_rep.working_areas[i];
    if (rep_->data_block_compressor) {
      wa.compress = rep_->data_block_compressor->ObtainWorkingArea();
    }
    if (rep_->data_block_verify_decompressor) {
      wa.verify = rep_->data_block_verify_decompressor->ObtainWorkingArea(
          rep_->data_block_compressor->GetPreferredCompressionType());
    }
    pc_rep.worker_threads.emplace_back([this, &wa] { BGWorker(wa); });
  }
#ifdef BBTB_PC_WATCHDOG
  // Start watchdog thread
  pc_rep.watchdog_thread = std::thread([&pc_rep] { pc_rep.BGWatchdog(); });
  pc_rep.live_emit.StoreRelaxed(true);
#endif  // BBTB_PC_WATCHDOG
}

void BlockBasedTableBuilder::StopParallelCompression(bool abort) {
  auto& pc_rep = *rep_->pc_rep;
  if (abort) {
    pc_rep.SetAbort(pc_rep.emit_thread_state);
  } else if (pc_rep.emit_thread_state !=
             ParallelCompressionRep::ThreadState::kEnd) {
    // In case we didn't do a final flush with no next key
    assert(rep_->props.num_data_blocks == 0);
    pc_rep.SetNoMoreToEmit(pc_rep.emit_thread_state, pc_rep.emit_slot);
  }
#ifdef BBTB_PC_WATCHDOG
  pc_rep.live_emit.StoreRelaxed(false);
#endif  // BBTB_PC_WATCHDOG
  assert(pc_rep.emit_thread_state == ParallelCompressionRep::ThreadState::kEnd);
  for (auto& thread : pc_rep.worker_threads) {
    thread.join();
  }
#ifdef BBTB_PC_WATCHDOG
  // Wake & shutdown watchdog thread
  {
    std::unique_lock<std::mutex> lock(pc_rep.watchdog_mutex);
    pc_rep.shutdown_watchdog = true;
    pc_rep.watchdog_cv.notify_all();
  }
  pc_rep.watchdog_thread.join();
#endif  // BBTB_PC_WATCHDOG
  rep_->pc_rep.reset();
}

Status BlockBasedTableBuilder::status() const { return rep_->GetStatus(); }

IOStatus BlockBasedTableBuilder::io_status() const {
  return rep_->GetIOStatus();
}

bool BlockBasedTableBuilder::ok() const { return rep_->StatusOk(); }

Status BlockBasedTableBuilder::InsertBlockInCacheHelper(
    const Slice& block_contents, const BlockHandle* handle,
    BlockType block_type) {
  Cache* block_cache = rep_->table_options.block_cache.get();
  Status s;
  auto helper =
      GetCacheItemHelper(block_type, rep_->ioptions.lowest_used_cache_tier);
  if (block_cache && helper && helper->create_cb) {
    CacheKey key = BlockBasedTable::GetCacheKey(rep_->base_cache_key, *handle);
    size_t charge;
    // NOTE: data blocks (and everything else) will be warmed in decompressed
    // state, so does not need a dictionary-aware decompressor. The only thing
    // needing a decompressor here (in create_context) is warming the
    // (de)compression dictionary, which will clone and save a dict-based
    // decompressor from the corresponding non-dict decompressor.
    s = WarmInCache(block_cache, key.AsSlice(), block_contents,
                    &rep_->create_context, helper, Cache::Priority::LOW,
                    &charge);
    if (LIKELY(s.ok())) {
      BlockBasedTable::UpdateCacheInsertionMetrics(
          block_type, nullptr /*get_context*/, charge, s.IsOkOverwritten(),
          rep_->ioptions.stats);
    } else {
      RecordTick(rep_->ioptions.stats, BLOCK_CACHE_ADD_FAILURES);
    }
  }
  return s;
}

void BlockBasedTableBuilder::WriteFilterBlock(
    MetaIndexBuilder* meta_index_builder) {
  if (rep_->filter_builder == nullptr || rep_->filter_builder->IsEmpty()) {
    // No filter block needed
    return;
  }
  if (!rep_->last_ikey.empty()) {
    // We might have been using AddWithPrevKey, so need PrevKeyBeforeFinish
    // to be safe. And because we are re-synchronized after buffered/parallel
    // operation, rep_->last_ikey is accurate.
    rep_->filter_builder->PrevKeyBeforeFinish(
        ExtractUserKeyAndStripTimestamp(rep_->last_ikey, rep_->ts_sz));
  }
  BlockHandle filter_block_handle;
  bool is_partitioned_filter = rep_->table_options.partition_filters;
  if (LIKELY(ok())) {
    rep_->props.num_filter_entries +=
        rep_->filter_builder->EstimateEntriesAdded();
    Status s = Status::Incomplete();
    while (LIKELY(ok()) && s.IsIncomplete()) {
      // filter_data is used to store the transferred filter data payload from
      // FilterBlockBuilder and deallocate the payload by going out of scope.
      // Otherwise, the payload will unnecessarily remain until
      // BlockBasedTableBuilder is deallocated.
      //
      // See FilterBlockBuilder::Finish() for more on the difference in
      // transferred filter data payload among different FilterBlockBuilder
      // subtypes.
      std::unique_ptr<const char[]> filter_owner;
      Slice filter_content;
      s = rep_->filter_builder->Finish(filter_block_handle, &filter_content,
                                       &filter_owner);

      assert(s.ok() || s.IsIncomplete() || s.IsCorruption());
      if (s.IsCorruption()) {
        rep_->SetStatus(s);
        break;
      }

      rep_->props.filter_size += filter_content.size();

      BlockType btype = is_partitioned_filter && /* last */ s.ok()
                            ? BlockType::kFilterPartitionIndex
                            : BlockType::kFilter;
      WriteMaybeCompressedBlock(filter_content, kNoCompression,
                                &filter_block_handle, btype);
    }
    rep_->filter_builder->ResetFilterBitsBuilder();
  }
  if (LIKELY(ok())) {
    // Add mapping from "<filter_block_prefix>.Name" to location
    // of filter data.
    std::string key;
    key = is_partitioned_filter ? BlockBasedTable::kPartitionedFilterBlockPrefix
                                : BlockBasedTable::kFullFilterBlockPrefix;
    key.append(rep_->table_options.filter_policy->CompatibilityName());
    meta_index_builder->Add(key, filter_block_handle);
  }
}

void BlockBasedTableBuilder::WriteIndexBlock(
    MetaIndexBuilder* meta_index_builder, BlockHandle* index_block_handle) {
  if (UNLIKELY(!ok())) {
    return;
  }
  IndexBuilder::IndexBlocks index_blocks;
  auto index_builder_status = rep_->index_builder->Finish(&index_blocks);
  if (LIKELY(ok()) && !index_builder_status.ok() &&
      !index_builder_status.IsIncomplete()) {
    // If the index builder failed for non-Incomplete errors, we should
    // mark the entire builder as having failed wit that status. However,
    // If the index builder failed with an incomplete error, we should
    // continue writing out any meta blocks that may have been generated.
    rep_->SetStatus(index_builder_status);
  }

  if (LIKELY(ok())) {
    for (const auto& item : index_blocks.meta_blocks) {
      BlockHandle block_handle;
      if (item.second.first == BlockType::kIndex) {
        WriteBlock(item.second.second, &block_handle, item.second.first);
      } else {
        assert(item.second.first == BlockType::kUserDefinedIndex);
        WriteMaybeCompressedBlock(item.second.second, kNoCompression,
                                  &block_handle, item.second.first);
      }
      if (UNLIKELY(!ok())) {
        break;
      }
      meta_index_builder->Add(item.first, block_handle);
    }
  }
  if (LIKELY(ok())) {
    if (rep_->table_options.enable_index_compression) {
      WriteBlock(index_blocks.index_block_contents, index_block_handle,
                 BlockType::kIndex);
    } else {
      WriteMaybeCompressedBlock(index_blocks.index_block_contents,
                                kNoCompression, index_block_handle,
                                BlockType::kIndex);
    }
  }
  // If there are more index partitions, finish them and write them out
  if (index_builder_status.IsIncomplete()) {
    bool index_building_finished = false;
    while (LIKELY(ok()) && !index_building_finished) {
      Status s =
          rep_->index_builder->Finish(&index_blocks, *index_block_handle);
      if (s.ok()) {
        index_building_finished = true;
      } else if (s.IsIncomplete()) {
        // More partitioned index after this one
        assert(!index_building_finished);
      } else {
        // Error
        rep_->SetStatus(s);
        return;
      }

      if (rep_->table_options.enable_index_compression) {
        WriteBlock(index_blocks.index_block_contents, index_block_handle,
                   BlockType::kIndex);
      } else {
        WriteMaybeCompressedBlock(index_blocks.index_block_contents,
                                  kNoCompression, index_block_handle,
                                  BlockType::kIndex);
      }
      // The last index_block_handle will be for the partition index block
    }
  }
  // If success and need to record in metaindex rather than footer...
  if (LIKELY(ok()) && !FormatVersionUsesIndexHandleInFooter(
                          rep_->table_options.format_version)) {
    meta_index_builder->Add(kIndexBlockName, *index_block_handle);
  }
}

void BlockBasedTableBuilder::WritePropertiesBlock(
    MetaIndexBuilder* meta_index_builder) {
  BlockHandle properties_block_handle;
  if (LIKELY(ok())) {
    PropertyBlockBuilder property_block_builder;
    rep_->props.filter_policy_name =
        rep_->table_options.filter_policy != nullptr
            ? rep_->table_options.filter_policy->Name()
            : "";
    rep_->props.index_size =
        rep_->index_builder->IndexSize() + kBlockTrailerSize;
    rep_->props.comparator_name = rep_->ioptions.user_comparator != nullptr
                                      ? rep_->ioptions.user_comparator->Name()
                                      : "nullptr";
    rep_->props.merge_operator_name =
        rep_->ioptions.merge_operator != nullptr
            ? rep_->ioptions.merge_operator->Name()
            : "nullptr";
    rep_->props.prefix_extractor_name =
        rep_->prefix_extractor ? rep_->prefix_extractor->AsString() : "nullptr";
    std::string property_collectors_names = "[";
    for (size_t i = 0;
         i < rep_->ioptions.table_properties_collector_factories.size(); ++i) {
      if (i != 0) {
        property_collectors_names += ",";
      }
      property_collectors_names +=
          rep_->ioptions.table_properties_collector_factories[i]->Name();
    }
    property_collectors_names += "]";
    rep_->props.property_collectors_names = property_collectors_names;

    rep_->PostPopulateCompressionProperties();

    if (rep_->table_options.index_type ==
        BlockBasedTableOptions::kTwoLevelIndexSearch) {
      assert(rep_->p_index_builder_ != nullptr);
      rep_->props.index_partitions = rep_->p_index_builder_->NumPartitions();
      rep_->props.top_level_index_size =
          rep_->p_index_builder_->TopLevelIndexSize(rep_->offset.LoadRelaxed());
    }
    rep_->props.index_key_is_user_key =
        !rep_->index_builder->separator_is_key_plus_seq();
    rep_->props.index_value_is_delta_encoded =
        rep_->use_delta_encoding_for_index_values;
    if (rep_->sampled_input_data_bytes.LoadRelaxed() > 0) {
      rep_->props.slow_compression_estimated_data_size = static_cast<uint64_t>(
          static_cast<double>(
              rep_->sampled_output_slow_data_bytes.LoadRelaxed()) /
              rep_->sampled_input_data_bytes.LoadRelaxed() *
              rep_->compressible_input_data_bytes.LoadRelaxed() +
          rep_->uncompressible_input_data_bytes.LoadRelaxed() + 0.5);
      rep_->props.fast_compression_estimated_data_size = static_cast<uint64_t>(
          static_cast<double>(
              rep_->sampled_output_fast_data_bytes.LoadRelaxed()) /
              rep_->sampled_input_data_bytes.LoadRelaxed() *
              rep_->compressible_input_data_bytes.LoadRelaxed() +
          rep_->uncompressible_input_data_bytes.LoadRelaxed() + 0.5);
    } else if (rep_->sample_for_compression > 0) {
      // We tried to sample but none were found. Assume worst-case
      // (compression ratio 1.0) so data is complete and aggregatable.
      rep_->props.slow_compression_estimated_data_size =
          rep_->compressible_input_data_bytes.LoadRelaxed() +
          rep_->uncompressible_input_data_bytes.LoadRelaxed();
      rep_->props.fast_compression_estimated_data_size =
          rep_->compressible_input_data_bytes.LoadRelaxed() +
          rep_->uncompressible_input_data_bytes.LoadRelaxed();
    }
    rep_->props.user_defined_timestamps_persisted =
        rep_->persist_user_defined_timestamps;

    assert(IsEmpty() || rep_->props.key_largest_seqno != UINT64_MAX);
    // Add basic properties
    property_block_builder.AddTableProperty(rep_->props);

    // Add use collected properties
    NotifyCollectTableCollectorsOnFinish(
        rep_->table_properties_collectors, rep_->ioptions.logger,
        &property_block_builder, rep_->props.user_collected_properties,
        rep_->props.readable_properties);

    Slice block_data = property_block_builder.Finish();
    TEST_SYNC_POINT_CALLBACK(
        "BlockBasedTableBuilder::WritePropertiesBlock:BlockData", &block_data);
    WriteMaybeCompressedBlock(block_data, kNoCompression,
                              &properties_block_handle, BlockType::kProperties);
  }
  if (LIKELY(ok())) {
#ifndef NDEBUG
    {
      uint64_t props_block_offset = properties_block_handle.offset();
      uint64_t props_block_size = properties_block_handle.size();
      TEST_SYNC_POINT_CALLBACK(
          "BlockBasedTableBuilder::WritePropertiesBlock:GetPropsBlockOffset",
          &props_block_offset);
      TEST_SYNC_POINT_CALLBACK(
          "BlockBasedTableBuilder::WritePropertiesBlock:GetPropsBlockSize",
          &props_block_size);
    }
#endif  // !NDEBUG

    const std::string* properties_block_meta = &kPropertiesBlockName;
    TEST_SYNC_POINT_CALLBACK(
        "BlockBasedTableBuilder::WritePropertiesBlock:Meta",
        &properties_block_meta);
    meta_index_builder->Add(*properties_block_meta, properties_block_handle);
  }
}

void BlockBasedTableBuilder::WriteCompressionDictBlock(
    MetaIndexBuilder* meta_index_builder) {
  Slice compression_dict;
  if (rep_->compressor_with_dict) {
    compression_dict = rep_->compressor_with_dict->GetSerializedDict();
  }
  if (!compression_dict.empty()) {
    BlockHandle compression_dict_block_handle;
    if (LIKELY(ok())) {
      WriteMaybeCompressedBlock(compression_dict, kNoCompression,
                                &compression_dict_block_handle,
                                BlockType::kCompressionDictionary);
      TEST_SYNC_POINT_CALLBACK(
          "BlockBasedTableBuilder::WriteCompressionDictBlock:RawDict",
          &compression_dict);
    }
    if (LIKELY(ok())) {
      meta_index_builder->Add(kCompressionDictBlockName,
                              compression_dict_block_handle);
    }
  }
}

void BlockBasedTableBuilder::WriteRangeDelBlock(
    MetaIndexBuilder* meta_index_builder) {
  if (LIKELY(ok()) && !rep_->range_del_block.empty()) {
    BlockHandle range_del_block_handle;
    WriteMaybeCompressedBlock(rep_->range_del_block.Finish(), kNoCompression,
                              &range_del_block_handle,
                              BlockType::kRangeDeletion);
    meta_index_builder->Add(kRangeDelBlockName, range_del_block_handle);
  }
}

void BlockBasedTableBuilder::WriteFooter(BlockHandle& metaindex_block_handle,
                                         BlockHandle& index_block_handle) {
  assert(LIKELY(ok()));
  Rep* r = rep_.get();
  // this is guaranteed by BlockBasedTableBuilder's constructor
  assert(r->table_options.checksum == kCRC32c ||
         r->table_options.format_version != 0);
  FooterBuilder footer;
  Status s = footer.Build(kBlockBasedTableMagicNumber,
                          r->table_options.format_version, r->get_offset(),
                          r->table_options.checksum, metaindex_block_handle,
                          index_block_handle, r->base_context_checksum);
  if (!s.ok()) {
    r->SetStatus(s);
    return;
  }
  IOOptions io_options;
  IOStatus ios =
      WritableFileWriter::PrepareIOOptions(r->write_options, io_options);
  if (!ios.ok()) {
    r->SetIOStatus(ios);
    return;
  }
  ios = r->file->Append(io_options, footer.GetSlice());
  if (ios.ok()) {
    r->pre_compression_size += footer.GetSlice().size();
    r->set_offset(r->get_offset() + footer.GetSlice().size());
  } else {
    r->SetIOStatus(ios);
  }
}

void BlockBasedTableBuilder::MaybeEnterUnbuffered(
    const Slice* first_key_in_next_block) {
  Rep* r = rep_.get();
  assert(r->state == Rep::State::kBuffered);
  // Don't yet enter unbuffered (early return) if none of the conditions are
  // met
  if (first_key_in_next_block != nullptr) {
    bool exceeds_buffer_limit =
        (r->buffer_limit != 0 && r->data_begin_offset > r->buffer_limit);
    if (!exceeds_buffer_limit) {
      bool exceeds_global_block_cache_limit = false;
      // Increase cache charging for the last buffered data block
      // only if the block is not going to be unbuffered immediately
      // and there exists a cache reservation manager
      if (r->compression_dict_buffer_cache_res_mgr != nullptr) {
        Status s =
            r->compression_dict_buffer_cache_res_mgr->UpdateCacheReservation(
                r->data_begin_offset);
        exceeds_global_block_cache_limit = s.IsMemoryLimit();
      }
      if (!exceeds_global_block_cache_limit) {
        return;
      }
    }
  }

  // Enter Unbuffered state
  r->state = Rep::State::kUnbuffered;
  const size_t kNumBlocksBuffered = r->data_block_buffers.size();
  if (kNumBlocksBuffered == 0) {
    // The below code is neither safe nor necessary for handling zero data
    // blocks.
    // For PostPopulateCompressionProperties()
    r->data_block_compressor = r->basic_compressor.get();
    return;
  }

  // Abstract algebra teaches us that a finite cyclic group (such as the
  // additive group of integers modulo N) can be generated by a number that is
  // coprime with N. Since N is variable (number of buffered data blocks), we
  // must then pick a prime number in order to guarantee coprimeness with any
  // N.
  //
  // One downside of this approach is the spread will be poor when
  // `kPrimeGeneratorRemainder` is close to zero or close to
  // `kNumBlocksBuffered`.
  //
  // Picked a random number between one and one trillion and then chose the
  // next prime number greater than or equal to it.
  const uint64_t kPrimeGenerator = 545055921143ull;
  // Can avoid repeated division by just adding the remainder repeatedly.
  const size_t kPrimeGeneratorRemainder = static_cast<size_t>(
      kPrimeGenerator % static_cast<uint64_t>(kNumBlocksBuffered));
  const size_t kInitSampleIdx = kNumBlocksBuffered / 2;

  Compressor::DictSampleArgs samples;
  size_t buffer_idx = kInitSampleIdx;
  for (size_t i = 0; i < kNumBlocksBuffered &&
                     samples.sample_data.size() < r->max_dict_sample_bytes;
       ++i) {
    size_t copy_len =
        std::min(r->max_dict_sample_bytes - samples.sample_data.size(),
                 r->data_block_buffers[buffer_idx].size());
    samples.sample_data.append(r->data_block_buffers[buffer_idx], 0, copy_len);
    samples.sample_lens.emplace_back(copy_len);

    buffer_idx += kPrimeGeneratorRemainder;
    if (buffer_idx >= kNumBlocksBuffered) {
      buffer_idx -= kNumBlocksBuffered;
    }
  }

  assert(samples.sample_data.size() > 0);

  // final sample data block flushed, now we can generate dictionary
  r->compressor_with_dict = r->basic_compressor->MaybeCloneSpecialized(
      CacheEntryRole::kDataBlock, std::move(samples));

  // The compressor might opt not to use a dictionary, in which case we
  // can use the same compressor as for e.g. index blocks.
  r->data_block_compressor = r->compressor_with_dict
                                 ? r->compressor_with_dict.get()
                                 : r->basic_compressor.get();
  Slice serialized_dict = r->data_block_compressor->GetSerializedDict();
  if (r->verify_decompressor) {
    if (serialized_dict.empty()) {
      // No dictionary
      r->data_block_verify_decompressor = r->verify_decompressor.get();
    } else {
      // Get an updated dictionary-aware decompressor for verification.
      Status s = r->verify_decompressor->MaybeCloneForDict(
          serialized_dict, &r->verify_decompressor_with_dict);
      // Dictionary support must be present on the decompressor side if it's
      // on the compressor side.
      assert(r->verify_decompressor_with_dict);
      if (r->verify_decompressor_with_dict) {
        r->data_block_verify_decompressor =
            r->verify_decompressor_with_dict.get();
        assert(s.ok());
      } else {
        assert(!s.ok());
        r->SetStatus(s);
      }
    }
  }

  auto get_iterator_for_block = [&r](size_t i) {
    auto& data_block = r->data_block_buffers[i];
    assert(!data_block.empty());

    Block reader{BlockContents{data_block}};
    DataBlockIter* iter = reader.NewDataIterator(
        r->internal_comparator.user_comparator(), kDisableGlobalSequenceNumber,
        nullptr /* iter */, nullptr /* stats */,
        false /*  block_contents_pinned */, r->persist_user_defined_timestamps);

    iter->SeekToFirst();
    assert(iter->Valid());
    return std::unique_ptr<DataBlockIter>(iter);
  };

  std::unique_ptr<DataBlockIter> iter = nullptr, next_block_iter = nullptr;

  for (size_t i = 0; ok() && i < r->data_block_buffers.size(); ++i) {
    if (iter == nullptr) {
      iter = get_iterator_for_block(i);
      assert(iter != nullptr);
    };

    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      if (r->filter_builder != nullptr) {
        // NOTE: AddWithPrevKey here would only save key copying if prev is
        // pinned (iter->IsKeyPinned()), which is probably rare with delta
        // encoding. OK to go from Add() here to AddWithPrevKey() in
        // unbuffered operation.
        r->filter_builder->Add(ExtractUserKeyAndStripTimestamp(key, r->ts_sz));
      }
      r->index_builder->OnKeyAdded(key, iter->value());
    }

    Slice first_key_in_loop_next_block;
    const Slice* first_key_in_loop_next_block_ptr;
    if (i + 1 < r->data_block_buffers.size()) {
      next_block_iter = get_iterator_for_block(i + 1);
      first_key_in_loop_next_block = next_block_iter->key();
      first_key_in_loop_next_block_ptr = &first_key_in_loop_next_block;
    } else {
      first_key_in_loop_next_block_ptr = first_key_in_next_block;
    }

    auto& data_block = r->data_block_buffers[i];
    iter->SeekToLast();
    assert(iter->Valid());
    if (r->IsParallelCompressionActive()) {
      EmitBlockForParallel(data_block, iter->key(),
                           first_key_in_loop_next_block_ptr);

    } else {
      EmitBlock(data_block, iter->key(), first_key_in_loop_next_block_ptr);
    }
    std::swap(iter, next_block_iter);
  }
  r->data_block_buffers.clear();
  r->data_begin_offset = 0;
  // Release all reserved cache for data block buffers
  if (r->compression_dict_buffer_cache_res_mgr != nullptr) {
    Status s = r->compression_dict_buffer_cache_res_mgr->UpdateCacheReservation(
        r->data_begin_offset);
    s.PermitUncheckedError();
  }
}

Status BlockBasedTableBuilder::Finish() {
  Rep* r = rep_.get();
  assert(r->state != Rep::State::kClosed);
  // To make sure properties block is able to keep the accurate size of index
  // block, we will finish writing all index entries first, in Flush().
  Flush(/*first_key_in_next_block=*/nullptr);
  if (rep_->state == Rep::State::kBuffered) {
    MaybeEnterUnbuffered(nullptr);
  }
  assert(r->state == Rep::State::kUnbuffered);
  if (r->IsParallelCompressionActive()) {
    StopParallelCompression(/*abort=*/false);
  }

  r->props.tail_start_offset = r->offset.LoadRelaxed();

  // Write meta blocks, metaindex block and footer in the following order.
  //    1. [meta block: filter]
  //    2. [meta block: index]
  //    3. [meta block: compression dictionary]
  //    4. [meta block: range deletion tombstone]
  //    5. [meta block: properties]
  //    6. [metaindex block]
  //    7. Footer
  BlockHandle metaindex_block_handle, index_block_handle;
  MetaIndexBuilder meta_index_builder;
  WriteFilterBlock(&meta_index_builder);
  WriteIndexBlock(&meta_index_builder, &index_block_handle);
  WriteCompressionDictBlock(&meta_index_builder);
  WriteRangeDelBlock(&meta_index_builder);
  WritePropertiesBlock(&meta_index_builder);
  if (LIKELY(ok())) {
    // flush the meta index block
    WriteMaybeCompressedBlock(meta_index_builder.Finish(), kNoCompression,
                              &metaindex_block_handle, BlockType::kMetaIndex);
  }
  if (LIKELY(ok())) {
    WriteFooter(metaindex_block_handle, index_block_handle);
  }
  r->state = Rep::State::kClosed;
  r->tail_size = r->offset.LoadRelaxed() - r->props.tail_start_offset;

  return r->GetStatus();
}

void BlockBasedTableBuilder::Abandon() {
  assert(rep_->state != Rep::State::kClosed);
  if (rep_->IsParallelCompressionActive()) {
    StopParallelCompression(/*abort=*/true);
  }
  rep_->state = Rep::State::kClosed;
  rep_->GetIOStatus().PermitUncheckedError();
}

uint64_t BlockBasedTableBuilder::NumEntries() const {
  return rep_->props.num_entries;
}

bool BlockBasedTableBuilder::IsEmpty() const {
  return rep_->props.num_entries == 0 && rep_->props.num_range_deletions == 0;
}

uint64_t BlockBasedTableBuilder::PreCompressionSize() const {
  return rep_->pre_compression_size;
}

uint64_t BlockBasedTableBuilder::FileSize() const {
  return rep_->offset.LoadRelaxed();
}

uint64_t BlockBasedTableBuilder::EstimatedFileSize() const {
  if (rep_->IsParallelCompressionActive()) {
    // Use upper bound on "inflight" data size to estimate
    return FileSize() + rep_->pc_rep->estimated_inflight_size.LoadRelaxed();
  } else {
    return FileSize();
  }
}

uint64_t BlockBasedTableBuilder::GetTailSize() const { return rep_->tail_size; }

bool BlockBasedTableBuilder::NeedCompact() const {
  for (const auto& collector : rep_->table_properties_collectors) {
    if (collector->NeedCompact()) {
      return true;
    }
  }
  return false;
}

TableProperties BlockBasedTableBuilder::GetTableProperties() const {
  return rep_->props;
}

std::string BlockBasedTableBuilder::GetFileChecksum() const {
  if (rep_->file != nullptr) {
    return rep_->file->GetFileChecksum();
  } else {
    return kUnknownFileChecksum;
  }
}

const char* BlockBasedTableBuilder::GetFileChecksumFuncName() const {
  if (rep_->file != nullptr) {
    return rep_->file->GetFileChecksumFuncName();
  } else {
    return kUnknownFileChecksumFuncName;
  }
}
void BlockBasedTableBuilder::SetSeqnoTimeTableProperties(
    const SeqnoToTimeMapping& relevant_mapping, uint64_t oldest_ancestor_time) {
  assert(rep_->props.seqno_to_time_mapping.empty());
  relevant_mapping.EncodeTo(rep_->props.seqno_to_time_mapping);
  rep_->props.creation_time = oldest_ancestor_time;
}

const std::string BlockBasedTable::kObsoleteFilterBlockPrefix = "filter.";
const std::string BlockBasedTable::kFullFilterBlockPrefix = "fullfilter.";
const std::string BlockBasedTable::kPartitionedFilterBlockPrefix =
    "partitionedfilter.";

}  // namespace ROCKSDB_NAMESPACE
