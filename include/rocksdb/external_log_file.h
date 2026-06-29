//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/metadata.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// EXPERIMENTAL: External log files are MANIFEST-tracked, append-only byte
// streams for application data. The feature lets an application keep a
// consistent RocksDB view of DB state plus related application files without
// storing those bytes as keys or values. Applications can also read and write
// large sequential file contents efficiently through the configured FileSystem.
//
// External log files are not RocksDB WAL files: RocksDB does not replay their
// contents into memtables, assign sequence numbers to their contents, define
// application record boundaries, or interpret bytes written by the application.
//
// Unlike external SST files, an external log file is registered in the
// MANIFEST when it is created or ingested. The application-provided name is
// the public lookup key and physical path persisted in MANIFEST. In the
// default DB-relative mode, that name is resolved under the DB directory. In
// explicit external path mode, that name is the caller-provided physical path
// supplied by the application.
//
// Until an external log file is sealed, the MANIFEST records the creation
// prefix for the unsealed file. Sync() makes bytes durable in the FileSystem
// but does not update MANIFEST size or checksum metadata. Seal() records the
// final logical size and checksum metadata in the MANIFEST. Checkpoint returns
// NotSupported while any external log file is registered. BackupEngine returns
// NotSupported while any external log file is registered and BackupEngine
// cannot preserve it. DestroyDB does not delete explicit external-path files
// outside the DB directory.
//
// DeleteExternalLogFile() and DeleteExternalLogFiles() are the RocksDB APIs
// that remove external log files: they remove MANIFEST entries and delete the
// physical files, using RocksDB's rate-limited purge machinery when
// configured. For explicit external paths, obsolete-file scanning does not
// discover or delete the files indirectly; the MANIFEST entry is the
// authority. Reopen and restore require the application to recreate the same
// explicit external path names before opening the DB. DB::GetLiveFiles()
// returns NotSupported for explicit external paths because it can only report
// DB-relative names.
//
// Readers and writers returned by this API must be destroyed or closed before
// the DB and ExternalLogFileManager that created them are destroyed.
//
// Thread safety: RocksDB synchronizes internal metadata, reference counts, and
// individual writer handles. Calls on one writer handle are serialized by that
// handle for append position, checksum state, file I/O, and closed/sealed
// state, and at most one active writer is allowed for a file. Callers must
// still keep each handle alive while another thread is using it. The
// application is responsible for synchronizing higher-level lifecycle decisions
// for an external log file, such as create vs. ingest, reopen/recovery
// policy, seal, and delete. Concurrent lifecycle calls can return Busy,
// NotFound, or InvalidArgument according to the order in which RocksDB observes
// them; RocksDB does not provide an application-level ordering or
// record-boundary protocol.

enum class ExternalLogFileState {
  // The file can still be reopened for append. Its MANIFEST metadata describes
  // the creation prefix until Seal() records final metadata, not necessarily
  // the latest Sync() point or current physical file size.
  kUnsealed,

  // The file is immutable. Its final size and whole-file checksum are recorded
  // in the MANIFEST.
  kSealed,
};

enum class ExternalLogFilePathType {
  // The name is a DB-relative physical path under the DB directory. This is
  // the default for RocksDB-owned external log files.
  kDBRelativePath,

  // The name is an explicit physical path outside the DB-relative naming
  // contract. It is stored in MANIFEST as provided, and the application must
  // recreate the same path before reopening or restoring the DB. Absolute
  // names are used as-is; relative names are resolved against the DB directory
  // and may include ".." components.
  kExternalPath,
};

struct ExternalLogFileOptions {
  // Application-provided lookup name and physical path. The name identifies the
  // file in
  // ListExternalLogFiles(), OpenExternalLogFileForRead(),
  // ReopenExternalLogFile, DeleteExternalLogFile(), and
  // DeleteExternalLogFiles(). It is persisted in MANIFEST as the public lookup
  // key. When path_type is kDBRelativePath, empty names, absolute paths, empty
  // path components, "." components, ".." components, and names that collide
  // with RocksDB-managed filenames are rejected. When path_type is
  // kExternalPath, the name is an explicit physical path; absolute names are
  // used as-is, and relative names are resolved against the DB directory and
  // may include ".." components.
  std::string name;

  // How RocksDB resolves the physical file path.
  ExternalLogFilePathType path_type = ExternalLogFilePathType::kDBRelativePath;

  // Storage temperature hint for the physical file.
  Temperature temperature = Temperature::kUnknown;

  // Whether RocksDB BackupEngine should include this external log file.
  // Currently unsupported. If true, CreateExternalLogFile() returns
  // Status::NotSupported(). BackupEngine also returns Status::NotSupported()
  // while any external log file is registered and BackupEngine cannot preserve
  // it.
  bool include_in_backup = false;
};

enum class ExternalLogFileIngestionMode {
  // Copy the source file into the registered external log file destination.
  // The source file remains caller-owned.
  kCopy,

  // Move the source file into the registered external log file destination.
  // After a successful ingest, the source path no longer belongs to the caller.
  kMove,

  // Hard-link the source file into the registered external log file
  // destination. The source path remains caller-owned, but both paths refer to
  // the same bytes.
  kLink,

  // Try kLink first, and fall back to kCopy when the FileSystem cannot link.
  kLinkOrCopy,
};

struct IngestExternalLogFileOptions {
  // Ingestion is only for prebuilt, immutable external log files. A file
  // ingested with these options is registered in MANIFEST as kSealed and
  // cannot later be reopened for append with ReopenExternalLogFile().
  //
  // The caller-provided checksum identifies the final logical prefix, but it
  // does not provide the internal rolling checksum state needed to continue
  // appending safely.

  // Application-provided lookup name and destination physical path to register
  // in MANIFEST. Same naming rules as ExternalLogFileOptions::name.
  std::string name;

  // How RocksDB resolves the destination physical file path.
  ExternalLogFilePathType path_type = ExternalLogFilePathType::kDBRelativePath;

  // Existing file to ingest. RocksDB transfers this source to the destination
  // path according to ingestion_mode. If source_path already equals the
  // resolved destination path, RocksDB only validates and registers the
  // existing file. RocksDB does not interpret its bytes or infer application
  // record boundaries.
  std::string source_path;

  // Final logical size to record for the sealed file. The checksum below must
  // cover exactly [0, logical_size). If logical_size is smaller than the
  // physical source file size, the extra bytes are ignored as a soft truncate.
  uint64_t logical_size = 0;

  // Caller-supplied checksum metadata for [0, logical_size). When
  // verify_file_checksum is false, RocksDB trusts these values and stores them
  // in MANIFEST without reading the whole file. When verify_file_checksum is
  // true, RocksDB reads the logical prefix and verifies these values before
  // committing MANIFEST metadata.
  std::string file_checksum;
  std::string file_checksum_func_name;

  // If true, verify file_checksum and file_checksum_func_name before recording
  // the ingested file as sealed. If false, RocksDB still validates arguments
  // and file existence, but it does not recompute the file checksum during
  // ingest.
  bool verify_file_checksum = true;

  // How RocksDB should make the source bytes available as the registered
  // external log file destination. This mirrors the file-transfer choices of
  // SST ingestion, but
  // this API does not apply any SST, column-family, sequence-number, key-range,
  // level-placement, or snapshot-consistency semantics.
  ExternalLogFileIngestionMode ingestion_mode =
      ExternalLogFileIngestionMode::kCopy;

  // Storage temperature hint for the ingested physical file.
  Temperature temperature = Temperature::kUnknown;

  // Whether RocksDB BackupEngine should include this ingested external log
  // file. Currently unsupported. If true, IngestExternalLogFile() and
  // IngestExternalLogFiles() return Status::NotSupported(). BackupEngine also
  // returns Status::NotSupported() while any external log file is registered
  // and BackupEngine cannot preserve it.
  bool include_in_backup = false;
};

struct ExternalLogFileInfo : public FileStorageInfo {
  // Inherited FileStorageInfo::size mirrors durable_size for external log
  // files. For unsealed writer snapshots, physical_size can be larger while
  // size and durable_size remain the MANIFEST-recorded prefix.

  // Application-provided lookup name and physical path persisted in MANIFEST.
  std::string name;

  ExternalLogFilePathType path_type = ExternalLogFilePathType::kDBRelativePath;

  // Whether the file is still appendable or has been sealed.
  ExternalLogFileState state = ExternalLogFileState::kUnsealed;

  // Bytes that are part of the committed DB state. For sealed files this equals
  // size. For unsealed files, live-file APIs must only expose this prefix.
  uint64_t durable_size = 0;

  // Best-effort physical size read from the filesystem when requested by
  // ListExternalLogFilesOptions::get_physical_size. It can be larger than
  // durable_size for unsealed files.
  uint64_t physical_size = 0;
};

struct ListExternalLogFilesOptions {
  bool include_unsealed = true;
  bool include_sealed = true;

  // Requires extra filesystem I/O. Leave false when MANIFEST metadata is
  // sufficient.
  bool get_physical_size = false;
};

struct ReopenExternalLogFileOptions {
  // When true, ReopenExternalLogFile() rebuilds checksum state by reading the
  // file prefix selected below. This lets the application append more data or
  // seal the file after crash recovery. This does not update MANIFEST size or
  // checksum metadata; Seal() records the final metadata.
  //
  // When false, RocksDB reopens without rebuilding appendable checksum state.
  // A later Seal() without SealExternalLogFileOptions::recompute_checksum can
  // fail if checksum state needed for the requested logical size is
  // unavailable.
  bool recompute_checksum = false;

  // When true, the application has inspected the file and selected the byte
  // prefix it wants RocksDB to use as the current append point. This can be
  // larger than the MANIFEST-recorded prefix after crash recovery. The
  // application is responsible for its own record-boundary and partial-record
  // checks before setting recovered_size. The recovered size affects the
  // returned writer handle only until the file is sealed.
  bool has_recovered_size = false;

  // Only meaningful when has_recovered_size is true. If recovered_size is
  // smaller than the physical file size, ReopenExternalLogFile() may need
  // filesystem truncate support before appending can preserve byte-stream
  // semantics. If truncate is unsupported, callers can instead Seal() with an
  // explicit logical_size to perform a soft truncate.
  uint64_t recovered_size = 0;
};

struct SealExternalLogFileOptions {
  // When true, Seal() records logical_size as the final file size instead of
  // the writer's current append position. This is a soft truncate: RocksDB
  // records only [0, logical_size) in the MANIFEST and checkpoint/live-file
  // APIs must ignore bytes after logical_size. The physical file is not
  // required to be truncated.
  bool has_logical_size = false;

  // Only meaningful when has_logical_size is true. The final checksum recorded
  // in MANIFEST must cover exactly [0, logical_size). If RocksDB does not
  // already have checksum state for this prefix and recompute_checksum is
  // false, Seal() can fail.
  uint64_t logical_size = 0;

  // Re-scan the file and recompute the final checksum for the selected logical
  // size before writing the seal record. This is useful when sealing a soft
  // truncated prefix or when the writer was reopened after crash recovery
  // without checksum recomputation.
  bool recompute_checksum = false;
};

enum class ExternalLogFileReaderVisibility {
  // The reader is limited to the size visible when it was opened. This is the
  // safer default for readers that need a stable view. For an unsealed file
  // with an active writer, this captures the writer's completed append
  // position. For an unsealed file without an active writer, this uses the
  // MANIFEST-recorded durable prefix; applications must reopen the file to
  // recover and expose any physical tail beyond that prefix.
  kSnapshotAtOpen,

  // The reader can observe bytes appended after it was opened. It still must
  // not expose bytes from an Append() call that has not completed successfully.
  kFollowWriter,
};

struct ExternalLogFileReaderOptions {
  ExternalLogFileReaderVisibility visibility =
      ExternalLogFileReaderVisibility::kSnapshotAtOpen;
};

struct VerifyExternalLogFileChecksumOptions {
  // Verify unsealed files against the last prefix recorded in MANIFEST. Sealed
  // files are always verified against their final logical size.
  bool include_unsealed = true;

  // When true, return an error if the selected file lacks checksum metadata.
  // When false, files with unavailable checksum metadata are skipped.
  bool fail_if_checksum_unavailable = true;
};

struct ExternalLogFileChecksumInfo {
  ExternalLogFileInfo file_info;
  // True when RocksDB recomputed and compared checksum metadata. False when
  // verification was skipped because checksum metadata or a checksum factory
  // was unavailable and fail_if_checksum_unavailable was false.
  bool verified = false;
  uint64_t verified_size = 0;
  std::string expected_checksum;
  std::string actual_checksum;
  std::string checksum_func_name;
};

class ExternalLogFileReader {
 public:
  virtual ~ExternalLogFileReader() = default;

  // Returns the name/path of the file this reader was opened for.
  virtual const std::string& Name() const = 0;

  // Reads bytes from the reader's visible prefix. The application owns record
  // boundaries and partial-record interpretation. For a snapshot reader, the
  // visible prefix is fixed when the reader is opened. For a follow-writer
  // reader, the visible prefix can grow after successful appends.
  virtual Status Read(const ReadOptions& options, uint64_t offset, size_t n,
                      Slice* result, char* scratch) = 0;

  // Returns the maximum offset currently visible to this reader. For a
  // follow-writer reader this value can increase over time.
  virtual uint64_t VisibleSize() const = 0;

  // Returns the metadata snapshot associated with this reader.
  virtual Status GetFileInfo(ExternalLogFileInfo* info) const = 0;
};

class ExternalLogFileWriter {
 public:
  virtual ~ExternalLogFileWriter() = default;

  // Returns the name/path of the file this writer was opened for.
  virtual const std::string& Name() const = 0;

  // Appends bytes to the external log file. The write goes through RocksDB's
  // FileSystem layer. If offset is non-null, it is populated with the starting
  // offset of the appended bytes.
  //
  // Mutating calls on the same writer handle are serialized by the handle.
  // Readers created through NewReader() or opened through
  // OpenExternalLogFileForRead() can read concurrently with appends.
  // Concurrent readers must not observe bytes from an Append() call that has
  // not completed successfully.
  virtual Status Append(const WriteOptions& options, const Slice& data,
                        uint64_t* offset = nullptr) = 0;

  // Flushes writer buffers. Flush can make appended bytes visible to
  // concurrent readers in this process, but it does not by itself make the
  // prefix durable or update MANIFEST metadata.
  virtual Status Flush(const WriteOptions& options) = 0;

  // Flushes and syncs the current file contents through RocksDB's FileSystem
  // layer. This does not update MANIFEST size or checksum metadata, and
  // therefore does not advance the prefix used by crash recovery, checkpoint,
  // or live-file enumeration. If info is non-null, it receives the current
  // MANIFEST metadata snapshot with physical_size set to the writer's append
  // position. Sync() is serialized with Append(), Seal(), and Close() on this
  // writer handle.
  virtual Status Sync(const WriteOptions& options,
                      ExternalLogFileInfo* info = nullptr) = 0;

  // Finalizes the file. Seal() closes the writer, records final size and
  // checksum metadata in the MANIFEST, and makes the file immutable. A file
  // reopened after crash recovery can be sealed again by the application.
  virtual Status Seal(const WriteOptions& options,
                      const SealExternalLogFileOptions& seal_options,
                      ExternalLogFileInfo* info = nullptr) = 0;

  Status Seal(const WriteOptions& options,
              ExternalLogFileInfo* info = nullptr) {
    return Seal(options, SealExternalLogFileOptions(), info);
  }

  // Closes this handle without sealing. The file remains MANIFEST-tracked and
  // can be reopened later while it is still unsealed.
  virtual Status Close(const WriteOptions& options) = 0;

  // Opens a reader over this file. The reader can be used concurrently with
  // later appends on this writer. Snapshot-at-open readers are limited to the
  // size visible when the reader was created; follow-writer readers can observe
  // later successful appends.
  virtual Status NewReader(const ExternalLogFileReaderOptions& options,
                           std::unique_ptr<ExternalLogFileReader>* reader) = 0;

  Status NewReader(std::unique_ptr<ExternalLogFileReader>* reader) {
    return NewReader(ExternalLogFileReaderOptions(), reader);
  }

  virtual Status GetFileInfo(ExternalLogFileInfo* info) const = 0;
};

class ExternalLogFileManager {
 public:
  virtual ~ExternalLogFileManager() = default;

  // Returns MANIFEST-tracked external log files after DB open or crash
  // recovery. RocksDB does not automatically recompute checksums, truncate
  // tails, or seal unsealed files during recovery; applications inspect this
  // list and explicitly reopen unsealed files as needed.
  virtual Status ListExternalLogFiles(
      const ListExternalLogFilesOptions& options,
      std::vector<ExternalLogFileInfo>* files) = 0;

  // Creates a new RocksDB-managed external log file, registers it in the
  // MANIFEST, and returns an appendable writer. If the process crashes after
  // this call returns OK, the file appears as kUnsealed in
  // ListExternalLogFiles().
  virtual Status CreateExternalLogFile(
      const ExternalLogFileOptions& options,
      std::unique_ptr<ExternalLogFileWriter>* writer) = 0;

  // Registers an existing byte-stream file as a sealed external log file. This
  // is the external-log bulk-load path. It records the name/path, final logical
  // size, and checksum metadata in the MANIFEST atomically with making the file
  // live. The resulting file is immutable and should be opened with
  // OpenExternalLogFileForRead(). It must not be reopened for writing:
  // ReopenExternalLogFile() only applies to files created through
  // CreateExternalLogFile() that are still kUnsealed.
  //
  // This API is separate from DB::IngestExternalFile(), which ingests external
  // SST files into a column family and applies LSM-specific semantics.
  virtual Status IngestExternalLogFile(
      const IngestExternalLogFileOptions& options,
      ExternalLogFileInfo* info = nullptr) = 0;

  // Atomically registers multiple existing byte-stream files as sealed
  // external log files. If this returns OK, all files are visible through the
  // MANIFEST. If it returns an error or the process crashes, recovery must not
  // expose a partial subset as ingested. When infos is non-null, it is filled
  // with one entry per input option on success.
  virtual Status IngestExternalLogFiles(
      const std::vector<IngestExternalLogFileOptions>& options,
      std::vector<ExternalLogFileInfo>* infos = nullptr) = 0;

  // Reopens an unsealed file after normal close or crash recovery. The caller
  // controls whether RocksDB reads the file to recompute checksum state.
  // Reopening initializes the returned writer's append point but does not
  // update MANIFEST size or checksum metadata; Seal() records final metadata.
  // Sealed files are immutable and should be opened with
  // OpenExternalLogFileForRead().
  virtual Status ReopenExternalLogFile(
      const std::string& name, const ReopenExternalLogFileOptions& options,
      std::unique_ptr<ExternalLogFileWriter>* writer) = 0;

  // Opens a tracked file for read. Sealed files can be read completely.
  // Unsealed files expose a byte prefix according to the reader visibility
  // options.
  virtual Status OpenExternalLogFileForRead(
      const ReadOptions& read_options, const std::string& name,
      const ExternalLogFileReaderOptions& reader_options,
      std::unique_ptr<ExternalLogFileReader>* reader) = 0;

  Status OpenExternalLogFileForRead(
      const ReadOptions& read_options, const std::string& name,
      std::unique_ptr<ExternalLogFileReader>* reader) {
    return OpenExternalLogFileForRead(read_options, name,
                                      ExternalLogFileReaderOptions(), reader);
  }

  // Verifies a tracked external log file against checksum metadata in the
  // MANIFEST. Sealed files are verified over their final logical size. Unsealed
  // files are verified over their MANIFEST-recorded prefix, which remains the
  // creation prefix until seal, when verify_options.include_unsealed is true.
  // If checksum metadata exists but this DB was opened without
  // options.file_checksum_gen_factory, verification fails unless
  // fail_if_checksum_unavailable is false. This API does not update MANIFEST
  // metadata.
  virtual Status VerifyExternalLogFileChecksum(
      const ReadOptions& read_options, const std::string& name,
      const VerifyExternalLogFileChecksumOptions& verify_options,
      ExternalLogFileChecksumInfo* info = nullptr) = 0;

  // Verifies all tracked external log files selected by verify_options.
  virtual Status VerifyExternalLogFileChecksums(
      const ReadOptions& read_options,
      const VerifyExternalLogFileChecksumOptions& verify_options,
      std::vector<ExternalLogFileChecksumInfo>* infos = nullptr) = 0;

  // Removes a tracked external log file from MANIFEST and deletes the physical
  // path named by the MANIFEST entry. Returns Busy if RocksDB still has an
  // active reader, writer, verifier, or physical-size listing reference.
  virtual Status DeleteExternalLogFile(const WriteOptions& options,
                                       const std::string& name) = 0;

  // Removes tracked external log files from MANIFEST and deletes the physical
  // paths named by the MANIFEST entries. The RocksDB implementation commits
  // the MANIFEST removals with one edit. Returns Busy without changing
  // MANIFEST if any requested file has an active reader, writer, verifier, or
  // physical-size listing reference. After a successful MANIFEST edit,
  // RocksDB attempts to delete every physical file; NotFound physical delete
  // results are ignored.
  virtual Status DeleteExternalLogFiles(
      const WriteOptions& options, const std::vector<std::string>& names) = 0;

  Status DeleteExternalLogFiles(const std::vector<std::string>& names) {
    return DeleteExternalLogFiles(WriteOptions(), names);
  }

  Status DeleteExternalLogFile(const std::string& name) {
    return DeleteExternalLogFile(WriteOptions(), name);
  }
};

}  // namespace ROCKSDB_NAMESPACE
