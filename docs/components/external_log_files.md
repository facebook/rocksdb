# External Log Files

External log files are experimental MANIFEST-tracked byte streams for
application data. They let an application maintain a consistent view of RocksDB
state plus related application files without storing those bytes as keys or
values. They also provide efficient sequential file read and write through the
configured RocksDB `FileSystem`.

These files are not RocksDB WALs. RocksDB does not replay their contents into
memtables, assign sequence numbers to their bytes, or define application record
boundaries. Applications own their record format and recovery policy.

## Paths

Each external log file has an application-provided name persisted in the
MANIFEST. The name is both the public lookup key used by
`ListExternalLogFiles()`, `OpenExternalLogFileForRead()`,
`ReopenExternalLogFile()`, `DeleteExternalLogFile()`, and
`DeleteExternalLogFiles()` and the physical path RocksDB uses for file I/O.

In the default `kDBRelativePath` mode, the name is a physical path relative to
the DB directory. Absolute names, empty path components, `.`, `..`, and names
that collide with RocksDB-managed filenames are rejected.

In `kExternalPath` mode, the name is an explicit physical path. Absolute names
are used as-is. Relative names are resolved against the DB directory and may
include `..` components. This keeps outside-DB tracking explicit through
`path_type` without requiring a second application-provided path string. RocksDB
stores the name in the MANIFEST as provided, and the application must recreate
the same external path names before reopening or restoring the DB.

## Lifecycle

`CreateExternalLogFile()` creates an unsealed file at the resolved physical
path and records it in the MANIFEST. The returned writer appends bytes through
the configured `FileSystem`. `Sync()` flushes and syncs the physical file but
does not update the MANIFEST prefix. `Seal()` syncs the file, records the final
logical size and checksum in the MANIFEST, and makes the file immutable.
Reopen recovery does not persist a new MANIFEST prefix; it only initializes the
returned writer's append point.

Unsealed files can be reopened after normal close or crash recovery with
`ReopenExternalLogFile()`. If the physical file is longer than the
MANIFEST-recorded durable prefix, the application must choose the recovered
prefix. RocksDB can recompute checksum state for the returned writer when a
checksum factory is configured; otherwise checksum recomputation and
verification fail rather than silently replacing existing checksum metadata.

`IngestExternalLogFile()` and `IngestExternalLogFiles()` register prebuilt
files as sealed external log files. If the source path already equals the
resolved destination path, RocksDB validates and registers the existing file
without renaming or copying it. Otherwise, ingest copies, moves, or links the
source to the destination path according to the ingestion mode. Multi-file
ingest commits all MANIFEST additions atomically after the physical transfers
complete; failed transfers are rolled back while the reserved file numbers
remain protected from obsolete-file cleanup.

`DeleteExternalLogFile()` removes one MANIFEST entry and deletes the physical
file. `DeleteExternalLogFiles()` removes multiple MANIFEST entries in one edit
and then deletes the physical files. Both APIs return `Busy` without changing
the MANIFEST if any requested file has an active reader, writer, verifier, or
physical-size listing reference. They are the only RocksDB APIs that delete
explicit external-path files. Obsolete-file scanning does not discover or
delete outside-DB external log files indirectly; the MANIFEST entry is the
authority. The delete path uses RocksDB's rate-limited purge machinery when
configured, and the directory-to-sync/trash behavior uses each physical file's
parent directory. After a successful MANIFEST edit, RocksDB attempts every
requested physical delete and ignores `NotFound` results.

## Checkpoint, Backup, And Destroy

Checkpoint returns `NotSupported` while any external log file is registered.
It does not silently create a checkpoint with missing external log files.

BackupEngine returns `NotSupported` while any external log file is registered
and BackupEngine cannot preserve it. Applications using BackupEngine must
preserve and restore the external files separately, then recreate the same
external paths before opening the restored DB.

`DestroyDB()` does not delete explicit external-path files outside the DB
directory. Use `DeleteExternalLogFile()` or application-specific cleanup to
remove them.

`DB::GetLiveFiles()` returns `NotSupported` when explicit external-path files
are registered because that API returns DB-relative names. Use
`DB::GetLiveFilesStorageInfo()` when code needs file directory and filename
metadata.

## Visibility

For sealed files, readers see the final logical prefix. For unsealed files with
an active writer, snapshot readers capture the writer's completed append
position at open time and follow readers can observe later successful appends.
For unsealed files without an active writer, readers expose the
MANIFEST-recorded durable prefix, which remains the creation prefix until seal.
Applications must reopen the file to recover any longer physical tail and seal
it to make that prefix part of committed MANIFEST metadata.

## Synchronization

RocksDB synchronizes internal metadata, reference counts, and individual writer
handles. Calls on one writer handle are serialized for append position,
checksum state, file I/O, and closed/sealed state. RocksDB also permits at most
one active writer for a file.

Applications are still responsible for synchronizing higher-level lifecycle
decisions for an external log file, such as create vs. ingest,
reopen/recovery policy, seal, and delete. Concurrent lifecycle calls can return
`Busy`, `NotFound`, or `InvalidArgument` according to the order in which
RocksDB observes them; RocksDB does not provide an application-level ordering
or record-boundary protocol.
