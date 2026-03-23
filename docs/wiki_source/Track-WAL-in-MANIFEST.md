We recommend to set [track_and_verify_wals_in_manifest](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/options.h#L493) to `true` for production, it has been enabled in production for the entire database cluster serving the social graph for all Meta apps.

The following is the design doc for the feature. The problem statement and design overview can help understand the background and usefulness of the feature. The design discussions on the internal implementation can help understand the tradeoffs of this feature, as well as shed light on the overall journaling code paths.

# Problem

Currently, on database recovery, RocksDB determines the alive on-disk content by replaying MANIFEST because the addition and deletion of SST and blob files are tracked in MANIFEST. But to recover the in-memory unflushed database content, RocksDB relies on listing the on-disk WALs and replay them in sequence. If a WAL is missing or corrupted, RocksDB has no way of knowing this, and will silently recover with corrupted content.

## RocksDB on Cloud Storage

When running RocksDB on Cloud Storage, customers may have a background job to asynchronously sync local WALs to the remote Cloud Storage. When the current DB is reopened, or a second instance or backup is opened, the WALs may not have been synced completely, or the WALs are not synced in increasing order of log number, without the ability to check whether there is missing or corrupted WAL, the recovered DB may be corrupted.

## Separated WAL directory

Some use cases separate WAL directory from DB directory, operations on the WAL directory may corrupt WALs by mistake. Tracking the WALs in MANIFEST can let RocksDB protect against those operational mistakes.

## Missing WALs during recovery

If the WAL file numbers are monotonically increasing by 1, we can rely on the numbers to detect missing WALs. But WAL and MANIFEST share the same space of file numbers, although the WAL file numbers are increasing, there can be gaps between them, so we cannot rely on WAL file numbers to detect missing WALs.

## Corrupted WALs during recovery

When replaying WALs during recovery, if we read a corrupted entry, currently, we cannot be sure whether the WAL is corrupted even after syncing, or it’s because the entry is not synced to disk. If we track WAL’s last synced size (and even the checksum on WAL closing) in MANIFEST, then if the corrupted entry is at a position of the WAL where it should have been synced, then we know that the WAL itself is corrupted. See https://github.com/facebook/rocksdb/issues/6288 for an example.

# Goal

Let MANIFEST not only track SST and blob files, but also track WALs. The meaning of tracking WALs is to track the lifecycle events (such as creation, closing, and becoming obsolete) and metadata (such as log number, size, checksum) of a WAL so that we can determine the alive WALs by replaying MANIFEST on recovery.

# Design

## Overview

Each WAL related event is added to a VersionEdit, the VersionEdit is persisted to MANIFEST through VersionSet::LogAndApply. On recovery, the alive WALs are first recovered from MANIFEST, then comparing them with the on-disk WALs to check whether there are missing or corrupted WALs. The following sections discuss the design details and tradeoffs.

## User interface

1. An option “track_and_verify_wals_in_manifest” is provided. When it’s true, WALs are tracked in MANIFEST and verified according to the above algorithm on recovery.
2. If the option is enabled, and later on disabled when reopening DB, the existing tracked WALs are deleted from MANIFEST on recovery.

## Backward compatibility

If an existing DB upgrades to a new version of RocksDB, and enables this feature, the old WALs are not tracked and won’t be verified.

## WAL verification on recovery

1. Recover the tracked information of WALs from MANIFEST and store them in VersionSet.
2. Get the set of WALs from VersionSet.
3. Iterate through the WALs in increasing order of log number, for each of them
    1. if the WAL is not synced, skip checking.
    2. Otherwise, if the WAL is missing on disk, or the WAL’s size is less than the last synced size, report an error.

## WAL Syncing

By default, RocksDB does not sync WALs for every write. This has the following implications:

1. if WAL directory is never synced, then during recovery, we cannot force the check of existence of the WAL on disk because the WAL’s inode metadata may not be persisted to disk yet when the machine dies.
2. if WAL is not synced after being closed, we cannot force the check of WAL size on recovery because there might be data asynchronously synced to disk after closing.

There are several ways to sync WALs:

1. users can call DB::SyncWAL, which will sync all the alive WALs
2. users can call DB::FlushWAL(true), which calls SyncWAL internally
3. users can call DB::Write with write_options.sync = true, which calls SyncWAL internally
4. if there are multiple column families, when flushing the memtable, the closed WALs (all the alive WALs except the current one) are synced

Whenever a WAL is synced, if there are WALs in the WAL directory that haven’t been synced before, the WAL directory will be fsynced.

There are 3 options to track the WAL syncing event in MANIFEST:

1. We’ll track the current WAL size whenever the WAL is synced. On recovery, we’ll only verify that the WAL’s on-disk size is no less than the last synced size.
    1. Pros: we know exactly to which point has the WAL been synced
    2. Cons:
        1. writing to MANIFEST for every synced write can make the write slower
        2. there might be too many WAL related records in MANIFEST if there are a lot of synced writes
2. Do not track every WAL syncing event, just track whether the WAL is ever synced. Ideally, if a WAL is synced when it’s closed, we can track the WAL’s synced size on closing. Currently, for multi-column family DBs, WALs are always synced on closing, but for single column family DBs, this is not the case.
    1. Pros:
        1. if a WAL is synced, the WAL must exist on disk, if not, it means there is a missing WAL
        2. won’t cause slower writes or overloaded records in MANIFEST
    2. Cons: may not be sure what’s the synced size of the WAL
3. When a WAL is synced, keep the synced size in memory (such as in a field of log::Writer). When a new WAL is created, write the last WAL’s last synced size to MANIFEST. When either SyncWAL, or FlushWAL(true), or write with options.sync = true, or SyncClosedWALs is called, all the alive WALs that are not fully synced will be fully synced, and these fully synced WALs’ sizes are written to MANIFEST.
    1. Pros:
        1. For WALs other than the current one, the last synced size is tracked, and it avoids the Cons in option 1.
    2. Cons:
        1. For the currently active WAL, its synced size is not written to MANIFEST.

Decision: option 3 is the best tradeoff for now.

## WAL recycling

When a WAL is recycled, it’s treated as the WAL is logically deleted, so a WAL deletion event will be written to MANIFEST.
When a recycled WAL is reused, the WAL file is renamed with a new file number, and a WAL creation event will be written to MANIFEST.
So from the perspective of MANIFEST, recycled WALs are treated in the same way as non-recycled WALs.

## WAL lifecycle events to track

 The lifecycle of a WAL is:

1. creation: a new empty WAL file is created on disk;
2. closing: a WAL file is completed and closed;
3. obsoletion:
    1. when column family data in a WAL have all been flushed to disk, the WAL is obsolete, which means it’s no longer needed during DB recovery. A background thread will asynchronously delete obsolete WALs from disk.
    2. during DB recovery, after replaying WALs, if the flush scheduler determines that a column family should be flushed, then the flush happens synchronously in WriteLevel0TableForRecovery, instead of happening through a background job.

Tracking WAL creation is straightforward. On creation, write the log number to MANIFEST to indicate that a new WAL is created.

There are two cases when tracking the WAL closing event:

1. if the WAL is closed and synced, write the log number and some integrity information of the WAL (such as size, checksum) to MANIFEST to indicate that the WAL is completed.
2. if the WAL is closed but not synced, then do not write the size or checksum of the WAL to MANIFEST. In this case, from the MANIFEST’s perspective, it’s the same as WAL creation, because on recovery, we only know the log number of the WAL, but not its size or checksum.


But tracking WAL obsoletion is not that obvious. There are several questions to consider:

Q1: Can we rely on min_log_number to determine obsolete WALs and don’t track WAL obsoletion events at all?

When a memtable is flushed, a VersionEdit is written to MANIFEST, recording a min_log_number, WALs with log number < min_log_number are no longer needed for the column family of the memtable. So on recovery, we can be sure that the WALs with the log number < min(min_log_number of all column families) are obsolete and can be ignored. So it seems like we don’t even need to track the WAL obsoletion event and just rely on the min_log_number. 

But the min_log_number is actually a lower bound, it does not exactly mean that logs starting from min_log_number must exist. This is because in a corner case, when a column family is empty and never flushed, its log number is set to the largest log number, but not persisted in MANIFEST. So let's say there are 2 column families, when creating the DB, the first WAL has log number 1, so it's persisted to MANIFEST for both column families. Then CF 0 is empty and never flushed, CF 1 is updated and flushed, so a new WAL with log number 2 is created and persisted to MANIFEST for CF 1. But CF 0's log number in MANIFEST is still 1. So on recovery, min_log_number is 1, but since log 1 only contains data for CF 1, and CF 1 is flushed, log 1 might have already been deleted from disk.

There are 2 options:

1. track WAL obsoletion event, so that the WAL related VersionEdits in MANIFEST can be the source of truth of which WALs are alive;
    1. Pros
        1. the WAL related VersionEdits in MANIFEST can be the source of truth of which WALs are alive.
    2. Cons
        1. need to design a new set of VersionEdits for the WAL obsoletion event.
2. make min_log_number be the exactly minimum log number that must be alive, by persisting the most recent log number for empty column families that are not flushed. 
    1. Pros
        1. no need for new VersionEdits for the WAL obsoletion event.
    2. Cons
        1. if there are N such column families, then every time a new WAL is created, we need to write N VersionEdits to MANIFEST to record the current log number as the min_log_number for the empty column families.

Decision: option 1 is chosen, because:

1. for each WAL, only 3 VersionEdits (creations, closing, obsoletion) is written to MANIFEST. While with option 2, if there are N empty column families, when a WAL is created, N VersionEdits are written to MANIFEST. So option 1 saves MANIFEST’s disk usage.
2. tracking the full lifecycle of WALs in MANIFEST enables more potential features. For example, we can do consistency checks such as a WAL must be closed before being obsolete; We can even add more information to the WAL obsoletion VersionEdit such as when the WAL becomes obsolete.
3. the min_log_number thing assumes WAL log numbers are in the increasing order, not sure whether this assumption might break in the future.


Q2: When should we track the obsoletion event?

A WAL is logically obsolete once the column family data in the WAL have all been flushed to disk. Since the WAL is asynchronously deleted from the disk in a background thread, the logical obsoletion of a WAL is earlier than the physical deletion. When the DB crashes or is closed, an obsolete WAL might still be on the disk.

So there are 2 options:

1. track the event when the WAL is physically deleted from disk.
    1. Cons
        1. there will be no source of truth for the logically alive WALs.
        2. consistency issues: if we write the VersionEdit before actually deleting the WAL, if DB crashes after writing the VersionEdit but before deleting the WAL, then on recovery, we’ll see a WAL on disk that shouldn’t exist according to the MANIFEST, should we report error or silently delete and ignore it? If we write the VersionEdit after actually deleting the WAL, and DB crashes in the middle, then there will never be a VersionEdit stating that the WAL is deleted.
2. track the event when the WAL is logically obsolete.
    1. Pros
        1. When there is no Version referring to a SST, it becomes obsolete and gets deleted from disk asynchronously. During compaction, a logical SST file deletion event is written to MANIFEST. So tracking logical obsoletion of WALs is consistent with what we do for SST files.
        2. MANIFEST has the source of truth for the logically alive WALs.
        3. the VersionEdit can be written together with the existing VersionEdits written after flushing memtable.

Decision: option 2 is chosen.

## Design of WAL related VersionEdits

Two new kinds of edits WalAddition and WalDeletion are added to VersionEdits.

An alternative design is WalCreation, WalClosing, and WalDeletion. The above design combines WalCreation and WalClosing together as WalAddition by making the WAL metadata such as size and checksum optional in WalAdditions, so when creating a new WAL, just create a WalAddition with no size/checksum. This is to make the VersionEdit and the logic for processing the WAL related VersionEdits simpler.

A VersionEdit can either contain WalAddition or WalDeletion, but not both, and cannot mix with other kinds of VersionEdits. It’s because WAL related VersionEdits need special handling in VersionSet::LogAndApply, mixing them with other VersionEdits make the internal logic of LogAndApply more complex (see the next section). This is similar to VersionEdits related to column family creation and drop, which are also unique VersionEdits and need special handling in LogAndApply.

## Special handling of WAL related VersionEdits in LogAndApply

Each column family keeps a list of Versions, each Version represents a state of the DB, and refers to the set of alive SST and blob files when the Version is created.

Version = VersionBuilder.build(Version, VersionEdits): a set of VersionEdits are applied to the current Version through VersionBuilder to create a new Version.

Each SST file corresponds to a column family, so the column family’s Versions track SST files' addition and deletion. But WAL related VersionEdits are special, WALs are not bound to any column family, all the column families share the same set of WALs. 

So there are two options to maintain the set of alive WALs:

1. maintain them in Versions of the default column family.
    1. Pros
        1. there is no need to modify the logic of LogAndApply for special handling of WAL related VersionEdits.
    2. Cons
        1. DB ID is persisted in MANIFEST in this way, but DB ID is written to MANIFEST only once when opening the DB, so it only creates one additional Version in default column family’s Version list. When tracking alive WALs, every time a WAL is created, closed, or becomes obsolete, a new Version is created and added to the default column family’s Version list, and the only difference of the Version is the addition/deletion of a WAL that is not specific to the default column family. This is a waste of memory, for example, MyRocks encountered a situation where there are more than 1300 Versions and each Version holds 58k SST files.
2. maintain them as a standalone data structure in VersionSet.
    1. Pros
        1. saves memory comparing to option 1.
    2. Cons
        1. need special logic in LogAndApply for WAL related VersionEdits.

Decision: option 2 is chosen, because we want to avoid Cons of option 1. Also, the special logic for handling WAL related VersionEdits in LogAndApply is not that complex, they can be handled in a similar way as VersionEdits related to column family creation and drop are handled: no support for group commit, no application to Versions, and no mixture of different types of VersionEdits.
