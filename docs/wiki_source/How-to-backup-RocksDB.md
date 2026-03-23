### Backup API

`BackupEngine` is an object managing a directory of backed-up DBs, with functions to create, restore, delete, and inspect backup DBs. The on-disk format is specific to BackupEngine so that (a) files can be shared between backups even without hard links, and (b) extra metadata can be associated with backups, for data integrity and other purposes. The C++ API is in `include/rocksdb/utilities/backup_engine.h`.

A key feature of BackupEngine is taking backups from one `FileSystem` abstraction to another, such as from a local filesystem to a remote filesystem that is not provided through an OS abstraction. Writes are bound by a configurable `RateLimiter` and can use many threads. Checksums are computed for added data integrity, with the best data integrity provided when the DB uses whole file checksums with `file_checksum_gen_factory`.

BackupEngine is thread-safe (though hasn't always been so), though complex safety rules apply if accessing a backup directory from more than one BackupEngine object simultaneously. (See the API documentation.) `BackupEngineReadOnly` provides a safer abstraction when writing is not intended.

### Creating and verifying a backup

In RocksDB, we have implemented an easy way to backup your DB and verify correctness. Here is a simple example:
 
```cpp
    #include "rocksdb/db.h"
    #include "rocksdb/utilities/backup_engine.h"

    #include <vector>

    using namespace rocksdb;

    int main() {
        Options options;                                                                                  
        options.create_if_missing = true;                                                                 
        DB* db;
        Status s = DB::Open(options, "/tmp/rocksdb", &db);
        assert(s.ok());
        db->Put(...); // do your thing

        BackupEngine* backup_engine;
        s = BackupEngine::Open(Env::Default(), BackupEngineOptions("/tmp/rocksdb_backup"), &backup_engine);
        assert(s.ok());
        s = backup_engine->CreateNewBackup(db);
        assert(s.ok());
        db->Put(...); // make some more changes
        s = backup_engine->CreateNewBackup(db);
        assert(s.ok());

        std::vector<BackupInfo> backup_info;
        backup_engine->GetBackupInfo(&backup_info);

        // you can get IDs from backup_info if there are more than two
        s = backup_engine->VerifyBackup(1 /* ID */);
        assert(s.ok());
        s = backup_engine->VerifyBackup(2 /* ID */);
        assert(s.ok());
        delete db;
        delete backup_engine;
    }
```

This simple example will create a couple backups in "/tmp/rocksdb_backup". Note that you can create and verify multiple backups using the same engine.

Backups are normally incremental (see `BackupEngineOptions::share_table_files`). You can create a new backup with `BackupEngine::CreateNewBackup()` and only the new data will be copied to backup directory (for more details on what gets copied, see [Under the hood](https://github.com/facebook/rocksdb/wiki/How-to-backup-RocksDB#under-the-hood)).

Once you have some backups saved, you can issue `BackupEngine::GetBackupInfo()` call to get a list of all backups together with information on timestamp and logical size of each backup. File details are optionally returned, from which sharing details can be determined. `GetBackupInfo()` even provides a way to open a backup in-place as a read-only DB, which could be useful for inspecting the exact state, etc. Backups are identified by simple increasing integer IDs, which can be saved in an output parameter when creating a new backup or taken from `GetBackupInfo()`.

When `BackupEngine::VerifyBackup()` is called, it checks the file sizes in the backup directory against the expected sizes recorded from the original DB directory. Checksum verification is optional but requires reading all the data. In either case, the purpose is to check for some sort of quiet failure during backup creation or accidental corruption afterward. `BackupEngine::Open()` essentially does a `VerifyBackup()`, without checksums, on each backup to determine whether to classify it as corrupt.

### Restoring a backup

Restoring is also easy:

```cpp
    #include "rocksdb/db.h"
    #include "rocksdb/utilities/backup_engine.h"

    using namespace rocksdb;

    int main() {
        BackupEngineReadOnly* backup_engine;
        Status s = BackupEngineReadOnly::Open(Env::Default(), BackupEngineOptions("/tmp/rocksdb_backup"), &backup_engine);
        assert(s.ok());
        s = backup_engine->RestoreDBFromBackup(1, "/tmp/rocksdb", "/tmp/rocksdb");
        assert(s.ok());
        delete backup_engine;
    }
```

This code will restore the first backup back to "/tmp/rocksdb". The first parameter of `BackupEngineReadOnly::RestoreDBFromBackup()` is the backup ID, second is target DB directory, and third is the target location of WAL files (in some DBs they are different from DB directory, but usually they are the same. See Options::wal_dir for more info). `BackupEngineReadOnly::RestoreDBFromLatestBackup()` will restore the DB from the latest backup, i.e., the one with the highest ID.

Checksum is calculated for any restored file and compared against the one stored during the backup time. If a checksum mismatch is detected, the restore process is aborted and `Status::Corruption` is returned.

### Backup directory structure

```
/tmp/rocksdb_backup/
├── meta
│   └── 1
├── private
│   └── 1
│       ├── CURRENT
│       ├── MANIFEST-000008
|       └── OPTIONS-000009
└── shared_checksum
    └── 000007_1498774076_590.sst
```

`meta` directory contains a "meta-file" describing each backup, where its name is the backup ID. For example, a meta-file contains a listing of all files belonging to that backup. The format is described fully in the implementation file (`utilities/backup/backup_engine.cc`).

`private` directory always contains non-SST/blob files (options, current, manifest, and WALs). In case `Options::share_table_files` is unset, it also contains the SST/blob files.

`shared_checksum` directory contains SST/blob files when both `Options::share_table_files` and `Options::share_files_with_checksum` are set. In this directory, files are named using their name in the original database, size, and checksum. These attributes uniquely identify files that can come from multiple RocksDB instances. 

Deprecated: `shared` directory (not shown) contains SST files when `Options::share_table_files` is set and `Options::share_files_with_checksum` is false. In this directory, files are named using only by their name in the original DB. In the presence of restoring from a backup other than latest, this could lead to corruption even when backing up only a single DB.

### Backup performance

Beware that backup engine's `Open()` takes time proportional to the number of existing backups since we initialize info about files in each existing backup. So if you target a remote file system (like HDFS), and you have a lot of backups, then initializing the backup engine can take some time due to all the network round-trips. We recommend to keep your backup engine alive and not to recreate it every time you need to do a backup or restore.

Another way to keep engine initialization fast is to remove unnecessary backups. To delete unnecessary backups, just call `PurgeOldBackups(N)`, where N is how many backups you'd like to keep. All backups except the N newest ones will be deleted. You can also choose to delete arbitrary backup with call `DeleteBackup(id)`.

Also beware that performance is decided by reading from local db and copying to backup. Since you may use different environments for reading and copying, the parallelism bottleneck can be on one of the two sides. For example, using more threads for backup (See Advanced usage) won't be helpful if local db is on HDD, because the bottleneck in this condition is disk reading capability, which is saturated. Also a poor small HDFS cluster cannot show good parallelism. It'll be beneficial if local db is on SSD and backup target is a high-capacity HDFS. In our benchmarks, using 16 threads will reduce the backup time to 1/3 of single-thread job.

### Under the hood

Creating backups is built on [checkpoint](https://github.com/facebook/rocksdb/wiki/Checkpoints). When you call `BackupEngine::CreateNewBackup()`, it does the following:

1. Disable file deletions
2. Get live files (this includes table files, current, options and manifest file).
3. Copy live files to the backup directory. Since table files are immutable and filenames unique, we don't copy a table file that is already present in the backup directory. Since version 6.12, we essentially have unique identifiers for SST files, using file numbers and DB session IDs in the SST file properties. Options, manifest and current files are always copied to the private directory, since they are not immutable.
4. If `flush_before_backup` was set to false, we also need to copy WAL files to the backup directory. We call `GetSortedWalFiles()` and copy all live files to the backup directory.
5. Re-enable file deletions

### Advanced usage

We can store user-defined metadata in the backups. Pass your metadata to `BackupEngine::CreateNewBackupWithMetadata()` and then read it back later using `BackupEngine::GetBackupInfo()`. For example, this can be used to identify backups using different identifiers from our auto-incrementing IDs.

We also backup and restore the options file now. After restore, you can load the options from db directory using `rocksdb::LoadLatestOptions()` or `rocksdb:: LoadOptionsFromFile()`. The limitation is that not everything in options object can be transformed to text in a file. You still need a few steps to manually set up [missing items](https://github.com/facebook/rocksdb/wiki/RocksDB-Options-File#unsupported-options) in options after restore and load. Good news is that you need much less than previously.

You need to instantiate some env and initialize `BackupEngineOptions::backup_env` for backup_target. Put your backup root directory in `BackupEngineOptions::backup_dir`. Under the directory the files will be organized in the structure mentioned above.

`BackupEngineOptions::max_background_operations` controls the number of threads used for copying files during backup and restore. For distributed file systems like HDFS, it can be very beneficial to increase the copy parallelism.

`BackupEngineOptions::info_log` is a Logger object that is used to print out LOG messages if not-nullptr. See [Logger wiki](https://github.com/facebook/rocksdb/wiki/Logger).

If `BackupEngineOptions::sync` is true, we will use `fsync(2)` to sync file data and metadata to disk after every file write, guaranteeing that backups will be consistent after a reboot or if machine crashes. Setting it to false will speed things up a bit, but some (newer) backups might be inconsistent. In most cases, everything should be fine, though.

If you set `BackupEngineOptions::destroy_old_data` to true, creating new `BackupEngine` will delete all the old backups in the backup directory.

`BackupEngine::CreateNewBackup()` method takes a parameter `flush_before_backup`, which is false by default. When `flush_before_backup` is true, `BackupEngine` will first issue a memtable flush and only then copy the DB files to the backup directory. This will minimize WAL files from being copied to the backup directory (since flush should obsolete existing WALs). If `flush_before_backup` is false, backup will not issue flush before starting the backup. In that case, the backup will also include WAL files corresponding to live memtables. Backup will be consistent with current state of the database regardless of `flush_before_backup` parameter.

### Further reading

For the implementation, see `utilities/backup/backup_engine.cc`.