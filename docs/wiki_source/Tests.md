RocksDB's correctness and performance are key to the [users](https://github.com/facebook/rocksdb/blob/main/USERS.md).

For each open source pull request, a set of unit tests and integration tests are run on cloud continuous testing infrastructures like Github Actions, CircleCI, and AppVeyor, that's what you see in the bottom status of your pull requests.

Inside Facebook's internal infrastructure, different configurations of the stress tests and performance tests are run continuously, if regressions are found, after triaging, either a patch is needed, or the pull request causing the regression needs to be reverted.

On Google's [continuous fuzzing platform for open source projects](https://github.com/google/oss-fuzz), a set of fuzzers defined in [the fuzz directory](https://github.com/facebook/rocksdb/tree/main/fuzz) are run continuously, regressions are reported to RocksDB developers as defined in [this line of code](https://github.com/google/oss-fuzz/blob/master/projects/rocksdb/project.yaml#L2).

Tests are the guardians for RocksDB's correctness and performance. Ideas on new testing strategies are welcomed. A great example on testing the databases comes from [SQLite](https://www.sqlite.org/testing.html), not by accident, SQLite also uses the [oss-fuzz platform](https://github.com/google/oss-fuzz/tree/master/projects/sqlite3) for fuzz testing.