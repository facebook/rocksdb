# Release Number
Release version has three parts: MAJOR.MINOR.PATCH. An example would be 6.8.2.

See [RocksDB version macros](https://github.com/facebook/rocksdb/wiki/RocksDB-version-macros) for how to keep track with version you're using.

There are not objective criteria to distinguish a major version and a minor version. All features and improvements can go to minor versions, and they are applicable to new options, option default changes or public API change if needed.

A major version is usually made when several major features or improvements become stable.

Only bug fixes go to patch releases. No option of public API change is will be made in patch releases within a minor version, unless they cannot be unavoidable to fix a critical bug.

# Compatibility Between Releases
See [[RocksDB Compatibility Between Different Releases]].

# Release Frequency
RocksDB has time-based periodic releases. We typically try to make a minor release once every month.

# Release Branching and Tagging
* The 'main' branch is used for code development. It should be somewhat stable and in usable state. It is not recommended for production use, though.
* When we release, we cut-off the branch from main. For example, release 3.0 will have a branch 3.0.fb. Once the branch is stable enough (bug fixes go to both main and the branch), we create a release tag with a name 'v3.0.0'. While we develop version 3.1 in main branch, we maintain '3.0.fb' branch and push bug fixes there. We release 3.0.1, 3.0.2, etc. as tags on a '3.0.fb' branch with names 'v3.0.X'. Once we release 3.1, we stop maintaining '3.0.fb' branch. We don't have a concept of long term supported releases.

Here is an example:
```
-----+---------------------+-----------------------------------> branch: main
     |                     |
     |                     +--------(*)---------(*)---------------> branch 3.2.fb
     |                             3.2.0        3.2.1
     |
     +-----(*)----------(*)---------(*)---> branch: 3.1.fb
          3.1.0        3.1.1       3.1.2
```
Bug fixes for release branches generally go through main first, and are then back-ported with `git cherry-pick`. (Developer note: creating a pull request against the branch is strongly recommended for CI validation ([example](https://github.com/facebook/rocksdb/pull/6720)), but push to the branch from command line.)

For example, a bug is fixed in main that needs to be back-ported to 3.1. Here is the order:
```
-----+---------------------+-----------------------------------[fix]-> branch: main
     |                     |                                     |  
     |                     |                                     V
     |                     +--------(*)---------(*)------------[fix]---(*)-> branch 3.2.fb
     |                             3.2.0        3.2.1            |    3.2.2
     |                                                           V
     +-----(*)----------(*)---------(*)------------------------[fix]---(*)-> branch: 3.1.fb
          3.1.0        3.1.1       3.1.2                              3.1.3
```

Before an official public release, a release branch is used within Facebook servers for two weeks or more. For example, the latest public release might be 3.1.2 while Facebook is using internal releases 3.2.1 etc. from the 3.2.fb branch. We encourage other people to try out these release branches also before they are tagged for official public release. When an official release is made, you can assume that it has been stably running in production for two weeks.

# Maven Release
(Under construction)