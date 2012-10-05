# [LevelDBJNI](https://github.com/fusesource/leveldbjni)

## [leveldbjni 1.3][1_3], released 2012-09-24
[1_3]: http://repo.fusesource.com/nexus/content/groups/public/org/fusesource/leveldbjni/leveldbjni/1.3

* Make Util.link work on windows too.
* Expose the CreateHardLinkW windows API call.
* Added Windows LevelDB Support
* Update to hawtjni 1.6.
* Support the db.compactRange method to force compaction of the leveldb files.
* Fixed bug need to get leveldbjni workin on the Zing JVM

## [leveldbjni 1.2][1_2], released 2012-02-27
[1_2]: http://repo.fusesource.com/nexus/content/groups/public/org/fusesource/leveldbjni/leveldbjni/1.2

* Document how to use the memory pools.
* Fixes issue #6 Support using a memory pool to reduce native memory allocation overhead.
* Update leveldb, hawtjni, and leveldb-api versions.
* Store the version in the factory class.
* Added a release guide.

## [leveldbjni 1.1][1_1], released 2011-09-29
[1_1]: http://repo.fusesource.com/nexus/content/groups/public/org/fusesource/leveldbjni/leveldbjni/1.1

* the all module needs at least one java file so that it produces a javadoc and src.zip
* Try to load the native lib when the JniDBFactory class is loaded.
* Fixes issue #1 : Bug on NativeBuffer offset
* Switch the license from CDDL to the New BSD license to match the license used in the leveldb project.
* Add the sonatype snapshot repo since that's where the leveldb-api is at currently.
* Pickup updates in the api module.
* Updating build instructions.
* implement repair and destroy.
* api updated
* Cleaner java package structure. We only need to expose one public class now since we are using the org.iq80.leveldb abstract api.
* Refactored so that the main user API is the abstract API defined in 'org.iq80.leveldb.api' package.

## [leveldbjni 1.0][1_0], released 2011-08-08
[1_0]: http://repo.fusesource.com/nexus/content/groups/public/org/fusesource/leveldbjni/leveldbjni/1.0

* Initial Release
* OS X Intel 32 and 64 bit support
* Linux Intel 32 and 64 bit support
