## Building external plugins together with RocksDB

RocksDB offers several plugin interfaces for developers to customize its behavior. One difficulty developers face is how to make their plugin available to end users. The approach discussed here involves building the external code together with the RocksDB code into a single binary. Note another approach we plan to support involves loading plugins dynamically from shared libraries.

### Discovery

We hope developers will mention their work in "PLUGINS.md" so users can easily discover and reuse solutions for customizing RocksDB.

### Directory organization

External plugins will be linked according to their name into a subdirectory of "plugin/". For example, a plugin called "dedupfs" would be linked into "plugin/dedupfs/".

### Build standard

Currently the only supported build system is make. In the plugin directory, files ending in the .mk extension can define the following variables.

* `$(PLUGIN_NAME)_SOURCES`: these files will be compiled and linked with RocksDB. They can access RocksDB public header files.
* `$(PLUGIN_NAME)_HEADERS`: these files will be installed in the RocksDB header directory. Their paths will be prefixed by "rocksdb/plugin/$(PLUGIN_NAME)/".
* `$(PLUGIN_NAME)_LDFLAGS`: these flags will be passed to the final link step. For example, library dependencies can be propagated here, or symbols can be forcibly included, e.g., for static registration.

Users will run the usual make commands from the RocksDB directory, specifying the plugins to include in a space-separated list in the variable `ROCKSDB_PLUGINS`.

### Example

For a working example, see [Dedupfs](https://github.com/ajkr/dedupfs).
