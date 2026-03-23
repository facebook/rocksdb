# Overview

RocksDB provides several classes that are extensible where alternative implementations may be used.  For example, there are multiple implementations of a `TableFactory` (`BlockBased`, `Plain`, `Cuckoo`) and a `Comparator` (`Bytewise`, `ReverseBitewise`) in the core RocksDB distribution.  Several other classes also provide alternative implementations.

# Developing RocksDB Extensions

RocksDB extensions should implement the [Customizable interface](https://github.com/facebook/rocksdb/wiki/The-Customizable-Class) interface.  By following this infrastructure, instances of the Customizable class can be created at run-time.  The basic steps to create and register a Customizable class are defined [here](https://github.com/facebook/rocksdb/wiki/The-Customizable-Class).  

Some extensions are optional and are considered plugins.  [Plugins](https://github.com/facebook/rocksdb/wiki/Object-Registry#plugins) are instances of classes that are not core to RocksDB and can be conditionally added to a RocksDB installation.  Plugins use a registration function to register themselves with the infrastructure.

# Using RocksDB Extensions Programmatically

Customizable extensions that are registered with the Object Registry can be created programmatically via the CreateFromString method.  For example, if `TableFactory::CreateFromString` is passed the `CuckooTable` id, the returned `TableFactory` will be a CuckooTableFactory.  Each Customizable class (TableFactory, Comparator, etc) has a `CreateFromString` method that takes an ID (or name-value pairs) and returns the requested interface type.  More information can be found here.

There is no need to create new APIs for each instance type (e.g. `NewCuckooTableFactory`) being created.

# Using RocksDB Extensions via the Options file

For most Customizable classes, they can be re-created from the Options file.  The requirements are:
1. The Customizable class is serializable and can be represented as a string;
2. The instance classes are registered with the Object Registry.  For plugins, this requirement means the plugin is compiled into the libraries.
3. The options are registered with the appropriate options.

If these properties are true, objects can be stored and re-created from the Options file without further programmatic intervention.


