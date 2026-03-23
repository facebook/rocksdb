RocksDB provides a robust extension architecture, allowing the developers and community to create custom ([implementations](https://github.com/facebook/rocksdb/wiki/Object-Registry#plugins)) of various classes.  For example, RocksDB supports different versions of the `TableFactory` (`PlainTable`, `BlockBasedTable`, `CuckooTable`) and `Env` (`PosixEnv`, `MemoryEnv`, `EncryptedEnv`, etc).  The developer is encouraged to write extensions for various RocksDB features, such as `CompactionFilter`, `MergeOperator`, and `Comparator`.

What has been lacking up until now is a standard way of creating and configuring these various object classes.  The `Customizable` class is meant to address many of those issues.  By inheriting from the [Configurable class](https://github.com/facebook/rocksdb/wiki/Developing-RocksDB-Configurable-Objects#configurable-class), `Customizable` objects are naturally configurable, giving the ability to configure and serialize customizable objects to the Options file without additional effort.  The `Customizable` class provides a common framework for identifying and creating instances of these objects.

Over time, more of the existing extension classes will be converted into `Customizable` classes.  This conversion will allow more alternative implementations to be available to the user and developer via simple configuration changes.

For terminology purposes in this document, the following terms will be used:
* Customizable:  base class from which others are derived.  
* Interface: base functional class (`TableFactory`, `MergeOperator`)
* Implementation: specific implementation of an interface (`BlockBasedTableFactory`, `StringAppendMergeOperator`);
* Instance: Specific run-time instantiation of an Implementation class. 

This document will describe the requirements and assumptions of each type of classes.

## The Interface Class

An Interface class inherits from `Customizable` class and provides a set of often abstract methods for the functionality of the class.  Examples of interface classes include `TableFactory` and `Env`.

In addition to inheriting from `Customizable`, customizable interface classes must implement static `Type` and `CreateFromString` methods. 

### The Interface Type Method

The `static const char *Type` method returns a string uniquely identifying this interface class (often this string will be the name of the interface, (e.g. `static const char *Type() { return "TableFactory"; }`.  Each interface must provide a unique type name.  The `Type` method is used by the [Object Registry](https://github.com/facebook/rocksdb/wiki/Object-Registry) to locate potential factories for instances of this class.  

### CreateFromString

The `Status CreateFromString(const ConfigOptions&, const std::string& id, T*)` method is the factory method used to create a new instance of an object of this type.  The `CreateFromString` method can create a configure a new instance of the given interface based on the input id.  All Customizable interface classes should have a `CreateFromString` method with this signature, only varying the type of the final argument.

The input `id` string can be a simple string (e.g. `BlockBasedTable`) identifying the name of the instance class to be created.  Alternatively, the `id` could be a set of name-value properties -- separated by a ";" -- that denote which instance class to create and configuration parameters.  For example, `id=BlockBasedTable; block_size=8192`, would create a `BlockBasedTableFactory` and initialize the `BlockBasedTableOptions.block_size=8192`.

The final argument to `CreateFromString` returns the newly created object.  Depending on the interface, this may be a raw (`T** result`), shared (`std::shared_ptr<T> *result`), or unique (`std::unique_ptr<T*>`) pointer.  On success, this will be the newly created object.

The `ConfigOptions` parameter controls the behavior of the `CreateFromString` method.  For example, if `ignore_unknown_options=true`, then errors will not be reported if an option is not found.

The `CreateFromString` method should support both "built-in" and "registered" objects.  The "built-in" types are built into RocksDB and are always available.  The "registered" types are factories registered with the [Object Registry](https://github.com/facebook/rocksdb/wiki/Object-Registry) at run-time.  Helper methods are defined in [include/rocksdb/utilities/customizable_util.h](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/utilities/customizable_util.h) to make it easier to write the `CreateFromString` factories.

## Implementation Classes

Implementation classes provide the implementation of the methods in the Interface classes (e.g. `BlockBasedTableFactory` and `PlainTableFactory` are instances of the `TableFactory` interface).  In addition to providing the functionality required by the interface, Instance classes must implement the `const char *Name() const` method.  This method specifies the name of this instance class (e.g., "BlockBasedTable") and is used for creating an instance of this class and will be found in the options files.  Generally speaking, the 

Implementation classes are strongly encouraged to provide a `static const char *kClassName()` method.  This method returns the name of the class for this instance and is used by the reflection classes, described below.  A good programming practice would be to write the `kName` and `kClassName` methods with respect to each other:

    static const char *kClassName() { return "PosixEnv"; }
    const char *kName() const override { return kClassName(); }

All Instances of a given Implementation should have the same Name.  If there is a need to differentiate between two instances of the same class, the GetId() method should be used.  For example, two instances of a `FixedPrefixTransform` class have the same Name (`rocksdb.FixedPrefixTransform`) but may have different IDs (`rocksdb.FixedPrefixTransform.11` and `rocksdb.FixedPrefixTransform.22`).  If different IDs are required for different instances, the Implementation class must provide its own implementation of the `GetID()` method.  Note that it is the ID that is stored in the Options file and is used passed to `CreateFromString`.

The Name/ID may be used by the [Object Registry](https://github.com/facebook/rocksdb/wiki/Object-Registry) to create new instances.  Since the Object Registry does regular expression matching of its fields, it is best/simplest to avoid special regex symbols in the Name/ID (otherwise these would need to be escaped as part of the Register process).

Implementation classes are `Configurable` classes.  Properties of an object that can be configured should be registered with the [Configurable](https://github.com/facebook/rocksdb/wiki/The-Configurable-Class) infrastructure in the class constructor:
```
  BlockBasedTableFactory::BlockBasedTableFactory(
    const BlockBasedTableOptions& _table_options)
    : table_options_(_table_options) {
     ...
     RegisterOptions(&table_options_, &block_based_table_type_info);
   }
```
See the [Configurable](https://github.com/facebook/rocksdb/wiki/The-Configurable-Class) page for more details on defining configurable options.

### Advanced Customizable Properties
The `IsInstanceOf` method can be used to tell if a given object is an instance of the input name.  Typically, this is equivalent to checking if the instance name is equivalent to the class name.  However, there may be some classes that are intermediary classes (like a `LRUCache` is an instance of a `ShardedCache` as well) or have alternative historical names ('skip_list' and 'SkipListFactory'.  In this case, the implementation class should implement `IsInstanceOf` to add itself to the check.

The `Customizable` base class provides reflection-like functionality via the `CheckCast<T>` method.  This method is useful if a specific implementation of the customizable is required.  For example:
```
    std::shared_ptr<TableFactory> table_factory;
    Status s = TableFactory::CreateFromString(ConfigOptions(), "BlockBasedTable", &table_factory);
    auto bbtf = table_factory->CheckCast<BlockBasedTableFactory>();
```
This example will set `bbtf` to the `BlockBasedTableFactory`.  If the table_factory was not block-based, `bbtf` would be set to `nullptr`.  The `CheckedCast` uses the `kClassName` method for the class in question.  

Some `Customizable` classes are wrappers for an alternative implementation (an `EnvWrapper` is-a `Env` and has-a `Env`).  These classes should override the `const Customizable *Inner() const` method to return the wrapped implementation. If a class is wrapped, the CheckedCast may traverse the inner class but the IsInstanceOf must not.  As an example:
```
   auto lru = NewLRUCache(...);
   auto clock = NewClockCache(...);
   auto wrapper = CacheWrapper(lru); // is-a and has-a cache
   lru->IsInstanceOf("LRUCache"); // returns true
   wrapper->Inner()->>IsInstanceOf("LRUCache"); // returns true
   wrapper->IsInstanceOf("LRUCache"); // returns false
   lru->CheckedCast<LRUCache>() == wrapper->CheckedCast<LRUCache>(); // Returns non-nullptr
   clock->CheckedCast<LRUCache>(); // Returns nullptr
   lru->CheckedCast<ShardedCache>(); // Returns non-nullptr
   clock->CheckedCast<ShardedCache>(); // Returns non-nullptr
```
## The Process for Implementing new Customizable Classes

The following steps outline a rough process for making an Interface and its Implementations fit into the `Customizable` methodology.

1. The basic hierarchy is established.  The Interface class is changed to inherit from `Customizable`.  The `Name` and `kClassName` methods are added to the Implementation classes.  
2. The `Type` and `CreateFromString` methods are added to the Interface class.  Any existing non-test implementations should be registered with the [Object Registry](https://github.com/facebook/rocksdb/wiki/Object-Registry#the-object-library), or created by the `CreateFromString` method.
3. Update the `OptionTypeInfo` that currently creates this object in the Options classes.  This typically involves removing the custom `OptionType` (e.g. `kMergeOperator`) and associated parsing/serialization.  The definition of the option in the `OptionsTypeMap` should be changed to use the appropriate `OptionTypeInfo Customizable` constructor.
4. For each of the Implementation classes, create an `OptionsTypeMap` and register any options as necessary
5. If necessary, the `GetId`, `IsInstanceOf`, and `Inner` methods are added to the implementation classes.
6. Add the appropriate tests
 