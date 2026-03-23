RocksDB provides an infrastructure for initializing configurable objects from their string representation, as well as saving these objects as strings.  The core functionality is provided via the `OptionTypeInfo` classes.  The `Configurable class wraps this functionality into a base class and provides extended functionality for managing these objects.

For most `Configurable` classes, the development process is:
* Make the class extend Configurable (or Customizable) as appropriate;
* Create a static OptionTypeMap of the options that defines the names and properties to be configured for this object;
```static std::unordered_map<std::string, OptionTypeInfo>
    block_based_table_type_info = {
#ifndef ROCKSDB_LITE
// Define the option mapping here
#endif // ROCKSDB_LITE
};
```
* Associate this map with the options struct in the constructor of the configurable class
```
BlockBasedTableFactory::BlockBasedTableFactory(
    const BlockBasedTableOptions& _table_options)
    : table_options_(_table_options) {
  RegisterOptions(&table_options_, &block_based_table_type_info);
}
```
These steps open up the methods in the `Configurable` infrastructure to this class, allowing it to be configured from and saved to a string, including to the Options file.

# OptionTypeInfo
The `OptionTypeInfo` class associates a field (via its offset), with how to configure that field.  The `OptionTypeInfo` contains the offset of the field, as well as its type (e.g, `kInt`, `kString`, etc).  The `OptionTypeFlags` provide additional information about how the infrastructure should treat this field.  

A map of string-to-`OptionTypeInfo` (referred to as an `OptionTypeMap`) defines the rules for configuring a struct.  Each entry in the map associates a name with its field and how the field is managed.  

For the majority of use cases, there should be a static `OptionTypeMap` for each structure being configured.  Using the `OptionTypeMap`, it is possible to implement `ToString` and `Parse` methods for a RocksDB struct (`GetDBOptionsFromString`, `GetStringFromDBOptions`, `GetBlockBasedTableOptionsFromString`, `GetStringFromBlockBasedTableOptions`, etc)

## Advanced Option Features
The `OptionTypeFlags` provide additional instructions about the field.  For example, if `kDontSerialize` is turned on, then the field will not be saved as a string.  

### Enumerated Types
The `OptionTypeInfo` supports generic enumerated types via the `OptionTypeInfo::Enum` constructor.  Option enums take an argument that is the mapping between the string and enum values.  This mapping controls how enums are represented as strings.

### Option Structures
Some options have embedded structures within them.  The `OptionTypeInfo::Struct` constructor handles them.  This constructor takes an additional `OptionTypeInfo` mapping that is used to process the fields in the structure.

### Vectors
A vector of options can be created using the `OptionTypeInfo::Vector` API.  This constructor takes an additional argument denoting the type of the vector elements.  This additional argument is used for determining how to parse and serialize individual elements in the vector.

### Pointers
Some members of the InstanceOption struct may be pointers.  To properly dereference pointers, they should use the proper `OptionTypeFlag` to indicate if it the member variable is a raw `(Pointer* p)`, shared `(std::shared<Pointer> p)`, or unique `(std::unique<Pointer> p)` pointer reference. Pointers are most typically used for `Configurable` and `Customizable` objects.

### Customizable Objects
An InstanceOption may contain other `Customizable` objects (for example, an `Env` inside of a `DBOptions`).  This type of `OptionTypeInfo` is typically created via the `OptionTypeInfo::AsCustom` methods.  The `AsCustom` methods will use the `Customizable` infrastructure to create and manage embedded customizable options.

### Lambda Function Pointers
An `OptionTypeInfo` can also be created specifying custom parse, serialize, and equality functions.  When specified, these functions replace the default behavior for the given `OptionType`.  These functions could be used, for example, to guarantee that an option is within a given range of values.

# Configurable class

The `Configurable` class provides the basic framework for managing the configuration of objects.  The `Configurable` class provides a standard interface for:
* Configuring an object from a string (`ConfigureFromMap`, `ConfigureFromString`, `ConfigureOption`);
* Saving an object’s configuration to a string (`GetOptionString`, `GetOption`, `ToString`);
* Comparing to configured objects for equivalence (`AreEquivalent`);
* Listing the configuration options for an object (`GetOptionsNames`);
* Get the underlying options struct(s) for an object (`GetOptions`)
* Initializing the object (`PrepareOptions`)
* Validating the configured objects (`ValidateOptions`)

These operations are typically provided by the `Configurable` class by inheriting from the class and registering the associated options with the infrastructure.  To register an object with the `Configurable` infrastructure, an `OptionTypeInfo` map and associated options object must be registered.  This registration is done in the constructor:
```
static std::unordered_map<std::string, OptionTypeInfo>
    block_based_table_type_info = { /* Register all the Options */};
BlockBasedTableFactory::BlockBasedTableFactory(
    const BlockBasedTableOptions& _table_options)
    : table_options_(_table_options) {
  InitializeOptions();
  RegisterOptions(&table_options_, &block_based_table_type_info);
}
```

Most `Configurable` classes use a struct (e.g, `PlainTableOptions`, `BlockBasedTableOptions`) to store their configuration data.  An instance of this struct is a member of the `Configurable` class and contains the configuration data for that instance.  These structs should typically define a "static const char *kName()" method that defines their name/class with this configurable object (e.g. `BlockBasedTableOptions`).  Structs that are registered via their kName can be retrieved via GetOptions (with type checking):
```
struct BlockBasedTableOptions {
  static const char* kName() { return "BlockTableOptions"; };
  // The Table Properties
};
const auto new_bbto = options.table_factory->GetOptions<BlockBasedTableOptions>();
```

Some objects may have multiple configuration objects, which can all be registered via calling `RegisterOptions` with the different option structs and maps.  When options are evaluated, they will be evaluated in the order in which they were registered.  This functionality can be used to guarantee that some options are evaluated first to guarantee certain values are evaluated first (for example, to allow "Test" options to be set before other options or to make sure certain values -- like env -- are set before others that may require that setting).


## Advanced Configurable Methods

The `Configurable` class also provides methods to initialize (`PrepareOptions`) and validate (`ValidateOptions`).  Classes should override these methods if they have any custom initialization or validation logic.  By default, these methods will initialize and validate, respectively any options that are themselves `Configurable` or `Customizable`.

`PrepareOptions` is used to complete any initialization of an object.  For example, the `PrepareOptions` for `Cache` may set up the shards.  Once an object has successfully been prepared, it is considered immutable and only options that are marked as `kMutable` can be changed via the configuration methods.

The `ValidateOptions` method is used to check that the settings for the current object are valid for this configuration.  Classes should override this method to apply any custom logic they may have.  
