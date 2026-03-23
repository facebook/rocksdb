# Object Registry Overview
The ObjectRegistry provides a location to register and manage extensions to RocksDB.  The ObjectRegistry contains:

* A collection of Object Libraries.  An Object Library contains a set of factories that can be used to create instances of classes.
* A collection of Plugins.  Plugins are extensions added to the RocksDB processes.  Plugins provide a means of registering and creating alternative implementations of a class that may not be part of the core RocksDB installation or process.
* A collection of Managed Objects.  Managed Objects are instances of classes that can be shared across configurations and database instances.

An `ObjectRegistry` can be obtained via `ObjectRegistry::Default` or `ObjectRegistry::NewInstance`.  An instance of a `ConfigOptions` has an ObjectRegistry.  An `ObjectRegistry` can be nested (parent-child), with the child values taking precedence over those in the parent.

# The Object Library
The ObjectRegistry contains a collection of object libraries.  An object library provides a set of Factories that can be used to create instances of classes.  Typically, libraries are divided into comparable functionality. For example, a "Test" library may register classes used for testing, whereas factories for certain features or plugins ("Cassandra", "HDFS") may be registered in their own libraries.  Classes provided as part of the core RocksDB are typically registered in the Default library.

Factories may be registered with a library individually or via a Registrar function.  A Registrar function typically registers factories individually.  The code below shows a registrar function and the factories being registered:
```
static int RegisterTestObjects(ObjectLibrary& library,
                               const std::string& /*arg*/) {
  size_t num_types;
  library.Register<TableFactory>(
      "MockTable",
      [](const std::string& /*uri*/, std::unique_ptr<TableFactory>* guard,
         std::string* /* errmsg */) {
        guard->reset(new mock::MockTableFactory());
        return guard->get();
      });
  return static_cast<int>(library.GetFactoryCount(&num_types));
}
config_options_.registry->AddLibrary("custom-tests", RegisterTestObjects, "");
```

This code snippet adds the "custom-tests" library to the registry.  The `RegisterTestObjects` registers a `TableFactory` to create a `MockTable` instance.

Factories are registered using a regular expression.  For example, the registered factory:
```
    lib->Register<EncryptionProvider>(
        "CTR(://test)?",
        [](const std::string& uri, std::unique_ptr<EncryptionProvider>* guard,
           std::string* /*errmsg*/) {
            ...
        });
```

Will match `CTR://test` or `CTR`.  The "uri" argument to the factory will be the name that invoked this factory (e.g "`CTR://test"`).

On success, a factory returns a pointer to the object.  If the object is not static -- and can be deleted -- the guard should contain the same pointer.  On error, errmsg should contain a message explaining the reason for the failure.

# Plugins

Plugins are extensions to RocksDB that are not part of the core RocksDB code.  Static plugins reside in the `plugins` subdirectory of a RocksDB installation and are compiled into RocksDB by adding them to the `ROCKSDB_PLUGINS` make variable (at this time, dynamically loadable plugins are not supported by RocksDB).  Plugins provide a means of registering RocksDB extensions for use by RocksDB tests and tools into the build process.  Plugins register themselves and their object libraries with the Object Registry to make their functionality available to RocksDB applications.

RocksDB plugins may be part of the core source tree or may be provided in third-party repositories.  Examples of built-in plugins include support for ClockCache, LuaCompactionFilter, MemkindMemoryAllocator, HDFS support, and Cassandra extensions.  Instructions for creating custom plugins can be found here.  Plugin functionality may or may not be tested as part of the core RocksDB test suite.

# Managed Objects

Managed objects provide a means of sharing customizable objects between instances.  For example, the block Cache used by multiple table factories would be a managed object.  Other examples of Managed Objects include Memory Allocators and Logger, which can be shared by multiple objects within or between database instances.  

Whether an object is managed or not is a property decided by the developer of the object based on its requirements.  Managed Objects must override the `Customizable::GetId` method (generally by returning `Customizable::GenerateIndividualId`).  The `LoadManagedObject` and `NewManagedObject` methods are helper functions for developers to manage the creation and lifetime of managed objects.

Managed Objects associate an ID with an instance of an object in the registry.  As long as an instance of that object exists, `GetManagedObject` will return the same object.  The `GetOrCreateManagedObject` will return an existing object (if one exists) or a newly created and configured one, if one does not exist.







