// An Env is an interface used by the rocksdb implementation to access
// operating system functionality like the filesystem etc.  Callers
// may wish to provide a custom Env object when opening a database to
// get fine gain control; e.g., to rate limit file system operations.
//
// All Env implementations are safe for concurrent access from
// multiple threads without any external synchronization.

#ifndef STORAGE_ROCKSDB_INCLUDE_EXTENSION_H_
#define STORAGE_ROCKSDB_INCLUDE_EXTENSION_H_

#include <string>
#include <vector>
#include <unordered_map>
#include "rocksdb/status.h"

namespace rocksdb {
struct DBOptions;
struct ColumnFamilyOptions;
  
using std::unique_ptr;
using std::shared_ptr;
  
enum ExtensionType : char {
  kExtensionEventListener = 0,
  kExtensionTableFactory,
  kExtensionUnknown
};

  
class Extension {
  public:
  virtual ~Extension() {}
  // Names starting with "rocksdb." are reserved and should not be used
  // by any clients of this package.
  virtual const char* Name() const = 0;
  // Sanitizes the specified DB Options and ColumnFamilyOptions.
  //
  // If the function cannot find a way to sanitize the input DB Options,
  // a non-ok Status will be returned.
  virtual Status SetOptions(const std::unordered_map<std::string, std::string> &,
			      const DBOptions &,
			      const ColumnFamilyOptions &) const {
      return Status::OK();
    }
    
    // Return a string that contains printable format of table configurations.
    // RocksDB prints configurations at DB Open().
    virtual std::string GetPrintableTableOptions() const {
      return "";
    }
    
    virtual Status GetOptionString(std::string* /*opt_string*/,
				   const std::string& /*delimiter*/) const {
      return Status::NotSupported(
				  "The table factory doesn't implement GetOptionString().");
    }
    
  };

  class DynamicLibrary;
  class EventListener;
  
  class ExtensionFactory {
  public:
    virtual const char *Name() const = 0;
    typedef Extension *(*ExtensionFactoryFunction)(const std::string & name, ExtensionType type);
    static Status LoadDynamicFactory(const std::shared_ptr<DynamicLibrary> & library, const std::string & method, std::shared_ptr<ExtensionFactory> * factory);
  public:
    virtual ~ExtensionFactory() { }
    virtual Extension *CreateExtensionObject(const std::string &,
					     ExtensionType) {
      return nullptr;
    }
  };

}  // namespace rocksdb

#endif  // STORAGE_ROCKSDB_INCLUDE_EXTENSION_H_
