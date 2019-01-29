// Class for loading Extensions into Extension objects into a rocksdb instance
// Extension classes are registered by name and type with an ExtensionLoader and 
// can be created and configured on demand later as required.

#pragma once

#include <functional>
#include <memory>
#include <regex>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#include "rocksdb/options.h"
#include "rocksdb/extensions.h"
#include "rocksdb/status.h"

namespace rocksdb {
struct ColumnFamilyOptions;
struct DBOptions;
class Extension;
class DynamicLibrary;
class Logger;
  
using std::shared_ptr;
using std::unique_ptr;

/**
 * ExtensionLoader is the class for loading extensions on demand.
 * Extensions are registered by type and a pattern, along with a 
 * FactoryMethod.  The ExtensionLoader can then create the appropriate
 * extension (via the Factory) when requested.  Extensions can be
 * registered by type/name or via a RegistrarFunction of a Dynamic Library.
 */

class ExtensionLoader {
public:
		     

  /**
   * The signature of the Factory Function for creating a new Extension.
   * @param string name       The name/pattern of the extension to create
   * @param DBOptions dbOpts  The DBOptions used to create this extension
   * @param ColumnFamilyOptions The ColumnFamilyOptions for this extension (if any)
   * @param unique_ptr<>        A deletion guard for this extension.  If the
   *                            guard is set upon return, the caller "owns"
   *                            deletion of this object. 
   * @return                    The new extension object (on success) or nullptr
   */
  typedef std::function<Extension*(const std::string &,
				   const DBOptions &,
				   const ColumnFamilyOptions *,
				   std::unique_ptr<Extension>*)> FactoryFunction;

  /**
   * The signature of the function for loading extension factories 
   * from a dynamic library.  This method is expected to register
   * factory functions in the supplied extension loader.
   * @param ExtensionLoader    The loader to load factories into.
   * @param std::string         Argument to the loader
   */
  typedef std::function<void(ExtensionLoader &,
			     const std::string & arg)> RegistrarFunction;
  
  /**
   * Registers the input function with this extension loader
   * @param type     The type of extension returned by the factory.
   * @param name     The name/pattern to match for this factory.
   * @param function The factory function for creating this extension
   * @return         The input factory
   */
  const FactoryFunction & RegisterFactory(const std::string & type,
					  const std::string & name,
					  const FactoryFunction & function);
  /**
   * Loads the FactoryFunction specified by method from the library and
   * registers it with the extension factory.
   * @param library  The library in which to locate the named method.
   * @param type     The type of extension returned by the factory.
   * @param name     The name/pattern to match for this factory.
   * @param method   The name of the method to load from the library
   * @returns        OK if the function was found, NotFound otherwise
   */
  Status RegisterLibraryFactory(const std::shared_ptr<DynamicLibrary> & library,
			 const std::string & type,
			 const std::string & name,
  			 const std::string & method);

  /**
   * Invokes the specified registrar to add factories this this loader.
   * @param library  The library in which to locate the named method.
   * @param method   The name of the method to load from the library
   * @param arg      The additional argument to pass to the RegistrarFunction
   */
  void RegisterFactories(const RegistrarFunction & registrar,
			 const std::string & arg) {
    return registrar(*this, arg);
  }
    
  /**
   * Loads the RegistrarFunction specified by method from the library and
   * invokes it to register factories with this extension factory.
   * @param library  The library in which to locate the named method.
   * @param method   The name of the method to load from the library
   * @param arg      The additional argument to pass to the RegistrarFunction
   */
  Status RegisterLibrary(const std::shared_ptr<DynamicLibrary> & library,
			 const std::string & method,
			 const std::string & arg);
  
  /**
   * Finds the FactoryFunction for the specified extension type and name.
   * @param type     The type of factory to locate.
   * @param name     The name of factory to locate.
   * @returns        The registered FactoryFunction for this type/name (or nullptr
   *                 if not found)
   */
  FactoryFunction FindFactory(const std::string  & type, const std::string & name);

  /**
   * Creates a new extension of the input name/type and stores the shared result
   * @param type     The type of factory to create.
   * @param name     The name of factory to create.
   * @param dbOpts   Options passed to the Extension Factory
   * @param cfOpts   Options passed to the Extension Factory
   * @param result   The newly created object
   * @returns        OK if the extension was successfully created, 
   */
  Status CreateSharedExtension(const std::string & type,
			       const std::string & name,
			       const DBOptions & dbOpts,
			       const ColumnFamilyOptions * cfOpts,
			       std::shared_ptr<Extension> *result);
  /**
   * Creates a new extension of the input name/type and stores the unique result
   * @param type     The type of factory to create.
   * @param name     The name of factory to create.
   * @param dbOpts   Options passed to the Extension Factory
   * @param cfOpts   Options passed to the Extension Factory
   * @param result   The newly created object
   * @returns        OK if the extension was successfully created, 
   */
  Extension *CreateUniqueExtension(const std::string & type,
				   const std::string & name,
				   const DBOptions & dbOpts,
				   const ColumnFamilyOptions * cfOpts,
				   std::unique_ptr<Extension> *guard);

  /**
   * Dumps information about the registered factories to the supplied logger.
   * @param log      The logger to write to.
   */
  void Dump(Logger* log) const;

  /**
   * Returns a new loader, with a link to the Default one. 
   */
  static std::shared_ptr<ExtensionLoader> Get();

  /**
   * Returns the Default loader.  
   */
  static std::shared_ptr<ExtensionLoader> Default();
private:
  //** The parent (default) loader for this loader
  std::shared_ptr<ExtensionLoader> parent;
  //** Any shared libraries used by this loader
  std::unordered_set<std::shared_ptr<DynamicLibrary> >  libraries;
  //** FactoryFunctions for this loader, organized by type
  std::unordered_map<std::string,
		     std::vector<std::pair<std::regex, FactoryFunction> > > factories;
};

}  // namespace rocksdb
