/**
 * Options for the Thrift leveldb server.
 * @author Dhruba Borthakur (dhruba@gmail.com)
 * Copyright 2012 Facebook
 */

#ifndef THRIFT_LEVELDB_SERVER_OPTIONS_
#define THRIFT_LEVELDB_SERVER_OPTIONS_

#include <sys/stat.h>
#include <sys/types.h>

#include "leveldb/db.h"
#include "leveldb/cache.h"

//
// These are configuration options for the entire server.
//
class ServerOptions {
 private:
  int num_threads_;                // number of thrift server threads
  int cache_numshardbits_;         // cache shards
  long cache_size_;                // cache size in bytes
  int port_;                       // port number
  std::string hostname_;           // host name of this machine
  std::string rootdir_;            // root directory of all DBs
  leveldb::Cache* cache_;          // the block cache

  // Number of concurrent threads to run.
  const static int DEFAULT_threads = 1;

  // Number of bytes to use as a cache of uncompressed data.
  // Default setting of 100 MB
  const static long DEFAULT_cache_size = 100 * 1024 * 1024;

  // Number of shards for the block cache is 2 ** DEFAULT_cache_numshardbits.
  // Negative means use default settings. This is applied only
  // if DEFAULT_cache_size is non-negative.
  const static int DEFAULT_cache_numshardbits = 6;

  // default port
  const static int DEFAULT_PORT = 6666;

public:
  ServerOptions() : num_threads_(DEFAULT_threads),
                    cache_numshardbits_(DEFAULT_cache_numshardbits),
                    cache_size_(DEFAULT_cache_size),
                    port_(DEFAULT_PORT),
                    cache_(NULL) {
    char* buf = new char[HOST_NAME_MAX];
    if (gethostname(buf, HOST_NAME_MAX) == 0) {
      hostname_ = buf;
    } else {
      hostname_ = "unknownhost";
      delete buf;
    }
    rootdir_ = "/tmp"; // default rootdir
  }

  //
  // Returns succes if all command line options are parsed successfully,
  // otherwise returns false.
  bool parseOptions(int argc, char** argv) {
    int n;
    long l;
    char junk;
    char* cports = NULL;
    for (int i = 1; i < argc; i++) {
      if (sscanf(argv[i], "--port=%d%c", &n, &junk) == 1) {
        port_ = n;
      } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
        num_threads_ = n;
      } else if (sscanf(argv[i], "--cache_size=%ld%c", &n, &junk) == 1) {
        cache_size_ = n;
      } else if (sscanf(argv[i], "--cache_numshardbits=%d%c", &n, &junk) == 1) {
        cache_numshardbits_ = n;
      } else if (strncmp(argv[i], "--hostname=", 10) == 0) {
        hostname_ = argv[i] + 10;
      } else if (strncmp(argv[i], "--rootdir=", 9) == 0) {
        rootdir_ = argv[i] + 9;
      } else {
        fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
        return false;
      }
    }
    return true;
  }

  // Create the directory format on disk. 
  // Returns true on success, false on failure
  bool createDirectories() {
    mode_t mode = 0755;
    const char* dir = getRootDirectory().c_str();
    if (mkpath(dir, mode) < 0) {
      fprintf(stderr, "Unable to create root directory %s\n", dir);
      return false;
    }
    dir = getDataDirectory().c_str();;
    if (mkpath(dir, mode) < 0) {
      fprintf(stderr, "Unable to create data directory %s\n", dir);
      return false;
    }
    dir = getConfigDirectory().c_str();;
    if (mkpath(dir, mode) < 0) {
      fprintf(stderr, "Unable to create config directory %s\n", dir);
      return false;
    }
    return true;
  }

  // create a cache instance that is shared by all DBs served by this server
  void createCache() {
    if (cache_numshardbits_ >= 1) {
      cache_ = leveldb::NewLRUCache(cache_size_, cache_numshardbits_);
    } else {
      cache_ = leveldb::NewLRUCache(cache_size_);
    }
  }

  // Returns the base server port
  int getPort() {
    return port_;
  }

  // Returns the assoc server port. Currently, it is one more than the base
  // server port. In fiture, the assoc service would be supported on multiple
  // ports, each port serving a distinct range of keys.
  int getAssocPort() {
    return port_ + 1;
  }

  // Returns the cache
  leveldb::Cache* getCache() {
    return cache_;
  }

  // Returns the configured number of server threads
  int getNumThreads() {
    return num_threads_;
  }

  // Returns the root directory where the server is rooted. 
  // The hostname is appended to the rootdir to arrive at the directory name.
  std::string getRootDirectory() {
    return rootdir_ + "/" + hostname_;
  }

  // Returns the directory where the server stores all users's DBs.
  std::string getDataDirectory() {
    return getRootDirectory() + "/userdata/";
  }

  // Returns the directory where the server stores all its configurations
  std::string getConfigDirectory() {
    return getRootDirectory() + "/config/";
  }

  // Returns the data directory for the specified DB
  std::string getDataDirectory(const std::string& dbname) {
    return getDataDirectory() + dbname;
  }

  // Returns true if the DB name is valid, otherwise return false
  bool isValidName(const std::string& dbname) {
    // The DB name cannot have '/' in the name
    if (dbname.find('/') < dbname.size()) {
      return false;
    }
    return true;
  }

 private:
  static int do_mkdir(const char *path, mode_t mode) {
    struct stat st;
    int  status = 0;

    if (stat(path, &st) != 0) {
      if (mkdir(path, mode) != 0) {
        status = -1;
      }
    } else if (!S_ISDIR(st.st_mode)) {
        errno = ENOTDIR;
        status = -1;
    }
    return(status);
  }

  // mkpath - ensure all directories in path exist
  static int mkpath(const char *path, mode_t mode)
  {
    char *pp;
    char *sp;
    int  status;
    char *newpath = strdup(path);

    status = 0;
    pp = newpath;
    while (status == 0 && (sp = strchr(pp, '/')) != 0) {
      if (sp != pp) {
        /* Neither root nor double slash in path */
        *sp = '\0';
        status = do_mkdir(newpath, mode);
        *sp = '/';
      }
      pp = sp + 1;
    }
    if (status == 0) {
      status = do_mkdir(path, mode);
    }
    free(newpath);
    return (status);
  }
};

#endif // THRIFT_LEVELDB_SERVER_OPTIONS_
