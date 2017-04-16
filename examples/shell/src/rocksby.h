#include <cstdio>
#include <iostream>
#include <stdlib.h>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

class DBWrapper {
  public:
	  DBWrapper(std::string kDBFileName) {
			std::string kDBPath = "/tmp/" + kDBFileName + "/";
      options = new rocksdb::Options();
      options->IncreaseParallelism();
      options->OptimizeLevelStyleCompaction();
      options->create_if_missing = true;

      status = rocksdb::DB::Open(*options, kDBPath, &db);

      if (!status.ok()) {
        std::cerr << "Trouble opening DB";
        exit(EXIT_FAILURE);
      }
    }

    bool put(const std::string key, const std::string value) {
      status = db->Put(rocksdb::WriteOptions(), key, value);

      return status.ok();
    }

		bool get_helper(const std::string key, std::string* value) {
			status = db->Get(rocksdb::ReadOptions(), key, value);

      return status.ok();
    }


		std::string* new_string_pointer() {
			return new std::string();
		}

		std::string string_from_pointer(std::string* pointer) {
			std::string string = "";
			string.assign(*pointer);

			return string;
		}

		void delete_string_pointer(std::string *s) {
			delete s;
		}

  private:
    rocksdb::Options* options;
    rocksdb::Status status;
    rocksdb::DB* db;
};
