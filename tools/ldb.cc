// Copyright (c) 2012 Facebook. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "util/ldb_cmd.h"

namespace leveldb {

class LDBCommandRunner {
public:

  static void PrintHelp(const char* exec_name) {
    std::string ret;
    ret.append("--- compact ----:\n");
    ret.append(exec_name);
    ret.append(" compact ");
    Compactor::Help(ret);

    ret.append("\n--- dump ----:\n");
    ret.append(exec_name);
    ret.append(" dump ");
    DBDumper::Help(ret);

    ret.append("\n---reduce_levels ----:\n");
    ret.append(exec_name);
    ret.append(" reduce_levels ");
    ReduceDBLevels::Help(ret);

    ret.append("\n---dump_wal----:\n");
    ret.append(exec_name);
    ret.append(" dump_wal ");
    WALDumper::Help(ret);
    fprintf(stderr, "%s\n", ret.c_str());
  }

  static void RunCommand(int argc, char** argv) {
    if (argc <= 2) {
      PrintHelp(argv[0]);
      exit(1);
    }
    const char* cmd = argv[1];
    std::string db_name;
    std::vector<std::string> args;
    for (int i = 2; i < argc; i++) {
      if (strncmp(argv[i], "--db=", strlen("--db=")) == 0) {
        db_name = argv[i] + strlen("--db=");
      } else {
        args.push_back(argv[i]);
      }
    }

    LDBCommand* cmdObj = NULL;
    if (strcmp(cmd, "compact") == 0) {
      // run compactor
      cmdObj = new Compactor(db_name, args);
    } else if (strcmp(cmd, "dump") == 0) {
      // run dump
      cmdObj = new DBDumper(db_name, args);
    } else if (strcmp(cmd, "reduce_levels") == 0) {
      // reduce db levels
      cmdObj = new ReduceDBLevels(db_name, args);
    } else if (strcmp(cmd, "dump_wal") == 0) {
      cmdObj = new WALDumper(args);
    } else {
      fprintf(stderr, "Unknown command: %s\n", cmd);
      PrintHelp(argv[0]);
      exit(1);
    }

    cmdObj->Run();
    LDBCommandExecuteResult ret = cmdObj->GetExecuteState();
    fprintf(stderr, "%s\n", ret.ToString().c_str());
    delete cmdObj;
  }
};

}

int main(int argc, char** argv) {
  leveldb::LDBCommandRunner::RunCommand(argc, argv);
}
