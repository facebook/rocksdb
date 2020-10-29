// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if defined(GFLAGS) && !defined(ROCKSDB_LITE) && defined(LIBZBD)

#include <cstdio>

#include "env/fs_zenfs.h"
#include "util/gflags_compat.h"
using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;

DEFINE_string(zbd, "", "Path to a zoned block device.");
DEFINE_string(aux_path, "",
              "Path for auxiliary file storage (log and lock files).");
DEFINE_bool(force, false, "Force file system creation.");
DEFINE_string(path, "", "Path to directory to list files under");
DEFINE_int32(finish_threshold, 0, "Finish used zones if less than x% left");

namespace ROCKSDB_NAMESPACE {

ZonedBlockDevice *zbd_open() {
  ZonedBlockDevice *zbd = new ZonedBlockDevice(FLAGS_zbd, nullptr);
  IOStatus open_status = zbd->Open();

  if (!open_status.ok()) {
    fprintf(stderr, "Failed to open zoned block device: %s, error: %s\n",
            FLAGS_zbd.c_str(), open_status.ToString().c_str());
    delete zbd;
    return nullptr;
  }

  return zbd;
}

Status zenfs_mount(ZonedBlockDevice *zbd, ZenFS **zenFS) {
  Status s;

  *zenFS = new ZenFS(zbd, FileSystem::Default(), nullptr);
  s = (*zenFS)->Mount();
  if (!s.ok()) {
    delete *zenFS;
    *zenFS = nullptr;
  }

  return s;
}

int zenfs_tool_mkfs() {
  Status s;

  if (FLAGS_aux_path.empty()) {
    fprintf(stderr, "You need to specify --aux_path\n");
    return 1;
  }

  ZonedBlockDevice *zbd = zbd_open();
  if (zbd == nullptr) return 1;

  ZenFS *zenFS;
  s = zenfs_mount(zbd, &zenFS);
  if ((s.ok() || !s.IsNotFound()) && !FLAGS_force) {
    fprintf(
        stderr,
        "Existing filesystem found, use --force if you want to replace it.\n");
    return 1;
  }

  if (zenFS != nullptr) delete zenFS;

  zbd = zbd_open();
  zenFS = new ZenFS(zbd, FileSystem::Default(), nullptr);

  if (FLAGS_aux_path.back() != '/') FLAGS_aux_path.append("/");

  s = zenFS->MkFS(FLAGS_aux_path, FLAGS_finish_threshold);
  if (!s.ok()) {
    fprintf(stderr, "Failed to create file system, error: %s\n",
            s.ToString().c_str());
    delete zenFS;
    return 1;
  }

  fprintf(stdout, "ZenFS file system created. Free space: %lu MB\n",
          zbd->GetFreeSpace() / (1024 * 1024));

  delete zenFS;
  return 0;
}

void list_children(ZenFS *zenFS, std::string path) {
  IOOptions opts;
  IODebugContext dbg;
  std::vector<std::string> result;
  IOStatus io_status = zenFS->GetChildren(path, opts, &result, &dbg);

  if (!io_status.ok()) return;

  for (const auto f : result) {
    fprintf(stdout, "%s\n", f.c_str());
  }
}

int zenfs_tool_list() {
  Status s;
  ZonedBlockDevice *zbd = zbd_open();
  if (zbd == nullptr) return 1;

  ZenFS *zenFS;
  s = zenfs_mount(zbd, &zenFS);
  if (!s.ok()) {
    fprintf(stderr, "Failed to mount filesystem, error: %s\n",
            s.ToString().c_str());
    return 1;
  }

  list_children(zenFS, FLAGS_path);

  return 0;
}

int zenfs_tool_lsuuid() {
  std::map<std::string, std::string>::iterator it;
  std::map<std::string, std::string> zenFileSystems = ListZenFileSystems();

  for (it = zenFileSystems.begin(); it != zenFileSystems.end(); it++)
    fprintf(stdout, "%s\t%s\n", it->first.c_str(), it->second.c_str());

  return 0;
}
}  // namespace ROCKSDB_NAMESPACE

int zenfs_tool(int argc, char **argv) {
  SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                  +" <command> [OPTIONS]...\nCommands: mkfs, list, ls-uuid");
  if (argc < 2) {
    fprintf(stderr, "You need to specify a command.\n");
    return 1;
  }

  std::string subcmd(argv[1]);
  ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_zbd.empty() && subcmd != "ls-uuid") {
    fprintf(stderr, "You need to specify a zoned block device using --zbd\n");
    return 1;
  }
  if (subcmd == "mkfs") {
    return ROCKSDB_NAMESPACE::zenfs_tool_mkfs();
  } else if (subcmd == "list") {
    return ROCKSDB_NAMESPACE::zenfs_tool_list();
  } else if (subcmd == "ls-uuid") {
    return ROCKSDB_NAMESPACE::zenfs_tool_lsuuid();
  } else {
    fprintf(stderr, "Subcommand not recognized: %s\n", subcmd.c_str());
    return 1;
  }

  return 0;
}

#endif  // defined(GFLAGS) && !defined(ROCKSDB_LITE) && defined(LIBZBD)
