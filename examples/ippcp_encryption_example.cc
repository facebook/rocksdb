//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  Copyright (c) 2020 Intel Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/ippcp_encryption_provider.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/options_util.h"

using namespace ROCKSDB_NAMESPACE;

std::string kDBPath = "/tmp/ippcp_encryption_example";

int main() {
#ifdef IPPCP
  DB* db;
  Options options;
  options.create_if_missing = true;

  std::shared_ptr<EncryptionProvider> provider;
  Status status = EncryptionProvider::CreateFromString(
      ConfigOptions(), IppcpEncryptionProvider::kName(), &provider);
  assert(status.ok());

  // Create AES-256 block cipher
  status =
      provider->AddCipher("", "a6d2ae2816157e2b3c4fcf098815f7xb",
                          IppcpEncryptionProvider::KeySize::AES_256, false);
  assert(status.ok());

  options.env = NewEncryptedEnv(Env::Default(), provider);

  status = DB::Open(options, kDBPath, &db);
  assert(status.ok());

  setbuf(stdout, NULL);
  printf("writing 1M records...");
  WriteOptions w_opts;
  for (int i = 0; i < 1000000; ++i) {
    status = db->Put(w_opts, std::to_string(i), std::to_string(i * i));
    assert(status.ok());
  }
  db->Flush(FlushOptions());
  printf("done.\n");

  printf("reading 1M records...");
  std::string value;
  ReadOptions r_opts;
  for (int i = 0; i < 1000000; ++i) {
    status = db->Get(r_opts, std::to_string(i), &value);
    assert(status.ok());
    assert(value == std::to_string(i * i));
  }
  printf("done.\n");

  // Close database
  status = db->Close();
  assert(status.ok());
  status = DestroyDB(kDBPath, options);
  assert(status.ok());

  return 0;
#else
  fprintf(stderr, "ipp-crypto library not found and requires SSE2+.");
#endif  // IPPCP
}
