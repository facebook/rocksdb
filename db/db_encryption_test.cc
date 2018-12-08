//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/env_encryption.h"
#include "rocksdb/extension_loader.h"
#include "rocksdb/perf_context.h"
#if !defined(ROCKSDB_LITE)
#include "extensions/extension_test.h"
#include "util/sync_point.h"
#endif
#include <iostream>
#include <string>

namespace rocksdb {

class DBEncryptionTest : public DBTestBase {
 public:
  DBEncryptionTest() : DBTestBase("/db_encryption_test") {}
};

TEST_F(DBEncryptionTest, CheckEncrypted) {
  ASSERT_OK(Put("foo567", "v1.fetdq"));
  ASSERT_OK(Put("bar123", "v2.dfgkjdfghsd"));
  Close();

  // Open all files and look for the values we've put in there.
  // They should not be found if encrypted, otherwise
  // they should be found.
  std::vector<std::string> fileNames;
  auto status = env_->GetChildren(dbname_, &fileNames);
  ASSERT_OK(status);

  auto defaultEnv = Env::Default();
  int hits = 0;
  for (auto it = fileNames.begin() ; it != fileNames.end(); ++it) {
    if ((*it == "..") || (*it == ".")) {
      continue;
    }
    auto filePath = dbname_ + "/" + *it;
    unique_ptr<SequentialFile> seqFile;
    auto envOptions = EnvOptions(CurrentOptions());
    status = defaultEnv->NewSequentialFile(filePath, &seqFile, envOptions);
    ASSERT_OK(status);

    uint64_t fileSize;
    status = defaultEnv->GetFileSize(filePath, &fileSize);
    ASSERT_OK(status);

    std::string scratch;
    scratch.reserve(fileSize);
    Slice data;
    status = seqFile->Read(fileSize, &data, (char*)scratch.data());
    ASSERT_OK(status);

    if (data.ToString().find("foo567") != std::string::npos) {
      hits++; 
      //std::cout << "Hit in " << filePath << "\n";
    }
    if (data.ToString().find("v1.fetdq") != std::string::npos) {
      hits++; 
      //std::cout << "Hit in " << filePath << "\n";
    }
    if (data.ToString().find("bar123") != std::string::npos) {
      hits++; 
      //std::cout << "Hit in " << filePath << "\n";
    }
    if (data.ToString().find("v2.dfgkjdfghsd") != std::string::npos) {
      hits++; 
      //std::cout << "Hit in " << filePath << "\n";
    }
    if (data.ToString().find("dfgk") != std::string::npos) {
      hits++; 
      //std::cout << "Hit in " << filePath << "\n";
    }
  }
  if (encrypted_env_) {
    ASSERT_EQ(hits, 0);
  } else {
    ASSERT_GE(hits, 4);
  }
}

#ifndef ROCKSDB_LITE

TEST_F(DBEncryptionTest, NewBlockCipher) {
  shared_ptr<BlockCipher> cipher;
  DBOptions dbOptions;
  AssertNewSharedExtension(dbOptions, "ROT13", true, &cipher);
  
  ASSERT_EQ(Status::InvalidArgument(), cipher->SanitizeOptions(dbOptions));
  ASSERT_OK(cipher->SetOption("rocksdb.encrypted.cipher.rot13.blocksize", "13"));
  ASSERT_OK(cipher->SanitizeOptions(dbOptions));
}

template<typename T> void TestConfigureFromString(Status expected,
						  const DBOptions & dbOptions,
						  T * extension,
						  const std::string & props,
						  Status sanitized) {
  if (expected.ok()) {
    // Setting a valid property works
    ASSERT_OK(extension->ConfigureFromString(dbOptions, props));
  } else {
    ASSERT_EQ(expected,
	      extension->ConfigureFromString(dbOptions, props));
  }
  if (sanitized.ok()) {
    // And no the extension is valid
    ASSERT_OK(extension->SanitizeOptions(dbOptions));
  } else {
    ASSERT_EQ(sanitized, extension->SanitizeOptions(dbOptions));
  }
}

TEST_F(DBEncryptionTest, ConfigureBlockCipherFromString) {
  shared_ptr<BlockCipher> cipher;
  DBOptions dbOptions;
  Status notfound = Status::NotFound();
  Status okay   = Status::OK();
  Status invalid = Status::InvalidArgument();
  // A new cipher is not valid until its properties are initialized
  AssertNewSharedExtension(dbOptions, "ROT13", true, &cipher);
  TestConfigureFromString(okay, dbOptions, cipher.get(), "", invalid);
  
  // Settting an unknown property fails
  TestConfigureFromString(notfound, dbOptions, cipher.get(),
			  "unknown=unknown", invalid);
  // Setting a valid property works
  TestConfigureFromString(okay, dbOptions, cipher.get(),
			  "rocksdb.encrypted.cipher.rot13.blocksize=13", okay);
}

TEST_F(DBEncryptionTest, NewCTRProvider) {
  shared_ptr<EncryptionProvider> provider;
  DBOptions dbOptions;
  AssertNewSharedExtension(dbOptions, "CTR", true, &provider);
  ASSERT_EQ(Status::InvalidArgument(), provider->SanitizeOptions(dbOptions));
  ASSERT_EQ(Status::NotFound(),
	    provider->SetOption("rocksdb.encrypted.cipher.rot13.blocksize", "13"));
  ASSERT_EQ(Status::InvalidArgument(),
  	    provider->SetOption("rocksdb.encrypted.provider.ctr.cipher.name",
  				"unknown"));
  ASSERT_EQ(Status::InvalidArgument(),
  	    provider->SetOption("rocksdb.encrypted.provider.ctr.cipher.name",
  				"ROT13"));
  ASSERT_EQ(Status::InvalidArgument(),
	    provider->SetOption(dbOptions,
				"rocksdb.encrypted.provider.ctr.cipher.name",
				"unknown"));
  ASSERT_OK(provider->SetOption(dbOptions,
				"rocksdb.encrypted.provider.ctr.cipher.name",
				"ROT13"));
  ASSERT_EQ(Status::InvalidArgument(), provider->SanitizeOptions(dbOptions));
  ASSERT_OK(provider->SetOption("rocksdb.encrypted.cipher.rot13.blocksize",
				"13"));
  
  ASSERT_OK(provider->SanitizeOptions(dbOptions));
}
  
TEST_F(DBEncryptionTest, ConfigureProviderFromString) {
  shared_ptr<EncryptionProvider> provider;
  DBOptions dbOptions;
  Status notfound = Status::NotFound();
  Status invalid = Status::InvalidArgument();
  Status okay = Status::OK();
  AssertNewSharedExtension(dbOptions, "CTR", true, &provider);
  // A new provider is not valid until its properties are initialized
  TestConfigureFromString(okay, dbOptions, provider.get(), "", invalid);
  TestConfigureFromString(notfound, dbOptions, provider.get(),
			  "unknown=unknown", invalid);
  // Cannot set the cipher properties until one is initialized
  TestConfigureFromString(notfound, dbOptions, provider.get(),
			  "rocksdb.encrypted.cipher.rot13.blocksize=13", invalid);
  TestConfigureFromString(invalid, dbOptions, provider.get(),
			  "rocksdb.encrypted.provider.ctr.cipher.name=unknown",
			  invalid);
  // Create a valid cipher but not initializing it also fails to sanitize
  TestConfigureFromString(okay, dbOptions, provider.get(),
			  "rocksdb.encrypted.provider.ctr.cipher.name=ROT13",
			  invalid);
			  
  // Create a valid cipher and initializing it works
  TestConfigureFromString(okay, dbOptions, provider.get(),
			  "rocksdb.encrypted.provider.ctr.cipher.name=ROT13;"
			  "rocksdb.encrypted.cipher.rot13.blocksize=13",
			  okay);
  // And one more time to make sure we have a "clean" provider
  AssertNewSharedExtension(dbOptions, "CTR", true, &provider);
  ASSERT_EQ(invalid, provider->SanitizeOptions(dbOptions));
  TestConfigureFromString(okay, dbOptions, provider.get(),
			  "rocksdb.encrypted.provider.ctr.cipher.name=ROT13;"
			  "rocksdb.encrypted.cipher.rot13.blocksize=13",
			  okay);
  // And one more time, with th properties in a different order...
  AssertNewSharedExtension(dbOptions, "CTR", true, &provider);
  TestConfigureFromString(okay, dbOptions, provider.get(),
			  "rocksdb.encrypted.cipher.rot13.blocksize=13;"
			  "rocksdb.encrypted.provider.ctr.cipher.name=ROT13;",
			  okay);
}

 TEST_F(DBEncryptionTest, ConfigureProviderFromProperties) {
  shared_ptr<EncryptionProvider> provider;
  DBOptions dbOptions;
  Status bad = Status::NotSupported(); //TODO
  Status okay = Status::OK();
  AssertNewSharedExtension(dbOptions, "CTR", true, &provider);
  ASSERT_EQ(Status::InvalidArgument(),
	    provider->ConfigureFromString(dbOptions,
					  "rocksdb.encrypted.provider.ctr.cipher={"
					  "name=unknown"
					  "}"));
  ASSERT_EQ(Status::NotFound(),
	    provider->ConfigureFromString(dbOptions,
					  "rocksdb.encrypted.provider.ctr.cipher={"
					  "name=ROT13;"
					  "options={unknown=unknown}"
					  "}"));
  ASSERT_OK(provider->ConfigureFromString(dbOptions,
					  "rocksdb.encrypted.provider.ctr.cipher={"
					  "name=ROT13;"
					  "options={rocksdb.encrypted.cipher.rot13.blocksize=13;}"
					  "}"));
  ASSERT_OK(provider->SanitizeOptions(dbOptions));
 }

static void AssertNewEnvironment(const DBOptions & dbOpts,
				 const std::string & name,
				 bool isValid,
				 std::unique_ptr<Env> *guard,
				 bool) {
  Env *env;
  AssertNewUniqueExtension(dbOpts, name, isValid, &env, guard, false);
  guard->reset(env);
}
  
TEST_F(DBEncryptionTest, NewEncryptedEnv) {
  std::unique_ptr<Env> encrypted;
  DBOptions dbOptions;
  AssertNewEnvironment(dbOptions, "unknown", false, &encrypted, false);
  AssertNewEnvironment(dbOptions, "encrypted", true, &encrypted, false);
  ASSERT_EQ(Status::InvalidArgument(), encrypted->SanitizeOptions(dbOptions));
  ASSERT_EQ(Status::NotFound(),
	    encrypted->SetOption("rocksdb.encrypted.cipher.rot13.blocksize", "13"));
  ASSERT_EQ(Status::NotFound(),
	    encrypted->SetOption("rocksdb.encrypted.provider.ctr.cipher.name",
				 "ROT13"));
  ASSERT_EQ(Status::InvalidArgument(),
	    encrypted->SetOption(dbOptions, "rocksdb.encrypted.env.provider.name",
				 "unknown"));
  ASSERT_OK(encrypted->SetOption(dbOptions, "rocksdb.encrypted.env.provider.name",
				 "CTR"));
  ASSERT_EQ(Status::InvalidArgument(), encrypted->SanitizeOptions(dbOptions));
  ASSERT_OK(encrypted->SetOption(dbOptions,
				 "rocksdb.encrypted.provider.ctr.cipher.name",
				 "ROT13"));
  ASSERT_EQ(Status::InvalidArgument(), encrypted->SanitizeOptions(dbOptions));
  ASSERT_OK(encrypted->SetOption("rocksdb.encrypted.cipher.rot13.blocksize", "13"));
  ASSERT_OK(encrypted->SanitizeOptions(dbOptions));
}

TEST_F(DBEncryptionTest, EnryptedEnvFromString) {
  unique_ptr<Env> encrypted;
  DBOptions dbOptions;
  Status invalid = Status::InvalidArgument();
  Status okay = Status::OK();
  Status notfound = Status::NotFound();
  
  AssertNewEnvironment(dbOptions, "encrypted", true, &encrypted, false);
  TestConfigureFromString(notfound, dbOptions, encrypted.get(),
			  "rocksdb.encrypted.cipher.rot13.blocksize=13;"
			  "rocksdb.encrypted.provider.ctr.cipher.name=ROT13;",
			  invalid);
  TestConfigureFromString(okay, dbOptions, encrypted.get(),
			  "rocksdb.encrypted.env.provider.name=CTR;", invalid);
  TestConfigureFromString(okay, dbOptions, encrypted.get(),
			  "rocksdb.encrypted.env.provider.name=CTR;"
			  "rocksdb.encrypted.provider.ctr.cipher.name=ROT13;",
			  invalid);
  TestConfigureFromString(okay, dbOptions, encrypted.get(),
			  "rocksdb.encrypted.cipher.rot13.blocksize=13;"
			  "rocksdb.encrypted.env.provider.name=CTR;"
			  "rocksdb.encrypted.provider.ctr.cipher.name=ROT13;",
			  okay);
  AssertNewEnvironment(dbOptions, "encrypted", true, &encrypted, false);
  TestConfigureFromString(okay, dbOptions, encrypted.get(),
			  "rocksdb.encrypted.cipher.rot13.blocksize=13;"
			  "rocksdb.encrypted.env.provider.name=CTR;"
			  "rocksdb.encrypted.provider.ctr.cipher.name=ROT13;",
			  okay);
  AssertNewEnvironment(dbOptions, "encrypted", true, &encrypted, false);
  TestConfigureFromString(okay, dbOptions, encrypted.get(),
			  "rocksdb.encrypted.cipher.rot13.blocksize=13;"
			  "rocksdb.encrypted.env.provider.name=CTR;"
			  "rocksdb.encrypted.provider.ctr.cipher.name=ROT13;",
			  okay);
  AssertNewEnvironment(dbOptions, "encrypted", true, &encrypted, false);
  TestConfigureFromString(okay, dbOptions, encrypted.get(),
			  "rocksdb.encrypted.env.provider.name=CTR;"
			  "rocksdb.encrypted.provider.ctr.cipher.name=ROT13;"
			  "rocksdb.encrypted.cipher.rot13.blocksize=13;", okay);
  AssertNewEnvironment(dbOptions, "encrypted", true, &encrypted, false);
  TestConfigureFromString(okay, dbOptions, encrypted.get(),
			  "rocksdb.encrypted.provider.ctr.cipher.name=ROT13;"
			  "rocksdb.encrypted.env.provider.name=CTR;"
			  "rocksdb.encrypted.cipher.rot13.blocksize=13;", okay);
}
#endif

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
