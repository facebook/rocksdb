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
  AssertNewSharedExtension(dbOptions, EncryptionConsts::kTypeBlockCipher,
			   EncryptionConsts::kCipherROT13, true, &cipher);
  
  ASSERT_EQ(Status::InvalidArgument(), cipher->SanitizeOptions(dbOptions));
  ASSERT_OK(cipher->SetOption("rocksdb.encrypted.cipher.rot13.blocksize", "13"));
  ASSERT_OK(cipher->SanitizeOptions(dbOptions));
}

template<typename T> void TestConfigureFromString(Status expected,
						    const DBOptions & dbOptions,
						  T * extension,
						  const std::string & props) {
  if (expected.ok()) {
    // Setting a valid property works
    ASSERT_OK(extension->ConfigureFromString(props, dbOptions));
    // And no the extension is valid
    ASSERT_OK(extension->SanitizeOptions(dbOptions));
  } else {
    ASSERT_EQ(expected,
	      extension->ConfigureFromString(props, dbOptions));
  }
}

TEST_F(DBEncryptionTest, ConfigureBlockCipherFromString) {
  shared_ptr<BlockCipher> cipher;
  DBOptions dbOptions;
  Status invalid = Status::InvalidArgument();
  // A new cipher is not valid until its properties are initialized
  AssertNewSharedExtension(dbOptions, EncryptionConsts::kTypeBlockCipher,
			   EncryptionConsts::kCipherROT13, true, &cipher);
  
  TestConfigureFromString(invalid, dbOptions, cipher.get(), "");
  // Settting an unknown property fails
  TestConfigureFromString(invalid, dbOptions, cipher.get(), "unknown=unknown");
  // Settting an unknown property fails to sanitize, even if we ignore errors
  ASSERT_EQ(invalid,
	    cipher->ConfigureFromString("unknown=unknown",
					dbOptions, nullptr, true, false));

  // Setting a valid property works
  TestConfigureFromString(Status::OK(), dbOptions, cipher.get(),
			  "rocksdb.encrypted.cipher.rot13.blocksize=13");
  // Invalid options work if ignored
  ASSERT_OK(cipher->ConfigureFromString("unknown=unknown;"
					"rocksdb.encrypted.cipher.rot13.blocksize=13",
					dbOptions, nullptr, true, false));
  ASSERT_OK(cipher->SanitizeOptions(dbOptions));
}

TEST_F(DBEncryptionTest, NewCTRProvider) {
  shared_ptr<EncryptionProvider> provider;
  DBOptions dbOptions;
  AssertNewSharedExtension(dbOptions, EncryptionConsts::kTypeProvider,
			   EncryptionConsts::kProviderCTR, true, &provider);
  
  ASSERT_EQ(Status::InvalidArgument(), provider->SanitizeOptions(dbOptions));
  ASSERT_EQ(Status::InvalidArgument(),
	    provider->SetOption("rocksdb.encrypted.cipher.rot13.blocksize", "13"));
  ASSERT_EQ(Status::InvalidArgument(),
	    provider->SetOption("rocksdb.encrypted.provider.ctr.cipher.name",
				"unknown"));
  ASSERT_EQ(Status::InvalidArgument(),
	    provider->SetOption("rocksdb.encrypted.provider.ctr.cipher.name",
				"ROT13"));
  ASSERT_EQ(Status::NotFound(),
	    provider->SetOption("rocksdb.encrypted.provider.ctr.cipher.name",
				"unknown", dbOptions, nullptr));
  ASSERT_OK(provider->SetOption("rocksdb.encrypted.provider.ctr.cipher.name",
				"ROT13", dbOptions, nullptr));
  ASSERT_EQ(Status::InvalidArgument(), provider->SanitizeOptions(dbOptions));
  ASSERT_OK(provider->SetOption("rocksdb.encrypted.cipher.rot13.blocksize",
				"13"));
  
  ASSERT_OK(provider->SanitizeOptions(dbOptions));
}
  
TEST_F(DBEncryptionTest, ConfigureProviderFromString) {
  shared_ptr<EncryptionProvider> provider;
  DBOptions dbOptions;
  Status invalid = Status::InvalidArgument();
  AssertNewSharedExtension(dbOptions, EncryptionConsts::kTypeProvider,
			   EncryptionConsts::kProviderCTR, true, &provider);

  // A new provider is not valid until its properties are initialized
  TestConfigureFromString(invalid, dbOptions, provider.get(), "");
  TestConfigureFromString(invalid, dbOptions, provider.get(), "unknown=unknown");
  ASSERT_EQ(Status::InvalidArgument(),
	    provider->ConfigureFromString("unknown=unknown", dbOptions, nullptr, true, false));

  // Cannot set the cipher properties until one is initialized
  TestConfigureFromString(invalid, dbOptions, provider.get(),
			  "rocksdb.encrypted.cipher.rot13.blocksize=13");
  // Create an invalid cipher fails
  TestConfigureFromString(Status::NotFound(), dbOptions, provider.get(),
			  "rocksdb.encrypted.provider.ctr.cipher.name=unknown");
  // Create a valid cipher but not initializing it also fails to sanitize
  TestConfigureFromString(invalid, dbOptions, provider.get(),
			  "rocksdb.encrypted.provider.ctr.cipher.name=ROT13");
  // Create a valid cipher and initializing it works
  TestConfigureFromString(Status::OK(), dbOptions, provider.get(),
			  "rocksdb.encrypted.provider.ctr.cipher.name=ROT13;"
			  "rocksdb.encrypted.cipher.rot13.blocksize=13");
  // And one more time to make sure we have a "clean" provider
  AssertNewSharedExtension(dbOptions, EncryptionConsts::kTypeProvider,
			   EncryptionConsts::kProviderCTR, true, &provider);
  ASSERT_EQ(Status::InvalidArgument(), provider->SanitizeOptions(dbOptions));
  TestConfigureFromString(Status::OK(), dbOptions, provider.get(),
			  "rocksdb.encrypted.provider.ctr.cipher.name=ROT13;"
			  "rocksdb.encrypted.cipher.rot13.blocksize=13");
  // And one more time, with th properties in a different order...
  AssertNewSharedExtension(dbOptions, EncryptionConsts::kTypeProvider,
			   EncryptionConsts::kProviderCTR, true, &provider);
  ASSERT_EQ(Status::InvalidArgument(), provider->SanitizeOptions(dbOptions));
  TestConfigureFromString(Status::OK(), dbOptions, provider.get(),
			  "rocksdb.encrypted.cipher.rot13.blocksize=13;"
			  "rocksdb.encrypted.provider.ctr.cipher.name=ROT13;");
}

TEST_F(DBEncryptionTest, NewEncryptedEnv) {
  Env *encrypted;
  unique_ptr<Env> guard;
  DBOptions dbOptions;
  AssertNewExtension(dbOptions, Env::kTypeEnvironment, "unknown", false,
		     &encrypted, false, &guard);
  AssertNewExtension(dbOptions, Env::kTypeEnvironment,
		     EncryptionConsts::kEnvEncrypted, true, &encrypted, false, &guard);
  ASSERT_EQ(Status::InvalidArgument(), encrypted->SanitizeOptions(dbOptions));
  guard.reset(encrypted); // Store in guard for clean-up
  ASSERT_EQ(Status::InvalidArgument(),
	    encrypted->SetOption("rocksdb.encrypted.cipher.rot13.blocksize", "13"));
  ASSERT_EQ(Status::InvalidArgument(),
	    encrypted->SetOption("rocksdb.encrypted.provider.ctr.cipher.name",
				 "ROT13"));
  ASSERT_EQ(Status::NotFound(),
	    encrypted->SetOption("rocksdb.encrypted.env.provider.name",
				 "unknown", dbOptions, nullptr));
  ASSERT_OK(encrypted->SetOption("rocksdb.encrypted.env.provider.name",
				 "CTR", dbOptions, nullptr));
  ASSERT_EQ(Status::InvalidArgument(), encrypted->SanitizeOptions(dbOptions));
  ASSERT_OK(encrypted->SetOption("rocksdb.encrypted.provider.ctr.cipher.name",
				 "ROT13", dbOptions, nullptr));
  ASSERT_EQ(Status::InvalidArgument(), encrypted->SanitizeOptions(dbOptions));
  ASSERT_OK(encrypted->SetOption("rocksdb.encrypted.cipher.rot13.blocksize", "13"));
  ASSERT_OK(encrypted->SanitizeOptions(dbOptions));
}

TEST_F(DBEncryptionTest, EnryptedEnvFromString) {
  Env *encrypted;
  unique_ptr<Env> guard;
  DBOptions dbOptions;
  Status invalid = Status::InvalidArgument();
  AssertNewExtension(dbOptions, Env::kTypeEnvironment,
		     EncryptionConsts::kEnvEncrypted, true, &encrypted, false, &guard);
  guard.reset(encrypted);
  TestConfigureFromString(invalid, dbOptions, encrypted,
			  "rocksdb.encrypted.cipher.rot13.blocksize=13;"
			  "rocksdb.encrypted.provider.ctr.cipher.name=ROT13;");
  TestConfigureFromString(invalid, dbOptions, encrypted,
			  "rocksdb.encrypted.env.provider.name=CTR;");
  TestConfigureFromString(invalid, dbOptions, encrypted,
			  "rocksdb.encrypted.env.provider.name=CTR;"
			  "rocksdb.encrypted.provider.ctr.cipher.name=ROT13;");
  TestConfigureFromString(Status::OK(), dbOptions, encrypted,
			  "rocksdb.encrypted.cipher.rot13.blocksize=13;"
			  "rocksdb.encrypted.env.provider.name=CTR;"
			  "rocksdb.encrypted.provider.ctr.cipher.name=ROT13;");
  AssertNewExtension(dbOptions, Env::kTypeEnvironment,
		     EncryptionConsts::kEnvEncrypted, true, &encrypted, false, &guard);
  guard.reset(encrypted);
  TestConfigureFromString(Status::OK(), dbOptions, encrypted,
			  "rocksdb.encrypted.cipher.rot13.blocksize=13;"
			  "rocksdb.encrypted.env.provider.name=CTR;"
			  "rocksdb.encrypted.provider.ctr.cipher.name=ROT13;");
  AssertNewExtension(dbOptions, Env::kTypeEnvironment,
		     EncryptionConsts::kEnvEncrypted, true, &encrypted, false, &guard);
  guard.reset(encrypted);
  TestConfigureFromString(Status::OK(), dbOptions, encrypted,
			  "rocksdb.encrypted.cipher.rot13.blocksize=13;"
			  "rocksdb.encrypted.env.provider.name=CTR;"
			  "rocksdb.encrypted.provider.ctr.cipher.name=ROT13;");
  AssertNewExtension(dbOptions, Env::kTypeEnvironment,
		     EncryptionConsts::kEnvEncrypted, true, &encrypted, false, &guard);
  guard.reset(encrypted);
  TestConfigureFromString(Status::OK(), dbOptions, encrypted,
			  "rocksdb.encrypted.env.provider.name=CTR;"
			  "rocksdb.encrypted.provider.ctr.cipher.name=ROT13;"
			  "rocksdb.encrypted.cipher.rot13.blocksize=13;");
  AssertNewExtension(dbOptions, Env::kTypeEnvironment,
		     EncryptionConsts::kEnvEncrypted, true, &encrypted, false, &guard);
  guard.reset(encrypted);
  TestConfigureFromString(Status::OK(), dbOptions, encrypted,
			  "rocksdb.encrypted.provider.ctr.cipher.name=ROT13;"
			  "rocksdb.encrypted.env.provider.name=CTR;"
			  "rocksdb.encrypted.cipher.rot13.blocksize=13;");
}
#endif

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
