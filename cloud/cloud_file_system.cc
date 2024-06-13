// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include "rocksdb/cloud/cloud_file_system.h"
#ifndef _WIN32_WINNT
#include <unistd.h>
#else
#include <windows.h>
#endif
#include <unordered_map>

#include "cloud/aws/aws_file_system.h"
#include "cloud/cloud_log_controller_impl.h"
#include "cloud/cloud_manifest.h"
#include "cloud/db_cloud_impl.h"
#include "cloud/filename.h"
#include "env/composite_env_wrapper.h"
#include "options/configurable_helper.h"
#include "options/options_helper.h"
#include "port/likely.h"
#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "rocksdb/cloud/cloud_log_controller.h"
#include "rocksdb/cloud/cloud_storage_provider_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

void CloudFileSystemOptions::Dump(Logger* log) const {
  auto provider = storage_provider.get();
  auto controller = cloud_log_controller.get();
  Header(log, "                         COptions.cloud_type: %s",
         (provider != nullptr) ? provider->Name() : "Unknown");
  Header(log, "                    COptions.src_bucket_name: %s",
         src_bucket.GetBucketName().c_str());
  Header(log, "                    COptions.src_object_path: %s",
         src_bucket.GetObjectPath().c_str());
  Header(log, "                  COptions.src_bucket_region: %s",
         src_bucket.GetRegion().c_str());
  Header(log, "                   COptions.dest_bucket_name: %s",
         dest_bucket.GetBucketName().c_str());
  Header(log, "                   COptions.dest_object_path: %s",
         dest_bucket.GetObjectPath().c_str());
  Header(log, "                 COptions.dest_bucket_region: %s",
         dest_bucket.GetRegion().c_str());
  Header(log, "                           COptions.log_type: %s",
         (controller != nullptr) ? controller->Name() : "None");
  Header(log, "               COptions.keep_local_sst_files: %d",
         keep_local_sst_files);
  Header(log, "               COptions.keep_local_log_files: %d",
         keep_local_log_files);
  Header(log, "             COptions.server_side_encryption: %d",
         server_side_encryption);
  Header(log, "                  COptions.encryption_key_id: %s",
         encryption_key_id.c_str());
  Header(log, "           COptions.create_bucket_if_missing: %s",
         create_bucket_if_missing ? "true" : "false");
  Header(log, "                         COptions.run_purger: %s",
         run_purger ? "true" : "false");
  Header(log, "           COptions.resync_on_open: %s",
         resync_on_open ? "true" : "false");
  Header(log, "             COptions.skip_dbid_verification: %s",
         skip_dbid_verification ? "true" : "false");
  Header(log, "           COptions.use_aws_transfer_manager: %s",
         use_aws_transfer_manager ? "true" : "false");
  Header(log, "           COptions.number_objects_listed_in_one_iteration: %d",
         number_objects_listed_in_one_iteration);
  Header(log, "   COptions.use_direct_io_for_cloud_download: %d",
         use_direct_io_for_cloud_download);
  Header(log, "        COptions.roll_cloud_manifest_on_open: %d",
         roll_cloud_manifest_on_open);
  Header(log, "                     COptions.cookie_on_open: %s",
         cookie_on_open.c_str());
  Header(log, "                 COptions.new_cookie_on_open: %s",
         new_cookie_on_open.c_str());
  Header(log, "COptions.delete_cloud_invisible_files_on_open: %d",
         delete_cloud_invisible_files_on_open);
  if (cloud_file_deletion_delay) {
    Header(log, "          COptions.cloud_file_deletion_delay: %ld",
           cloud_file_deletion_delay->count());
  }
}

bool CloudFileSystemOptions::GetNameFromEnvironment(const char* name,
                                                    const char* alt,
                                                    std::string* result) {
  char* value = getenv(name);  // See if name is set in the environment
  if (value == nullptr &&
      alt != nullptr) {   // Not set.  Do we have an alt name?
    value = getenv(alt);  // See if alt is in the environment
  }
  if (value != nullptr) {   // Did we find the either name/alt in the env?
    result->assign(value);  // Yes, update result
    return true;            // And return success
  } else {
    return false;  // No, return not found
  }
}
void CloudFileSystemOptions::TEST_Initialize(const std::string& bucket,
                                             const std::string& object,
                                             const std::string& region) {
  src_bucket.TEST_Initialize(bucket, object, region);
  dest_bucket = src_bucket;
}

BucketOptions::BucketOptions() {
  if (!CloudFileSystemOptions::GetNameFromEnvironment(
          "ROCKSDB_CLOUD_TEST_BUCKET_PREFIX", "ROCKSDB_CLOUD_BUCKET_PREFIX",
          &prefix_)) {
    prefix_ = "rockset.";
  }
  if (CloudFileSystemOptions::GetNameFromEnvironment(
          "ROCKSDB_CLOUD_TEST_BUCKET_NAME", "ROCKSDB_CLOUD_BUCKET_NAME",
          &bucket_)) {
    name_ = prefix_ + bucket_;
  }
  CloudFileSystemOptions::GetNameFromEnvironment(
      "ROCKSDB_CLOUD_TEST_OBECT_PATH", "ROCKSDB_CLOUD_OBJECT_PATH", &object_);
  CloudFileSystemOptions::GetNameFromEnvironment(
      "ROCKSDB_CLOUD_TEST_REGION", "ROCKSDB_CLOUD_REGION", &region_);
}

void BucketOptions::SetName() {
  if (bucket_.empty()) {
    name_.clear();
  } else {
    name_ = prefix_ + bucket_;
  }
}

void BucketOptions::SetBucketName(const std::string& bucket,
                                  const std::string& prefix) {
  if (!prefix.empty()) {
    prefix_ = prefix;
  }

  bucket_ = bucket;
  SetName();
}

void BucketOptions::SetBucketPrefix(std::string prefix) {
  prefix_ = std::move(prefix);
  SetName();
}

// Initializes the bucket properties

void BucketOptions::TEST_Initialize(const std::string& bucket,
                                    const std::string& object,
                                    const std::string& region) {
  std::string prefix;
  // If the bucket name is not set, then the bucket name is not set,
  // Set it to either the value of the environment variable or geteuid
  if (bucket_.empty()) {
    std::string uid;
#ifdef _WIN32_WINNT
    char user_name[257];  // UNLEN + 1
    DWORD dwsize = sizeof(user_name);
    if (!::GetUserName(user_name, &dwsize)) {
      uid = "unknown";
    } else {
      uid = std::string(user_name, static_cast<std::string::size_type>(dwsize));
    }
#else
    uid = std::to_string(geteuid());
#endif
    if (EndsWith(bucket, ".")) {
      SetBucketName(bucket + uid);
    } else {
      SetBucketName(bucket + "." + uid);
    }
  }
  if (object_.empty()) {
    object_ = object;
  }
  if (region_.empty()) {
    region_ = region;
  }
}

static std::unordered_map<std::string, OptionTypeInfo>
    bucket_options_type_info = {
        {"object",
         {0, OptionType::kString, OptionVerificationType::kNormal,
          OptionTypeFlags::kCompareNever,
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const std::string& value, void* addr) {
            auto bucket = static_cast<BucketOptions*>(addr);
            bucket->SetObjectPath(value);
            return Status::OK();
          },
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const void* addr, std::string* value) {
            auto bucket = static_cast<const BucketOptions*>(addr);
            *value = bucket->GetObjectPath();
            return Status::OK();
          },
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const void* addr1, const void* addr2, std::string* /*mismatch*/) {
            auto bucket1 = static_cast<const BucketOptions*>(addr1);
            auto bucket2 = static_cast<const BucketOptions*>(addr2);
            return bucket1->GetObjectPath() == bucket2->GetObjectPath();
          }}},
        {"region",
         {0, OptionType::kString, OptionVerificationType::kNormal,
          OptionTypeFlags::kCompareNever,
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const std::string& value, void* addr) {
            auto bucket = static_cast<BucketOptions*>(addr);
            bucket->SetRegion(value);
            return Status::OK();
          },
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const void* addr, std::string* value) {
            auto bucket = static_cast<const BucketOptions*>(addr);
            *value = bucket->GetRegion();
            return Status::OK();
          },
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const void* addr1, const void* addr2, std::string* /*mismatch*/) {
            auto bucket1 = static_cast<const BucketOptions*>(addr1);
            auto bucket2 = static_cast<const BucketOptions*>(addr2);
            return bucket1->GetRegion() == bucket2->GetRegion();
          }}},
        {"prefix",
         {0, OptionType::kString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone,
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const std::string& value, void* addr) {
            auto bucket = static_cast<BucketOptions*>(addr);
            bucket->SetBucketName(bucket->GetBucketName(false), value);
            return Status::OK();
          },
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const void* addr, std::string* value) {
            auto bucket = static_cast<const BucketOptions*>(addr);
            *value = bucket->GetBucketPrefix();
            return Status::OK();
          },
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const void* addr1, const void* addr2, std::string* /*mismatch*/) {
            auto bucket1 = static_cast<const BucketOptions*>(addr1);
            auto bucket2 = static_cast<const BucketOptions*>(addr2);
            return bucket1->GetBucketPrefix() == bucket2->GetBucketPrefix();
          }}},
        {"bucket",
         {0, OptionType::kString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone,
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const std::string& value, void* addr) {
            auto bucket = static_cast<BucketOptions*>(addr);
            bucket->SetBucketName(value);
            return Status::OK();
          },
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const void* addr, std::string* value) {
            auto bucket = static_cast<const BucketOptions*>(addr);
            *value = bucket->GetBucketName(false);
            return Status::OK();
          },
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const void* addr1, const void* addr2, std::string* /*mismatch*/) {
            auto bucket1 = static_cast<const BucketOptions*>(addr1);
            auto bucket2 = static_cast<const BucketOptions*>(addr2);
            return bucket1->GetBucketName(false) ==
                   bucket2->GetBucketName(false);
          }}},
        {"TEST",
         {0, OptionType::kUnknown, OptionVerificationType::kAlias,
          OptionTypeFlags::kNone,
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const std::string& value, void* addr) {
            auto bucket = static_cast<BucketOptions*>(addr);
            std::string name = value;
            std::string path;
            std::string region;
            auto pos = name.find(":");
            if (pos != std::string::npos) {
              path = name.substr(pos + 1);
              name = name.substr(0, pos);
            }
            pos = path.find("?");
            if (pos != std::string::npos) {
              region = path.substr(pos + 1);
              path = path.substr(0, pos);
            }
            bucket->TEST_Initialize(name, path, region);
            return Status::OK();
          }}},
};

static CloudFileSystemOptions dummy_ceo_options;
template <typename T1>
int offset_of(T1 CloudFileSystemOptions::*member) {
  return int(size_t(&(dummy_ceo_options.*member)) - size_t(&dummy_ceo_options));
}

const std::unordered_map<std::string, OptionTypeInfo>
    CloudFileSystemOptions::cloud_fs_option_type_info = {
        {"keep_local_sst_files",
         {offset_of(&CloudFileSystemOptions::keep_local_sst_files),
          OptionType::kBoolean}},
        {"keep_local_log_files",
         {offset_of(&CloudFileSystemOptions::keep_local_log_files),
          OptionType::kBoolean}},
        {"create_bucket_if_missing",
         {offset_of(&CloudFileSystemOptions::create_bucket_if_missing),
          OptionType::kBoolean}},
        {"validate_filesize",
         {offset_of(&CloudFileSystemOptions::validate_filesize),
          OptionType::kBoolean}},
        {"skip_dbid_verification",
         {offset_of(&CloudFileSystemOptions::skip_dbid_verification),
          OptionType::kBoolean}},
        {"resync_on_open",
         {offset_of(&CloudFileSystemOptions::resync_on_open),
          OptionType::kBoolean}},
        {"skip_cloud_children_files",
         {offset_of(&CloudFileSystemOptions::skip_cloud_files_in_getchildren),
          OptionType::kBoolean}},
        {"constant_sst_file_size_in_manager",
         {offset_of(&CloudFileSystemOptions::
                        constant_sst_file_size_in_sst_file_manager),
          OptionType::kInt64T}},
        {"run_purger",
         {offset_of(&CloudFileSystemOptions::run_purger),
          OptionType::kBoolean}},
        {"purger_periodicity_ms",
         {offset_of(&CloudFileSystemOptions::purger_periodicity_millis),
          OptionType::kUInt64T}},

        {"provider",
         {offset_of(&CloudFileSystemOptions::storage_provider),
          OptionType::kConfigurable, OptionVerificationType::kByNameAllowNull,
          (OptionTypeFlags::kShared | OptionTypeFlags::kCompareLoose |
           OptionTypeFlags::kCompareNever | OptionTypeFlags::kAllowNull),
          [](const ConfigOptions& opts, const std::string& /*name*/,
             const std::string& value, void* addr) {
            auto provider =
                static_cast<std::shared_ptr<CloudStorageProvider>*>(addr);
            return CloudStorageProvider::CreateFromString(opts, value,
                                                          provider);
          }}},
        {"controller",
         {offset_of(&CloudFileSystemOptions::cloud_log_controller),
          OptionType::kConfigurable, OptionVerificationType::kByNameAllowNull,
          (OptionTypeFlags::kShared | OptionTypeFlags::kCompareLoose |
           OptionTypeFlags::kCompareNever | OptionTypeFlags::kAllowNull),
          // Creates a new TableFactory based on value
          [](const ConfigOptions& opts, const std::string& /*name*/,
             const std::string& value, void* addr) {
            auto controller =
                static_cast<std::shared_ptr<CloudLogController>*>(addr);
            Status s =
                CloudLogController::CreateFromString(opts, value, controller);
            return s;
          }}},
        {"src", OptionTypeInfo::Struct(
                    "src", &bucket_options_type_info,
                    offset_of(&CloudFileSystemOptions::src_bucket),
                    OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
        {"dest", OptionTypeInfo::Struct(
                     "dest", &bucket_options_type_info,
                     offset_of(&CloudFileSystemOptions::dest_bucket),
                     OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
        {"TEST",
         {0, OptionType::kUnknown, OptionVerificationType::kAlias,
          OptionTypeFlags::kNone,
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const std::string& value, void* addr) {
            auto copts = static_cast<CloudFileSystemOptions*>(addr);
            std::string name;
            std::string path;
            std::string region;
            auto pos = value.find(":");
            if (pos != std::string::npos) {
              name = value.substr(0, pos);
              path = value.substr(pos + 1);
            }
            pos = path.find("?");
            if (pos != std::string::npos) {
              region = path.substr(pos + 1);
              path = path.substr(0, pos);
            }
            copts->src_bucket.TEST_Initialize(name, path, region);
            copts->dest_bucket.TEST_Initialize(name, path, region);
            return Status::OK();
          }}},
};

Status CloudFileSystemOptions::Configure(const ConfigOptions& config_options,
                                         const std::string& opts_str) {
  std::string current;
  Status s;
  if (!config_options.ignore_unknown_options) {
    s = Serialize(config_options, &current);
    if (!s.ok()) {
      return s;
    }
  }
  if (s.ok()) {
    s = OptionTypeInfo::ParseStruct(
        config_options, CloudFileSystemOptions::kName(),
        &cloud_fs_option_type_info, CloudFileSystemOptions::kName(), opts_str,
        reinterpret_cast<char*>(this));
    if (!s.ok()) {  // Something went wrong.  Attempt to reset
      OptionTypeInfo::ParseStruct(
          config_options, CloudFileSystemOptions::kName(),
          &cloud_fs_option_type_info, CloudFileSystemOptions::kName(), current,
          reinterpret_cast<char*>(this));
    }
  }
  return s;
}

Status CloudFileSystemOptions::Serialize(const ConfigOptions& config_options,
                                         std::string* value) const {
  return OptionTypeInfo::SerializeStruct(
      config_options, CloudFileSystemOptions::kName(),
      &cloud_fs_option_type_info, CloudFileSystemOptions::kName(),
      reinterpret_cast<const char*>(this), value);
}

Status CloudFileSystemEnv::NewAwsFileSystem(
    const std::shared_ptr<FileSystem>& base_fs,
    const std::string& src_cloud_bucket, const std::string& src_cloud_object,
    const std::string& src_cloud_region, const std::string& dest_cloud_bucket,
    const std::string& dest_cloud_object, const std::string& dest_cloud_region,
    const CloudFileSystemOptions& cloud_options,
    const std::shared_ptr<Logger>& logger, CloudFileSystem** cfs) {
  CloudFileSystemOptions options = cloud_options;
  if (!src_cloud_bucket.empty())
    options.src_bucket.SetBucketName(src_cloud_bucket);
  if (!src_cloud_object.empty())
    options.src_bucket.SetObjectPath(src_cloud_object);
  if (!src_cloud_region.empty()) options.src_bucket.SetRegion(src_cloud_region);
  if (!dest_cloud_bucket.empty())
    options.dest_bucket.SetBucketName(dest_cloud_bucket);
  if (!dest_cloud_object.empty())
    options.dest_bucket.SetObjectPath(dest_cloud_object);
  if (!dest_cloud_region.empty())
    options.dest_bucket.SetRegion(dest_cloud_region);
  return NewAwsFileSystem(base_fs, options, logger, cfs);
}

int DoRegisterCloudObjects(ObjectLibrary& library, const std::string& arg) {
  int count = 0;
  // Register the FileSystem types
  library.AddFactory<FileSystem>(
      CloudFileSystemImpl::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<FileSystem>* guard,
         std::string* /*errmsg*/) {
        guard->reset(new CloudFileSystemImpl(CloudFileSystemOptions(),
                                             FileSystem::Default(),
                                             nullptr /*logger*/));
        return guard->get();
      });
  count++;

  count += CloudFileSystemImpl::RegisterAwsObjects(library, arg);

  // Register the Cloud Log Controllers

  library.AddFactory<CloudLogController>(
      CloudLogControllerImpl::kKafka(),
      [](const std::string& /*uri*/, std::unique_ptr<CloudLogController>* guard,
         std::string* errmsg) {
        Status s = CloudLogControllerImpl::CreateKafkaController(guard);
        if (!s.ok()) {
          *errmsg = s.ToString();
        }
        return guard->get();
      });
  count++;

  return count;
}

void CloudFileSystemEnv::RegisterCloudObjects(const std::string& arg) {
  static std::once_flag do_once;
  std::call_once(do_once, [&]() {
    auto library = ObjectLibrary::Default();
    DoRegisterCloudObjects(*library, arg);
  });
}

std::unique_ptr<Env> CloudFileSystemEnv::NewCompositeEnvFromFs(FileSystem* fs,
                                                               Env* env) {
  // We need a shared_ptr<FileSystem> pointing to "this", to initialize the
  // env wrapper, but we don't want that shared_ptr to own the lifecycle for
  // "this". Creating a shared_ptr with a custom no-op deleter instead.
  std::shared_ptr<FileSystem> fsPtr(fs, [](auto* /*p*/) { /*noop*/ });
  return std::make_unique<CompositeEnvWrapper>(env, fsPtr);
}

Status CloudFileSystemEnv::CreateFromString(
    const ConfigOptions& config_options, const std::string& value,
    std::unique_ptr<CloudFileSystem>* result) {
  RegisterCloudObjects();
  std::string id;
  std::unordered_map<std::string, std::string> options;
  Status s;
  if (value.find('=') == std::string::npos) {
    id = value;
  } else {
    s = StringToMap(value, &options);
    if (s.ok()) {
      auto iter = options.find("id");
      if (iter != options.end()) {
        id = iter->second;
        options.erase(iter);
      } else {
        id = CloudFileSystemImpl::kClassName();
      }
    }
  }
  if (!s.ok()) {
    return s;
  }
  ConfigOptions copy = config_options;
  std::unique_ptr<FileSystem> fs;
  copy.invoke_prepare_options = false;  // Prepare here, not there
  s = ObjectRegistry::NewInstance()->NewUniqueObject<FileSystem>(id, &fs);
  if (s.ok()) {
    auto* cfs = dynamic_cast<CloudFileSystemImpl*>(fs.get());
    assert(cfs);
    if (!options.empty()) {
      s = cfs->ConfigureFromMap(copy, options);
    }
    if (s.ok() && config_options.invoke_prepare_options) {
      auto env = NewCompositeEnvFromFs(cfs, copy.env);
      copy.invoke_prepare_options = config_options.invoke_prepare_options;
      copy.env = env.get();
      s = cfs->PrepareOptions(copy);
      if (s.ok()) {
        Options tmp;
        s = cfs->ValidateOptions(tmp, tmp);
      }
    }
  }

  if (s.ok()) {
    result->reset(static_cast<CloudFileSystem*>(fs.release()));
  }

  return s;
}

Status CloudFileSystemEnv::CreateFromString(
    const ConfigOptions& config_options, const std::string& value,
    const CloudFileSystemOptions& cloud_options,
    std::unique_ptr<CloudFileSystem>* result) {
  RegisterCloudObjects();
  std::string id;
  std::unordered_map<std::string, std::string> options;
  Status s;
  if (value.find("=") == std::string::npos) {
    id = value;
  } else {
    s = StringToMap(value, &options);
    if (s.ok()) {
      auto iter = options.find("id");
      if (iter != options.end()) {
        id = iter->second;
        options.erase(iter);
      } else {
        id = CloudFileSystemImpl::kClassName();
      }
    }
  }
  if (!s.ok()) {
    return s;
  }
  ConfigOptions copy = config_options;
  std::unique_ptr<FileSystem> fs;
  copy.invoke_prepare_options = false;  // Prepare here, not there
  s = ObjectRegistry::NewInstance()->NewUniqueObject<FileSystem>(id, &fs);
  if (s.ok()) {
    auto* cfs = dynamic_cast<CloudFileSystemImpl*>(fs.get());
    assert(cfs);
    auto copts = cfs->GetOptions<CloudFileSystemOptions>();
    *copts = cloud_options;
    if (!options.empty()) {
      s = cfs->ConfigureFromMap(copy, options);
    }
    if (s.ok() && config_options.invoke_prepare_options) {
      auto env = NewCompositeEnvFromFs(cfs, copy.env);
      copy.invoke_prepare_options = config_options.invoke_prepare_options;
      copy.env = env.get();
      s = cfs->PrepareOptions(copy);
      if (s.ok()) {
        Options tmp;
        s = cfs->ValidateOptions(tmp, tmp);
      }
    }
  }

  if (s.ok()) {
    result->reset(static_cast<CloudFileSystem*>(fs.release()));
  }

  return s;
}

#ifndef USE_AWS
Status CloudFileSystemEnv::NewAwsFileSystem(
    const std::shared_ptr<FileSystem>& /*base_fs*/,
    const CloudFileSystemOptions& /*options*/,
    const std::shared_ptr<Logger>& /*logger*/, CloudFileSystem** /*cfs*/) {
  return Status::NotSupported("RocksDB Cloud not compiled with AWS support");
}
#else
Status CloudFileSystemEnv::NewAwsFileSystem(
    const std::shared_ptr<FileSystem>& base_fs,
    const CloudFileSystemOptions& options,
    const std::shared_ptr<Logger>& logger, CloudFileSystem** cfs) {
  CloudFileSystemEnv::RegisterCloudObjects();
  // Dump out cloud fs options
  options.Dump(logger.get());

  Status st = AwsFileSystem::NewAwsFileSystem(base_fs, options, logger, cfs);
  if (st.ok()) {
    // store a copy of the logger
    auto* cloud = static_cast<CloudFileSystemImpl*>(*cfs);
    cloud->info_log_ = logger;

    // start the purge thread only if there is a destination bucket
    if (options.dest_bucket.IsValid() && options.run_purger) {
      cloud->purge_thread_ = std::thread([cloud] { cloud->Purger(); });
    }
  }
  return st;
}
#endif

std::unique_ptr<Env> CloudFileSystemEnv::NewCompositeEnv(
    Env* env, const std::shared_ptr<FileSystem>& fs) {
  return std::make_unique<CompositeEnvWrapper>(env, fs);
}

IOStatus CloudFileSystemEnv::LoadCloudManifest(
    const std::string& dbname, const std::shared_ptr<FileSystem>& fs,
    const std::string& cookie, std::unique_ptr<CloudManifest>* cloud_manifest) {
  std::unique_ptr<SequentialFileReader> reader;
  auto cloud_manifest_file_name = MakeCloudManifestFile(dbname, cookie);
  auto s = SequentialFileReader::Create(fs, cloud_manifest_file_name,
                                        FileOptions(), &reader, nullptr /*dbg*/,
                                        nullptr /* rate_limiter */);
  if (s.ok()) {
    s = CloudManifest::LoadFromLog(std::move(reader), cloud_manifest);
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
