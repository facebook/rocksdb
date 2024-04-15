
#include <string>

#include "rocksdb/file_system.h"

namespace ROCKSDB_NAMESPACE {
// A FileSystem that links files to a destination directory from a
// corresponding source directory on demand. The decision to link
// depends on the file type, with appendable or rename-able files, such as,
// descriptors, logs, CURRENT, being read in place in the remote directory,
// and SST files being linked. In the future, files read in place may be
// mirrored to the local directory, so the local dir has a complete database
// for troubleshooting purposes.

class OnDemandFileSystem : public FileSystemWrapper {
 public:
  OnDemandFileSystem(const std::shared_ptr<FileSystem>& target,
                     const std::string& src_path, const std::string& dest_path)
      : FileSystemWrapper(target), src_path_(src_path), dest_path_(dest_path) {}

  const char* Name() const override { return "OnDemandFileSystem"; }

  IOStatus NewSequentialFile(const std::string& fname,
                             const FileOptions& file_opts,
                             std::unique_ptr<FSSequentialFile>* result,
                             IODebugContext* dbg) override;

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& file_opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override;

  IOStatus NewWritableFile(const std::string& fname,
                           const FileOptions& file_opts,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override;

  IOStatus NewDirectory(const std::string& name, const IOOptions& io_opts,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override;

  IOStatus FileExists(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override;

  IOStatus GetChildren(const std::string& dir, const IOOptions& options,
                       std::vector<std::string>* result,
                       IODebugContext* dbg) override;

  IOStatus GetChildrenFileAttributes(const std::string& dir,
                                     const IOOptions& options,
                                     std::vector<FileAttributes>* result,
                                     IODebugContext* dbg) override;

  IOStatus CreateDir(const std::string& dirname, const IOOptions& options,
                     IODebugContext* dbg) override;

  IOStatus CreateDirIfMissing(const std::string& dirname,
                              const IOOptions& options,
                              IODebugContext* dbg) override;

  IOStatus GetFileSize(const std::string& fname, const IOOptions& options,
                       uint64_t* file_size, IODebugContext* dbg) override;

 private:
  bool CheckPathAndAdjust(std::string& path);
  bool LookupFileType(const std::string& name, FileType* type);

  const std::string src_path_;
  const std::string dest_path_;
};

// A wrapper class around an FSSequentialFile object. Its mainly
// intended to be used for appendable files like MANIFEST and logs.
// Beneath the covers, it tracks when EOF is reached, and reopens
// the file in order to read the latest appended data. This is
// necessary on some distributed file systems as they may have
// stale metadata about the file.
// TODO: Mirror the data read to a local file for troubleshooting
//       purposes, as well as recovery in case the source dir is
//       deleted.
class OnDemandSequentialFile : public FSSequentialFile {
 public:
  OnDemandSequentialFile(std::unique_ptr<FSSequentialFile>&& file,
                         OnDemandFileSystem* fs, const FileOptions& file_opts,
                         const std::string& path)
      : file_(std::move(file)),
        fs_(fs),
        file_opts_(file_opts),
        path_(path),
        eof_(false),
        offset_(0) {}

  virtual ~OnDemandSequentialFile() {}

  IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                char* scratch, IODebugContext* dbg) override;

  IOStatus Skip(uint64_t n) override;

  bool use_direct_io() const override;

  size_t GetRequiredBufferAlignment() const override;

  IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
    return IOStatus::NotSupported("InvalidateCache not supported.");
  }

  IOStatus PositionedRead(uint64_t /*offset*/, size_t /*n*/,
                          const IOOptions& /*options*/, Slice* /*result*/,
                          char* /*scratch*/, IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported("PositionedRead");
  }

  Temperature GetTemperature() const override;

 private:
  std::unique_ptr<FSSequentialFile> file_;
  OnDemandFileSystem* fs_;
  const FileOptions file_opts_;
  const std::string path_;
  bool eof_;
  uint64_t offset_;
};

std::shared_ptr<FileSystem> NewOnDemandFileSystem(
    const std::shared_ptr<FileSystem>& fs, std::string src_path,
    std::string dest_path);

}  // namespace ROCKSDB_NAMESPACE
