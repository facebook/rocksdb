#include <utility>

#include "rocksdb/listener.h"
#include "include/org_rocksdb_test_TestableEventListener.h"

using namespace ROCKSDB_NAMESPACE;

/*
 * Class:     org_rocksdb_test_TestableEventListener
 * Method:    invokeAllCallbacks
 * Signature: (J)V
 */
void  Java_org_rocksdb_test_TestableEventListener_invokeAllCallbacks
  (JNIEnv *, jclass, jlong jhandle) {
  const auto& el = *reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::EventListener>*>(jhandle);
  el->OnFlushCompleted(nullptr, FlushJobInfo());
  el->OnFlushBegin(nullptr, FlushJobInfo());
  el->OnTableFileDeleted(TableFileDeletionInfo());
  el->OnCompactionBegin(nullptr, CompactionJobInfo());
  el->OnCompactionCompleted(nullptr, CompactionJobInfo());
  el->OnTableFileCreated(TableFileCreationInfo());
  el->OnTableFileCreationStarted(TableFileCreationBriefInfo());
  el->OnMemTableSealed(MemTableInfo());
  el->OnColumnFamilyHandleDeletionStarted(nullptr);
  el->OnExternalFileIngested(nullptr, ExternalFileIngestionInfo());
  Status status;
  el->OnBackgroundError(BackgroundErrorReason::kFlush, &status);
  el->OnStallConditionsChanged(WriteStallInfo());
  const FileOperationInfo op_info = FileOperationInfo(FileOperationType::kRead, "",
      std::make_pair(std::chrono::system_clock::now(), std::chrono::steady_clock::now()),
      std::chrono::steady_clock::now(), status);
  el->OnFileReadFinish(op_info);
  el->OnFileWriteFinish(op_info);
  el->OnFileFlushFinish(op_info);
  el->OnFileSyncFinish(op_info);
  el->OnFileRangeSyncFinish(op_info);
  el->OnFileTruncateFinish(op_info);
  el->OnFileCloseFinish(op_info);
  el->ShouldBeNotifiedOnFileIO();
  bool auto_recovery;
  el->OnErrorRecoveryBegin(BackgroundErrorReason::kFlush, status, &auto_recovery);
  el->OnErrorRecoveryCompleted(status);
}
