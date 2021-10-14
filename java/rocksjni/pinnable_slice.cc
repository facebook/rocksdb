#include "include/org_rocksdb_PinnableSlice.h"
#include "rocksdb/slice.h"

/*
 * Class:     org_rocksdb_PinnableSlice
 * Method:    deletePinnableSlice
 * Signature: (J)V
 */
void Java_org_rocksdb_PinnableSlice_deletePinnableSlice(JNIEnv*, jclass,
                                                        jlong handle) {
  auto* pinnable_slice =
      reinterpret_cast<ROCKSDB_NAMESPACE::PinnableSlice*>(handle);
  assert(pinnable_slice != nullptr);
  pinnable_slice->Reset();
  delete pinnable_slice;
}