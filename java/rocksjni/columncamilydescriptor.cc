
#include <jni.h>
#include <stdio.h>
#include <stdlib.h>

#include "include/org_rocksdb_ColumnFamilyDescriptor.h"
#include "rocksjni/portal.h"


/*
 * Class:     org_rocksdb_ColumnFamilyDescriptor
 * Method:    createNativeInstance
 * Signature: ([BJ)J
 */
jlong Java_org_rocksdb_ColumnFamilyDescriptor_createNativeInstance
    (JNIEnv* env, jclass, jbyteArray jname, jlong jcf_options_handle) {

  jboolean  has_exception = JNI_FALSE;

  auto cf_name = ROCKSDB_NAMESPACE::JniUtil::byteString<std::string>(env, jname,
     [](const char* str_data, const size_t str_len) {
          return std::string(str_data, str_len);
          }, &has_exception);

  if(has_exception) {
    return 0;
  }

  auto cf_options = reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jcf_options_handle);

  ROCKSDB_NAMESPACE::ColumnFamilyDescriptor *cf_descriptor =
      new ROCKSDB_NAMESPACE::ColumnFamilyDescriptor(cf_name,  *cf_options );

  return GET_CPLUSPLUS_POINTER(cf_descriptor);
}

/*
 * Class:     org_rocksdb_ColumnFamilyDescriptor
 * Method:    getName
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_ColumnFamilyDescriptor_getName
    (JNIEnv* env, jclass, jlong jcf_descriptor_handle) {
  auto cf_options = reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyDescriptor*>(jcf_descriptor_handle);

  return ROCKSDB_NAMESPACE::JniUtil::copyBytes(env, cf_options->name);

}

/*
 * Class:     org_rocksdb_ColumnFamilyDescriptor
 * Method:    getOption
 * Signature: (J)J
 */
jlong Java_org_rocksdb_ColumnFamilyDescriptor_getOption
    (JNIEnv* env, jclass, jlong jcf_descriptor_handle) {
  auto cf_options = reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyDescriptor*>(jcf_descriptor_handle);

  return GET_CPLUSPLUS_POINTER(&cf_options->options); //TODO -> Should we copy options and leave it on Java developer to delete?
                                                      // At the moment we setup owningHandle_=false
}

/*
 * Class:     org_rocksdb_ColumnFamilyDescriptor
 * Method:    disposeJni
 * Signature: (J)V
 */
void Java_org_rocksdb_ColumnFamilyDescriptor_disposeJni
    (JNIEnv *, jclass, jlong jcf_descriptor_handle) {
  auto cf_options = reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyDescriptor*>(jcf_descriptor_handle);

  delete(cf_options);

}
