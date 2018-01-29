/*
 * @author cristian.lorenzetto@gmail.com
 * */


#include <jni.h>
#include <assert.h>
#include <string>
#include <memory>
#include "util/logging.h"
#include "rocksdb/slice.h"
#include "include/org_rocksdb_AbstractAssociativeMergeOperator.h"
#include "rocksjni/portal.h"
#include "rocksdb/merge_operator.h"
#include "utilities/merge_operators.h"
using namespace rocksdb;
 // anonymous namespace

        class JNIAbstractAssociativeMergeOperator : public AssociativeMergeOperator {

        public:
            virtual bool Merge(const Slice &key,
                               const Slice *existing_value,
                               const Slice &value,
                               std::string *new_value,
                               Logger *logger) const override {
                assert(new_value);
                JNIEnv *env;

                int getEnvStat = jvm->GetEnv((void **) &env, jvmVersion);
                if (getEnvStat == JNI_EDETACHED) {
                    if (jvm->AttachCurrentThread((void **) &env, NULL) != JNI_OK) {
                        jclass jc = env->FindClass("java/lang/Error");
                        if (jc) env->ThrowNew(jc, "unable to attach JNIEnv");
                        return false;
                    }
                } else if (getEnvStat == JNI_EVERSION) {
                    jclass jc = env->FindClass("java/lang/Error");
                    if (jc) env->ThrowNew(jc, "unable to create JNIEnv:version not supported");
                    return false;

                }

                jbyteArray jb0, jb1, jb2;
                size_t s0 = key.size() * 2;
                size_t s1 = existing_value->size() * 2;
                size_t s2 = value.size() * 2;
                jb0 = env->NewByteArray(s0);
                jb1 = env->NewByteArray(s1);
                jb2 = env->NewByteArray(s2);
                jbyte *buf0;
                jbyte *buf1;
                jbyte *buf2;
                buf0 = (jbyte *) key.data();
                buf1 = (jbyte *) existing_value->data();
                buf2 = (jbyte *) value.data();

                env->SetByteArrayRegion(jb0, 0, s0, buf0);
                env->SetByteArrayRegion(jb1, 0, s1, buf1);
                env->SetByteArrayRegion(jb2, 0, s2, buf2);

                jbyteArray jresult = (jbyteArray) env->CallObjectMethod(obj, method, jb0, jb1, jb2);
                jthrowable ex = env->ExceptionOccurred();


                int len = env->GetArrayLength(jresult) / 2;
                char *result = (char *) env->GetByteArrayElements(jresult, 0);


                env->ReleaseByteArrayElements(jb0, buf0, JNI_ABORT);
                env->ReleaseByteArrayElements(jb1, buf1, JNI_ABORT);
                env->ReleaseByteArrayElements(jb1, buf2, JNI_ABORT);

                if (ex) {


                    env->Throw(ex);
                    jvm->DetachCurrentThread();
                    return false;
                } else {

                    new_value->clear();
                    new_value->assign(result, len);
                    jvm->DetachCurrentThread();
                    return true;
                }


            }

            virtual const char *Name() const override {
                return "JNIAbstractAssociativeMergeOperator";
            }

            bool init(JNIEnv *e, jobject hook, jmethodID m) {
                method = m;
                obj = hook;
                jvmVersion = e->GetVersion();
                return e->GetJavaVM(&jvm) == JNI_OK;
            }

        private:
            jmethodID method;
            jobject obj;
            JavaVM *jvm;
            jint jvmVersion;
        };






 void  Java_org_rocksdb_AbstractAssociativeMergeOperator_initOperator(
        JNIEnv* env, jobject jobj, jlong jhandle,jobject hook){
auto* op =
        reinterpret_cast<std::shared_ptr<rocksdb::MergeOperator>*>(jhandle);
jclass cls = env->GetObjectClass(hook);
jmethodID mid = env->GetMethodID( cls, "merge", "([B[B[B)[B");
     if (mid==0) {
         jclass jc = env->FindClass("java/lang/Error");
         if (jc) env->ThrowNew(jc, "unable find method:byte[] merge(byte[],byte[],byte[])");
         else
             assert(mid != 0);
       } else {
         bool checkInit=((JNIAbstractMergeOperator *) op)->init(env, hook, mid);
         if (!checkInit){
             jclass jc = env->FindClass("java/lang/Error");
             if (jc) env->ThrowNew(jc, "unable to init");
             else
                 assert(checkInit);
         }
     }
     }

jlong Java_org_rocksdb_AbstractAssociativeMergeOperator_newOperator(
        JNIEnv* env, jclass jclazz) {
    shared_ptr<rocksdb::MergeOperator> p= std::make_shared<JNIAbstractMergeOperator>();
    auto* op = new std::shared_ptr<rocksdb::MergeOperator>(p);
    return reinterpret_cast<jlong>(op);
}

void Java_org_rocksdb_AbstractAssociativeMergeOperator_disposeInternal(
        JNIEnv* env, jclass jclazz, jlong jhandle) {
    auto* op =
            reinterpret_cast<std::shared_ptr<rocksdb::MergeOperator>*>(jhandle);
    delete op;
}
