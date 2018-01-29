/*
 * @author cristian.lorenzetto@gmail.com
 * */


//#include <iostream>
#include <jni.h>
#include <rocksjni/init.h>
#include <assert.h>
#include <string>
#include <memory>
#include "util/logging.h"
#include "rocksdb/slice.h"
#include "include/org_rocksdb_AbstractAssociativeMergeOperator.h"
#include "rocksjni/portal.h"
#include "rocksjni/init.h"
#include "rocksdb/merge_operator.h"
#include "utilities/merge_operators.h"



namespace rocksdb {

    namespace JNIAbstractAssociativeMergeOperator {

        static jmethodID method;


        class JNIMergeOperator : public AssociativeMergeOperator {

        public:
            virtual bool Merge(const Slice &key,
                               const Slice *existing_value,
                               const Slice &value,
                               std::string *new_value,
                               Logger *logger) const override {


                JNIEnv *env = rocksdb::getEnv();
                if (env == NULL) return false;
                jbyteArray jb0, jb1, jb2;
                jbyte *buf0;
                jbyte *buf1;
                jbyte *buf2;

                size_t s0 = key.size() * sizeof(char);
                buf0 = (jbyte *) key.data();
                jb0 = env->NewByteArray(s0);
                env->SetByteArrayRegion(jb0, 0, s0, buf0);

                if (existing_value != NULL) {
                    size_t s1 = existing_value->size() * sizeof(char);
                    buf1 = (jbyte *) existing_value->data();
                    jb1 = env->NewByteArray(s1);

                    env->SetByteArrayRegion(jb1, 0, s1, buf1);
                } else {
                    buf1 = NULL;
                    jb1 = NULL;
                }

                size_t s2 = value.size() * sizeof(char);
                buf2 = (jbyte *) value.data();
                jb2 = env->NewByteArray(s2);
                env->SetByteArrayRegion(jb2, 0, s2, buf2);


                jbyteArray jresult = (jbyteArray) env->CallObjectMethod(obj, rocksdb::JNIAbstractAssociativeMergeOperator::method, jb0, jb1, jb2);
                jthrowable ex = env->ExceptionOccurred();

                env->ReleaseByteArrayElements(jb0, buf0, JNI_COMMIT);
               // env->DeleteLocalRef(jb0);
                if (jb1 != NULL) env->ReleaseByteArrayElements(jb1, buf1, JNI_COMMIT);
                env->ReleaseByteArrayElements(jb2, buf2, JNI_COMMIT);


                if (ex) {

                    if (jresult!= nullptr) {
                        char *result = (char *) env->GetByteArrayElements(jresult, 0);
                        env->ReleaseByteArrayElements(jresult, (jbyte*)result, JNI_ABORT);
                    }
                    env->Throw(ex);

                    return false;
                } else {
                    int len = env->GetArrayLength(jresult) / sizeof(char);
                    char *result = (char *) env->GetByteArrayElements(jresult, 0);
                    new_value->clear();
                    new_value->assign(result, len);
                    env->ReleaseByteArrayElements(jresult, (jbyte*)result,  JNI_ABORT);

                    return true;
                }


            }

            virtual const char *Name() const override {
                return "JNIAbstractAssociativeMergeOperator";
            }

            void destroy(JNIEnv *env) {

                env->DeleteWeakGlobalRef(obj);


            }

            void init(JNIEnv *e, jobject hook) {

                obj = e->NewWeakGlobalRef(hook);

            }

        private:

            jobject obj;

        };// end of class



       static void staticInit(JNIEnv *env) {

            jclass jc = env->FindClass("org/rocksdb/AbstractAssociativeMergeOperator");
            if (jc==nullptr) {
                jc = env->FindClass("java/lang/Error");
                env->ThrowNew(jc, "unable to find AbstractAssociativeMergeOperator");
            }
            method = env->GetMethodID(jc, "merge", "([B[B[B)[B");
            if (method == 0) {
                jc = env->FindClass("java/lang/Error");
                env->ThrowNew(jc, "unable to find method merge");
            }
        }
    }

}
initialize {

rocksdb::setLoader( &rocksdb::JNIAbstractAssociativeMergeOperator::staticInit);

}


jboolean  Java_org_rocksdb_AbstractAssociativeMergeOperator_initOperator(
        JNIEnv* env, jobject hook, jlong jhandle){
    std::shared_ptr<rocksdb::JNIAbstractAssociativeMergeOperator::JNIMergeOperator>*  op =
            reinterpret_cast<std::shared_ptr<rocksdb::JNIAbstractAssociativeMergeOperator::JNIMergeOperator>*>(jhandle);
    op->get()->init(env, hook);

    return true;
     }

jlong Java_org_rocksdb_AbstractAssociativeMergeOperator_newOperator(
        JNIEnv* env, jclass jclazz) {
    std::shared_ptr<rocksdb::MergeOperator> p= std::make_shared<rocksdb::JNIAbstractAssociativeMergeOperator::JNIMergeOperator>();
    auto* op = new std::shared_ptr<rocksdb::MergeOperator>(p);
    return reinterpret_cast<jlong>(op);
}

void Java_org_rocksdb_AbstractAssociativeMergeOperator_disposeInternal(
        JNIEnv* env, jobject obj, jlong jhandle) {
    std::shared_ptr<rocksdb::JNIAbstractAssociativeMergeOperator::JNIMergeOperator>*  op =
            reinterpret_cast<std::shared_ptr<rocksdb::JNIAbstractAssociativeMergeOperator::JNIMergeOperator>*>(jhandle);
    op->get()->destroy(env);
    delete op;
}
