/*
 * @author cristian.lorenzetto@gmail.com
 * */


#include <iostream>
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
        static jclass rtClass;
        static jfieldID rtField;
        static jmethodID rtConstructor;


        class JNIMergeOperator : public AssociativeMergeOperator {

        public:
            virtual bool Merge(const Slice &key,
                               const Slice *existing_value,
                               const Slice &value,
                               std::string *new_value,
                               Logger*) const override {


                JNIEnv *env = rocksdb::getEnv();
                if (env == NULL) return false;
                jbyteArray jb0, jb1, jb2;
                jbyte *buf0;
                jbyte *buf1;
                jbyte *buf2;

                size_t s0 = key.size() * sizeof(char);
                buf0 = (jbyte *) key.data();
                jb0 = env->NewByteArray(static_cast<jint>(s0));
                env->SetByteArrayRegion(jb0, 0, static_cast<jint>(s0), buf0);

                if (existing_value != NULL) {
                    size_t s1 = existing_value->size() * sizeof(char);
                    buf1 = (jbyte *) existing_value->data();
                    jb1 = env->NewByteArray(static_cast<jint>(s1));

                    env->SetByteArrayRegion(jb1, 0, static_cast<jint>(s1), buf1);
                } else {
                    buf1 = NULL;
                    jb1 = NULL;
                }

                size_t s2 = value.size() * sizeof(char);
                buf2 = (jbyte *) value.data();
                jb2 = env->NewByteArray(static_cast<jint>(s2));
                env->SetByteArrayRegion(jb2, 0, static_cast<jint>(s2), buf2);
              

               jobject rtobject = env->NewObject( rtClass, rtConstructor);
                jbyteArray jresult = (jbyteArray) env->CallObjectMethod(obj, rocksdb::JNIAbstractAssociativeMergeOperator::method, jb0, jb1, jb2,rtobject);
                jthrowable ex = env->ExceptionOccurred();

                env->DeleteLocalRef(jb0);
               // env->DeleteLocalRef(jb0);
                if (jb1 != NULL) env->DeleteLocalRef(jb1);
                env->DeleteLocalRef(jb2);
                env->DeleteLocalRef(rtobject);

                if (ex) {

                    if (jresult!= nullptr) {
                        char *result = (char *) env->GetByteArrayElements(jresult, 0);
                        jboolean rtr=env->GetBooleanField(rtobject, rtField);
                        env->ReleaseByteArrayElements(jresult, (jbyte*)result, rtr?JNI_COMMIT:JNI_ABORT);
                    }
                    env->Throw(ex);

                    return false;
                } else {
                    int len = env->GetArrayLength(jresult) / sizeof(char);
                    char *result = (char *) env->GetByteArrayElements(jresult, 0);
                    new_value->clear();
                    new_value->assign(result, len);
                    jboolean rtr=env->GetBooleanField(rtobject, rtField);
                    env->ReleaseByteArrayElements(jresult, (jbyte*)result, rtr?JNI_COMMIT:JNI_ABORT);

                    return true;
                }


            }

            virtual const char *Name() const override {
                return "JNIAbstractAssociativeMergeOperator";
            }

            void destroy(JNIEnv *env) {

                env->DeleteGlobalRef(obj);


            }

            void init(JNIEnv *e, jobject hook) {

                obj = e->NewGlobalRef(hook);

            }

        private:

            jobject obj;

        };// end of class

        static void staticDestroy(JNIEnv *env){
            env->DeleteGlobalRef(reinterpret_cast<jobject>(JNIAbstractAssociativeMergeOperator::rtClass));

          }


        static void staticInit(JNIEnv *env) {

            jclass cls = env->FindClass("org/rocksdb/AbstractAssociativeMergeOperator");
            if (cls==nullptr) {
                cls = env->FindClass("java/lang/Error");
                env->ThrowNew(cls, "unable to find AbstractAssociativeMergeOperator");
            }
            method = env->GetMethodID(cls, "merge", "([B[B[BLorg/rocksdb/ReturnType;)[B");
            if (method == 0) {
                cls = env->FindClass("java/lang/Error");
                env->ThrowNew(cls, "unable to find method merge");
            }
           jclass a = env->FindClass("Lorg/rocksdb/ReturnType;");
           if (a==0){
               cls = env->FindClass("java/lang/Error");
               env->ThrowNew(cls, "unable to find object org.rocksdb.ReturnType");

           } else  JNIAbstractAssociativeMergeOperator::rtClass= reinterpret_cast<jclass>(env->NewGlobalRef(a));
            rtConstructor = env->GetMethodID( rtClass, "<init>", "()V");
            if (rtConstructor==0){
                cls = env->FindClass("java/lang/Error");
                env->ThrowNew(cls, "unable to find field ReturnType.<init>");

            }


           JNIAbstractAssociativeMergeOperator::rtField = env->GetFieldID( rtClass, "isArgumentReference", "Z");
           if (JNIAbstractAssociativeMergeOperator::rtField==0){
               cls = env->FindClass("java/lang/Error");
               env->ThrowNew(cls, "unable to find field ReturnType.isArgumentReference");

           }
        }
    }

}
initialize {

rocksdb::setLoader( &rocksdb::JNIAbstractAssociativeMergeOperator::staticInit);
rocksdb::setUnloader( &rocksdb::JNIAbstractAssociativeMergeOperator::staticDestroy);
}


jboolean  Java_org_rocksdb_AbstractAssociativeMergeOperator_initOperator(
        JNIEnv* env, jobject hook, jlong jhandle){
    std::shared_ptr<rocksdb::JNIAbstractAssociativeMergeOperator::JNIMergeOperator>*  op =
            reinterpret_cast<std::shared_ptr<rocksdb::JNIAbstractAssociativeMergeOperator::JNIMergeOperator>*>(jhandle);
    op->get()->init(env, hook);

    return true;
     }

jlong Java_org_rocksdb_AbstractAssociativeMergeOperator_newOperator(
        JNIEnv*, jclass) {
    std::shared_ptr<rocksdb::MergeOperator> p= std::make_shared<rocksdb::JNIAbstractAssociativeMergeOperator::JNIMergeOperator>();
    auto* op = new std::shared_ptr<rocksdb::MergeOperator>(p);
    return reinterpret_cast<jlong>(op);
}

void Java_org_rocksdb_AbstractAssociativeMergeOperator_disposeInternal(
        JNIEnv* env, jobject, jlong jhandle) {
    std::shared_ptr<rocksdb::JNIAbstractAssociativeMergeOperator::JNIMergeOperator>*  op =
            reinterpret_cast<std::shared_ptr<rocksdb::JNIAbstractAssociativeMergeOperator::JNIMergeOperator>*>(jhandle);
    op->get()->destroy(env);
    delete op;
}
