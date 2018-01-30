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
#include "include/org_rocksdb_AbstractNotAssociativeMergeOperator.h"
#include "rocksjni/portal.h"
#include "rocksdb/merge_operator.h"
#include "utilities/merge_operators.h"



namespace rocksdb {

    namespace JNIAbstractNotAssociativeMergeOperator {

        static jclass bytearrayClass;
        static jmethodID merge;
        static jmethodID full_merge;
        static jmethodID multi_merge;
        static jmethodID should_merge;

        class JNIMergeOperator : public MergeOperator {

        public:
            JNIMergeOperator(jboolean
            _allowSingleOperand,
            jboolean _allowShouldMerge, jboolean
            _allowPartialMultiMerge) {
                allowShouldMerge = _allowShouldMerge;
                allowPartialMultiMerge = _allowPartialMultiMerge;
                allowSingleOperand = _allowSingleOperand;

            }

            virtual bool AllowSingleOperand() const { return allowSingleOperand; }

            virtual bool PartialMerge(const Slice &key,
                                      const Slice &lop,
                                      const Slice &rop,
                                      std::string *new_value,
                                      Logger *logger) const {


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


                size_t s1 = lop.size() * sizeof(char);
                buf1 = (jbyte *) lop.data();
                jb1 = env->NewByteArray(s1);

                env->SetByteArrayRegion(jb1, 0, s1, buf1);


                size_t s2 = rop.size() * sizeof(char);
                buf2 = (jbyte *) rop.data();
                jb2 = env->NewByteArray(s2);
                env->SetByteArrayRegion(jb2, 0, s2, buf2);


                jbyteArray jresult = (jbyteArray) env->CallObjectMethod(obj, merge, jb0, jb1, jb2);
                jthrowable ex = env->ExceptionOccurred();

                env->ReleaseByteArrayElements(jb0, buf0, JNI_COMMIT);
                env->ReleaseByteArrayElements(jb1, buf1, JNI_COMMIT);
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

            virtual bool PartialMergeMulti(const Slice &key,
                                           const std::deque<Slice> &operands,
                                           std::string *new_value, Logger *logger) const {
                if (!allowPartialMultiMerge) return false;

                JNIEnv *env = rocksdb::getEnv();
                if (env == NULL) return false;


                size_t size = operands.size();
                jobjectArray oa = env->NewObjectArray(size, bytearrayClass, NULL);
                Slice slice;
                jbyteArray jb;
                for (size_t i = 0; i < size; i++) {
                    slice = operands.at(i);
                    jb = env->NewByteArray(slice.size());
                    env->SetByteArrayRegion(jb, 0, slice.size(), (jbyte *) slice.data());
                    env->SetObjectArrayElement(oa, i, jb);
                }
                size_t s0 = key.size() * sizeof(char);
                jb = env->NewByteArray(s0);
                jbyte* buf=(jbyte *) key.data();
                env->SetByteArrayRegion(jb, 0, s0,buf );


                jbyteArray jresult = (jbyteArray) env->CallObjectMethod(obj, multi_merge, jb, oa);
                jthrowable ex = env->ExceptionOccurred();


                env->ReleaseByteArrayElements(jb, buf, JNI_COMMIT);
                for (size_t i = 0; i < size; i++) {
                    jb = (jbyteArray) env->GetObjectArrayElement(oa, i);
                    jbyte *r =  env->GetByteArrayElements(jb, 0);
                    env->ReleaseByteArrayElements(jb, r, JNI_COMMIT);

                }
                env->DeleteLocalRef(oa);


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


            virtual bool ShouldMerge(const std::vector<Slice> &operands) const {
                if (!allowShouldMerge) return false;

                JNIEnv *env = rocksdb::getEnv();
                if (env == NULL) return false;
                size_t size = operands.size();
                jobjectArray oa = env->NewObjectArray(size, bytearrayClass, NULL);
                Slice slice;
                jbyteArray jb;
                for (size_t i = 0; i < size; i++) {
                    slice = operands.at(i);
                    jb = env->NewByteArray(slice.size());
                    env->SetByteArrayRegion(jb, 0, slice.size(), (jbyte *) slice.data());
                    env->SetObjectArrayElement(oa, i, jb);
                }


                jboolean jresult = env->CallBooleanMethod(obj, should_merge, oa);
                jthrowable ex = env->ExceptionOccurred();

                for (size_t i = 0; i < size; i++) {
                    jb = (jbyteArray) env->GetObjectArrayElement(oa, i);
                    jbyte *r =  env->GetByteArrayElements(jb, 0);
                    env->ReleaseByteArrayElements(jb, r, JNI_COMMIT);

                }
                env->DeleteLocalRef(oa);


                if (ex) {


                    env->Throw(ex);

                    return false;
                } else {


                    return (bool) jresult;
                }


            }

            virtual bool FullMergeV2(const MergeOperationInput &merge_in,
                                     MergeOperationOutput *merge_out) const override {
                JNIEnv *env = rocksdb::getEnv();
                if (env == NULL) return false;
                jbyteArray jb0, jb1, jb;
                jbyte *buf0;
                jbyte *buf1;

                Slice key = merge_in.key;
                size_t s0 = key.size() * sizeof(char);
                buf0 = (jbyte *) key.data();
                jb0 = env->NewByteArray(s0);
                env->SetByteArrayRegion(jb0, 0, s0, buf0);
                const Slice *existing_value = merge_in.existing_value;

                if (existing_value != NULL) {
                    size_t s1 = existing_value->size() * sizeof(char);
                    buf1 = (jbyte *) existing_value->data();
                    jb1 = env->NewByteArray(s1);

                    env->SetByteArrayRegion(jb1, 0, s1, buf1);
                } else {
                    buf1 = NULL;
                    jb1 = NULL;
                }

                std::vector<rocksdb::Slice> operands = merge_in.operand_list;

                size_t size = operands.size();


                jobjectArray oa = env->NewObjectArray(size, bytearrayClass, NULL);
                Slice slice;
                for (size_t i = 0; i < size; i++) {

                    slice = operands.at(i);
                    jb = env->NewByteArray(slice.size());
                    env->SetByteArrayRegion(jb, 0, slice.size(), (jbyte *) slice.data());
                    env->SetObjectArrayElement(oa, i, jb);
                }


                jbyteArray jresult = (jbyteArray) env->CallObjectMethod(obj, full_merge, jb0, jb1, oa);
                jthrowable ex = env->ExceptionOccurred();


                env-> ReleaseByteArrayElements(jb0, buf0, JNI_COMMIT);//key
                if (existing_value != NULL)  env->ReleaseByteArrayElements(jb1, buf1, JNI_COMMIT);//old value
                for (size_t i = 0; i < size; i++) {
                    jb = (jbyteArray) env->GetObjectArrayElement(oa, i);
                    jbyte *r =  env->GetByteArrayElements(jb, 0);
                    env->ReleaseByteArrayElements(jb, r, JNI_COMMIT);

                }
                env->DeleteLocalRef(oa);


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
                    merge_out->new_value.clear();
                    merge_out->new_value.assign(result, len);
                    env->ReleaseByteArrayElements(jresult, (jbyte*)result,  JNI_ABORT);

                    return true;
                }


            }

            virtual const char *Name() const override {
                return "JNIAbstractNotAssociativeMergeOperator";
            }

            void destroy(JNIEnv *env) {

                env->DeleteWeakGlobalRef(obj);


            }

            void init(JNIEnv *e, jobject hook) {

                obj = e->NewWeakGlobalRef(hook);


            }


        private:

            jobject obj;
            jboolean allowSingleOperand;
            jboolean allowShouldMerge;
            jboolean allowPartialMultiMerge;

        };
        static void staticDestroy(JNIEnv *env){
            env->DeleteGlobalRef(reinterpret_cast<jobject>(JNIAbstractNotAssociativeMergeOperator::bytearrayClass));
        }
        static void staticInit(JNIEnv *env) {

            jclass cls = env->FindClass("org/rocksdb/AbstractNotAssociativeMergeOperator");
            if (cls== nullptr) {
                cls = env->FindClass("java/lang/Error");
                env->ThrowNew(cls, "unable to find AbstractNotAssociativeMergeOperator");
            }

            JNIAbstractNotAssociativeMergeOperator::merge = env->GetMethodID(cls, "partialMerge", "([B[B[B)[B");

            JNIAbstractNotAssociativeMergeOperator::multi_merge = env->GetMethodID(cls, "partialMultiMerge",
                                                                                   "([B[[B)[B");

            JNIAbstractNotAssociativeMergeOperator::should_merge = env->GetMethodID(cls, "shouldMerge", "([[B)Z");

            JNIAbstractNotAssociativeMergeOperator::full_merge = env->GetMethodID(cls, "fullMerge", "([B[B[[B)[B");


            if (JNIAbstractNotAssociativeMergeOperator::merge == 0) {
                cls = env->FindClass("java/lang/Error");
                env->ThrowNew(cls, "unable to find method partialMerge");
            }

            if (JNIAbstractNotAssociativeMergeOperator::multi_merge == 0) {
                cls = env->FindClass("java/lang/Error");
                env->ThrowNew(cls, "unable to find method partialMultiMerge");
            }
            if (JNIAbstractNotAssociativeMergeOperator::should_merge == 0) {
                cls = env->FindClass("java/lang/Error");
                env->ThrowNew(cls, "unable to find method shouldMerge");
            }
            if (JNIAbstractNotAssociativeMergeOperator::full_merge == 0) {
                cls = env->FindClass("java/lang/Error");
                env->ThrowNew(cls, "unable to find method fullMerge");
            }
            jclass a = env->FindClass("[B");
            if (a==0){
                cls = env->FindClass("java/lang/Error");
                env->ThrowNew(cls, "unable to find object []");

            } else  JNIAbstractNotAssociativeMergeOperator::bytearrayClass= reinterpret_cast<jclass>(env->NewGlobalRef(a));
        }

    }

}

initialize {

rocksdb::setLoader( &rocksdb::JNIAbstractNotAssociativeMergeOperator::staticInit);
rocksdb::setUnloader( &rocksdb::JNIAbstractNotAssociativeMergeOperator::staticDestroy);

}


void  Java_org_rocksdb_AbstractNotAssociativeMergeOperator_initOperator(
        JNIEnv* env, jobject hook, jlong jhandle){

     std::shared_ptr<rocksdb::JNIAbstractNotAssociativeMergeOperator::JNIMergeOperator>*  op =
            reinterpret_cast<std::shared_ptr<rocksdb::JNIAbstractNotAssociativeMergeOperator::JNIMergeOperator>*>(jhandle);
    op->get()->init(env, hook);


}

 jlong  Java_org_rocksdb_AbstractNotAssociativeMergeOperator_newOperator
(
        JNIEnv* env, jclass jclazz,
        jboolean allowSingleOperand,
        jboolean allowShouldMerge,
        jboolean allowPartialMultiMerge) {
    std::shared_ptr<rocksdb::MergeOperator> p= std::make_shared<rocksdb::JNIAbstractNotAssociativeMergeOperator::JNIMergeOperator>(allowSingleOperand,allowShouldMerge,allowPartialMultiMerge);
    auto* op = new std::shared_ptr<rocksdb::MergeOperator>(p);
    return reinterpret_cast<jlong>(op);
}

void Java_org_rocksdb_AbstractNotAssociativeMergeOperator_disposeInternal(
        JNIEnv* env, jobject obj, jlong jhandle) {
    std::shared_ptr<rocksdb::JNIAbstractNotAssociativeMergeOperator::JNIMergeOperator>*  op =
            reinterpret_cast<std::shared_ptr<rocksdb::JNIAbstractNotAssociativeMergeOperator::JNIMergeOperator>*>(jhandle);
    op->get()->destroy(env);
    delete op;
}

