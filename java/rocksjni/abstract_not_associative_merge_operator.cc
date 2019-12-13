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
    /*
     * benchmark: executing 100.000 get(associated every call to 1 fullMerge java call)  it is executed in <<500ms. )
     *
     */
    namespace JNIAbstractNotAssociativeMergeOperator {

        static jclass bytearrayClass;
        static jfieldID rtField;
        static jclass rtClass;
        static jmethodID rtConstructor;
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

            virtual bool AllowSingleOperand() const override { return allowSingleOperand; }

            virtual bool PartialMerge(const Slice &key,
                                      const Slice &lop,
                                      const Slice &rop,
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


                size_t s1 = lop.size() * sizeof(char);
                buf1 = (jbyte *) lop.data();
                jb1 = env->NewByteArray(static_cast<jint>(s1));

                env->SetByteArrayRegion(jb1, 0, static_cast<jint>(s1), buf1);


                size_t s2 = rop.size() * sizeof(char);
                buf2 = (jbyte *) rop.data();
                jb2 = env->NewByteArray(static_cast<jint>(s2));
                env->SetByteArrayRegion(jb2, 0, static_cast<jint>(s2), buf2);

                jobject rtobject = env->NewObject( rtClass, rtConstructor);

                jbyteArray jresult = (jbyteArray) env->CallObjectMethod(obj, merge, jb0, jb1, jb2,rtobject);
                jthrowable ex = env->ExceptionOccurred();

                env->DeleteLocalRef(jb0);
                env->DeleteLocalRef(jb1);
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

            virtual bool PartialMergeMulti(const Slice &key,
                                           const std::deque<Slice> &operands,
                                           std::string *new_value, Logger*) const override {
                if (!allowPartialMultiMerge) return false;

                JNIEnv *env = rocksdb::getEnv();
                if (env == NULL) return false;


                size_t size = operands.size();
                jobjectArray oa = env->NewObjectArray(static_cast<jint>(size), bytearrayClass, NULL);
                Slice slice;
                jbyteArray jb;
                for (size_t i = 0; i < size; i++) {
                    slice = operands.at(i);
                    jb = env->NewByteArray(static_cast<jint>(slice.size()));
                    env->SetByteArrayRegion(jb, 0, static_cast<jint>(slice.size()), (jbyte *) slice.data());
                    env->SetObjectArrayElement(oa, static_cast<jint>(i), jb);
                    //TODO(AR) need to do loop cleanup of jb later!
                }
                size_t s0 = key.size() * sizeof(char);
                jb = env->NewByteArray(static_cast<jint>(s0));
                jbyte* buf=(jbyte *) key.data();
                env->SetByteArrayRegion(jb, 0, static_cast<jint>(s0), buf);
                jobject rtobject = env->NewObject( rtClass, rtConstructor);


                jbyteArray jresult = (jbyteArray) env->CallObjectMethod(obj, multi_merge, jb, oa,rtobject);
                jthrowable ex = env->ExceptionOccurred();


                env->DeleteLocalRef(jb);
                for (size_t i = 0; i < size; i++) {
                    jb = (jbyteArray) env->GetObjectArrayElement(oa, static_cast<jint>(i));
                    jbyte *r =  env->GetByteArrayElements(jb, 0);
                    env->ReleaseByteArrayElements(jb, r, JNI_COMMIT);

                }
                env->DeleteLocalRef(oa);
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


            virtual bool ShouldMerge(const std::vector<Slice> &operands) const override {
                if (!allowShouldMerge) return false;

                JNIEnv *env = rocksdb::getEnv();
                if (env == NULL) return false;
                size_t size = operands.size();
                jobjectArray oa = env->NewObjectArray(static_cast<jint>(size), bytearrayClass, NULL);
                Slice slice;
                jbyteArray jb;
                for (size_t i = 0; i < size; i++) {
                    slice = operands.at(i);
                    jb = env->NewByteArray(static_cast<jint>(slice.size()));
                    env->SetByteArrayRegion(jb, 0, static_cast<jint>(slice.size()), (jbyte *) slice.data());
                    env->SetObjectArrayElement(oa, static_cast<jint>(i), jb);
                    //TODO(AR) need to do loop cleanup of jb later
                }


                jboolean jresult = env->CallBooleanMethod(obj, should_merge, oa);
                jthrowable ex = env->ExceptionOccurred();

                for (size_t i = 0; i < size; i++) {
                    jb = (jbyteArray) env->GetObjectArrayElement(oa, static_cast<jint>(i));
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
              //  std::cout<<"enter in fullmerge2\n";
              //  std::cout.flush();
                JNIEnv *env = rocksdb::getEnv();
                if (env == NULL) return false;
                jbyteArray jb0, jb1, jb;
                jbyte *buf0;
                jbyte *buf1;

                Slice key = merge_in.key;
                size_t s0 = key.size() * sizeof(char);
                buf0 = (jbyte *) key.data();
                jb0 = env->NewByteArray(static_cast<jint>(s0));
                env->SetByteArrayRegion(jb0, 0, static_cast<jint>(s0), buf0);
                const Slice *existing_value = merge_in.existing_value;

                if (existing_value != NULL) {
                    size_t s1 = existing_value->size() * sizeof(char);
                    buf1 = (jbyte *) existing_value->data();
                    jb1 = env->NewByteArray(static_cast<jint>(s1));

                    env->SetByteArrayRegion(jb1, 0, static_cast<jint>(s1), buf1);
                } else {
                    buf1 = NULL;
                    jb1 = NULL;
                }

                std::vector<rocksdb::Slice> operands = merge_in.operand_list;

                size_t size = operands.size();


                jobjectArray oa = env->NewObjectArray(static_cast<jint>(size), bytearrayClass, NULL);
                Slice slice;
                for (size_t i = 0; i < size; i++) {

                    slice = operands.at(i);
                    jb = env->NewByteArray(static_cast<jint>(slice.size()));
                    env->SetByteArrayRegion(jb, 0, static_cast<jint>(slice.size()), (jbyte *) slice.data());
                    env->SetObjectArrayElement(oa, static_cast<jint>(i), jb);
                    //TODO(AR) need to do loop cleanup of jb later!
                }
                jobject rtobject = env->NewObject( rtClass, rtConstructor);

                //std::cout<<"calling\n";
                //std::cout.flush();
                jbyteArray jresult = (jbyteArray) env->CallObjectMethod(obj, full_merge, jb0, jb1, oa,rtobject);
              //  std::cout<<"exiting\n";
               // std::cout.flush();
                jthrowable ex = env->ExceptionOccurred();

                //TODO(AR) jb1 and jb are not cleaned up

                env->DeleteLocalRef(jb0);//key
                if (jb1 != NULL) env->DeleteLocalRef(jb1);//old value
                for (size_t i = 0; i < size; i++) {
                    jb = (jbyteArray) env->GetObjectArrayElement(oa, static_cast<jint>(i));
                    jbyte *r =  env->GetByteArrayElements(jb, 0);
                    env->ReleaseByteArrayElements(jb, r,  JNI_COMMIT);

                }
                env->DeleteLocalRef(oa);
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
                    jboolean rtr = env->GetBooleanField(rtobject, rtField);
                    if (rtr) {
                        if (result==(char*)buf0) merge_out->existing_operand=key;
                        else if(result==(char*)buf1) merge_out->existing_operand=(*existing_value);
                        else {
                            bool found=false;
                            for (size_t i = 0; i < size; i++) {

                                slice = operands.at(i);
                                if (result==slice.data()) {
                                    merge_out->existing_operand = slice;
                                    found=true;
                                    break;
                                }
                            }
                            if (!found){
                                    env->ReleaseByteArrayElements(jresult, (jbyte *) result, JNI_ABORT);
                                    jclass cls = env->FindClass("java/lang/Error");
                                    env->ThrowNew(cls, "unable match a existing reference:reference is released");


                                return false;


                            }
                        }

                        env->ReleaseByteArrayElements(jresult, (jbyte *) result, JNI_COMMIT);

                    }
                    else{
                        merge_out->new_value.clear();
                    merge_out->new_value.assign(result, len);

                    env->ReleaseByteArrayElements(jresult, (jbyte *) result,  JNI_ABORT);
                   }
                   // std::cout<<"outing in fullmerge2\n";

                   // rocksdb::detachCurrentThread();
                    return true;
                }


            }

            virtual const char *Name() const override {
                return "JNIAbstractNotAssociativeMergeOperator";
            }

            void destroy(JNIEnv *env) {
               // std::cout<<"delete global ref 1\n";

                env->DeleteGlobalRef(obj);


            }

            void init(JNIEnv *e, jobject hook) {
                //std::cout<<"init global ref \n";
                obj = e->NewGlobalRef(hook);


            }


        private:

            jobject obj;
            jboolean allowSingleOperand;
            jboolean allowShouldMerge;
            jboolean allowPartialMultiMerge;

        };
        static void staticDestroy(JNIEnv *env){
          //  std::cout<<"static destroy\n";
            env->DeleteGlobalRef(reinterpret_cast<jobject>(JNIAbstractNotAssociativeMergeOperator::rtClass));

            env->DeleteGlobalRef(reinterpret_cast<jobject>(JNIAbstractNotAssociativeMergeOperator::bytearrayClass));

        }
        static void staticInit(JNIEnv *env) {
            //std::cout<<"static init\n";
            jclass cls = env->FindClass("org/rocksdb/AbstractNotAssociativeMergeOperator");
            if (cls== nullptr) {
                cls = env->FindClass("java/lang/Error");
                env->ThrowNew(cls, "unable to find AbstractNotAssociativeMergeOperator");
            }

            JNIAbstractNotAssociativeMergeOperator::merge = env->GetMethodID(cls, "partialMerge", "([B[B[BLorg/rocksdb/ReturnType;)[B");

            JNIAbstractNotAssociativeMergeOperator::multi_merge = env->GetMethodID(cls, "partialMultiMerge",
                                                                                   "([B[[BLorg/rocksdb/ReturnType;)[B");

            JNIAbstractNotAssociativeMergeOperator::should_merge = env->GetMethodID(cls, "shouldMerge", "([[B)Z");

            JNIAbstractNotAssociativeMergeOperator::full_merge = env->GetMethodID(cls, "fullMerge", "([B[B[[BLorg/rocksdb/ReturnType;)[B");


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
            jclass a = env->FindClass("Lorg/rocksdb/ReturnType;");
            if (a==0){
                cls = env->FindClass("java/lang/Error");
                env->ThrowNew(cls, "unable to find object org.rocksdb.ReturnType");

            } else  JNIAbstractNotAssociativeMergeOperator::rtClass= reinterpret_cast<jclass>(env->NewGlobalRef(a));
            rtConstructor = env->GetMethodID( a, "<init>", "()V");
            if (rtConstructor==0){
                cls = env->FindClass("java/lang/Error");
                env->ThrowNew(cls, "unable to find field ReturnType.<init>");

            }
            JNIAbstractNotAssociativeMergeOperator::rtField = env->GetFieldID( a, "isArgumentReference", "Z");
            if (JNIAbstractNotAssociativeMergeOperator::rtField==0){
                cls = env->FindClass("java/lang/Error");
                env->ThrowNew(cls, "unable to find field ReturnType.isArgumentReference");

            }
            a = env->FindClass("[B");
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
        JNIEnv*, jclass,
        jboolean allowSingleOperand,
        jboolean allowShouldMerge,
        jboolean allowPartialMultiMerge) {
    std::shared_ptr<rocksdb::MergeOperator> p= std::make_shared<rocksdb::JNIAbstractNotAssociativeMergeOperator::JNIMergeOperator>(allowSingleOperand,allowShouldMerge,allowPartialMultiMerge);
    auto* op = new std::shared_ptr<rocksdb::MergeOperator>(p);
    return reinterpret_cast<jlong>(op);
}

void Java_org_rocksdb_AbstractNotAssociativeMergeOperator_disposeInternal(
        JNIEnv* env, jobject, jlong jhandle) {
    std::shared_ptr<rocksdb::JNIAbstractNotAssociativeMergeOperator::JNIMergeOperator>*  op =
            reinterpret_cast<std::shared_ptr<rocksdb::JNIAbstractNotAssociativeMergeOperator::JNIMergeOperator>*>(jhandle);
    op->get()->destroy(env);
    delete op;
}

