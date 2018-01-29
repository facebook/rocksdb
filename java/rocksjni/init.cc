//
// Created by cristian on 27/01/18.
//
#include <jni.h>
#include <rocksjni/init.h>
#include "include/org_rocksdb_util_Environment.h"
#include <assert.h>
#include <pthread.h>
#include <vector>
//#include <iostream>
#include "init.h"


namespace rocksdb {
     JavaVM *JVM;
#ifdef JNI_VERSION_1_10
    jint jvmVersion=JNI_VERSION_1_10;
#elif defined   JNI_VERSION_1_9
      jint jvmVersion=JNI_VERSION_1_9;
#elif defined   JNI_VERSION_1_8
     jint jvmVersion=JNI_VERSION_1_8;
#elif defined   JNI_VERSION_1_7
     jint jvmVersion=JNI_VERSION_1_7;
#elif defined   JNI_VERSION_1_6
    jint jvmVersion=JNI_VERSION_1_6;
#endif

    typedef void (*callback) (JNIEnv *);

     std::vector<callback> loaders;
     std::vector<callback> unloaders;
     pthread_key_t CURRENT_ATTACHED_ENV;

    void setLoader(callback func){

        rocksdb::loaders.push_back(func);
    }
    void setUnloader(callback func){

        rocksdb::unloaders.push_back(func);
    }

    struct JNI_CONTEXT{
        JNIEnv * env;
        bool attached;
    } ;

    void _detachCurrentThread(void *addr) {


      //  std::cout<< "detaching threadid"<< pthread_self()<<"\n";
        JNI_CONTEXT* context;

        context= static_cast<JNI_CONTEXT*>(pthread_getspecific(rocksdb::CURRENT_ATTACHED_ENV));


        if (context!= nullptr) {
           // std::cout<< "deta 2"<<context<<"\n";

            if (context->attached){
            //    std::cout<< "deta execution\n";
                rocksdb::JVM->DetachCurrentThread();

            }
            if (addr== nullptr)pthread_setspecific(rocksdb::CURRENT_ATTACHED_ENV, nullptr);
            delete context;

        }

    }

    void detachCurrentThread() {
        _detachCurrentThread(nullptr);
    }



     JNIEnv *getEnv() {

         JNI_CONTEXT* context = (JNI_CONTEXT*)pthread_getspecific(rocksdb::CURRENT_ATTACHED_ENV);

       // std::cout << "1" << rocksdb::JVM << "\n";
        if (context == NULL) {
            JNIEnv * env;
            int getEnvStat = rocksdb::JVM->GetEnv((void **) &env, rocksdb::jvmVersion);
           // std::cout << "2" << rocksdb::JVM << "\n";
            if (getEnvStat == JNI_EDETACHED) {
               // std::cout << "2a" << rocksdb::JVM << "\n";
                if (rocksdb::JVM->AttachCurrentThread((void **) &env, NULL) !=
                    JNI_OK) {// expensive operation : do not detach for every invocation

                    jclass jc = env->FindClass("java/lang/Error");
                    if (jc) env->ThrowNew(jc, "unable to attach JNIEnv");

                    return NULL;
                } else {
                   // std::cout << "3"  "\n";
                    context=new JNI_CONTEXT();
                    context->attached=true;
                    context->env=env;
                    pthread_setspecific(rocksdb::CURRENT_ATTACHED_ENV, context);
                   // atexit([] { detach_current_thread(NULL); });
                  //  std::cout << "4"<<context<< "\n";
                    return env;
                }
            } else if (getEnvStat == JNI_EVERSION) {

                jclass jc = env->FindClass("java/lang/Error");
                if (jc) env->ThrowNew(jc, "unable to create JNIEnv:version not supported");

                return NULL;

            }else {//already attached
               // std::cout << "3a\n";
                context=new JNI_CONTEXT();
                context->attached=false;
                context->env=env;
                pthread_setspecific(rocksdb::CURRENT_ATTACHED_ENV, context);
             //   atexit([] { detach_current_thread(NULL); });
               // std::cout << "4b "<<context<<"\n";
              //  std::cout<< "threadid"<< pthread_self()<<"\n";
                return env;
            }
        } else {
            return context->env;
        }


    }
}

JNIEXPORT void JNICALL Java_org_rocksdb_util_Environment_detachCurrentThreadIfPossible
(JNIEnv * env, jclass clazz){
  rocksdb::_detachCurrentThread(env);

}

jint JNI_OnLoad(JavaVM *vm, void *reserved) {
   // std::cout << "init\n";
    rocksdb::JVM = vm;
    pthread_key_create(&rocksdb::CURRENT_ATTACHED_ENV, rocksdb::_detachCurrentThread);
   // atexit([] { ::pthread_exit(0); });
    JNIEnv* env=rocksdb::getEnv();

    for (auto const& func : rocksdb::loaders)
    {

        (*func)(env);
    }
    rocksdb::detachCurrentThread();
    //std::cout << "init2\n";

    return rocksdb::jvmVersion;
}

void JNI_OnUnload(JavaVM *vm, void *reserved){
    JNIEnv* env=rocksdb::getEnv();
    rocksdb::callback func;
    for (unsigned i = rocksdb::unloaders.size(); i-- > 0; )
    {
        func=rocksdb::unloaders[i];
        (*func)(env);
    }
    rocksdb::detachCurrentThread();
}