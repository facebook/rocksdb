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
     int dbInstances=0;
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


       // std::cout<< "detaching threadid"<< pthread_self()<<" "<<addr <<" "<<static_cast<JNI_CONTEXT*>(addr)<<"\n";

        JNI_CONTEXT* context;

        context= static_cast<JNI_CONTEXT*>(addr);
        //std::cout<< "context="<< context<<"\n";

        if (context!= nullptr) {
          //  std::cout<< "detaching 1"<<context<<"\n";

            if (context->attached){
               // std::cout<< "deta execution"<<rocksdb::JVM<<"\n";

                assert(rocksdb::JVM!=nullptr);
                    rocksdb::JVM->DetachCurrentThread();

            }


            delete context;



        }

    }

    void detachCurrentThread() {

        JNI_CONTEXT*  context= static_cast<JNI_CONTEXT*>(pthread_getspecific(rocksdb::CURRENT_ATTACHED_ENV));
        pthread_setspecific(rocksdb::CURRENT_ATTACHED_ENV, nullptr);
        _detachCurrentThread(context);
    }

    void  destroyJNIContext(){



        //    std::cout << "destroy jni context\n";
            JNIEnv *env = rocksdb::getEnv();
            rocksdb::callback func;
            for (size_t i = rocksdb::unloaders.size(); i-- > 0;) {
                func = rocksdb::unloaders[i];
                (*func)(env);
            }
        rocksdb::unloaders.clear();
            rocksdb::detachCurrentThread();

    }


     JNIEnv *getEnv() {

         JNI_CONTEXT* context = (JNI_CONTEXT*)pthread_getspecific(rocksdb::CURRENT_ATTACHED_ENV);

       // std::cout << "get env: threadid "<< pthread_self()<<"\n";
        if (context == NULL) {
            JNIEnv * env;
            int getEnvStat = rocksdb::JVM->GetEnv((void **) &env, rocksdb::jvmVersion);

            if (getEnvStat == JNI_EDETACHED) {

                if (rocksdb::JVM->AttachCurrentThread((void **) &env, NULL) !=
                    JNI_OK) {// expensive operation : do not detach for every invocation

                    jclass jc = env->FindClass("java/lang/Error");
                    if (jc) env->ThrowNew(jc, "unable to attach JNIEnv");

                    return NULL;
                } else {

                    context=new JNI_CONTEXT();
                  //  std::cout << "attached" <<context<< "\n";
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
               // std::cout << " already attached\n";
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
(JNIEnv*, jclass){
  rocksdb::detachCurrentThread();

}



jint JNI_OnLoad(JavaVM *vm, void*) {
  //  std::cout << "load vm\n";
    rocksdb::JVM = vm;
    pthread_key_create(&rocksdb::CURRENT_ATTACHED_ENV, rocksdb::_detachCurrentThread);
    //std::cout << "init jni context\n";
    JNIEnv *env = rocksdb::getEnv();

    for (auto const &func : rocksdb::loaders) {

        (*func)(env);
    }
    rocksdb::loaders.clear();
    rocksdb::detachCurrentThread();
    return rocksdb::jvmVersion;
}

/*
 * fixed unload bug: this api is not called
 *
 * */

void JNI_OnUnload(JavaVM*, void*){
    rocksdb::JVM = nullptr;
}