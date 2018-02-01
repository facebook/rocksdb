//
// Created by cristian on 27/01/18.
//


#pragma once
#ifndef ROCKSDB_INIT_H
#define ROCKSDB_INIT_H


#include <jni.h>
#include <pthread.h>
#ifndef UNIQUE_IDENTIFIER
#define CONCATENATE(s1, s2) s1##s2
#define EXPAND_THEN_CONCATENATE(s1, s2) CONCATENATE(s1, s2)
#ifdef __COUNTER__
#define UNIQUE_IDENTIFIER(prefix) EXPAND_THEN_CONCATENATE(prefix, __COUNTER__)
#else
#define UNIQUE_IDENTIFIER(prefix) EXPAND_THEN_CONCATENATE(prefix, __LINE__)
#endif /* COUNTER */
#endif /* UNIQUE_IDENTIFIER */
#define STATIC_BLOCK_IMPL1(prefix) \
    STATIC_BLOCK_IMPL2(CONCATENATE(prefix,_fn),CONCATENATE(prefix,_var))

#define STATIC_BLOCK_IMPL2(function_name,var_name) \
static void function_name(); \
static int var_name __attribute((unused)) = (function_name(), 0) ; \
static void function_name()

#define initialize STATIC_BLOCK_IMPL1(UNIQUE_IDENTIFIER(_static_block_))

/*
 * this module introduce a strategy for attaching/detaching thread fastly.
 * the detaching is not executed after every api execution but it is executed automatically when thread is terminating.
 * In addition the module introduce the loaders/unloaders.
 * Using the macro block "inizialize" in your module ,you define a block it will be executed on vm loading or vm unloading:
 * this permits to preload all variables used by routines.
 * The detaching is added on closing method for DB because the main thread could not to call automatically the detaching.
 *
 *
 * */

namespace rocksdb {
     extern JavaVM *JVM;
     extern jint jvmVersion;
     extern JNIEnv *getEnv();

     extern void  setLoader(void (*) (JNIEnv *));
     extern void  setUnloader(void (*) (JNIEnv *));
     extern void detachCurrentThread();
     extern  void  destroyJNIContext();



}
#endif //ROCKSDB_INIT_H
