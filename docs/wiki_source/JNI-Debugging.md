If you are a Java developer working with JNI code, debugging it can be particularly hard, for example if you are experiencing an unexpected SIGSEGV and don't know why.

There are several techniques which we can use to try and help get to the bottom of these:

1. [[Interpreting hs_err_pid files | JNI-Debugging#interpreting-hs_err_pid-files]]
2. [[ASAN | JNI-Debugging#asan]]
3. [[C++ Debugger | JNI-Debugging#c-debugger]]


# Interpreting hs_err_pid files
If the JVM crashes whilst executing our native C++ code via JNI, then it will typically write an `error report file` to a file, which on Linux may be named like `/tmp/jvm-8666/hs_error.log`, or on a Mac may be named like`hs_err_pid76448.log` in the same location that the `java` process was launched from.

Such a error report file might look like (Mac):
```hs_err
#
# A fatal error has been detected by the Java Runtime Environment:
#
#  SIGSEGV (0xb) at pc=0x00007fff87283132, pid=76448, tid=5891
#
# JRE version: Java(TM) SE Runtime Environment (7.0_80-b15) (build 1.7.0_80-b15)
# Java VM: Java HotSpot(TM) 64-Bit Server VM (24.80-b11 mixed mode bsd-amd64 compressed oops)
# Problematic frame:
# C  [libsystem_c.dylib+0x1132]  strlen+0x12
#
# Failed to write core dump. Core dumps have been disabled. To enable core dumping, try "ulimit -c unlimited" before starting Java again
#
# If you would like to submit a bug report, please visit:
#   http://bugreport.java.com/bugreport/crash.jsp
# The crash happened outside the Java Virtual Machine in native code.
# See problematic frame for where to report the bug.
#

---------------  T H R E A D  ---------------

Current thread (0x00007fc3c2007800):  JavaThread "main" [_thread_in_native, id=5891, stack(0x000070000011a000,0x000070000021a000)]

siginfo:si_signo=SIGSEGV: si_errno=0, si_code=1 (SEGV_MAPERR), si_addr=0x00000000fffffff0

Registers:
RAX=0x00000000ffffffff, RBX=0x00000000ffffffff, RCX=0x00000000ffffffff, RDX=0x00000000ffffffff
RSP=0x0000700000216450, RBP=0x0000700000216450, RSI=0x0000000000000007, RDI=0x00000000fffffff0
R8 =0x00000000fffffffc, R9 =0x00007fc3c143f8e8, R10=0x00000000ffffffff, R11=0x0000000102ac7f40
R12=0x00007fc3c288a600, R13=0x00007fc3c1442bf8, R14=0x00007fc3c288a638, R15=0x00007fc3c288a638
RIP=0x00007fff87283132, EFLAGS=0x0000000000010206, ERR=0x0000000000000004
  TRAPNO=0x000000000000000e

Top of Stack: (sp=0x0000700000216450)
0x0000700000216450:   0000700000216490 0000000112bb538a
0x0000700000216460:   0000000000000000 0000000000000000
0x0000700000216470:   0000000000000000 00007fc3c289c800
0x0000700000216480:   00007fc3c288a638 00007fc3c143c0e8
0x0000700000216490:   00007000002166f0 0000000112bcd82c
0x00007000002164a0:   303733312e34333a 00007fc3c288a608
0x00007000002164b0:   0000700000216ca8 697265766f636552
0x00007000002164c0:   206d6f726620676e 74736566696e616d
0x00007000002164d0:   4d203a656c696620 2d54534546494e41
0x00007000002164e0:   00007fc3c289c800 00007fc3c143a870
0x00007000002164f0:   0000000000000060 00007fc3c1530e40
0x0000700000216500:   00007fc300000006 0000000100c2d000
0x0000700000216510:   00007fc3c1500000 0000700000216df0
0x0000700000216520:   0000700000216550 0000700000216df8
0x0000700000216530:   0000700000216e00 00007fc3c28d7000
0x0000700000216540:   0000000100c30a00 0000000000000006
0x0000700000216550:   0000700000216590 00007fff91b94154
0x0000700000216560:   00007fc3c28a6808 0000000000000004
0x0000700000216570:   0000000100c47a00 0000000100c2d000
0x0000700000216580:   0000000100c48e00 0000000000001400
0x0000700000216590:   0000700000216680 00007fff91b90ee5
0x00007000002165a0:   0000000000000001 00007fc3c1530e46
0x00007000002165b0:   00007000002166a0 00007fff91b90a26
0x00007000002165c0:   0000000000001400 0000000100c30a00
0x00007000002165d0:   00007000002166c0 00007fff91b90a26
0x00007000002165e0:   00007fc3c28a6808 0000000000000006
0x00007000002165f0:   0000000100c47a00 0000000100c2d000
0x0000700000216600:   0000000000001400 0000000100c30a00
0x0000700000216610:   0000700000216700 00000000000006e8
0x0000700000216620:   0000000000001002 0000000000c31e00
0x0000700000216630:   0000000000001002 0000000100c48e00
0x0000700000216640:   ff80000000001002 00000000c153ffff 

Instructions: (pc=0x00007fff87283132)
0x00007fff87283112:   0e 01 f3 0f 7f 44 0f 01 5d c3 90 90 90 90 55 48
0x00007fff87283122:   89 e5 48 89 f9 48 89 fa 48 83 e7 f0 66 0f ef c0
0x00007fff87283132:   66 0f 74 07 66 0f d7 f0 48 83 e1 0f 48 83 c8 ff
0x00007fff87283142:   48 d3 e0 21 c6 74 17 0f bc c6 48 29 d7 48 01 f8 

Register to memory mapping:

RAX=0x00000000ffffffff is an unknown value
RBX=0x00000000ffffffff is an unknown value
RCX=0x00000000ffffffff is an unknown value
RDX=0x00000000ffffffff is an unknown value
RSP=0x0000700000216450 is pointing into the stack for thread: 0x00007fc3c2007800
RBP=0x0000700000216450 is pointing into the stack for thread: 0x00007fc3c2007800
RSI=0x0000000000000007 is an unknown value
RDI=0x00000000fffffff0 is an unknown value
R8 =0x00000000fffffffc is an unknown value
R9 =0x00007fc3c143f8e8 is an unknown value
R10=0x00000000ffffffff is an unknown value
R11=0x0000000102ac7f40 is at entry_point+0 in (nmethod*)0x0000000102ac7e10
R12=0x00007fc3c288a600 is an unknown value
R13=0x00007fc3c1442bf8 is an unknown value
R14=0x00007fc3c288a638 is an unknown value
R15=0x00007fc3c288a638 is an unknown value


Stack: [0x000070000011a000,0x000070000021a000],  sp=0x0000700000216450,  free space=1009k
Native frames: (J=compiled Java code, j=interpreted, Vv=VM code, C=native code)
C  [libsystem_c.dylib+0x1132]  strlen+0x12
C  [librocksdbjni-osx.jnilib+0x1c38a]  rocksdb::InternalKeyComparator::InternalKeyComparator(rocksdb::Comparator const*)+0x4a
C  [librocksdbjni-osx.jnilib+0x3482c]  rocksdb::ColumnFamilyData::ColumnFamilyData(unsigned int, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, rocksdb::Version*, rocksdb::Cache*, rocksdb::WriteBufferManager*, rocksdb::ColumnFamilyOptions const&, rocksdb::DBOptions const*, rocksdb::EnvOptions const&, rocksdb::ColumnFamilySet*)+0x7c
C  [librocksdbjni-osx.jnilib+0x382dd]  rocksdb::ColumnFamilySet::CreateColumnFamily(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, unsigned int, rocksdb::Version*, rocksdb::ColumnFamilyOptions const&)+0x7d
C  [librocksdbjni-osx.jnilib+0x12ca6f]  rocksdb::VersionSet::CreateColumnFamily(rocksdb::ColumnFamilyOptions const&, rocksdb::VersionEdit*)+0xaf
C  [librocksdbjni-osx.jnilib+0x12d831]  rocksdb::VersionSet::Recover(std::__1::vector<rocksdb::ColumnFamilyDescriptor, std::__1::allocator<rocksdb::ColumnFamilyDescriptor> > const&, bool)+0xc51
C  [librocksdbjni-osx.jnilib+0x80df4]  rocksdb::DBImpl::Recover(std::__1::vector<rocksdb::ColumnFamilyDescriptor, std::__1::allocator<rocksdb::ColumnFamilyDescriptor> > const&, bool, bool, bool)+0x244
C  [librocksdbjni-osx.jnilib+0x9c0aa]  rocksdb::DB::Open(rocksdb::DBOptions const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, std::__1::vector<rocksdb::ColumnFamilyDescriptor, std::__1::allocator<rocksdb::ColumnFamilyDescriptor> > const&, std::__1::vector<rocksdb::ColumnFamilyHandle*, std::__1::allocator<rocksdb::ColumnFamilyHandle*> >*, rocksdb::DB**)+0xb0a
C  [librocksdbjni-osx.jnilib+0x9b138]  rocksdb::DB::Open(rocksdb::Options const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, rocksdb::DB**)+0x448
C  [librocksdbjni-osx.jnilib+0x1234c]  std::__1::__function::__func<rocksdb::Status (*)(rocksdb::Options const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, rocksdb::DB**), std::__1::allocator<rocksdb::Status (*)(rocksdb::Options const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, rocksdb::DB**)>, rocksdb::Status (rocksdb::Options const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, rocksdb::DB**)>::operator()(rocksdb::Options const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, rocksdb::DB**&&)+0x1c
C  [librocksdbjni-osx.jnilib+0xd84b]  rocksdb_open_helper(JNIEnv_*, long, _jstring*, std::__1::function<rocksdb::Status (rocksdb::Options const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, rocksdb::DB**)>)+0x8b
C  [librocksdbjni-osx.jnilib+0xd9ab]  Java_org_rocksdb_RocksDB_open__JLjava_lang_String_2+0x4b
j  org.rocksdb.RocksDB.open(JLjava/lang/String;)J+0
j  org.rocksdb.RocksDB.open(Lorg/rocksdb/Options;Ljava/lang/String;)Lorg/rocksdb/RocksDB;+9
j  org.rocksdb.util.BytewiseComparatorTest.openDatabase(Ljava/nio/file/Path;Lorg/rocksdb/AbstractComparator;)Lorg/rocksdb/RocksDB;+28
j  org.rocksdb.util.BytewiseComparatorTest.java_vs_java_directBytewiseComparator()V+37
v  ~StubRoutines::call_stub
V  [libjvm.dylib+0x2dc898]  JavaCalls::call_helper(JavaValue*, methodHandle*, JavaCallArguments*, Thread*)+0x22a
V  [libjvm.dylib+0x2dc668]  JavaCalls::call(JavaValue*, methodHandle, JavaCallArguments*, Thread*)+0x28
V  [libjvm.dylib+0x468428]  Reflection::invoke(instanceKlassHandle, methodHandle, Handle, bool, objArrayHandle, BasicType, objArrayHandle, bool, Thread*)+0x9fc
V  [libjvm.dylib+0x46888e]  Reflection::invoke_method(oopDesc*, Handle, objArrayHandle, Thread*)+0x16e
V  [libjvm.dylib+0x329247]  JVM_InvokeMethod+0x166
j  sun.reflect.NativeMethodAccessorImpl.invoke0(Ljava/lang/reflect/Method;Ljava/lang/Object;[Ljava/lang/Object;)Ljava/lang/Object;+0
j  sun.reflect.NativeMethodAccessorImpl.invoke(Ljava/lang/Object;[Ljava/lang/Object;)Ljava/lang/Object;+87
j  sun.reflect.DelegatingMethodAccessorImpl.invoke(Ljava/lang/Object;[Ljava/lang/Object;)Ljava/lang/Object;+6
j  java.lang.reflect.Method.invoke(Ljava/lang/Object;[Ljava/lang/Object;)Ljava/lang/Object;+57
j  org.junit.runners.model.FrameworkMethod$1.runReflectiveCall()Ljava/lang/Object;+15
j  org.junit.internal.runners.model.ReflectiveCallable.run()Ljava/lang/Object;+1
j  org.junit.runners.model.FrameworkMethod.invokeExplosively(Ljava/lang/Object;[Ljava/lang/Object;)Ljava/lang/Object;+10
j  org.junit.internal.runners.statements.InvokeMethod.evaluate()V+12
j  org.junit.runners.ParentRunner.runLeaf(Lorg/junit/runners/model/Statement;Lorg/junit/runner/Description;Lorg/junit/runner/notification/RunNotifier;)V+17
j  org.junit.runners.BlockJUnit4ClassRunner.runChild(Lorg/junit/runners/model/FrameworkMethod;Lorg/junit/runner/notification/RunNotifier;)V+30
j  org.junit.runners.BlockJUnit4ClassRunner.runChild(Ljava/lang/Object;Lorg/junit/runner/notification/RunNotifier;)V+6
j  org.junit.runners.ParentRunner$3.run()V+12
j  org.junit.runners.ParentRunner$1.schedule(Ljava/lang/Runnable;)V+1
j  org.junit.runners.ParentRunner.runChildren(Lorg/junit/runner/notification/RunNotifier;)V+44
j  org.junit.runners.ParentRunner.access$000(Lorg/junit/runners/ParentRunner;Lorg/junit/runner/notification/RunNotifier;)V+2
j  org.junit.runners.ParentRunner$2.evaluate()V+8
j  org.junit.runners.ParentRunner.run(Lorg/junit/runner/notification/RunNotifier;)V+20
j  org.junit.runners.Suite.runChild(Lorg/junit/runner/Runner;Lorg/junit/runner/notification/RunNotifier;)V+2
j  org.junit.runners.Suite.runChild(Ljava/lang/Object;Lorg/junit/runner/notification/RunNotifier;)V+6
j  org.junit.runners.ParentRunner$3.run()V+12
j  org.junit.runners.ParentRunner$1.schedule(Ljava/lang/Runnable;)V+1
j  org.junit.runners.ParentRunner.runChildren(Lorg/junit/runner/notification/RunNotifier;)V+44
j  org.junit.runners.ParentRunner.access$000(Lorg/junit/runners/ParentRunner;Lorg/junit/runner/notification/RunNotifier;)V+2
j  org.junit.runners.ParentRunner$2.evaluate()V+8
j  org.junit.runners.ParentRunner.run(Lorg/junit/runner/notification/RunNotifier;)V+20
j  org.junit.runner.JUnitCore.run(Lorg/junit/runner/Runner;)Lorg/junit/runner/Result;+37
j  org.junit.runner.JUnitCore.run(Lorg/junit/runner/Request;)Lorg/junit/runner/Result;+5
j  org.junit.runner.JUnitCore.run(Lorg/junit/runner/Computer;[Ljava/lang/Class;)Lorg/junit/runner/Result;+6
j  org.junit.runner.JUnitCore.run([Ljava/lang/Class;)Lorg/junit/runner/Result;+5
j  org.rocksdb.test.RocksJunitRunner.main([Ljava/lang/String;)V+93
v  ~StubRoutines::call_stub
V  [libjvm.dylib+0x2dc898]  JavaCalls::call_helper(JavaValue*, methodHandle*, JavaCallArguments*, Thread*)+0x22a
V  [libjvm.dylib+0x2dc668]  JavaCalls::call(JavaValue*, methodHandle, JavaCallArguments*, Thread*)+0x28
V  [libjvm.dylib+0x31004e]  jni_invoke_static(JNIEnv_*, JavaValue*, _jobject*, JNICallType, _jmethodID*, JNI_ArgumentPusher*, Thread*)+0xe6
V  [libjvm.dylib+0x3092d5]  jni_CallStaticVoidMethodV+0x9c
V  [libjvm.dylib+0x31c28e]  checked_jni_CallStaticVoidMethod+0x16f
C  [java+0x30fe]  JavaMain+0x91d
C  [libsystem_pthread.dylib+0x399d]  _pthread_body+0x83
C  [libsystem_pthread.dylib+0x391a]  _pthread_body+0x0
C  [libsystem_pthread.dylib+0x1351]  thread_start+0xd

Java frames: (J=compiled Java code, j=interpreted, Vv=VM code)
j  org.rocksdb.RocksDB.open(JLjava/lang/String;)J+0
j  org.rocksdb.RocksDB.open(Lorg/rocksdb/Options;Ljava/lang/String;)Lorg/rocksdb/RocksDB;+9
j  org.rocksdb.util.BytewiseComparatorTest.openDatabase(Ljava/nio/file/Path;Lorg/rocksdb/AbstractComparator;)Lorg/rocksdb/RocksDB;+28
j  org.rocksdb.util.BytewiseComparatorTest.java_vs_java_directBytewiseComparator()V+37
v  ~StubRoutines::call_stub
j  sun.reflect.NativeMethodAccessorImpl.invoke0(Ljava/lang/reflect/Method;Ljava/lang/Object;[Ljava/lang/Object;)Ljava/lang/Object;+0
j  sun.reflect.NativeMethodAccessorImpl.invoke(Ljava/lang/Object;[Ljava/lang/Object;)Ljava/lang/Object;+87
j  sun.reflect.DelegatingMethodAccessorImpl.invoke(Ljava/lang/Object;[Ljava/lang/Object;)Ljava/lang/Object;+6
j  java.lang.reflect.Method.invoke(Ljava/lang/Object;[Ljava/lang/Object;)Ljava/lang/Object;+57
j  org.junit.runners.model.FrameworkMethod$1.runReflectiveCall()Ljava/lang/Object;+15
j  org.junit.internal.runners.model.ReflectiveCallable.run()Ljava/lang/Object;+1
j  org.junit.runners.model.FrameworkMethod.invokeExplosively(Ljava/lang/Object;[Ljava/lang/Object;)Ljava/lang/Object;+10
j  org.junit.internal.runners.statements.InvokeMethod.evaluate()V+12
j  org.junit.runners.ParentRunner.runLeaf(Lorg/junit/runners/model/Statement;Lorg/junit/runner/Description;Lorg/junit/runner/notification/RunNotifier;)V+17
j  org.junit.runners.BlockJUnit4ClassRunner.runChild(Lorg/junit/runners/model/FrameworkMethod;Lorg/junit/runner/notification/RunNotifier;)V+30
j  org.junit.runners.BlockJUnit4ClassRunner.runChild(Ljava/lang/Object;Lorg/junit/runner/notification/RunNotifier;)V+6
j  org.junit.runners.ParentRunner$3.run()V+12
j  org.junit.runners.ParentRunner$1.schedule(Ljava/lang/Runnable;)V+1
j  org.junit.runners.ParentRunner.runChildren(Lorg/junit/runner/notification/RunNotifier;)V+44
j  org.junit.runners.ParentRunner.access$000(Lorg/junit/runners/ParentRunner;Lorg/junit/runner/notification/RunNotifier;)V+2
j  org.junit.runners.ParentRunner$2.evaluate()V+8
j  org.junit.runners.ParentRunner.run(Lorg/junit/runner/notification/RunNotifier;)V+20
j  org.junit.runners.Suite.runChild(Lorg/junit/runner/Runner;Lorg/junit/runner/notification/RunNotifier;)V+2
j  org.junit.runners.Suite.runChild(Ljava/lang/Object;Lorg/junit/runner/notification/RunNotifier;)V+6
j  org.junit.runners.ParentRunner$3.run()V+12
j  org.junit.runners.ParentRunner$1.schedule(Ljava/lang/Runnable;)V+1
j  org.junit.runners.ParentRunner.runChildren(Lorg/junit/runner/notification/RunNotifier;)V+44
j  org.junit.runners.ParentRunner.access$000(Lorg/junit/runners/ParentRunner;Lorg/junit/runner/notification/RunNotifier;)V+2
j  org.junit.runners.ParentRunner$2.evaluate()V+8
j  org.junit.runners.ParentRunner.run(Lorg/junit/runner/notification/RunNotifier;)V+20
j  org.junit.runner.JUnitCore.run(Lorg/junit/runner/Runner;)Lorg/junit/runner/Result;+37
j  org.junit.runner.JUnitCore.run(Lorg/junit/runner/Request;)Lorg/junit/runner/Result;+5
j  org.junit.runner.JUnitCore.run(Lorg/junit/runner/Computer;[Ljava/lang/Class;)Lorg/junit/runner/Result;+6
j  org.junit.runner.JUnitCore.run([Ljava/lang/Class;)Lorg/junit/runner/Result;+5
j  org.rocksdb.test.RocksJunitRunner.main([Ljava/lang/String;)V+93
v  ~StubRoutines::call_stub

---------------  P R O C E S S  ---------------

... truncated for brevity!

```

## Stack
The most interesting part of the trace is likely the stack frames. For example consider this frame:
```
C  [librocksdbjni-osx.jnilib+0x1c38a]  rocksdb::InternalKeyComparator::InternalKeyComparator(rocksdb::Comparator const*)+0x4a
```
We can see that something went wrong from a function (in this case a constructor) in `rocksdb::InternalKeyComparator`, however how do we relate these back to file and line-numbers in our source code?

We have to translate the offsets provided in the trace:

### Mac OS X
On a Mac this would look like:
```bash
$ atos -o java/target/librocksdbjni-osx.jnilib 0x1c38a
ava_org_rocksdb_Logger_setInfoLogLevel (in librocksdbjni-osx.jnilib) (loggerjnicallback.cc:152)
```

### Linux
On a Linux system this would look like:
```bash
$ addr2line -e java/target/librocksjni-linux64.so 0x1c38a
```

** TODO **

# ASAN
[ASAN](https://github.com/google/sanitizers/wiki/AddressSanitizer) (Google Address Sanitizer) attempts to detect a whole range of memory and range issues and can be compiled into your code, at runtime, it will report some memory or buffer-range violations.

## Mac (Apple LLVM 7.3.0)
1. Set JDK 7 as required by RocksJava
    ```bash
    export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.7.0_80.jdk/Contents/Home
    ```

2. Ensure a clean start:
    ```bash
    make clean jclean
    ```

3. Compile the Java test suite with ASAN compiled in:
    ```bash
    DEBUG_LEVEL=2 COMPILE_WITH_ASAN=true make jtest_compile
    ```

4. Execute the entire Java Test Suite:
    ```bash
    make jtest_run
    ```

   or for a single test (e.g. `ComparatorTest`), execute:

    ```bash
    cd java
    java -ea -Xcheck:jni -Djava.library.path=target -cp "target/classes:target/test-classes:test-libs/junit-4.12.jar:test-libs/hamcrest-core-1.3.jar:test-libs/mockito-all-1.10.19.jar:test-libs/cglib-2.2.2.jar:test-libs/assertj-core-1.7.1.jar:target/*" org.rocksdb.test.RocksJunitRunner org.rocksdb.ComparatorTest
    ```

*NOTE*: if you see an error like:
```
==20705==ERROR: Interceptors are not working. This may be because AddressSanitizer is loaded too late (e.g. via dlopen). Please launch the executable with:
DYLD_INSERT_LIBRARIES=/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib/clang/8.1.0/lib/darwin/libclang_rt.asan_osx_dynamic.dylib
"interceptors not installed" && 0
```

Then you need to first execute:
```
$ export 
DYLD_INSERT_LIBRARIES=/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib/clang/14.0.0/lib/darwin/libclang_rt.asan_osx_dynamic.dylib
```
NOTE [bug](https://bugs.llvm.org/show_bug.cgi?id=31861) that this MAY NOT WORK on the command line, but in which case set `DYLD_INSERT_LIBRARIES` in the environment of your *IntelliJ* run configuration, and try that.

If ASAN detects an issue, you will see output similar to the following:
```bash
Run: org.rocksdb.BackupableDBOptionsTest testing now -> destroyOldData 
Run: org.rocksdb.BackupEngineTest testing now -> deleteBackup 
=================================================================
==80632==ERROR: AddressSanitizer: unknown-crash on address 0x7fd93940d6e8 at pc 0x00011cebe075 bp 0x70000020ffe0 sp 0x70000020ffd8
WRITE of size 8 at 0x7fd93940d6e8 thread T0
    #0 0x11cebe074 in rocksdb::PosixLogger::PosixLogger(__sFILE*, unsigned long long (*)(), rocksdb::Env*, rocksdb::InfoLogLevel) posix_logger.h:47
    #1 0x11cebc847 in rocksdb::PosixLogger::PosixLogger(__sFILE*, unsigned long long (*)(), rocksdb::Env*, rocksdb::InfoLogLevel) posix_logger.h:53
    #2 0x11ce9888c in rocksdb::(anonymous namespace)::PosixEnv::NewLogger(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, std::__1::shared_ptr<rocksdb::Logger>*) env_posix.cc:574
    #3 0x11c09a3e3 in rocksdb::CreateLoggerFromOptions(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, rocksdb::DBOptions const&, std::__1::shared_ptr<rocksdb::Logger>*) auto_roll_logger.cc:166
    #4 0x11c3a8a55 in rocksdb::SanitizeOptions(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, rocksdb::DBOptions const&) db_impl.cc:143
    #5 0x11c3ac2f3 in rocksdb::DBImpl::DBImpl(rocksdb::DBOptions const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&) db_impl.cc:307
    #6 0x11c3b38b4 in rocksdb::DBImpl::DBImpl(rocksdb::DBOptions const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&) db_impl.cc:350
    #7 0x11c4497bc in rocksdb::DB::Open(rocksdb::DBOptions const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, std::__1::vector<rocksdb::ColumnFamilyDescriptor, std::__1::allocator<rocksdb::ColumnFamilyDescriptor> > const&, std::__1::vector<rocksdb::ColumnFamilyHandle*, std::__1::allocator<rocksdb::ColumnFamilyHandle*> >*, rocksdb::DB**) db_impl.cc:5665
    #8 0x11c447b74 in rocksdb::DB::Open(rocksdb::Options const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, rocksdb::DB**) db_impl.cc:5633
    #9 0x11bff8ca4 in rocksdb::Status std::__1::__invoke_void_return_wrapper<rocksdb::Status>::__call<rocksdb::Status (*&)(rocksdb::Options const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, rocksdb::DB**), rocksdb::Options const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, rocksdb::DB**>(rocksdb::Status (*&&&)(rocksdb::Options const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, rocksdb::DB**), rocksdb::Options const&&&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&&&, rocksdb::DB**&&) __functional_base:437
    #10 0x11bff89ff in std::__1::__function::__func<rocksdb::Status (*)(rocksdb::Options const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, rocksdb::DB**), std::__1::allocator<rocksdb::Status (*)(rocksdb::Options const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, rocksdb::DB**)>, rocksdb::Status (rocksdb::Options const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, rocksdb::DB**)>::operator()(rocksdb::Options const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, rocksdb::DB**&&) functional:1437
    #11 0x11bff269b in std::__1::function<rocksdb::Status (rocksdb::Options const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, rocksdb::DB**)>::operator()(rocksdb::Options const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, rocksdb::DB**) const functional:1817
    #12 0x11bfd6edb in rocksdb_open_helper(JNIEnv_*, long, _jstring*, std::__1::function<rocksdb::Status (rocksdb::Options const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, rocksdb::DB**)>) rocksjni.cc:37
    #13 0x11bfd723e in Java_org_rocksdb_RocksDB_open__JLjava_lang_String_2 rocksjni.cc:55
    #14 0x10be77757  (<unknown module>)
    #15 0x10be6b174  (<unknown module>)
    #16 0x10be6b232  (<unknown module>)
    #17 0x10be654e6  (<unknown module>)
    #18 0x10b6dc897 in JavaCalls::call_helper(JavaValue*, methodHandle*, JavaCallArguments*, Thread*) (libjvm.dylib+0x2dc897)
    #19 0x10b6dc667 in JavaCalls::call(JavaValue*, methodHandle, JavaCallArguments*, Thread*) (libjvm.dylib+0x2dc667)
    #20 0x10b868427 in Reflection::invoke(instanceKlassHandle, methodHandle, Handle, bool, objArrayHandle, BasicType, objArrayHandle, bool, Thread*) (libjvm.dylib+0x468427)
    #21 0x10b86888d in Reflection::invoke_method(oopDesc*, Handle, objArrayHandle, Thread*) (libjvm.dylib+0x46888d)
    #22 0x10b729246 in JVM_InvokeMethod (libjvm.dylib+0x329246)
    #23 0x10be77757  (<unknown module>)
    #24 0x10be6b232  (<unknown module>)
    #25 0x10be6b232  (<unknown module>)
    #26 0x10be6b8e0  (<unknown module>)
    #27 0x10be6b232  (<unknown module>)
    #28 0x10be6b232  (<unknown module>)
    #29 0x10be6b232  (<unknown module>)
    #30 0x10be6b232  (<unknown module>)
    #31 0x10be6b057  (<unknown module>)
    #32 0x10be6b057  (<unknown module>)
    #33 0x10be6b057  (<unknown module>)
    #34 0x10be6b057  (<unknown module>)
    #35 0x10be6b057  (<unknown module>)
    #36 0x10be6b057  (<unknown module>)
    #37 0x10be6b057  (<unknown module>)
    #38 0x10be6b705  (<unknown module>)
    #39 0x10be6b705  (<unknown module>)
    #40 0x10be6b057  (<unknown module>)
    #41 0x10be6b057  (<unknown module>)
    #42 0x10be6b057  (<unknown module>)
    #43 0x10be6b057  (<unknown module>)
    #44 0x10be6b057  (<unknown module>)
    #45 0x10be6b057  (<unknown module>)
    #46 0x10be6b057  (<unknown module>)
    #47 0x10be6b057  (<unknown module>)
    #48 0x10be6b705  (<unknown module>)
    #49 0x10be6b705  (<unknown module>)
    #50 0x10be6b057  (<unknown module>)
    #51 0x10be6b057  (<unknown module>)
    #52 0x10be6b057  (<unknown module>)
    #53 0x10be6b057  (<unknown module>)
    #54 0x10be6b232  (<unknown module>)
    #55 0x10be6b232  (<unknown module>)
    #56 0x10be6b232  (<unknown module>)
    #57 0x10be6b232  (<unknown module>)
    #58 0x10be654e6  (<unknown module>)
    #59 0x10b6dc897 in JavaCalls::call_helper(JavaValue*, methodHandle*, JavaCallArguments*, Thread*) (libjvm.dylib+0x2dc897)
    #60 0x10b6dc667 in JavaCalls::call(JavaValue*, methodHandle, JavaCallArguments*, Thread*) (libjvm.dylib+0x2dc667)
    #61 0x10b71004d in jni_invoke_static(JNIEnv_*, JavaValue*, _jobject*, JNICallType, _jmethodID*, JNI_ArgumentPusher*, Thread*) (libjvm.dylib+0x31004d)
    #62 0x10b7092d4 in jni_CallStaticVoidMethodV (libjvm.dylib+0x3092d4)
    #63 0x10b71c28d in checked_jni_CallStaticVoidMethod (libjvm.dylib+0x31c28d)
    #64 0x109fdd0fd in JavaMain (java+0x1000030fd)
    #65 0x7fff8df9c99c in _pthread_body (libsystem_pthread.dylib+0x399c)
    #66 0x7fff8df9c919 in _pthread_start (libsystem_pthread.dylib+0x3919)
    #67 0x7fff8df9a350 in thread_start (libsystem_pthread.dylib+0x1350)

AddressSanitizer can not describe address in more detail (wild memory access suspected).
SUMMARY: AddressSanitizer: unknown-crash posix_logger.h:47 in rocksdb::PosixLogger::PosixLogger(__sFILE*, unsigned long long (*)(), rocksdb::Env*, rocksdb::InfoLogLevel)
Shadow bytes around the buggy address:
  0x1ffb27281a80: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
  0x1ffb27281a90: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
  0x1ffb27281aa0: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
  0x1ffb27281ab0: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
  0x1ffb27281ac0: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
=>0x1ffb27281ad0: 00 00 00 00 00 00 00 00 00 04 00 00 00[04]00 00
  0x1ffb27281ae0: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
  0x1ffb27281af0: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
  0x1ffb27281b00: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
  0x1ffb27281b10: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
  0x1ffb27281b20: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
Shadow byte legend (one shadow byte represents 8 application bytes):
  Addressable:           00
  Partially addressable: 01 02 03 04 05 06 07 
  Heap left redzone:       fa
  Heap right redzone:      fb
  Freed heap region:       fd
  Stack left redzone:      f1
  Stack mid redzone:       f2
  Stack right redzone:     f3
  Stack partial redzone:   f4
  Stack after return:      f5
  Stack use after scope:   f8
  Global redzone:          f9
  Global init order:       f6
  Poisoned by user:        f7
  Container overflow:      fc
  Array cookie:            ac
  Intra object redzone:    bb
  ASan internal:           fe
  Left alloca redzone:     ca
  Right alloca redzone:    cb
==80632==ABORTING
make[1]: *** [run_test] Abort trap: 6
make: *** [jtest_run] Error 2

```


The output from ASAN shows a stack-trace with file names and line numbers of our C++ code that led to the issue, hopefully this helps shed some light on where the issue is occurring and perhaps why.

Unfortunately all of those `(<unknown module>)` are execution paths inside the JVM, ASAN cannot discover them because the JVM we are using was not itself build with support for ASAN. We could attempt to build our own JVM from the OpenJDK project and include ASAN, but at the moment that process for Mac OS X seems to be broken: https://github.com/hgomez/obuildfactory/issues/51.


** TODO ** Note the path of the DSO for libasan on Mac OS X: `/Library/Developer/CommandLineTools/usr/lib/clang/7.3.0/lib/darwin/libclang_rt.asan_osx_dynamic.dylib`


## Linux (CentOS 7) (GCC 4.8.5)
1. Set JDK 7 as required by RocksJava
    ```bash
    export JAVA_HOME="/usr/lib/jvm/java-1.7.0-openjdk"
    export PATH="${PATH}:${JAVA_HOME}/bin"
    ```
   You might also need to run `sudo alternatives --config java` and select OpenJDK 7.

2. Ensure a clean start:
    ```bash
    make clean jclean
    ```

3. Compile the Java test suite with ASAN compiled in:
    ```bash
    DEBUG_LEVEL=2 COMPILE_WITH_ASAN=true make jtest_compile
    ```

4. Execute the entire Java Test Suite:
    ```bash
    LD_PRELOAD=/usr/lib64/libasan.so.0 make jtest_run
    ```

    or for a single test (e.g. `ComparatorTest`), execute:

    ```bash
    cd java
    LD_PRELOAD=/usr/lib64/libasan.so.0 java -ea -Xcheck:jni -Djava.library.path=target -cp "target/classes:target/test-classes:test-libs/junit-4.12.jar:test-libs/hamcrest-core-1.3.jar:test-libs/mockito-all-1.10.19.jar:test-libs/cglib-2.2.2.jar:test-libs/assertj-core-1.7.1.jar:target/*" org.rocksdb.test.RocksJunitRunner org.rocksdb.ComparatorTest
    ```

If ASAN detects an issue, you will see output similar to the following:
```bash
Run: org.rocksdb.util.BytewiseComparatorTest testing now -> java_vs_java_directBytewiseComparator 
ASAN:SIGSEGV
=================================================================
==4665== ERROR: AddressSanitizer: SEGV on unknown address 0x0000fffffff0 (pc 0x7fd481f913e5 sp 0x7fd48599e308 bp 0x7fd48599e340 T1)
AddressSanitizer can not provide additional info.
    #0 0x7fd481f913e4 (/usr/lib64/libc-2.17.so+0x1633e4)
    #1 0x7fd48282da65 (/usr/lib64/libasan.so.0.0.0+0xfa65)
    #2 0x7fd481be5944 (/usr/lib64/libstdc++.so.6.0.19+0xbf944)
    #3 0x7fd3c57bcfc2 (/home/aretter/rocksdb/java/target/librocksdbjni-linux64.so+0x714fc2)
    #4 0x7fd3c57edb07 (/home/aretter/rocksdb/java/target/librocksdbjni-linux64.so+0x745b07)
    #5 0x7fd3c57f215d (/home/aretter/rocksdb/java/target/librocksdbjni-linux64.so+0x74a15d)
    #6 0x7fd3c59f3774 (/home/aretter/rocksdb/java/target/librocksdbjni-linux64.so+0x94b774)
    #7 0x7fd3c59eb598 (/home/aretter/rocksdb/java/target/librocksdbjni-linux64.so+0x943598)
    #8 0x7fd3c58a2c11 (/home/aretter/rocksdb/java/target/librocksdbjni-linux64.so+0x7fac11)
    #9 0x7fd3c58c64bf (/home/aretter/rocksdb/java/target/librocksdbjni-linux64.so+0x81e4bf)
    #10 0x7fd3c58c5bc8 (/home/aretter/rocksdb/java/target/librocksdbjni-linux64.so+0x81dbc8)
    #11 0x7fd3c57a7bc4 (/home/aretter/rocksdb/java/target/librocksdbjni-linux64.so+0x6ffbc4)
    #12 0x7fd3c57a5fc5 (/home/aretter/rocksdb/java/target/librocksdbjni-linux64.so+0x6fdfc5)
    #13 0x7fd3c579bd80 (/home/aretter/rocksdb/java/target/librocksdbjni-linux64.so+0x6f3d80)
    #14 0x7fd3c579bef7 (/home/aretter/rocksdb/java/target/librocksdbjni-linux64.so+0x6f3ef7)
    #15 0x7fd47c86ae97 (+0x14e97)
Thread T1 created by T0 here:
    #0 0x7fd482828c3a (/usr/lib64/libasan.so.0.0.0+0xac3a)
    #1 0x7fd4823fd7cf (/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.101-2.6.6.1.el7_2.x86_64/jre/lib/amd64/jli/libjli.so+0x97cf)
    #2 0x7fd4823f8386 (/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.101-2.6.6.1.el7_2.x86_64/jre/lib/amd64/jli/libjli.so+0x4386)
    #3 0x7fd4823f8e38 (/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.101-2.6.6.1.el7_2.x86_64/jre/lib/amd64/jli/libjli.so+0x4e38)
    #4 0x400774 (/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.101-2.6.6.1.el7_2.x86_64/jre-abrt/bin/java+0x400774)
    #5 0x7fd481e4fb14 (/usr/lib64/libc-2.17.so+0x21b14)
==4665== ABORTING
make[1]: *** [run_test] Error 1
make[1]: Leaving directory `/home/aretter/rocksdb/java'
make: *** [jtest_run] Error 2
```

The addresses presented in the stack-trace from GCC ASAN on Linux, can be translated into file and line-numbers by using `addr2line`, for example:

Given the stack frame (from above):
```bash
#3 0x7fd3c57bcfc2 (/home/aretter/rocksdb/java/target/librocksdbjni-linux64.so+0x714fc2)
```

We can translate it with the command:

```bash
$ addr2line -e java/target/librocksdbjni-linux64.so 0x714fc2
/home/aretter/rocksdb/./db/dbformat.h:126 
```

## Linux (Ubuntu 16.04) (GCC 5.4.0)
1. Set JDK 7 as required by RocksJava
    ```bash
    export JAVA_HOME="/usr/lib/jvm/java-7-openjdk-amd64"
    export PATH="${PATH}:${JAVA_HOME}/bin"
    ```
   You might also need to run `sudo alternatives --config java` and select OpenJDK 7.

2. Ensure a clean start:
    ```bash
    make clean jclean
    ```

3. Compile the Java test suite with ASAN compiled in:
    ```bash
    DEBUG_LEVEL=2 COMPILE_WITH_ASAN=true make jtest_compile
    ```

4. Execute the entire Java Test Suite:
    ```bash
    LD_PRELOAD=/usr/lib/gcc/x86_64-linux-gnu/5.4.0/libasan.so make jtest_run
    ```

    or for a single test (e.g. `ComparatorTest`), execute:

    ```bash
    cd java
    LD_PRELOAD=/usr/lib/gcc/x86_64-linux-gnu/5.4.0/libasan.so java -ea -Xcheck:jni -Djava.library.path=target -cp "target/classes:target/test-classes:test-libs/junit-4.12.jar:test-libs/hamcrest-core-1.3.jar:test-libs/mockito-all-1.10.19.jar:test-libs/cglib-2.2.2.jar:test-libs/assertj-core-1.7.1.jar:target/*" org.rocksdb.test.RocksJunitRunner org.rocksdb.ComparatorTest
    ```

# C++ Debugger
When things get desperate you can also run your RocksJava tests through the C++ debugger, to trace the C++ JNI code in RocksJava.

## lldb (Mac)

1. Set JDK 7 as required by RocksJava
    ```bash
    export JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk1.7.0_80.jdk/Contents/Home"
    export PATH="${PATH}:${JAVA_HOME}/bin"
    ```

2. Ensure a clean start:
    ```bash
    make clean jclean
    ```

3. Compile the RocksJava statically:
    ```bash
    DEBUG_LEVEL=2 make rocksdbjavastatic
    ```

4. Start LLDB with a single RocksJava test:
```bash
    lldb -- /Library/Java/JavaVirtualMachines/jdk1.7.0_80.jdk/Contents/Home/bin/java -ea -Xcheck:jni -Djava.library.path=target -cp "target/classes:target/test-classes:test-libs/junit-4.12.jar:test-libs/hamcrest-core-1.3.jar:test-libs/mockito-all-1.10.19.jar:test-libs/cglib-2.2.2.jar:test-libs/assertj-core-1.7.1.jar:target/*" org.rocksdb.test.RocksJunitRunner org.rocksdb.ComparatorTest
```

5. Using LLDB with RocksJava:

You can then start the RocksJava test under lldb:
```lldb
(lldb) run
```

You will *likely* need to instruct gdb not to stop on internal SIGSEGV and SIGBUS signals generated by the JVM:
```lldb
(lldb) pro hand -p true -s false SIGSEGV
(lldb) pro hand -p true -s false SIGBUS
```

## gdb (Linux)

1. Set JDK 7 as required by RocksJava
    ```bash
    export JAVA_HOME="/usr/lib/jvm/java-7-openjdk-amd64"
    export PATH="${PATH}:${JAVA_HOME}/bin"
    ```
   You might also need to run `sudo alternatives --config java` and select OpenJDK 7.

2. Ensure a clean start:
    ```bash
    make clean jclean
    ```

3. Compile the RocksJava statically:
    ```bash
    DEBUG_LEVEL=2 make rocksdbjavastatic
    ```

4. Start GDB with a single RocksJava test:
```bash
    gdb --args java -ea -Xcheck:jni -Djava.library.path=target -cp "target/classes:target/test-classes:test-libs/junit-4.12.jar:test-libs/hamcrest-core-1.3.jar:test-libs/mockito-all-1.10.19.jar:test-libs/cglib-2.2.2.jar:test-libs/assertj-core-1.7.1.jar:target/*" org.rocksdb.test.RocksJunitRunner org.rocksdb.ComparatorTest
```

5. Using GDB with RocksJava:

You will *likely* need to instruct gdb not to stop on internal SIGSEGV signals generated by the JVM:
```gdb
gdb> handle SIGSEGV pass noprint nostop
```

You can then start the RocksJava test under gdb:
```gdb
gdb> start
```