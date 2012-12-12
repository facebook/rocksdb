dnl ---------------------------------------------------------------------------
dnl  Copyright (C) 2009-2011 FuseSource Corp.
dnl  http://fusesource.com
dnl  
dnl  Licensed under the Apache License, Version 2.0 (the "License");
dnl  you may not use this file except in compliance with the License.
dnl  You may obtain a copy of the License at
dnl  
dnl     http://www.apache.org/licenses/LICENSE-2.0
dnl  
dnl  Unless required by applicable law or agreed to in writing, software
dnl  distributed under the License is distributed on an "AS IS" BASIS,
dnl  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
dnl  See the License for the specific language governing permissions and
dnl  limitations under the License.
dnl ---------------------------------------------------------------------------
dnl ---------------------------------------------------------------------------
dnl SYNOPSIS:
dnl
dnl   WITH_JNI_JDK()
dnl
dnl   Adds the --with-jni-jdk=PATH option.  If not provided, it searches 
dnl   for the JDK in the default OS locations.
dnl
dnl      This macro calls:
dnl        AC_SUBST(JNI_JDK)
dnl        AC_SUBST(JNI_EXTRA_CFLAGS)
dnl        AC_SUBST(JNI_EXTRA_LDFLAGS)
dnl
dnl AUTHOR: <a href="http://hiramchirino.com">Hiram Chrino</a>
dnl ---------------------------------------------------------------------------

AC_DEFUN([WITH_JNI_JDK],
[
  AC_PREREQ([2.61])
  AC_ARG_WITH(jni-jdk,
		[AS_HELP_STRING([--with-jni-jdk=PATH],
			[Location of the Java Development Kit.  Defaults to your JAVA_HOME setting and falls back to where it is typically installed on your OS])],
    [
      if test "$withval" = "no" || test "$withval" = "yes"; then
        AC_MSG_ERROR([--with-jni-jdk: PATH to JDK not supplied])
      fi
      CHECK_JNI_JDK([$withval], [], [AC_MSG_ERROR([JDK not found. Invalid --with-jni-jdk PATH])])
    ],[

      if test -n "$JAVA_HOME" ; then 
        AC_MSG_NOTICE([JAVA_HOME was set, checking to see if it's a JDK we can use...])
        CHECK_JNI_JDK([$JAVA_HOME], [], [])
      fi

			__JNI_GUESS=`which javac`
      AS_IF(test -z "$JNI_JDK" && test -n "$__JNI_GUESS", [
        AC_MSG_NOTICE([javac was on your path, checking to see if it's part of a JDK we can use...])
			  # transitively resolve the symbolic links to javac
				while file -h "$__JNI_GUESS" 2>/dev/null | grep " symbolic link to " >/dev/null; do
				  __JNI_LINK=$( file -h $__JNI_GUESS | sed 's/.*symbolic link to //' | sed "s/'$//" | sed 's/^`//' )
				  __JNI_GUESS=$(cd $(dirname $__JNI_GUESS); cd $(dirname $__JNI_LINK); echo "$(pwd)/$(basename $__JNI_LINK)")
				done
				# move 2 dirs up to the home dir...
				__JNI_GUESS=$(dirname $(dirname $__JNI_GUESS))
				CHECK_JNI_JDK([$__JNI_GUESS], [], [],[])
      ],[])

      AS_IF(test -z "$JNI_JDK", [
        case "$host_os" in
           darwin*) __JNI_GUESS="/System/Library/Frameworks/JavaVM.framework";;
                 *) __JNI_GUESS="/usr";;
        esac
        AC_MSG_NOTICE([Taking a guess as to where your OS installs the JDK by default...])
        CHECK_JNI_JDK([$__JNI_GUESS], [], [AC_MSG_ERROR([JDK not found. Please use the --with-jni-jdk option])])
      ],[])
    ])
])

dnl ---------------------------------------------------------------------------
dnl
dnl   JNI_CHECK_JDK_HOME(PATH, [ACTION-SUCCESS], [ACTION-FAILURE])
dnl
dnl   Tests to see if the given path is a valid JDK home location with
dnl   with a JNI headers and library that can be compiled against.
dnl
dnl      This macro calls:
dnl 
dnl        AC_SUBST(JNI_JDK)
dnl        AC_SUBST(JNI_EXTRA_CFLAGS)
dnl        AC_SUBST(JNI_EXTRA_LDFLAGS)
dnl
dnl AUTHOR: <a href="http://hiramchirino.com">Hiram Chrino</a>
dnl ---------------------------------------------------------------------------
AC_DEFUN([CHECK_JNI_JDK],[
  AC_PREREQ([2.61])
  __JNI_JDK_HOME="$1"
  AC_MSG_CHECKING(if '$__JNI_JDK_HOME' is a JDK)
  # OSX had to be a little different.
  case "$host_os" in
       darwin*) __JNI_INCLUDE="$__JNI_JDK_HOME/Headers";;
             *) __JNI_INCLUDE="$__JNI_JDK_HOME/include";; 
  esac  
    
  AS_IF(test -r "$__JNI_INCLUDE/jni.h",[
  
    # Also include the os specific include dirs in the JNI_CFLAGS
    __JNI_CFLAGS="-I$__JNI_INCLUDE"
    case "$host_os" in
         bsdi*) __JNI_INCLUDE_EXTRAS="bsdos";;
        linux*) __JNI_INCLUDE_EXTRAS="linux genunix";;
          osf*) __JNI_INCLUDE_EXTRAS="alpha";;
      solaris*) __JNI_INCLUDE_EXTRAS="solaris";;
        mingw*) __JNI_INCLUDE_EXTRAS="win32";;
       cygwin*) __JNI_INCLUDE_EXTRAS="win32";;
             *) __JNI_INCLUDE_EXTRAS="genunix";;
    esac
    
    for f in $__JNI_INCLUDE_EXTRAS ; do
      if test -d "$__JNI_INCLUDE/$f"; then
        __JNI_CFLAGS="$__JNI_CFLAGS -I$__JNI_INCLUDE/$f"
      fi
    done
    
    saved_CPPFLAGS="$CPPFLAGS"
    CPPFLAGS="$CPPFLAGS $__JNI_CFLAGS"
    JNI_VERSION="1_2"
    AC_LANG_PUSH(C)
    AC_COMPILE_IFELSE(
      [AC_LANG_PROGRAM([[@%:@include <jni.h>]],[[
        #ifndef JNI_VERSION_$JNI_VERSION
        #  error JNI version $JNI_VERSION is not supported.
        #endif
      ]])
    ],[ 
    
      JNI_JDK=$"$__JNI_JDK_HOME"
      JNI_EXTRA_CFLAGS="$__JNI_CFLAGS"    
      AC_SUBST(JNI_JDK)
      AC_SUBST(JNI_EXTRA_CFLAGS)
      case $host_os in
        darwin*)
            JNI_EXTRA_LDFLAGS="-shrext .jnilib -dynamiclib" ;;
      esac
      AC_SUBST(JNI_EXTRA_LDFLAGS)
      
      
      AC_MSG_RESULT([yes])
      $2
    ],[ 
      AC_MSG_RESULT([no])
      $3 
    ])
    AC_LANG_POP()
    CPPFLAGS="$saved_CPPFLAGS"
  ],[
    AC_MSG_RESULT([no])
    $3
  ])    
])

