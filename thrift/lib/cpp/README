Thrift C++ Software Library

License
=======

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.

Using Thrift with C++
=====================

The Thrift C++ libraries are built using the GNU tools. Follow the instructions
in the top-level README, or run bootstrap.sh in this folder to generate the
Makefiles.

In case you do not want to open another README file, do this:
  ./bootstrap.sh
  ./configure (--with-boost=/usr/local)
  make
  sudo make install

Thrift is divided into two libraries.

libthrift
  The core Thrift library contains all the core Thrift code. It requires
  boost shared pointers, pthreads, and librt.

libthriftnb
  This library contains the Thrift nonblocking server, which uses libevent.
  To link this library you will also need to link libevent.

Linking Against Thrift
======================

After you build and install Thrift the libraries are installed to
/usr/local/lib by default. Make sure this is in your LDPATH.

On Linux, the best way to do this is to ensure that /usr/local/lib is in
your /etc/ld.so.conf and then run /sbin/ldconfig.

Depending upon whether you are linking dynamically or statically and how
your build environment it set up, you may need to include additional
libraries when linking against thrift, such as librt and/or libpthread. If
you are using libthriftnb you will also need libevent.

Dependencies
============

boost shared pointers
http://www.boost.org/libs/smart_ptr/smart_ptr.htm

libevent (for libthriftnb only)
http://monkey.org/~provos/libevent/
