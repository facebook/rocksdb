#!/bin/sh
# Install gflags for mac developers.

set -e

DIR=`mktemp -d /tmp/rocksdb_gflags_XXXX`

cd $DIR
wget https://gflags.googlecode.com/files/gflags-2.0.tar.gz
tar xvfz gflags-2.0.tar.gz
cd gflags-2.0

./configure
make
make install

# Add include/lib path for g++
echo 'export LIBRARY_PATH+=":/usr/local/lib"' >> ~/.bash_profile
echo 'export CPATH+=":/usr/local/include"' >> ~/.bash_profile

echo ""
echo "-----------------------------------------------------------------------------"
echo "|                         Installation Completed                            |"
echo "-----------------------------------------------------------------------------"
echo "Please run `. ~/bash_profile` to be able to compile with gflags"
