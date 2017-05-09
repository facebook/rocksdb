#!/usr/bin/env bash

set -e

# CentOS 5 is now end of life so we need to switch its repos over to the vault
sed -i -e '/^mirrorlist=/d' -e 's%^#baseurl=http://mirror.centos.org/centos/$releasever/%baseurl=http://vault.centos.org/5.11/%g' /etc/yum.repos.d/CentOS-Base.repo
sed -i -e '/^mirrorlist=/d' -e 's%^#baseurl=http://mirror.centos.org/centos/$releasever/%baseurl=http://vault.centos.org/5.11/%g' /etc/yum.repos.d/CentOS-fasttrack.repo
if [ -f /etc/yum.repos.d/libselinux.repo ]; then
	sed -i -e '/^mirrorlist=/d' -e 's%^#baseurl=http://mirror.centos.org/centos/$releasever/%baseurl=http://vault.centos.org/5.11/%g' /etc/yum.repos.d/libselinux.repo
fi

# enable EPEL
sudo yum -y install epel-release

# install all required packages for rocksdb that are available through yum
ARCH=$(uname -i)
sudo yum -y install openssl java-1.7.0-openjdk-devel.$ARCH zlib-devel bzip2-devel lz4-devel snappy-devel

# install gcc/g++ 4.8.2 via CERN (http://linux.web.cern.ch/linux/devtoolset/)
sudo wget -O /etc/yum.repos.d/slc5-devtoolset.repo http://linuxsoft.cern.ch/cern/devtoolset/slc5-devtoolset.repo
sudo wget -O /etc/pki/rpm-gpg/RPM-GPG-KEY-cern http://ftp.mirrorservice.org/sites/ftp.scientificlinux.org/linux/scientific/obsolete/51/i386/RPM-GPG-KEYs/RPM-GPG-KEY-cern
sudo yum -y install devtoolset-2

# install gflags
wget https://github.com/gflags/gflags/archive/v2.0.tar.gz -O gflags-2.0.tar.gz
tar xvfz gflags-2.0.tar.gz; cd gflags-2.0; scl enable devtoolset-2 ./configure; scl enable devtoolset-2 make; sudo make install
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

# install zstandard
wget https://github.com/facebook/zstd/archive/v1.1.3.tar.gz -O zstd-1.1.3.tar.gz
tar zxvf zstd-1.1.3.tar.gz; cd zstd-1.1.3; scl enable devtoolset-2 make; sudo make install

# set java home so we can build rocksdb jars
export JAVA_HOME=/usr/lib/jvm/java-1.7.0

# build rocksdb
cd /rocksdb
scl enable devtoolset-2 'make jclean clean'
scl enable devtoolset-2 'PORTABLE=1 make rocksdbjavastatic'
cp /rocksdb/java/target/librocksdbjni-* /rocksdb-build
cp /rocksdb/java/target/rocksdbjni-* /rocksdb-build

