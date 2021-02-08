FROM ubuntu:18.04

ENV DEBIAN_FRONTEND=noninteractive
SHELL ["/bin/bash", "-c"]

RUN apt-get update
RUN apt-get install -y wget build-essential software-properties-common git \
    curl g++-6 libbz2-dev libcurl4-openssl-dev libgflags-dev libsnappy-dev zlib1g-dev \
    libssl-dev python libtbb-dev ccache

ENV CXX /usr/bin/g++-6
ENV CC /usr/bin/gcc-6
RUN mkdir -p /ax-install/cmake
RUN mkdir -p /ax-install/aws-install

WORKDIR /ax-install
RUN curl -JLO https://cmake.org/files/v3.12/cmake-3.12.0-Linux-x86_64.tar.gz
WORKDIR /usr/local
RUN tar xfz /ax-install/cmake-3.12.0-Linux-x86_64.tar.gz --strip-components=1

WORKDIR /ax-install
RUN git clone -q --single-branch --depth 1 --branch 1.8.8 https://github.com/aws/aws-sdk-cpp.git
WORKDIR /ax-install/aws-install
RUN cmake -DCMAKE_BUILD_TYPE=Release -DENABLE_TESTING=OFF '-DBUILD_ONLY=kinesis;core;s3;transfer' ../aws-sdk-cpp/
RUN make -j 8 && make install

WORKDIR /ax-install
ENV LIBRDKAFKA_VERSION=0.11.3
RUN wget https://github.com/edenhill/librdkafka/archive/v${LIBRDKAFKA_VERSION}.tar.gz
RUN tar -zxvf v${LIBRDKAFKA_VERSION}.tar.gz
WORKDIR /ax-install/librdkafka-${LIBRDKAFKA_VERSION}
RUN ./configure && make && make install

# Use ccache wrapper in order to cache the builds for subsequent tests
RUN /usr/sbin/update-ccache-symlinks
ENV CXX /usr/lib/ccache/g++-6
ENV CC /usr/lib/ccache/gcc-6
ENV CCACHE_DIR /opt/rocksdb-cloud/src/.cache

RUN ldconfig
WORKDIR /
