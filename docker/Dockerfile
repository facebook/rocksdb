FROM ubuntu:18.04

ENV DEBIAN_FRONTEND=noninteractive
SHELL ["/bin/bash", "-c"]

RUN apt-get update
RUN apt-get install -y wget build-essential software-properties-common git \
    curl g++-6 libbz2-dev libcurl4-openssl-dev libgflags-dev libsnappy-dev zlib1g-dev \
    libssl-dev

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
RUN cmake -DCMAKE_BUILD_TYPE=Release -DENABLE_TESTING=OFF '-DBUILD_ONLY=s3;kinesis' ../aws-sdk-cpp/
RUN make -j 8 && make install
RUN ldconfig
WORKDIR /
