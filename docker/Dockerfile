FROM ubuntu:trusty
RUN apt-get update
RUN apt-get install -y build-essential
RUN apt-get install -y software-properties-common 
RUN add-apt-repository -y ppa:ubuntu-toolchain-r/test
RUN apt-get install -y wget
RUN wget -O - http://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -
RUN apt-add-repository -y "deb http://apt.llvm.org/precise/ llvm-toolchain-precise-3.6 main"
RUN apt-get update -y
RUN apt-get install -y clang-3.6 curl g++-6 libbz2-dev libcurl4-openssl-dev libgflags-dev libsnappy-dev zlib1g-dev
RUN apt-get install -y git
ENV CXX /usr/bin/g++-6
ENV CC /usr/bin/gcc-6
RUN mkdir -p /ax-install/cmake
RUN mkdir -p /ax-install/aws-install
WORKDIR /ax-install/cmake
RUN wget https://cmake.org/files/v3.7/cmake-3.7.2.tar.gz && tar xzvf cmake-3.7.2.tar.gz
WORKDIR /ax-install/cmake/cmake-3.7.2/
RUN ./bootstrap && make -j 8 && make install
WORKDIR /ax-install
RUN git clone https://github.com/aws/aws-sdk-cpp.git
WORKDIR /ax-install/aws-install
RUN cmake -DCMAKE_BUILD_TYPE=Release '-DBUILD_ONLY=s3;kinesis' ../aws-sdk-cpp/
RUN make -j 8 && make install
RUN ldconfig
WORKDIR /

