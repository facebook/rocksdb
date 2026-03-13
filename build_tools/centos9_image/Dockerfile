FROM quay.io/centos/centos:stream9
SHELL ["/bin/bash", "-euxo", "pipefail", "-c"]

RUN dnf -y update \
 && dnf -y install --setopt=install_weak_deps=False --nodocs dnf-plugins-core \
 && dnf config-manager --set-enabled crb \
 && dnf -y install --setopt=install_weak_deps=False --nodocs epel-release \
 && dnf -y install --setopt=install_weak_deps=False --nodocs \
      gcc gcc-c++ make \
      m4 perl-core \
      hostname \
      git curl-minimal ca-certificates \
      which findutils \
      tar gzip bzip2 \
      wget \
      cmake \
      python3 \
      help2man \
      liburing-devel \
      patchelf binutils-devel \
      jemalloc-devel \
      libaio-devel \
      # Added because failing to build libunwind tests and unable to modify the manifest
      libunwind-devel \
      # Gflags headers are needed to build glog
      gflags-devel \
      openssl-devel \
      java-1.8.0-openjdk-devel \
 && dnf -y clean all \
 && rm -rf /var/cache/dnf

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
ENV PATH="$JAVA_HOME/bin:$PATH"
