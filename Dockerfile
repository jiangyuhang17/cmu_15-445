FROM ubuntu:18.04
CMD bash

# Install Ubuntu packages.
# Please add packages in alphabetical order.
ARG DEBIAN_FRONTEND=non-interactive

RUN sed -i s@/archive.ubuntu.com/@/mirrors.aliyun.com/@g /etc/apt/sources.list && \
    apt-get clean && \
    apt-get -y update && \
    apt-get -y install \
      build-essential \
      apt-utils \
      clang-8 \
      clang-format-8 \
      clang-tidy-8 \
      cmake \
      doxygen \
      git \
      g++-7 \
      pkg-config \
      valgrind \
      zlib1g-dev \
      openssh-server \
      rsync \
      sudo \
      vim && \
    useradd -d "/home/jyh" -m -s "/bin/bash" jyh && \
    chmod u+w /etc/sudoers && \
    echo "jyh    ALL=(ALL:ALL) NOPASSWD:ALL" >> /etc/sudoers && \
    chmod u-w /etc/sudoers && \
    echo "PermitRootLogin yes" >> /etc/ssh/sshd_config