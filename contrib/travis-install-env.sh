#!/bin/bash
#
# Builds and installs gRPC on a box (from sources).
#
# In order to build using Travis (http://travis-ci.org) we must be able to
# build our development environment. This script does that. It is intended
# to run from file .travis.yml as root on Ubuntu 12.04 LTS.

mkdir pre_build
cd pre_build
git clone https://github.com/grpc/grpc.git
cd grpc
# Currently we can build against this gRPC label.
git checkout 75c60c1a43a2b2a7b21967d7ea6d2dda5a351348
git submodule update --init
apt-get install -y build-essential autoconf libtool
make
make install prefix=/opt
